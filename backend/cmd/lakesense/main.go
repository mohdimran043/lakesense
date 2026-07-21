// Command lakesense is the LakeSense control plane: the API server plus the
// background workers (collector, rule engine, escalation, anomaly, quality),
// all in one binary. Workers run as goroutines coordinated by context and an
// errgroup, with graceful shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/lakesense/lakesense/backend/internal/anomaly"
	"github.com/lakesense/lakesense/backend/internal/api"
	"github.com/lakesense/lakesense/backend/internal/channels"
	"github.com/lakesense/lakesense/backend/internal/collector"
	"github.com/lakesense/lakesense/backend/internal/config"
	"github.com/lakesense/lakesense/backend/internal/escalation"
	"github.com/lakesense/lakesense/backend/internal/rules"
	"github.com/lakesense/lakesense/backend/internal/runner"
	"github.com/lakesense/lakesense/backend/internal/scheduler"
	"github.com/lakesense/lakesense/backend/internal/seed"
	"github.com/lakesense/lakesense/backend/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Subcommands: `serve` (default) runs the API + workers; `seed` populates
	// demo data through the real ingestion path.
	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}

	var err error
	switch cmd {
	case "serve":
		err = run(logger)
	case "seed":
		err = runSeed(logger, args)
	case "doctor":
		os.Exit(runDoctor(args))
	default:
		fmt.Fprintf(os.Stderr, "lakesense: unknown command %q (want: serve | seed | doctor)\n", cmd)
		os.Exit(2)
	}
	if err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// runSeed applies migrations (so a fresh DB works) and seeds demo history.
func runSeed(logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	days := fs.Int("days", 14, "days of synthetic history to generate")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	logger.Info("seeding demo data", "days", *days)
	if err := seed.Run(ctx, st.Pool, *days); err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	logger.Info("seed complete")
	return nil
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Apply migrations before opening the pool for serving.
	logger.Info("applying migrations")
	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	// Live intelligence: every ingested event is evaluated against the pipeline's
	// rules, opening deduplicated incidents and dispatching alerts through the
	// channel adapters. This is the one alerting path (spec 4.2–4.4) now running
	// on real events, not just unit tests.
	notifier := channels.New(channels.NewPgResolver(st.Pool), nil, nil)
	escWorker := escalation.NewWorker(escalation.NewPgStore(st.Pool), escalation.NewPgPolicies(st.Pool),
		escalation.NewPgSchedules(st.Pool), notifier, nil)
	ruleEngine := rules.NewEngine(rules.NewPgStore(st.Pool), notifier, nil)
	ruleLoader := rules.NewPgLoader(st.Pool)
	process := func(ctx context.Context, pipelineID int64, e collector.Event) {
		ruleSet, err := ruleLoader.LoadRules(ctx, pipelineID)
		if err != nil {
			logger.Error("load rules", "pipeline_id", pipelineID, "err", err)
			return
		}
		if err := ruleEngine.Evaluate(ctx, pipelineID, e, ruleSet); err != nil {
			logger.Error("evaluate rules", "pipeline_id", pipelineID, "err", err)
		}
	}

	// The runner executes pipelines: it drives lsengine and pipes the event
	// stream through the collector ingester (the same path seed uses), now with
	// the live rule processor attached.
	ingest := collector.NewIngester(collector.NewPgSink(st.Pool), collector.WithProcessor(process)).Ingest
	run := runner.New(runner.NewExecEngine(cfg.EnginePath), ingest, runner.NewPgLoader(st.Pool), cfg.DataDir, nil)

	// Anomaly detection: score each pipeline's latest throughput against its own
	// baseline and, on an outlier, emit an anomaly_detected event down the same
	// ingest→rules→alert path (so a rule on "anomaly_detected" pages someone).
	emitAnomaly := func(ctx context.Context, pipelineID int64, metric string, res anomaly.Result) error {
		ev := fmt.Sprintf(`{"v":1,"event":"anomaly_detected","pipeline_id":%d,"payload":{"metric":%q,"value":%g,"expected":%g,"score":%g,"method":%q}}`,
			pipelineID, metric, res.Value, res.Expected, res.Score, res.Method)
		_, err := ingest(ctx, pipelineID, strings.NewReader(ev))
		return err
	}
	anomalyWorker := anomaly.NewWorker(anomaly.NewPgSource(st.Pool), emitAnomaly, "rows_written", nil)

	// trigger starts a run without blocking the scheduler's tick.
	trigger := func(id int64) {
		go func() {
			if _, err := run.Run(context.Background(), id); err != nil {
				logger.Error("scheduled run failed", "pipeline_id", id, "err", err)
			}
		}()
	}
	sched := scheduler.New(scheduler.NewPgLister(st.Pool), trigger, 30*time.Second, nil, logger)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st.Pool, logger, run),
		ReadHeaderTimeout: 10 * time.Second,
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	// Scheduler: trigger due pipelines on a fixed tick until shutdown.
	g.Go(func() error {
		logger.Info("scheduler started", "interval", "30s")
		return sched.Run(gctx)
	})

	// Escalation: climb unacked incidents up their policy on a fixed tick.
	g.Go(func() error {
		logger.Info("escalation worker started", "interval", "30s")
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				if err := escWorker.Tick(gctx); err != nil {
					logger.Error("escalation tick", "err", err)
				}
			}
		}
	})

	// Anomaly detection: score recent throughput on a slower tick.
	g.Go(func() error {
		logger.Info("anomaly worker started", "interval", "5m")
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				if err := anomalyWorker.Tick(gctx); err != nil {
					logger.Error("anomaly tick", "err", err)
				}
			}
		}
	})

	// Graceful shutdown when the context is cancelled (signal or worker error).
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		logger.Info("shutting down")
		return srv.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
