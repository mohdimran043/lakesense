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

	"github.com/lakesense/lakesense/backend/internal/api"
	"github.com/lakesense/lakesense/backend/internal/collector"
	"github.com/lakesense/lakesense/backend/internal/config"
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

	// The runner executes pipelines: it drives lsengine and pipes the event
	// stream through the collector ingester (the same path seed uses).
	ingest := collector.NewIngester(collector.NewPgSink(st.Pool)).Ingest
	run := runner.New(runner.NewExecEngine(cfg.EnginePath), ingest, runner.NewPgLoader(st.Pool), cfg.DataDir, nil)

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
