// Package scheduler is the in-process pipeline scheduler: a ticker-driven worker
// that triggers a pipeline run when its schedule is due. It is deliberately not
// Temporal — one goroutine plus the pipelines table, with a fake-clock-testable
// due-ness rule (decision logged 2026-07-19: control plane = one Go binary +
// Postgres).
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Schedule is a pipeline's scheduling state as the scheduler sees it.
type Schedule struct {
	PipelineID int64
	Cron       string // "@hourly" | "@daily" | "@every <dur>" | "" (manual)
	LastSync   *time.Time
}

// Lister returns the schedulable pipelines (active, with a non-empty schedule).
type Lister interface {
	Active(ctx context.Context) ([]Schedule, error)
}

// Trigger runs one pipeline to completion. It MUST block until the run finishes
// so the scheduler can track in-flight runs and never overlap the same pipeline.
type Trigger func(pipelineID int64)

// Scheduler triggers due pipelines on a fixed tick, never overlapping a pipeline
// with itself: a never-synced pipeline is "due" on every tick until it syncs, so
// without in-flight tracking a slow run would be started repeatedly.
type Scheduler struct {
	lister   Lister
	trigger  Trigger
	interval time.Duration
	now      func() time.Time
	logger   *slog.Logger

	mu      sync.Mutex
	running map[int64]struct{}
}

// New builds a Scheduler. interval is how often it wakes to check due-ness.
func New(lister Lister, trigger Trigger, interval time.Duration, now func() time.Time, logger *slog.Logger) *Scheduler {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		lister: lister, trigger: trigger, interval: interval, now: now, logger: logger,
		running: map[int64]struct{}{},
	}
}

// Run ticks until ctx is cancelled, triggering due pipelines each tick.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.tick(ctx)
		}
	}
}

// tick performs one scheduling pass: list active pipelines and trigger the due
// ones that are not already running. Each triggered run executes in its own
// goroutine and clears its in-flight mark on completion. Exposed for
// deterministic testing without a real ticker.
func (s *Scheduler) tick(ctx context.Context) {
	scheds, err := s.lister.Active(ctx)
	if err != nil {
		s.logger.Error("scheduler: list active pipelines", "err", err)
		return
	}
	now := s.now()
	for _, sc := range scheds {
		if !isDue(sc.Cron, sc.LastSync, now) {
			continue
		}
		if !s.markRunning(sc.PipelineID) {
			continue // already running — never overlap a pipeline with itself
		}
		id := sc.PipelineID
		go func() {
			defer s.clearRunning(id)
			s.trigger(id)
		}()
	}
}

// markRunning records a pipeline as in-flight, returning false if it already was.
func (s *Scheduler) markRunning(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.running[id]; ok {
		return false
	}
	s.running[id] = struct{}{}
	return true
}

func (s *Scheduler) clearRunning(id int64) {
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
}

// isDue reports whether a pipeline should run now given its schedule and last
// sync. An empty schedule is manual-only (never due). A pipeline that has never
// synced is due immediately. Otherwise it is due once one interval has elapsed.
func isDue(cron string, lastSync *time.Time, now time.Time) bool {
	interval, ok := parseInterval(cron)
	if !ok {
		return false // manual or unparseable → never auto-run
	}
	if lastSync == nil {
		return true
	}
	return now.Sub(*lastSync) >= interval
}

// parseInterval maps a schedule string to a minimum interval between runs.
// Supported v1 forms: "@hourly", "@daily", "@weekly", "@every <duration>".
func parseInterval(cron string) (time.Duration, bool) {
	switch strings.TrimSpace(cron) {
	case "":
		return 0, false
	case "@hourly":
		return time.Hour, true
	case "@daily", "@midnight":
		return 24 * time.Hour, true
	case "@weekly":
		return 7 * 24 * time.Hour, true
	}
	if rest, found := strings.CutPrefix(strings.TrimSpace(cron), "@every "); found {
		if d, err := time.ParseDuration(strings.TrimSpace(rest)); err == nil && d > 0 {
			return d, true
		}
	}
	return 0, false
}

// Describe renders a schedule for logs/diagnostics.
func Describe(cron string) string {
	if d, ok := parseInterval(cron); ok {
		return fmt.Sprintf("every %s", d)
	}
	return "manual"
}
