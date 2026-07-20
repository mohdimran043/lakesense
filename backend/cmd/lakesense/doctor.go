package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lakesense/lakesense/backend/internal/config"
	"github.com/lakesense/lakesense/backend/internal/store"
)

// runDoctor is the one-shot self-diagnostic (`lakesense doctor`), also used as
// the container healthcheck. It verifies config, DB reachability, that
// migrations are applied, and reports last-sync recency. Exits 0 when healthy,
// 1 otherwise. --json emits machine-readable output.
func runDoctor(args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit JSON instead of human-readable output")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var checks []check
	add := func(name string, err error, detail string) {
		checks = append(checks, check{Name: name, OK: err == nil, Detail: detail, err: err})
	}

	cfg, err := config.Load()
	add("config", err, "environment configuration loaded")
	if err != nil {
		return report(checks, *asJSON)
	}

	st, err := store.Open(ctx, cfg.DatabaseURL)
	add("database", err, "control-plane database reachable")
	if err != nil {
		return report(checks, *asJSON)
	}
	defer st.Close()

	var pipelineCount int
	err = st.Pool.QueryRow(ctx, `SELECT count(*) FROM pipelines`).Scan(&pipelineCount)
	add("migrations", err, fmt.Sprintf("schema present (%d pipelines)", pipelineCount))

	var stale int
	if err == nil {
		err = st.Pool.QueryRow(ctx,
			`SELECT count(*) FROM pipelines
			 WHERE status = 'active'
			   AND (last_sync_at IS NULL OR last_sync_at < now() - interval '2 days')`).Scan(&stale)
		add("freshness", err, fmt.Sprintf("%d active pipelines with no recent sync", stale))
	}

	return report(checks, *asJSON)
}

type check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
	err    error
}

func report(checks []check, asJSON bool) int {
	healthy := true
	for _, c := range checks {
		if !c.OK {
			healthy = false
		}
	}
	if asJSON {
		out := map[string]any{"healthy": healthy, "checks": checks}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
	} else {
		for _, c := range checks {
			mark := "✓"
			if !c.OK {
				mark = "✗"
			}
			fmt.Printf("%s %-12s %s\n", mark, c.Name, c.Detail)
			if c.err != nil {
				fmt.Printf("    → %v\n", c.err)
			}
		}
		if healthy {
			fmt.Println("\nlakesense: healthy")
		} else {
			fmt.Println("\nlakesense: UNHEALTHY")
		}
	}
	if healthy {
		return 0
	}
	return 1
}
