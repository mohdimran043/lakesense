// Package config loads LakeSense control-plane configuration from the
// environment. Secrets (database URL, API keys) come only from env vars — the
// repo ships .env.example, never .env (standing rule 4).
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config is the fully-resolved control-plane configuration.
type Config struct {
	// HTTPAddr is the API listen address, e.g. ":8080".
	HTTPAddr string
	// DatabaseURL is the Postgres DSN for control-plane metadata.
	DatabaseURL string
	// EnginePath is the lsengine binary invoked per pipeline run.
	EnginePath string
	// DataDir is the root for per-pipeline run scratch (configs, state, output).
	DataDir string
	// AnthropicAPIKey enables LLM enrichment; empty ⇒ graceful non-LLM fallback.
	AnthropicAPIKey string
	// AnthropicModel is the model id used for enrichment.
	AnthropicModel string
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration
}

// Load reads configuration from the environment, applying documented defaults.
// DatabaseURL is required; everything else has a sensible default so a bare
// `docker compose up` works.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        env("LAKESENSE_HTTP_ADDR", ":8080"),
		DatabaseURL:     os.Getenv("LAKESENSE_DATABASE_URL"),
		EnginePath:      env("LAKESENSE_ENGINE_PATH", "lsengine"),
		DataDir:         env("LAKESENSE_DATA_DIR", filepath.Join(os.TempDir(), "lakesense")),
		AnthropicAPIKey: os.Getenv("ANTHROPIC_API_KEY"),
		AnthropicModel:  env("LAKESENSE_ANTHROPIC_MODEL", "claude-opus-4-8"),
		ShutdownTimeout: 15 * time.Second,
	}
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return Config{}, fmt.Errorf("LAKESENSE_DATABASE_URL is required")
	}
	return cfg, nil
}

// LLMEnabled reports whether LLM features are configured. Every LLM feature
// must still degrade gracefully when this is false.
func (c Config) LLMEnabled() bool { return strings.TrimSpace(c.AnthropicAPIKey) != "" }

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
