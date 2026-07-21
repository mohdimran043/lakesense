package rules

import (
	"context"
	"sync"
	"time"
)

// RuleLoader loads the applicable rule set for a pipeline. PgLoader implements
// it; CachedLoader decorates it.
type RuleLoader interface {
	LoadRules(ctx context.Context, pipelineID int64) ([]Rule, error)
}

// CachedLoader caches a pipeline's rule set for a short TTL. The live processor
// evaluates every ingested event, so without this a single sync (thousands of
// events) would issue a rule query per event. New/edited rules take effect
// within the TTL. Concurrency-safe.
type CachedLoader struct {
	inner RuleLoader
	ttl   time.Duration
	now   func() time.Time

	mu      sync.Mutex
	entries map[int64]cacheEntry
}

type cacheEntry struct {
	rules []Rule
	at    time.Time
}

// NewCachedLoader wraps inner with a TTL cache.
func NewCachedLoader(inner RuleLoader, ttl time.Duration) *CachedLoader {
	return &CachedLoader{inner: inner, ttl: ttl, now: time.Now, entries: map[int64]cacheEntry{}}
}

// LoadRules returns the cached rule set when fresh, otherwise loads and caches it.
func (c *CachedLoader) LoadRules(ctx context.Context, pipelineID int64) ([]Rule, error) {
	c.mu.Lock()
	if e, ok := c.entries[pipelineID]; ok && c.now().Sub(e.at) < c.ttl {
		c.mu.Unlock()
		return e.rules, nil
	}
	c.mu.Unlock()

	rules, err := c.inner.LoadRules(ctx, pipelineID)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.entries[pipelineID] = cacheEntry{rules: rules, at: c.now()}
	c.mu.Unlock()
	return rules, nil
}
