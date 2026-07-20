// Package correlate is LakeSense's alert storm-suppression layer. When a shared
// dependency dies, dozens of pipelines fail near-simultaneously with related
// errors; correlation collapses them into ONE incident thread instead of fifty
// pages. It complements the rule engine's per-fingerprint dedup: dedup groups
// repeats of the exact same failure, correlation groups DIFFERENT failures that
// share a root cause within a time window.
//
// The clustering is a hybrid (decided by brainstorm): an exact normalized
// signature match first — connector + error code, or a digit/ID-stripped
// message signature — then a token-Jaccard fallback for near-duplicate
// free-text messages. Pure Go, in-process, no external service.
package correlate

import (
	"regexp"
	"sort"
	"strings"
	"time"
)

// Signal is one failure observation offered for correlation.
type Signal struct {
	Connector string
	ErrorCode string
	Message   string
	At        time.Time
}

// Correlator assigns each signal a correlation key, joining it to a recent
// cluster when one matches within the window. Not safe for concurrent use; the
// collector holds one and feeds it serially.
type Correlator struct {
	window     time.Duration
	similarity float64 // Jaccard threshold for the fuzzy fallback
	clusters   []*cluster
}

type cluster struct {
	key       string
	signature string
	tokens    map[string]struct{}
	lastSeen  time.Time
}

// New builds a Correlator. A zero window defaults to 5 minutes; a zero
// threshold defaults to 0.6.
func New(window time.Duration, similarity float64) *Correlator {
	if window <= 0 {
		window = 5 * time.Minute
	}
	if similarity <= 0 {
		similarity = 0.6
	}
	return &Correlator{window: window, similarity: similarity}
}

// Assign returns the correlation key for a signal and whether it opened a new
// cluster (the first of a storm). A returned isNew=false means the signal is a
// member of an existing storm and its alert should be suppressed.
func (c *Correlator) Assign(s Signal) (key string, isNew bool) {
	c.prune(s.At)
	sig := Signature(s.Connector, s.ErrorCode, s.Message)
	tokens := tokenSet(normalize(s.Message))

	// 1) exact signature match.
	for _, cl := range c.clusters {
		if cl.signature == sig {
			cl.lastSeen = s.At
			return cl.key, false
		}
	}
	// 2) fuzzy match: same connector + high token overlap (only for free-text
	// messages, i.e. no shared error code to key on).
	if s.ErrorCode == "" && len(tokens) > 0 {
		for _, cl := range c.clusters {
			if strings.HasPrefix(cl.signature, s.Connector+"|") && jaccard(tokens, cl.tokens) >= c.similarity {
				cl.lastSeen = s.At
				return cl.key, false
			}
		}
	}
	// 3) new cluster.
	cl := &cluster{key: sig, signature: sig, tokens: tokens, lastSeen: s.At}
	c.clusters = append(c.clusters, cl)
	return cl.key, true
}

// prune drops clusters older than the window so a later, unrelated storm with
// the same signature opens a fresh cluster.
func (c *Correlator) prune(now time.Time) {
	kept := c.clusters[:0]
	for _, cl := range c.clusters {
		if now.Sub(cl.lastSeen) <= c.window {
			kept = append(kept, cl)
		}
	}
	c.clusters = kept
}

var (
	numRE   = regexp.MustCompile(`\d+`)
	hexRE   = regexp.MustCompile(`0x[0-9a-fA-F]+|[0-9a-fA-F]{8,}`)
	quoteRE = regexp.MustCompile(`"[^"]*"|'[^']*'`)
	spaceRE = regexp.MustCompile(`\s+`)
)

// Signature is the stable clustering key. With an error code it is
// connector|code (the strongest signal). Without one it is connector| plus a
// normalized message signature so "…after 3 retries" and "…after 5 retries"
// collapse to the same cluster.
func Signature(connector, code, message string) string {
	if code != "" {
		return connector + "|" + code
	}
	return connector + "|" + normalize(message)
}

// normalize strips the variable parts of an error message so semantically equal
// messages share a signature: quoted literals, hex/long ids, and digits are
// removed, case is folded, whitespace collapsed.
func normalize(message string) string {
	m := strings.ToLower(message)
	m = quoteRE.ReplaceAllString(m, "?")
	m = hexRE.ReplaceAllString(m, "?")
	m = numRE.ReplaceAllString(m, "?")
	m = spaceRE.ReplaceAllString(m, " ")
	return strings.TrimSpace(m)
}

func tokenSet(normalized string) map[string]struct{} {
	set := map[string]struct{}{}
	for _, tok := range strings.Fields(normalized) {
		if tok != "?" {
			set[tok] = struct{}{}
		}
	}
	return set
}

// jaccard is |A∩B| / |A∪B| over token sets.
func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for t := range a {
		if _, ok := b[t]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

// ActiveClusters returns the current cluster keys (sorted) — for tests and
// observability.
func (c *Correlator) ActiveClusters() []string {
	keys := make([]string, len(c.clusters))
	for i, cl := range c.clusters {
		keys[i] = cl.key
	}
	sort.Strings(keys)
	return keys
}
