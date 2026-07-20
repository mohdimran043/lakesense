package correlate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func TestSameCodeSameConnectorCollapses(t *testing.T) {
	c := New(5*time.Minute, 0.6)
	// A shared Postgres outage hits three pipelines within seconds.
	k1, new1 := c.Assign(Signal{Connector: "postgres", ErrorCode: "connection_refused", At: t0})
	k2, new2 := c.Assign(Signal{Connector: "postgres", ErrorCode: "connection_refused", At: t0.Add(2 * time.Second)})
	k3, new3 := c.Assign(Signal{Connector: "postgres", ErrorCode: "connection_refused", At: t0.Add(5 * time.Second)})

	assert.True(t, new1, "first failure opens the incident")
	assert.False(t, new2, "storm members are suppressed")
	assert.False(t, new3)
	assert.Equal(t, k1, k2)
	assert.Equal(t, k1, k3)
	assert.Len(t, c.ActiveClusters(), 1)
}

func TestDifferentConnectorsStaySeparate(t *testing.T) {
	c := New(5*time.Minute, 0.6)
	_, n1 := c.Assign(Signal{Connector: "postgres", ErrorCode: "connection_refused", At: t0})
	_, n2 := c.Assign(Signal{Connector: "mysql", ErrorCode: "connection_refused", At: t0})
	assert.True(t, n1)
	assert.True(t, n2, "a MySQL failure is a different root cause than a Postgres one")
	assert.Len(t, c.ActiveClusters(), 2)
}

func TestNormalizedMessageCollapsesVaryingNumbers(t *testing.T) {
	c := New(5*time.Minute, 0.6)
	_, n1 := c.Assign(Signal{Connector: "postgres", Message: "connection refused after 3 retries", At: t0})
	_, n2 := c.Assign(Signal{Connector: "postgres", Message: "connection refused after 5 retries", At: t0.Add(time.Second)})
	assert.True(t, n1)
	assert.False(t, n2, "digit-only differences collapse via the normalized signature")
}

func TestFuzzyMatchOnNearDuplicateMessages(t *testing.T) {
	c := New(5*time.Minute, 0.5)
	_, n1 := c.Assign(Signal{Connector: "mongodb", Message: "collection orders is locked by another writer", At: t0})
	_, n2 := c.Assign(Signal{Connector: "mongodb", Message: "collection orders locked by writer", At: t0.Add(time.Second)})
	assert.True(t, n1)
	assert.False(t, n2, "high token overlap ⇒ same cluster")
}

func TestUnrelatedMessagesStaySeparate(t *testing.T) {
	c := New(5*time.Minute, 0.6)
	_, n1 := c.Assign(Signal{Connector: "postgres", Message: "connection refused", At: t0})
	_, n2 := c.Assign(Signal{Connector: "postgres", Message: "permission denied for table widgets", At: t0.Add(time.Second)})
	assert.True(t, n1)
	assert.True(t, n2, "different failures are not force-merged")
}

func TestWindowExpiryOpensFreshCluster(t *testing.T) {
	c := New(time.Minute, 0.6)
	_, n1 := c.Assign(Signal{Connector: "postgres", ErrorCode: "connection_refused", At: t0})
	// Same signature, but well after the window: a new, unrelated storm.
	_, n2 := c.Assign(Signal{Connector: "postgres", ErrorCode: "connection_refused", At: t0.Add(10 * time.Minute)})
	assert.True(t, n1)
	assert.True(t, n2, "an old cluster is pruned; the later failure opens a fresh incident")
	require.Len(t, c.ActiveClusters(), 1)
}

func TestSignatureAndNormalize(t *testing.T) {
	assert.Equal(t, "postgres|cdc_position_lost", Signature("postgres", "cdc_position_lost", "anything"))
	assert.Equal(t, "lost slot ? at lsn ?", normalize(`Lost slot "s1" at LSN 0x1A2B3C4D`))
}
