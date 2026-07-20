package envs

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lakesense/lakesense/backend/internal/configver"
)

func devConfig() configver.Config {
	return configver.Config{
		Name:        "orders",
		Source:      configver.Endpoint{Type: "postgres", Settings: map[string]string{"host": "dev-db", "database": "shop", "password": "devpw", "sslmode": "disable"}},
		Destination: configver.Endpoint{Type: "iceberg", Settings: map[string]string{"path": "s3://dev-lake/orders"}},
		Schedule:    "@daily",
		Streams:     []configver.Stream{{Name: "public.orders", Mode: "cdc"}},
	}
}

func TestPromoteOverridesCredentialsPreservesStreams(t *testing.T) {
	prod := Promote(devConfig(), Overrides{
		Source:      map[string]string{"host": "prod-db", "password": "prodpw"},
		Destination: map[string]string{"path": "s3://prod-lake/orders"},
	})

	// Credentials swapped...
	assert.Equal(t, "prod-db", prod.Source.Settings["host"])
	assert.Equal(t, "prodpw", prod.Source.Settings["password"])
	assert.Equal(t, "s3://prod-lake/orders", prod.Destination.Settings["path"])
	// ...non-credential settings preserved...
	assert.Equal(t, "shop", prod.Source.Settings["database"])
	assert.Equal(t, "disable", prod.Source.Settings["sslmode"])
	// ...and behavior (streams, schedule, types) unchanged.
	assert.Equal(t, devConfig().Streams, prod.Streams)
	assert.Equal(t, "@daily", prod.Schedule)
	assert.Equal(t, "postgres", prod.Source.Type)
}

func TestPromoteDoesNotMutateSource(t *testing.T) {
	dev := devConfig()
	_ = Promote(dev, Overrides{Source: map[string]string{"host": "prod-db"}})
	assert.Equal(t, "dev-db", dev.Source.Settings["host"], "source config must be untouched")
}

func TestMissingCredentialsWarnsBeforeLeak(t *testing.T) {
	// Only the source host overridden — password/database and dest path remain
	// dev values, which must be surfaced before promoting.
	missing := MissingCredentials(devConfig(), Overrides{Source: map[string]string{"host": "prod-db"}})
	assert.Contains(t, missing, "source.password")
	assert.Contains(t, missing, "source.database")
	assert.Contains(t, missing, "destination.path")
	assert.NotContains(t, missing, "source.host") // this one was overridden

	// Fully specified → nothing missing.
	none := MissingCredentials(devConfig(), Overrides{
		Source:      map[string]string{"host": "p", "database": "p", "password": "p"},
		Destination: map[string]string{"path": "p"},
	})
	assert.Empty(t, none)
}
