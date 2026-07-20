package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMigrateURLScheme(t *testing.T) {
	cases := map[string]string{
		"postgres://u:p@h:5432/db?sslmode=disable": "pgx5://u:p@h:5432/db?sslmode=disable",
		"postgresql://u:p@h:5432/db":               "pgx5://u:p@h:5432/db",
		"pgx5://u:p@h/db":                          "pgx5://u:p@h/db", // already correct
	}
	for in, want := range cases {
		assert.Equalf(t, want, migrateURL(in), "migrateURL(%q)", in)
	}
}
