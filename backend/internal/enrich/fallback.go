package enrich

import "fmt"

// fallback builds a deterministic, non-LLM enrichment from the failure's own
// fields. It is what makes the product fully functional with no API key: the
// alert still carries a plausible root cause, an affected table, a concrete
// next step, and a severity — just not model-authored prose.
func fallback(f Failure) Enrichment {
	tables := []string(nil)
	if f.Stream != "" {
		tables = []string{f.Stream}
	}
	return Enrichment{
		RootCause:      fallbackRootCause(f),
		AffectedTables: tables,
		SuggestedFix:   fallbackFix(f),
		Severity:       fallbackSeverity(f),
		Source:         "fallback",
	}
}

// knownCauses maps engine error codes to human root causes and fixes so the
// fallback is specific, not generic.
var knownCauses = map[string]struct{ cause, fix string }{
	"connection_refused": {
		"The pipeline could not reach the source database.",
		"Verify the source is running and reachable, then check host/port/credentials in the pipeline config.",
	},
	"cdc_position_lost": {
		"The change-data-capture position was no longer available on the source (WAL/binlog aged out).",
		"Re-run an initial backfill to re-anchor CDC, then resume streaming.",
	},
	"replica_identity_partial": {
		"A source table lacks a full replica identity, so updates/deletes cannot be captured completely.",
		"Set REPLICA IDENTITY FULL (or a unique index) on the affected table.",
	},
	"wal_level": {
		"The source is not configured for logical replication.",
		"Set wal_level=logical on the source and restart it, then re-check the connection.",
	},
	"schema_mismatch": {
		"The source schema changed in a way the destination could not absorb automatically.",
		"Review the schema-change event and reconcile the destination table, then re-run the sync.",
	},
}

func fallbackRootCause(f Failure) string {
	if k, ok := knownCauses[f.ErrorCode]; ok {
		return k.cause
	}
	if f.ErrorMessage != "" {
		return fmt.Sprintf("The sync failed with: %s", f.ErrorMessage)
	}
	return "The sync failed for an unspecified reason."
}

func fallbackFix(f Failure) string {
	if k, ok := knownCauses[f.ErrorCode]; ok {
		return k.fix
	}
	if f.Retryable {
		return "The error is marked retryable; re-run the sync, and if it recurs inspect the source and connector config."
	}
	return "Inspect the pipeline's latest run logs and the source system, then re-run once the underlying issue is resolved."
}

// fallbackSeverity maps a failure to a severity: a non-retryable hard failure is
// critical; a retryable one is a warning.
func fallbackSeverity(f Failure) string {
	if f.Retryable {
		return "warning"
	}
	return "critical"
}

func fallbackPostmortem(f Failure, resolution string) string {
	return fmt.Sprintf(`## Incident postmortem

**What happened:** %s

**Impact:** Pipeline %s%s was interrupted.

**Follow-up:** %s%s`,
		fallbackRootCause(f),
		orNone(f.Pipeline),
		streamClause(f.Stream),
		fallbackFix(f),
		resolutionClause(resolution),
	)
}

func streamClause(stream string) string {
	if stream == "" {
		return ""
	}
	return fmt.Sprintf(" (stream %s)", stream)
}

func resolutionClause(resolution string) string {
	if resolution == "" {
		return ""
	}
	return fmt.Sprintf(" Resolution: %s.", resolution)
}
