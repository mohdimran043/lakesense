// Package buildinfo carries version metadata stamped at build time via -ldflags.
package buildinfo

// Version is the control-plane version, overridden at build time:
//
//	-ldflags "-X github.com/lakesense/lakesense/backend/internal/buildinfo.Version=v0.1.0"
var Version = "dev"
