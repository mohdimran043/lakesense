// Package buildinfo exposes version metadata stamped at build time via
// -ldflags "-X github.com/lakesense/lakesense/engine/internal/buildinfo.Version=v0.1.0".
package buildinfo

// Version is the engine version; "dev" for unstamped local builds.
var Version = "dev"
