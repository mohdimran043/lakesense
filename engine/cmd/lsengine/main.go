// lsengine is the LakeSense replication engine CLI.
package main

import (
	"os"

	"github.com/lakesense/lakesense/engine/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
