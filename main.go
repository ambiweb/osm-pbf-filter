package main

import (
	"os"

	"github.com/ambiweb/osm-pbf-filter/cli"
)

func main() {
	os.Exit(cli.Run())
}
