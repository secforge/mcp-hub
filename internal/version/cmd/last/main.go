// Command last prints internal/version.LastRelease, so the release script
// can check the constant matches the tag it is about to publish rather
// than trusting that someone remembered to bump it.
//
// A tiny program rather than a grep because the constant is Go source: a
// grep would keep working after someone reformatted the line and stop
// meaning anything after someone moved it.
package main

import (
	"fmt"

	"github.com/secforge/mcp-hub/internal/version"
)

func main() { fmt.Println(version.LastRelease) }
