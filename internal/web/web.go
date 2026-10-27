// Package web embeds the single-page dashboard (DESIGN §10.3).
package web

import (
	"embed"
	"io/fs"
)

//go:embed static
var staticFS embed.FS

// FS returns the embedded UI filesystem rooted at static/.
func FS() fs.FS {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	return sub
}
