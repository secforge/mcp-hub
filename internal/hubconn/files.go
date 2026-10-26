package hubconn

import (
	"encoding/json"

	"github.com/secforge/mcp-hub/internal/wire"
)

// FilesLimits is what the server declares about one-time files here, and
// whether it offers them on this session at all.
func (c *Conn) FilesLimits() (wire.FilesFeature, bool) {
	raw, ok := c.features[wire.FeatureFiles]
	if !ok {
		return wire.FilesFeature{}, false
	}
	var f wire.FilesFeature
	_ = json.Unmarshal(raw, &f)
	return f, true
}

// FileUpload reserves room for a one-time file of size stored bytes with
// that SHA-256 (hex), kept for expiresIn seconds (0 for the server's
// default), and waits for the answer: a "fileUploadReady", or an "error"
// carrying the server's code.
func (c *Conn) FileUpload(size int64, sha256 string, expiresIn int64) (Event, bool, error) {
	return c.awaitAnswer("fileUploadReady", func(id string) any {
		return wire.FileUpload{Type: wire.TypeFileUpload, ID: id, Size: size, SHA256: sha256, ExpiresIn: expiresIn}
	})
}
