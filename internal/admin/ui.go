package admin

import (
	_ "embed"
	"net/http"
)

//go:embed ui.html
var uiHTML string

// serveUI serves the single-page admin UI.
func serveUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(uiHTML))
}
