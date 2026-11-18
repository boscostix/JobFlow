package metrics

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueueScrapeReportsUnavailableDepth(t *testing.T) {
	var logs bytes.Buffer
	handler := QueueHandler(func(context.Context) (int64, int64, error) {
		return 0, 0, errors.New("database diagnostic")
	}, slog.New(slog.NewJSONHandler(&logs, nil)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "queue depth unavailable") ||
		strings.Contains(rec.Body.String(), "database diagnostic") {
		t.Fatalf("unexpected response %d: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(logs.String(), "database diagnostic") {
		t.Fatal(logs.String())
	}
}
