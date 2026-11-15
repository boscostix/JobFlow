package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/tasks"
)

type fakeStore struct {
	creates int
	err     error
}

func (s *fakeStore) Create(context.Context, string, json.RawMessage) (jobs.Job, error) {
	s.creates++
	return jobs.Job{ID: "job_test", Status: "QUEUED"}, s.err
}
func (s *fakeStore) Get(context.Context, string) (jobs.Job, error) {
	if s.err != nil {
		return jobs.Job{}, s.err
	}
	return jobs.Job{ID: "job_test", Status: "QUEUED"}, nil
}
func (s *fakeStore) Ping(context.Context) error { return s.err }
func TestSubmissionValidation(t *testing.T) {
	valid := `{"type":"add_numbers","payload":{"a":1,"b":2}}`
	for _, tc := range []struct {
		name, body, key string
		code            int
	}{
		{"valid", valid, "", 202},
		{"bad JSON", "{", "", 400},
		{"case alias envelope", `{"Type":"add_numbers","payload":{"a":1,"b":2}}`, "", 400},
		{"duplicate envelope", `{"type":"add_numbers","type":"add_numbers","payload":{"a":1,"b":2}}`, "", 400},
		{"case alias payload", `{"type":"add_numbers","payload":{"a":1,"A":2,"b":3}}`, "", 400},
		{"unknown task", `{"type":"unknown","payload":{}}`, "", 400},
		{"invalid payload", `{"type":"add_numbers","payload":{"a":1}}`, "", 400},
		{"unsupported retry", `{"type":"add_numbers","payload":{"a":1,"b":2},"max_retries":3}`, "", 400},
		{"trailing JSON", valid + ` {}`, "", 400},
		{"idempotency", valid, "key", 400},
		{"large body", strings.Repeat(" ", int(MaxBodyBytes)) + valid, "", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeStore{}
			h := New(s, tasks.DefaultRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
			r := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(tc.body))
			if tc.key != "" {
				r.Header.Set("Idempotency-Key", tc.key)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.code {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			if tc.code != 202 && s.creates != 0 {
				t.Fatal("invalid request persisted")
			}
			if tc.code == 202 && (s.creates != 1 || w.Header().Get("Location") != "/api/v1/jobs/job_test") {
				t.Fatal("missing creation or location")
			}
		})
	}
}
func TestStorageErrors(t *testing.T) {
	s := &fakeStore{err: errors.New("database offline")}
	h := New(s, tasks.DefaultRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, tc := range []struct {
		method, path, body string
		code               int
	}{
		{"POST", "/api/v1/jobs", `{"type":"add_numbers","payload":{"a":1,"b":2}}`, 503},
		{"GET", "/api/v1/jobs/job_test", "", 503},
		{"GET", "/health", "", 503},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.code {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
	s.err = jobs.ErrNotFound
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/missing", nil))
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
	s.err = nil
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}
