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

func (s *fakeStore) Submit(context.Context, string, json.RawMessage, jobs.Options) (jobs.Job, error) {
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
		{"supported retry", `{"type":"add_numbers","payload":{"a":1,"b":2},"max_retries":3}`, "", 202},
		{"trailing JSON", valid + ` {}`, "", 400},
		{"idempotency", valid, "key", 202},
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

func (s *fakeStore) Retry(context.Context, string) (jobs.Job, error) {
	return jobs.Job{ID: "job_test", Status: "QUEUED"}, s.err
}

func TestReliabilityValidationAndErrors(t *testing.T) {
	for _, options := range []string{`"max_retries":-1`, `"max_retries":101`, `"max_retries":1.5`, `"timeout_seconds":0`, `"timeout_seconds":86401`, `"timeout_seconds":"1"`} {
		s := &fakeStore{}
		h := New(s, tasks.DefaultRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(`{"type":"add_numbers","payload":{"a":1,"b":2},`+options+`}`)))
		if w.Code != 400 || s.creates != 0 {
			t.Fatalf("%s status=%d creates=%d", options, w.Code, s.creates)
		}
	}
	for _, key := range []string{"", "two words", strings.Repeat("x", 201)} {
		s := &fakeStore{}
		h := New(s, tasks.DefaultRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
		req := httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(`{"type":"add_numbers","payload":{"a":1,"b":2}}`))
		req.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 400 || s.creates != 0 {
			t.Fatalf("key %q status %d", key, w.Code)
		}
	}
	s := &fakeStore{}
	h := New(s, tasks.DefaultRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, tc := range []struct {
		err  error
		code int
	}{{nil, 202}, {jobs.ErrNotFound, 404}, {jobs.ErrInvalidTransition, 409}, {errors.New("offline"), 503}} {
		s.err = tc.err
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/jobs/job_test/retry", nil))
		if w.Code != tc.code {
			t.Fatalf("retry %v: %d", tc.err, w.Code)
		}
	}
	s.err = jobs.ErrIdempotencyConflict
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(`{"type":"add_numbers","payload":{"a":1,"b":2}}`)))
	if w.Code != 409 {
		t.Fatal(w.Code)
	}
}
