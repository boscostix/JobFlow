package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/tasks"
	"github.com/go-chi/chi/v5"
)

const MaxBodyBytes int64 = 64 << 10

type Store interface {
	Submit(context.Context, string, json.RawMessage, jobs.Options) (jobs.Job, error)
	Get(context.Context, string) (jobs.Job, error)
	Ping(context.Context) error
	Retry(context.Context, string) (jobs.Job, error)
}
type server struct {
	store    Store
	registry tasks.Registry
	log      *slog.Logger
}

func New(store Store, registry tasks.Registry, logger *slog.Logger) http.Handler {
	s := server{store, registry, logger}
	r := chi.NewRouter()
	r.Post("/api/v1/jobs", s.submit)
	r.Get("/api/v1/jobs/{id}", s.get)
	r.Get("/health", s.health)
	r.Post("/api/v1/jobs/{id}/retry", s.retry)
	return r
}
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
func (s server) submit(w http.ResponseWriter, r *http.Request) {
	key := ""
	if values, ok := r.Header["Idempotency-Key"]; ok {
		if len(values) != 1 || len(values[0]) < 1 || len(values[0]) > 200 {
			writeError(w, 400, "Idempotency-Key must be one nonempty value of at most 200 bytes")
			return
		}
		key = values[0]
		for _, c := range key {
			if c < 33 || c > 126 {
				writeError(w, 400, "Idempotency-Key must contain visible ASCII characters")
				return
			}
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "could not read request body")
		}
		return
	}
	var req struct {
		Type           string          `json:"type"`
		Payload        json.RawMessage `json:"payload"`
		MaxRetries     *int32          `json:"max_retries"`
		TimeoutSeconds *int32          `json:"timeout_seconds"`
	}
	if err := tasks.Decode(raw, &req, "type", "payload", "max_retries", "timeout_seconds"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}
	o := jobs.Options{TimeoutSeconds: 300, IdempotencyKey: key}
	if req.MaxRetries != nil {
		o.MaxRetries = *req.MaxRetries
	}
	if req.TimeoutSeconds != nil {
		o.TimeoutSeconds = *req.TimeoutSeconds
	}
	if err := o.Validate(); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	h, ok := s.registry[req.Type]
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown task type")
		return
	}
	if err := h.Validate(req.Payload); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	j, err := s.store.Submit(ctx, req.Type, req.Payload, o)
	if errors.Is(err, jobs.ErrIdempotencyConflict) {
		writeError(w, 409, err.Error())
		return
	}
	if err != nil {
		s.log.Error("job submission failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "could not persist job")
		return
	}
	w.Header().Set("Location", "/api/v1/jobs/"+j.ID)
	writeJSON(w, http.StatusAccepted, map[string]string{"job_id": j.ID, "status": j.Status})
}
func (s server) get(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	j, err := s.store.Get(ctx, chi.URLParam(r, "id"))
	if errors.Is(err, jobs.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		s.log.Error("job lookup failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "could not retrieve job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}
func (s server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s server) retry(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	j, err := s.store.Retry(ctx, chi.URLParam(r, "id"))
	if errors.Is(err, jobs.ErrNotFound) {
		writeError(w, 404, err.Error())
		return
	}
	if errors.Is(err, jobs.ErrInvalidTransition) {
		writeError(w, 409, "only dead-letter or legacy failed jobs can be retried")
		return
	}
	if err != nil {
		writeError(w, 503, "could not retry job")
		return
	}
	writeJSON(w, 202, map[string]string{"job_id": j.ID, "status": j.Status})
}
