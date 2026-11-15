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
	Create(context.Context, string, json.RawMessage) (jobs.Job, error)
	Get(context.Context, string) (jobs.Job, error)
	Ping(context.Context) error
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
	if _, ok := r.Header["Idempotency-Key"]; ok {
		writeError(w, http.StatusBadRequest, "idempotency keys are not supported yet")
		return
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
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := tasks.Decode(raw, &req, "type", "payload"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
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
	j, err := s.store.Create(ctx, req.Type, req.Payload)
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
