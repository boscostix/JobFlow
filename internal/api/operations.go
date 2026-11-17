package api

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/go-chi/chi/v5"
)

var timestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-](0[0-9]|1[0-9]|2[0-3]):[0-5][0-9])$`)

func parseSchedule(value string) (*time.Time, error) {
	if timestampPattern.MatchString(value) {
		t, err := time.Parse(time.RFC3339Nano, value)
		if err == nil {
			return &t, nil
		}
	}
	return nil, errors.New("scheduled_at must be an RFC3339 timestamp with an offset and up to six fractional digits")
}

func listOptions(raw string) (jobs.ListOptions, error) {
	options := jobs.ListOptions{Limit: 25}
	params, err := url.ParseQuery(raw)
	if err != nil {
		return options, errors.New("malformed query parameters")
	}
	for key, values := range params {
		value := values[0]
		if len(values) != 1 || value == "" || len(value) > 128 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return options, errors.New("query parameters must be single nonempty values of at most 128 bytes")
		}
		switch key {
		case "status":
			switch value {
			case "QUEUED", "RUNNING", "COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED":
				options.Status = value
			default:
				return options, errors.New("invalid job status")
			}
		case "type":
			options.TaskType = value
		case "q":
			options.Search = value
		case "limit":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 100 {
				return options, errors.New("limit must be an integer from 1 to 100")
			}
			options.Limit = int32(n)
		case "cursor":
			decoded, err := base64.RawURLEncoding.DecodeString(value)
			stamp, id, ok := strings.Cut(string(decoded), "|")
			if err != nil || !ok || id == "" || !utf8.ValidString(id) || strings.ContainsRune(id, 0) {
				return options, errors.New("invalid cursor")
			}
			at, err := parseSchedule(stamp)
			if err != nil || at.UTC().Year() < 1 || at.UTC().Year() > 9999 {
				return options, errors.New("invalid cursor")
			}
			options.Before, options.BeforeID = *at, id
		default:
			return options, errors.New("unknown query parameter")
		}
	}
	return options, nil
}

func (s server) list(w http.ResponseWriter, r *http.Request) {
	options, err := listOptions(r.URL.RawQuery)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	limit := options.Limit
	options.Limit++
	items, err := s.store.List(ctx, options)
	if s.operationError(w, err, "could not list jobs") {
		return
	}
	var cursor string
	if len(items) > int(limit) {
		items = items[:limit]
		last := items[len(items)-1]
		cursor = base64.RawURLEncoding.EncodeToString([]byte(last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.ID))
	}
	writeJSON(w, 200, struct {
		Jobs       []jobs.Job `json:"jobs"`
		NextCursor string     `json:"next_cursor,omitempty"`
	}{items, cursor})
}

func (s server) attempts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := s.store.Attempts(ctx, chi.URLParam(r, "id"))
	if s.operationError(w, err, "could not retrieve attempts") {
		return
	}
	writeJSON(w, 200, map[string][]jobs.Attempt{"attempts": items})
}

func (s server) workers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := s.store.Workers(ctx)
	if s.operationError(w, err, "could not list workers") {
		return
	}
	writeJSON(w, 200, map[string][]jobs.WorkerStatus{"workers": items})
}

func (s server) cancel(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	j, err := s.store.Cancel(ctx, chi.URLParam(r, "id"))
	if errors.Is(err, jobs.ErrInvalidTransition) {
		writeError(w, 409, "only queued or running jobs can be cancelled")
		return
	}
	if s.operationError(w, err, "could not cancel job") {
		return
	}
	code := http.StatusOK
	if j.Status == "RUNNING" {
		code = http.StatusAccepted
	}
	writeJSON(w, code, j)
}

func (s server) operationError(w http.ResponseWriter, err error, message string) bool {
	if errors.Is(err, jobs.ErrNotFound) {
		writeError(w, 404, err.Error())
	} else if err != nil {
		s.log.Error(message, "error", err)
		writeError(w, 503, message)
	}
	return err != nil
}
