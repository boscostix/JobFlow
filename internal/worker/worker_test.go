package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/tasks"
)

type fakeStore struct {
	mu         sync.Mutex
	claimed    bool
	complete   bool
	failure    string
	persistErr error
	saved      chan struct{}
}

func (s *fakeStore) Claim(context.Context, string) (jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed {
		return jobs.Job{}, jobs.ErrNotFound
	}
	s.claimed = true
	return jobs.Job{ID: "j", TaskType: "test", Payload: json.RawMessage(`{}`)}, nil
}
func (s *fakeStore) Complete(context.Context, jobs.Job, json.RawMessage) error {
	s.complete = true
	if s.saved != nil {
		close(s.saved)
	}
	return s.persistErr
}
func (s *fakeStore) Fail(_ context.Context, _ jobs.Job, message string) error {
	s.failure = message
	if s.saved != nil {
		close(s.saved)
	}
	return s.persistErr
}

type handler struct {
	run func(context.Context) (json.RawMessage, error)
}

func (h handler) Validate(json.RawMessage) error { return nil }
func (h handler) Execute(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	return h.run(ctx)
}
func TestOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		h       handler
		failure string
	}{
		{"success", handler{func(context.Context) (json.RawMessage, error) { return json.RawMessage(`{"ok":true}`), nil }}, ""},
		{"failure", handler{func(context.Context) (json.RawMessage, error) { return nil, errors.New("boom") }}, "boom"},
		{"panic", handler{func(context.Context) (json.RawMessage, error) { panic("boom") }}, "handler panic"},
		{"bad result", handler{func(context.Context) (json.RawMessage, error) { return nil, nil }}, "invalid JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeStore{}
			w := Worker{ID: "test-worker", Concurrency: 1, Store: s, Registry: tasks.Registry{"test": tc.h}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			err := w.execute(context.Background(), jobs.Job{ID: "j", TaskType: "test"})
			if err != nil {
				t.Fatal(err)
			}
			if tc.failure == "" && !s.complete {
				t.Fatal("not completed")
			}
			if tc.failure != "" && !strings.Contains(s.failure, tc.failure) {
				t.Fatalf("failure=%q", s.failure)
			}
		})
	}
}
func TestPersistenceFailureStopsWorker(t *testing.T) {
	s := &fakeStore{persistErr: errors.New("offline")}
	calls := 0
	w := Worker{ID: "test-worker", Concurrency: 1, Store: s, Registry: tasks.Registry{"test": handler{func(context.Context) (json.RawMessage, error) { calls++; return json.RawMessage(`{}`), nil }}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), PollInterval: time.Millisecond}
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("persistence failure ignored")
	}
	if calls != 1 {
		t.Fatal("handler rerun")
	}
}
func TestShutdownFinishesInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s := &fakeStore{saved: make(chan struct{})}
	w := Worker{ID: "test-worker", Concurrency: 1, Store: s, Registry: tasks.Registry{"test": handler{func(ctx context.Context) (json.RawMessage, error) {
		close(started)
		<-release
		return json.RawMessage(`{}`), ctx.Err()
	}}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), PollInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("did not start")
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not stop")
	}
	if !s.complete {
		t.Fatal("in-flight task not completed")
	}
	// An already cancelled worker must not claim anything.
	s2 := &fakeStore{}
	w.Store = s2
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if s2.claimed {
		t.Fatal("claimed during shutdown")
	}
}

func (s *fakeStore) Renew(context.Context, jobs.Job) error   { return nil }
func (s *fakeStore) Recover(context.Context) error           { return nil }
func (s *fakeStore) Heartbeat(context.Context, string) error { return nil }

type renewalStore struct {
	*fakeStore
	renew func(context.Context) error
}

func (s renewalStore) Renew(ctx context.Context, _ jobs.Job) error { return s.renew(ctx) }

type blockingWarnings struct {
	slog.Handler
	release <-chan struct{}
}

func (h blockingWarnings) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.Handler = h.Handler.WithAttrs(attrs)
	return h
}

func (h blockingWarnings) Handle(ctx context.Context, record slog.Record) error {
	if record.Level == slog.LevelWarn {
		<-h.release
	}
	return h.Handler.Handle(ctx, record)
}

func TestRenewalFailurePersistsBeforeWarning(t *testing.T) {
	release := make(chan struct{})
	s := &fakeStore{saved: make(chan struct{})}
	h := handler{func(ctx context.Context) (json.RawMessage, error) {
		<-ctx.Done()
		return json.RawMessage(`{"ok":true}`), nil
	}}
	w := Worker{
		ID: "test-worker", Store: renewalStore{s, func(context.Context) error { return errors.New("renewal unavailable") }}, LeaseDuration: 30 * time.Millisecond,
		Registry: tasks.Registry{"test": h},
		Log:      slog.New(blockingWarnings{Handler: slog.NewTextHandler(io.Discard, nil), release: release}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.execute(ctx, jobs.Job{ID: "j", TaskType: "test"}) }()
	t.Cleanup(func() {
		close(release)
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("attempt did not finish after releasing warning sink")
		}
	})
	select {
	case <-s.saved:
		if s.complete || s.failure != context.Canceled.Error() {
			t.Fatalf("unexpected outcome: complete=%v failure=%q", s.complete, s.failure)
		}
	case <-ctx.Done():
		t.Fatal("warning blocked renewal cancellation or outcome persistence")
	}
}

func TestHandlerDeadlineExcludesRenewalCleanup(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := &fakeStore{}
	started := make(chan struct{})
	h := handler{func(context.Context) (json.RawMessage, error) {
		<-started
		return json.RawMessage(`{"ok":true}`), nil
	}}
	store := renewalStore{s, func(context.Context) error {
		close(started)
		// A slow renewal may still be unwinding after the execution deadline.
		<-parent.Done()
		return parent.Err()
	}}
	w := Worker{
		ID: "test-worker", Store: store, LeaseDuration: 30 * time.Millisecond,
		Registry: tasks.Registry{"test": h},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := w.execute(parent, jobs.Job{ID: "j", TaskType: "test"}); err != nil {
		t.Fatal(err)
	}
	if !s.complete {
		t.Fatalf("renewal cleanup changed handler outcome: %s", s.failure)
	}
}
