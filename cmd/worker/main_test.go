package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/tasks"
	"github.com/boscostix/JobFlow/internal/worker"
)

type blockedPersistence struct{ entered, release chan struct{} }

func (s blockedPersistence) Claim(context.Context, string) (jobs.Job, error) {
	return jobs.Job{ID: "test", TaskType: "add_numbers", Payload: json.RawMessage(`{"a":1,"b":2}`)}, nil
}
func (s blockedPersistence) Complete(context.Context, jobs.Job, json.RawMessage) error {
	close(s.entered)
	<-s.release
	return nil
}
func (s blockedPersistence) Fail(context.Context, jobs.Job, string) error {
	panic("unexpected handler failure")
}

func TestShutdownDeadlineDoesNotWaitForBlockedPersistenceCleanup(t *testing.T) {
	s := blockedPersistence{make(chan struct{}), make(chan struct{})}
	w := worker.Worker{ID: "test-worker", Concurrency: 1, Store: s, Registry: tasks.DefaultRegistry(), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), PollInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	cleanup := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		result <- runWorker(ctx, 20*time.Millisecond, func(ctx context.Context) error { defer close(finished); return w.Run(ctx) }, func() { cleanup <- struct{}{}; <-s.release })
	}()
	defer func() {
		close(s.release)
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("worker failed to finish after release")
		}
	}()
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("outcome write did not start")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "shutdown exceeded") {
			t.Fatalf("unexpected result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked waiting for active persistence")
	}
	select {
	case <-cleanup:
		t.Fatal("pool cleanup started before outstanding connection returned")
	default:
	}
}

func TestNormalShutdownClosesPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closed := false
	err := runWorker(ctx, time.Second, func(ctx context.Context) error { <-ctx.Done(); return nil }, func() { closed = true })
	if err != nil || !closed {
		t.Fatalf("err=%v closed=%v", err, closed)
	}
}

func (s blockedPersistence) Renew(context.Context, jobs.Job) error   { return nil }
func (s blockedPersistence) Recover(context.Context) error           { return nil }
func (s blockedPersistence) Heartbeat(context.Context, string) error { return nil }
