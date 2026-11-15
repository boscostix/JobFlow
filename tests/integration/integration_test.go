//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/boscostix/JobFlow/internal/api"
	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/store"
	"github.com/boscostix/JobFlow/internal/tasks"
	"github.com/boscostix/JobFlow/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every test owns a random schema. Never truncate or migrate existing user tables.
func database(t *testing.T) (*pgxpool.Pool, *pgxpool.Config) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests (use a development PostgreSQL database)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	var token [12]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	schema := "jobflow_test_" + hex.EncodeToString(token[:])
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("clean schema: %v", err)
		}
		admin.Close(ctx)
	})
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000001_jobs.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	return pool, cfg
}
func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func TestDurableAPISubmissionAndWorker(t *testing.T) {
	pool, cfg := database(t)
	registry := tasks.DefaultRegistry()
	s := store.New(pool)
	server := httptest.NewServer(api.New(s, registry, logger()))
	resp, err := http.Post(server.URL+"/api/v1/jobs", "application/json", bytes.NewBufferString(`{"type":"add_numbers","payload":{"a":20,"b":22}}`))
	if err != nil {
		t.Fatal(err)
	}
	var accepted struct {
		ID     string `json:"job_id"`
		Status string `json:"status"`
	}
	err = json.NewDecoder(resp.Body).Decode(&accepted)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 202 || accepted.Status != "QUEUED" || accepted.ID == "" {
		t.Fatalf("accept: %+v code=%d err=%v", accepted, resp.StatusCode, err)
	}
	server.Close()
	// Recreate the API with a new pool to establish that the job lives in PostgreSQL.
	restarted, err := pgxpool.NewWithConfig(context.Background(), cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	server = httptest.NewServer(api.New(store.New(restarted), registry, logger()))
	defer server.Close()
	resp, err = http.Get(server.URL + "/api/v1/jobs/" + accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	var queued jobs.Job
	err = json.NewDecoder(resp.Body).Decode(&queued)
	resp.Body.Close()
	if err != nil || queued.Status != "QUEUED" {
		t.Fatalf("after restart: %+v err=%v", queued, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := worker.Worker{Store: s, Registry: registry, Log: logger(), PollInterval: 10 * time.Millisecond}
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("worker did not stop")
		}
	})
	completed := await(t, s, accepted.ID, "COMPLETED")
	var result struct {
		Sum int `json:"sum"`
	}
	if err := json.Unmarshal(completed.Result, &result); err != nil || result.Sum != 42 {
		t.Fatalf("result=%s err=%v", completed.Result, err)
	}
	failed, err := s.Create(context.Background(), "add_numbers", json.RawMessage(`{"a":9223372036854775807,"b":1}`))
	if err != nil {
		t.Fatal(err)
	}
	outcome := await(t, s, failed.ID, "FAILED")
	if outcome.LastError == nil || *outcome.LastError != "integer addition overflow" {
		t.Fatalf("failure=%+v", outcome)
	}
	resp, err = http.Get(server.URL + "/api/v1/jobs/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatal(resp.StatusCode)
	}
}
func await(t *testing.T, s *store.Store, id, status string) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := s.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == status {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", id, status)
	return jobs.Job{}
}
func TestClaimAndTerminalTransitions(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	j, err := s.Create(ctx, "add_numbers", json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	// Two overlapping claims exercise the real PostgreSQL locking statement.
	var wg sync.WaitGroup
	out := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.Claim(ctx); out <- err }()
	}
	wg.Wait()
	close(out)
	claims := 0
	for err := range out {
		if err == nil {
			claims++
		} else if !errors.Is(err, jobs.ErrNotFound) {
			t.Fatal(err)
		}
	}
	if claims != 1 {
		t.Fatalf("claims=%d", claims)
	}
	if err := s.Complete(ctx, j.ID, json.RawMessage(`{"sum":3}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, j.ID, "late failure"); !errors.Is(err, jobs.ErrInvalidTransition) {
		t.Fatalf("terminal mutation: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET status='QUEUED', started_at=NULL, completed_at=NULL, result=NULL WHERE id=$1", j.ID); err == nil {
		t.Fatal("database accepted invalid terminal transition")
	}
	persisted, err := s.Get(ctx, j.ID)
	if err != nil || persisted.Status != "COMPLETED" {
		t.Fatalf("terminal result changed: %+v %v", persisted, err)
	}
}

type blockingHandler struct{ started, release chan struct{} }

func (h blockingHandler) Validate(json.RawMessage) error { return nil }
func (h blockingHandler) Execute(context.Context, json.RawMessage) (json.RawMessage, error) {
	close(h.started)
	<-h.release
	return json.RawMessage(`{"ok":true}`), nil
}
func TestExecutionReleasesClaimLockAndGracefulShutdown(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	j, err := s.Create(ctx, "blocking", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	h := blockingHandler{make(chan struct{}), make(chan struct{})}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	w := worker.Worker{Store: s, Registry: tasks.Registry{"blocking": h}, Log: logger(), PollInterval: time.Millisecond}
	go func() { done <- w.Run(workerCtx) }()
	var release sync.Once
	unblock := func() { release.Do(func() { close(h.release) }) }
	defer unblock()
	select {
	case <-h.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// NOWAIT would fail immediately if handler execution retained the claim row lock.
	var status string
	err = tx.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1 FOR UPDATE NOWAIT", j.ID).Scan(&status)
	_ = tx.Rollback(ctx)
	if err != nil || status != "RUNNING" {
		t.Fatalf("claim lock retained: status=%s err=%v", status, err)
	}
	cancel()
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not finish")
	}
	got, err := s.Get(ctx, j.ID)
	if err != nil || got.Status != "COMPLETED" {
		t.Fatalf("shutdown outcome: %+v %v", got, err)
	}
}
func TestInvalidRequestDoesNotPersist(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	server := httptest.NewServer(api.New(s, tasks.DefaultRegistry(), logger()))
	defer server.Close()
	for _, body := range []string{`{"Type":"add_numbers","payload":{"a":1,"b":2}}`, `{"type":"add_numbers","payload":{"a":null,"A":1,"b":2}}`, `{"type":"add_numbers","payload":{"a":1,"A":2,"b":3}}`, `{"type":"add_numbers","payload":{"a":1,"a":2,"b":3}}`, `{}`, `{"type":"add_numbers","payload":{"a":1}}`, `{"type":"add_numbers","payload":{"a":1,"b":2},"scheduled_at":"2026-12-01T09:00:00Z"}`} {
		resp, err := http.Post(server.URL+"/api/v1/jobs", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatal(resp.StatusCode)
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal(fmt.Sprintf("invalid requests persisted %d jobs", count))
	}
}
