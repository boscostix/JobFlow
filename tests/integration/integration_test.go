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
	files, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("migration files: %v", err)
	}
	for _, file := range files {
		sql, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
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
	w := worker.Worker{ID: "test-worker", Concurrency: 1, Store: s, Registry: registry, Log: logger(), PollInterval: 10 * time.Millisecond}
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
	outcome := await(t, s, failed.ID, "DEAD_LETTER")
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
		go func() { defer wg.Done(); _, err := s.Claim(ctx, "claim-test"); out <- err }()
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
	if err := s.Complete(ctx, owned(t, s, j.ID), json.RawMessage(`{"sum":3}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, owned(t, s, j.ID), "late failure"); !errors.Is(err, jobs.ErrInvalidTransition) {
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
	w := worker.Worker{ID: "test-worker", Concurrency: 1, Store: s, Registry: tasks.Registry{"blocking": h}, Log: logger(), PollInterval: time.Millisecond}
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

// Each independent worker has a separate connection pool, as separate processes do.
// Blocking handlers fill every slot before any completion can free capacity.
type concurrentHandler struct {
	started      chan<- struct{}
	release      <-chan struct{}
	mu           sync.Mutex
	active, peak int
	seen         map[int]int
}

func (h *concurrentHandler) Validate(json.RawMessage) error { return nil }
func (h *concurrentHandler) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var n int
	if err := json.Unmarshal(payload, &n); err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.active++
	if h.active > h.peak {
		h.peak = h.active
	}
	h.seen[n]++
	h.mu.Unlock()
	defer func() { h.mu.Lock(); h.active--; h.mu.Unlock() }()
	h.started <- struct{}{}
	<-h.release
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if n%2 == 1 {
		return nil, errors.New("expected failure")
	}
	return json.RawMessage(`{"ok":true}`), nil
}
func waitStarted(t *testing.T, started <-chan struct{}, n int) {
	t.Helper()
	for range n {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("execution slots did not fill")
		}
	}
}
func assertHistory(t *testing.T, pool *pgxpool.Pool, count int) {
	t.Helper()
	var total, mismatches int
	err := pool.QueryRow(context.Background(), `SELECT count(*), count(*) FILTER (WHERE
 a.status <> CASE WHEN j.status='DEAD_LETTER' THEN 'FAILED' ELSE j.status END OR a.started_at <> j.started_at OR
 a.completed_at IS DISTINCT FROM j.completed_at OR a.result IS DISTINCT FROM j.result OR
 a.last_error IS DISTINCT FROM j.last_error OR a.attempt_number <> 1 OR a.worker_id = '')
 FROM jobs j JOIN job_attempts a ON a.job_id=j.id`).Scan(&total, &mismatches)
	if err != nil || total != count || mismatches != 0 {
		t.Fatalf("history total=%d mismatches=%d err=%v", total, mismatches, err)
	}
}
func TestDistributedConcurrencyAndHistory(t *testing.T) {
	pool, cfg := database(t)
	s := store.New(pool)
	ctx := context.Background()
	const total = 30
	ids := make([]string, 0, total)
	for n := range total {
		j, err := s.Create(ctx, "concurrent", json.RawMessage(fmt.Sprint(n)))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	started := make(chan struct{}, total)
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 3)
	handlers := make([]*concurrentHandler, 0, 3)
	for n := range 3 {
		p, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		h := &concurrentHandler{started: started, release: release, seen: make(map[int]int)}
		handlers = append(handlers, h)
		w := worker.Worker{ID: fmt.Sprintf("worker-%d", n), Concurrency: 2, Store: store.New(p), Registry: tasks.Registry{"concurrent": h}, Log: logger(), PollInterval: time.Millisecond}
		go func() { done <- w.Run(workerCtx) }()
	}
	// Release/drain before deferred pool closes, even when an assertion fails.
	defer func() {
		cancel()
		unblock.Do(func() { close(release) })
		for range 3 {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("worker did not drain")
			}
		}
	}()
	waitStarted(t, started, 6)
	select {
	case <-started:
		t.Fatal("local concurrency exceeded")
	case <-time.After(100 * time.Millisecond):
	}
	var running, attempts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='RUNNING'").Scan(&running); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_attempts").Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if running != 6 || attempts != 6 {
		t.Fatalf("running=%d attempts=%d", running, attempts)
	}
	unblock.Do(func() { close(release) })
	for n, id := range ids {
		status := "COMPLETED"
		if n%2 == 1 {
			status = "DEAD_LETTER"
		}
		await(t, s, id, status)
	}
	cancel()
	// Awaiting database outcomes does not establish that handlers have returned;
	// use their mutex to inspect accounting while workers finish their loops.
	seen := make(map[int]int)
	for _, h := range handlers {
		h.mu.Lock()
		if h.peak != 2 {
			t.Errorf("worker peak=%d", h.peak)
		}
		for n, count := range h.seen {
			seen[n] += count
		}
		h.mu.Unlock()
	}
	for n := range total {
		if seen[n] != 1 {
			t.Errorf("job %d executed %d times", n, seen[n])
		}
	}
	assertHistory(t, pool, total)
	var owners int
	if err := pool.QueryRow(ctx, "SELECT count(DISTINCT worker_id) FROM job_attempts").Scan(&owners); err != nil || owners != 3 {
		t.Fatalf("owners=%d err=%v", owners, err)
	}
}
func TestConcurrentShutdownDrainsOnlyClaimedJobs(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	for n := range 6 {
		if _, err := s.Create(ctx, "concurrent", json.RawMessage(fmt.Sprint(n))); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	h := &concurrentHandler{started: started, release: release, seen: make(map[int]int)}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	w := worker.Worker{ID: "shutdown-worker", Concurrency: 3, Store: s, Registry: tasks.Registry{"concurrent": h}, Log: logger(), PollInterval: time.Millisecond}
	go func() { done <- w.Run(workerCtx) }()
	var unblock sync.Once
	defer func() { cancel(); unblock.Do(func() { close(release) }) }()
	waitStarted(t, started, 3)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned before in-flight work drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not drain")
	}
	var queued, terminal int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='QUEUED'),count(*) FILTER (WHERE status IN ('COMPLETED','DEAD_LETTER')) FROM jobs`).Scan(&queued, &terminal); err != nil {
		t.Fatal(err)
	}
	if queued != 3 || terminal != 3 {
		t.Fatalf("queued=%d terminal=%d", queued, terminal)
	}
	assertHistory(t, pool, 3)
}
func TestAttemptWritesAreAtomic(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	j, err := s.Create(ctx, "add_numbers", json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	// Force insertion failure after the queue row was updated.
	_, err = pool.Exec(ctx, `INSERT INTO job_attempts(job_id,attempt_number,worker_id,status,started_at) VALUES($1,1,'conflict','RUNNING',now())`, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, "worker"); err == nil {
		t.Fatal("claim accepted failed attempt insert")
	}
	got, err := s.Get(ctx, j.ID)
	if err != nil || got.Status != "QUEUED" {
		t.Fatalf("claim was not rolled back: %+v %v", got, err)
	}
	if _, err = pool.Exec(ctx, "DELETE FROM job_attempts WHERE job_id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `CREATE FUNCTION reject_attempt_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END; $$;
 CREATE TRIGGER reject_attempt BEFORE UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION reject_attempt_write();`)
	if err != nil {
		t.Fatal(err)
	}
	for _, finish := range []func() error{
		func() error { return s.Complete(ctx, owned(t, s, j.ID), json.RawMessage(`{"sum":3}`)) },
		func() error { return s.Fail(ctx, owned(t, s, j.ID), "failure") },
	} {
		if err := finish(); err == nil {
			t.Fatal("accepted failed history update")
		}
		got, err = s.Get(ctx, j.ID)
		if err != nil || got.Status != "RUNNING" {
			t.Fatalf("outcome was not rolled back: %+v %v", got, err)
		}
		assertHistory(t, pool, 1)
	}
	if _, err = pool.Exec(ctx, "DROP TRIGGER reject_attempt ON job_attempts"); err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, owned(t, s, j.ID), json.RawMessage(`{"sum":3}`)); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, pool, 1)
}

func TestClaimSkipsLockedJob(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, err := s.Create(ctx, "add_numbers", json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Create(ctx, "add_numbers", json.RawMessage(`{"a":3,"b":4}`))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", first.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(ctx, "other-worker")
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("did not skip locked job: %s %v", claimed.ID, err)
	}
}
func TestAttemptMigrationBackfillsLegacyJobs(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	// Recreate the Milestone 1 schema before applying migrations 2 and 3.
	if _, err := pool.Exec(ctx, "DROP TABLE idempotency_keys,workers,job_attempts,jobs CASCADE; DROP FUNCTION enforce_job_transition()"); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000001_jobs.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"QUEUED", "RUNNING", "COMPLETED", "FAILED"} {
		j := jobs.Job{ID: "legacy-" + status}
		_, err := pool.Exec(ctx, `INSERT INTO jobs(id,task_type,payload) VALUES($1,'add_numbers','{"a":1,"b":2}')`, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if status == "QUEUED" {
			continue
		}
		if _, err = pool.Exec(ctx, "UPDATE jobs SET status='RUNNING',started_at=now() WHERE id=$1", j.ID); err != nil {
			t.Fatal(err)
		}
		switch status {
		case "COMPLETED":
			_, err = pool.Exec(ctx, `UPDATE jobs SET status='COMPLETED',completed_at=now(),result='{"sum":3}' WHERE id=$1`, j.ID)
		case "FAILED":
			_, err = pool.Exec(ctx, "UPDATE jobs SET status='FAILED',completed_at=now(),last_error='legacy failure' WHERE id=$1", j.ID)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000002_job_attempts.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	migration, err = os.ReadFile(filepath.Join("..", "..", "migrations", "000003_reliability.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, pool, 3)
	var legacy int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM job_attempts WHERE worker_id='legacy-unknown'").Scan(&legacy); err != nil || legacy != 3 {
		t.Fatalf("legacy=%d err=%v", legacy, err)
	}
	var running string
	if err = pool.QueryRow(ctx, "SELECT id FROM jobs WHERE status='RUNNING'").Scan(&running); err != nil {
		t.Fatal(err)
	}
	if err = s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, pool, 3)
	if _, err = s.Claim(ctx, "new-worker"); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, pool, 4)
}

func owned(t *testing.T, s *store.Store, id string) jobs.Job {
	t.Helper()
	j, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
