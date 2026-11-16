//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boscostix/JobFlow/internal/api"
	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/store"
	"github.com/boscostix/JobFlow/internal/tasks"
	"github.com/boscostix/JobFlow/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func mustSubmit(t *testing.T, s *store.Store, task string, retries, timeout int32) jobs.Job {
	t.Helper()
	j, err := s.Submit(context.Background(), task, json.RawMessage(`{"a":1,"b":2}`), jobs.Options{MaxRetries: retries, TimeoutSeconds: timeout})
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func mustClaim(t *testing.T, s *store.Store, owner string) jobs.Job {
	t.Helper()
	j, err := s.Claim(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func expire(t *testing.T, p *pgxpool.Pool, id string) {
	t.Helper()
	if _, err := p.Exec(context.Background(), "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
}
func requireStale(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, jobs.ErrInvalidTransition) {
		t.Fatalf("expected ownership rejection: %v", err)
	}
}
func claimEventually(t *testing.T, s *store.Store, owner string) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := s.Claim(context.Background(), owner)
		if err == nil {
			return j
		}
		if !errors.Is(err, jobs.ErrNotFound) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("retry did not become eligible")
	return jobs.Job{}
}
func TestLeaseRecoveryFencesEveryMutation(t *testing.T) {
	p, _ := database(t)
	s := store.New(p)
	ctx := context.Background()
	mustSubmit(t, s, "add_numbers", 1, 300)
	old := mustClaim(t, s, "same-owner")
	// Wrong owner and token fail even while the lease is valid.
	wrong := old
	wrong.LeaseOwner = "impostor"
	requireStale(t, s.Renew(ctx, wrong))
	requireStale(t, s.Complete(ctx, wrong, json.RawMessage(`{}`)))
	wrong = old
	wrong.FencingToken++
	requireStale(t, s.Fail(ctx, wrong, "bad"))
	expire(t, p, old.ID)
	requireStale(t, s.Renew(ctx, old))
	requireStale(t, s.Complete(ctx, old, json.RawMessage(`{}`)))
	requireStale(t, s.Fail(ctx, old, "late"))
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.Recover(ctx) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	queued := owned(t, s, old.ID)
	if queued.Status != "QUEUED" || queued.AttemptCount != 1 {
		t.Fatalf("recovery: %+v", queued)
	}
	if _, err := s.Claim(ctx, "early"); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("backoff ignored: %v", err)
	}
	// Reusing the same worker ID must still reject the earlier token.
	next := claimEventually(t, s, "same-owner")
	if next.FencingToken <= old.FencingToken || next.AttemptCount != 2 {
		t.Fatalf("new attempt: %+v", next)
	}
	requireStale(t, s.Renew(ctx, old))
	requireStale(t, s.Complete(ctx, old, json.RawMessage(`{"stale":true}`)))
	requireStale(t, s.Fail(ctx, old, "stale"))
	if err := s.Complete(ctx, next, json.RawMessage(`{"sum":3}`)); err != nil {
		t.Fatal(err)
	}
	requireStale(t, s.Fail(ctx, old, "stale after terminal"))
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM job_attempts WHERE job_id=$1 AND ((attempt_number=1 AND status='FAILED' AND last_error='lease expired') OR (attempt_number=2 AND status='COMPLETED'))`, old.ID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("history %d %v", count, err)
	}
}

func TestRenewalAndRecoverySerialize(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprint(expired), func(t *testing.T) {
			p, _ := database(t)
			s := store.New(p)
			ctx := context.Background()
			mustSubmit(t, s, "add_numbers", 0, 300)
			j := mustClaim(t, s, "owner")
			if expired {
				expire(t, p, j.ID)
			}
			// Hold the row while both contenders start; recovery skips locked rows,
			// renewal must recheck expiry after obtaining the lock.
			tx, err := p.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err = tx.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID); err != nil {
				t.Fatal(err)
			}
			renew := make(chan error, 1)
			go func() { renew <- s.Renew(ctx, j) }()
			if err = s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			err = <-renew
			if expired {
				requireStale(t, err)
			} else if err != nil {
				t.Fatal(err)
			}
			if err = s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			got := owned(t, s, j.ID)
			want := "RUNNING"
			if expired {
				want = "DEAD_LETTER"
			}
			if got.Status != want {
				t.Fatalf("status=%s want %s", got.Status, want)
			}
		})
	}
}

func TestLeaseCannotBeRevivedAfterWaitingForLock(t *testing.T) {
	p, _ := database(t)
	s := store.NewWithLease(p, 300*time.Millisecond)
	ctx := context.Background()
	mustSubmit(t, s, "add_numbers", 0, 300)
	j := mustClaim(t, s, "owner")
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Renew(ctx, j) }()
	time.Sleep(time.Until(*j.LeaseExpiresAt) + 30*time.Millisecond)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	requireStale(t, <-done)
	if err = s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got := owned(t, s, j.ID); got.Status != "DEAD_LETTER" {
		t.Fatal(got.Status)
	}
}

func TestRetryBudgetBackoffAndManualRetry(t *testing.T) {
	p, _ := database(t)
	s := store.New(p)
	ctx := context.Background()
	mustSubmit(t, s, "add_numbers", 2, 300)
	var id string
	for attempt := int32(1); attempt <= 3; attempt++ {
		j := claimEventually(t, s, "owner")
		id = j.ID
		if j.AttemptCount != attempt {
			t.Fatalf("attempt=%d", j.AttemptCount)
		}
		before := time.Now()
		if err := s.Fail(ctx, j, fmt.Sprintf("failure %d", attempt)); err != nil {
			t.Fatal(err)
		}
		got := owned(t, s, id)
		if attempt < 3 {
			ceiling := time.Second * time.Duration(1<<(attempt-1))
			if got.Status != "QUEUED" || got.NextRunAt.Before(before.Add(ceiling/2-50*time.Millisecond)) || got.NextRunAt.After(time.Now().Add(ceiling)) {
				t.Fatalf("bad backoff: %+v", got)
			}
			if _, err := s.Claim(ctx, "too-early"); !errors.Is(err, jobs.ErrNotFound) {
				t.Fatalf("early retry %v", err)
			}
		} else if got.Status != "DEAD_LETTER" {
			t.Fatal(got.Status)
		}
	}
	var attempts int
	if err := p.QueryRow(ctx, "SELECT count(*) FROM job_attempts WHERE job_id=$1 AND status='FAILED'", id).Scan(&attempts); err != nil || attempts != 3 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	if _, err := s.Claim(ctx, "exhausted"); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("exceeded retry limit: %v", err)
	}
	// Two manual retries race; only one can move the dead letter to QUEUED.
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.Retry(ctx, id); results <- err }()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			requireStale(t, err)
		}
	}
	if successes != 1 {
		t.Fatalf("manual retries=%d", successes)
	}
	j := mustClaim(t, s, "manual")
	if j.AttemptCount != 4 {
		t.Fatal(j.AttemptCount)
	}
	if err := s.Fail(ctx, j, "new cycle"); err != nil {
		t.Fatal(err)
	}
	if owned(t, s, id).Status != "QUEUED" {
		t.Fatal("manual retry did not reset budget")
	}
	if _, err := s.Retry(ctx, id); err == nil {
		t.Fatal("retry accepted queued job")
	}
	if err := p.QueryRow(ctx, "SELECT count(*) FROM job_attempts WHERE job_id=$1", id).Scan(&attempts); err != nil || attempts != 4 {
		t.Fatalf("history erased: %d %v", attempts, err)
	}
}

type functionHandler func(context.Context, json.RawMessage) (json.RawMessage, error)

func (h functionHandler) Validate(json.RawMessage) error { return nil }
func (h functionHandler) Execute(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
	return h(ctx, p)
}
func runTestWorker(t *testing.T, s *store.Store, lease time.Duration, h tasks.Handler) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := worker.Worker{ID: "reliable-worker", Concurrency: 1, LeaseDuration: lease, Store: s, Registry: tasks.Registry{"test": h}, Log: logger(), PollInterval: 10 * time.Millisecond}
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("worker failed to drain")
		}
	})
	return cancel
}
func TestCooperativeTimeoutAndHeartbeat(t *testing.T) {
	p, _ := database(t)
	lease := 600 * time.Millisecond
	s := store.NewWithLease(p, lease)
	j := mustSubmit(t, s, "test", 0, 1)
	started := make(chan tasks.Execution, 1)
	runTestWorker(t, s, lease, functionHandler(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		info, ok := tasks.ExecutionFromContext(ctx)
		if !ok {
			return nil, errors.New("missing execution info")
		}
		started <- info
		<-ctx.Done()
		return json.RawMessage(`{"too_late":true}`), nil
	}))
	select {
	case e := <-started:
		if e.JobID != j.ID || e.FencingToken != 1 {
			t.Fatalf("metadata %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no handler")
	}
	got := await(t, s, j.ID, "DEAD_LETTER")
	if got.LastError == nil || *got.LastError != "context deadline exceeded" {
		t.Fatalf("timeout: %+v", got)
	}
	var fresh bool
	if err := p.QueryRow(context.Background(), "SELECT heartbeat_at>started_at AND heartbeat_at>clock_timestamp()-interval '1 second' FROM workers WHERE id='reliable-worker'").Scan(&fresh); err != nil || !fresh {
		t.Fatalf("heartbeat %v %v", fresh, err)
	}
	var count int
	if err := p.QueryRow(context.Background(), "SELECT count(*) FROM job_attempts WHERE status='FAILED' AND last_error='context deadline exceeded'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("timeout history %d %v", count, err)
	}
}
func TestNoncooperativeTimeoutStopsRenewalAndKeepsSlot(t *testing.T) {
	p, _ := database(t)
	lease := 300 * time.Millisecond
	s := store.NewWithLease(p, lease)
	j := mustSubmit(t, s, "test", 0, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	// Registered before worker cleanup so a failing assertion still releases it.
	h := functionHandler(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		<-release
		return json.RawMessage(`{}`), nil
	})
	runTestWorker(t, s, lease, h)
	defer unblock()
	await(t, s, j.ID, "DEAD_LETTER")
	// Maintenance recovered the expired lease while the handler still occupied its slot.
	second := mustSubmit(t, s, "test", 0, 1)
	time.Sleep(150 * time.Millisecond)
	if got := owned(t, s, second.ID); got.Status != "QUEUED" {
		t.Fatal("uncooperative handler released local slot")
	}
	unblock()
	await(t, s, second.ID, "COMPLETED")
	if got := owned(t, s, j.ID); got.Status != "DEAD_LETTER" {
		t.Fatal("late return overwrote recovered result")
	}
}

func TestConcurrentIdempotentSubmissions(t *testing.T) {
	p, _ := database(t)
	s := store.New(p)
	server := httptest.NewServer(api.New(s, tasks.DefaultRegistry(), logger()))
	defer server.Close()
	submit := func(body string) (int, jobs.Job, error) {
		req, err := http.NewRequest("POST", server.URL+"/api/v1/jobs", strings.NewReader(body))
		if err != nil {
			return 0, jobs.Job{}, err
		}
		req.Header.Set("Idempotency-Key", "same-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, jobs.Job{}, err
		}
		defer resp.Body.Close()
		var out struct {
			ID     string `json:"job_id"`
			Status string `json:"status"`
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, jobs.Job{ID: out.ID, Status: out.Status}, err
	}
	bodies := []string{`{"type":"add_numbers","payload":{"a":1,"b":2}}`, `{"payload":{"b":2,"a":1},"type":"add_numbers","timeout_seconds":300,"max_retries":0}`}
	type result struct {
		code int
		j    jobs.Job
		err  error
	}
	results := make(chan result, 12)
	for n := range 12 {
		go func() { code, j, err := submit(bodies[n%2]); results <- result{code, j, err} }()
	}
	id := ""
	for range 12 {
		r := <-results
		if r.err != nil || r.code != 202 || r.j.ID == "" {
			t.Fatalf("submission %+v", r)
		}
		if id == "" {
			id = r.j.ID
		}
		if r.j.ID != id {
			t.Fatal("duplicate job")
		}
	}
	for _, body := range []string{`{"type":"add_numbers","payload":{"a":2,"b":2}}`, `{"type":"add_numbers","payload":{"a":1,"b":2},"max_retries":1}`, `{"type":"add_numbers","payload":{"a":1,"b":2},"timeout_seconds":1}`} {
		code, _, err := submit(body)
		if err != nil || code != 409 {
			t.Fatalf("conflict %d %v", code, err)
		}
	}
	claimed := mustClaim(t, s, "owner")
	if err := s.Complete(context.Background(), claimed, json.RawMessage(`{"sum":3}`)); err != nil {
		t.Fatal(err)
	}
	code, j, err := submit(bodies[0])
	if err != nil || code != 202 || j.ID != id || j.Status != "COMPLETED" {
		t.Fatalf("terminal replay %d %+v %v", code, j, err)
	}
	var count int
	if err := p.QueryRow(context.Background(), "SELECT count(*) FROM jobs").Scan(&count); err != nil || count != 1 {
		t.Fatalf("unused candidates persisted %d %v", count, err)
	}
	// Restarting the API/pool cannot forget the persisted key.
	server.Close()
	server = httptest.NewServer(api.New(store.New(p), tasks.DefaultRegistry(), logger()))
	defer server.Close()
	code, j, err = submit(bodies[1])
	if err != nil || code != 202 || j.ID != id {
		t.Fatalf("restart replay %d %+v %v", code, j, err)
	}
}

// Run the real worker in a separate OS process, with a test-only handler that
// records an external effect then waits. No fault hooks enter production code.
func TestCrashWorkerProcess(t *testing.T) {
	if os.Getenv("JOBFLOW_CRASH_CHILD") != "1" {
		return
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = os.Getenv("JOBFLOW_CRASH_SCHEMA")
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	h := effectHandler(p, true)
	w := worker.Worker{ID: "crash-child", Concurrency: 1, LeaseDuration: 600 * time.Millisecond, Store: store.NewWithLease(p, 600*time.Millisecond), Registry: tasks.Registry{"test": h}, Log: logger(), PollInterval: 10 * time.Millisecond}
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func effectHandler(p *pgxpool.Pool, block bool) tasks.Handler {
	return functionHandler(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		info, ok := tasks.ExecutionFromContext(ctx)
		if !ok {
			return nil, errors.New("missing job ID")
		}
		// An external unique operation key makes the repeated effect safe. This is
		// handler-side deduplication, not exactly-once execution by JobFlow.
		_, err := p.Exec(ctx, "INSERT INTO effects(job_id) VALUES($1) ON CONFLICT DO NOTHING", info.JobID)
		if err != nil {
			return nil, err
		}
		if block {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
}
func TestProcessCrashRecoveryAndEffectDeduplication(t *testing.T) {
	p, cfg := database(t)
	ctx := context.Background()
	s := store.NewWithLease(p, 600*time.Millisecond)
	if _, err := p.Exec(ctx, "CREATE TABLE effects(job_id text PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	j := mustSubmit(t, s, "test", 1, 300)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestCrashWorkerProcess$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "JOBFLOW_CRASH_CHILD=1", "JOBFLOW_CRASH_SCHEMA="+cfg.ConnConfig.RuntimeParams["search_path"])
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		var n int
		if err := p.QueryRow(ctx, "SELECT count(*) FROM effects WHERE job_id=$1", j.ID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			seen = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !seen {
		t.Fatal("child did not execute effect")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err == nil {
		t.Fatalf("child did not crash: %s", output.String())
	}
	runTestWorker(t, s, 600*time.Millisecond, effectHandler(p, false))
	got := await(t, s, j.ID, "COMPLETED")
	if got.AttemptCount != 2 {
		t.Fatalf("attempts=%d", got.AttemptCount)
	}
	var effects, failed, completed int
	if err := p.QueryRow(ctx, "SELECT count(*) FROM effects").Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, "SELECT count(*) FILTER(WHERE status='FAILED' AND worker_id='crash-child' AND last_error='lease expired'),count(*) FILTER(WHERE status='COMPLETED' AND worker_id='reliable-worker') FROM job_attempts WHERE job_id=$1", j.ID).Scan(&failed, &completed); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || failed != 1 || completed != 1 {
		t.Fatalf("effects=%d failed=%d completed=%d", effects, failed, completed)
	}
}

func TestRecoveryRollsBackWhenHistoryWriteFails(t *testing.T) {
	p, _ := database(t)
	s := store.New(p)
	ctx := context.Background()
	mustSubmit(t, s, "add_numbers", 1, 300)
	j := mustClaim(t, s, "owner")
	expire(t, p, j.ID)
	_, err := p.Exec(ctx, `CREATE FUNCTION reject_recovery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'history unavailable'; END $$;
 CREATE TRIGGER reject_recovery BEFORE UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION reject_recovery();`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Recover(ctx); err == nil {
		t.Fatal("recovery ignored failed history write")
	}
	got := owned(t, s, j.ID)
	if got.Status != "RUNNING" || got.AttemptCount != 1 || got.FencingToken != j.FencingToken {
		t.Fatalf("partial recovery: %+v", got)
	}
	var status string
	if err = p.QueryRow(ctx, "SELECT status FROM job_attempts WHERE job_id=$1", j.ID).Scan(&status); err != nil || status != "RUNNING" {
		t.Fatalf("history %s %v", status, err)
	}
	if _, err = p.Exec(ctx, "DROP TRIGGER reject_recovery ON job_attempts"); err != nil {
		t.Fatal(err)
	}
	if err = s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got = owned(t, s, j.ID); got.Status != "QUEUED" {
		t.Fatal(got.Status)
	}
}

func TestCompletionCannotWinAfterWaitingPastExpiry(t *testing.T) {
	p, _ := database(t)
	s := store.NewWithLease(p, 300*time.Millisecond)
	ctx := context.Background()
	mustSubmit(t, s, "add_numbers", 0, 300)
	j := mustClaim(t, s, "owner")
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Complete(ctx, j, json.RawMessage(`{}`)) }()
	time.Sleep(time.Until(*j.LeaseExpiresAt) + 30*time.Millisecond)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	requireStale(t, <-done)
	if err = s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if owned(t, s, j.ID).Status != "DEAD_LETTER" {
		t.Fatal("expired completion won")
	}
}
