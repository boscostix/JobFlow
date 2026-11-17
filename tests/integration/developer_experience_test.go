//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/boscostix/JobFlow/internal/api"
	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/store"
	"github.com/boscostix/JobFlow/internal/tasks"
)

func requestJSON[T any](t *testing.T, method, endpoint, body, key string, status int) T {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || resp.StatusCode != status {
		t.Fatalf("%s %s: status=%d want %d body=%+v decode=%v", method, endpoint, resp.StatusCode, status, result, err)
	}
	return result
}

func TestScheduledSubmissionAndOperatorAPI(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	server := httptest.NewServer(api.New(s, tasks.DefaultRegistry(), logger()))
	defer server.Close()
	base := server.URL + "/api/v1"
	var dbNow time.Time
	if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&dbNow); err != nil {
		t.Fatal(err)
	}
	submitAt := func(at time.Time, key string, code int) map[string]string {
		body := fmt.Sprintf(`{"type":"add_numbers","payload":{"a":20,"b":22},"scheduled_at":%q,"max_retries":1}`, at.Format(time.RFC3339Nano))
		return requestJSON[map[string]string](t, "POST", base+"/jobs", body, key, code)
	}
	future := dbNow.Add(1200 * time.Millisecond).UTC()
	id := submitAt(future, "scheduled", 202)["job_id"]
	if _, err := s.Claim(ctx, "early"); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("future job became eligible early: %v", err)
	}
	offset := future.In(time.FixedZone("offset", -6*3600))
	if same := submitAt(offset, "scheduled", 202)["job_id"]; same != id {
		t.Fatalf("equivalent instant returned %s, want %s", same, id)
	}
	submitAt(future.Add(time.Microsecond), "scheduled", 409)
	requestJSON[map[string]string](t, "POST", base+"/jobs", `{"type":"add_numbers","payload":{"a":20,"b":22},"max_retries":1}`, "scheduled", 409)
	j := requestJSON[jobs.Job](t, "GET", base+"/jobs/"+id, "", "", 200)
	for _, invalid := range []string{`42`, `""`, `"2026-11-31T10:00:00Z"`, `"2026-11-17T10:00:00.1234567Z"`, `"2026-11-17T10:00:00+24:00"`, `"0001-01-01T00:00:00+01:00"`} {
		body := `{"type":"add_numbers","payload":{"a":20,"b":22},"scheduled_at":` + invalid + `}`
		requestJSON[map[string]string](t, "POST", base+"/jobs", body, "", 400)
	}
	if j.ScheduledAt == nil || !j.ScheduledAt.Equal(future) || !j.NextRunAt.Equal(future) {
		t.Fatalf("invalid schedule: %+v", j)
	}
	past := dbNow.Add(-time.Hour)
	pastID := submitAt(past, "past", 202)["job_id"]
	claimed := mustClaim(t, s, "first")
	if claimed.ID != pastID || claimed.ScheduledAt == nil || !claimed.ScheduledAt.Equal(past) {
		t.Fatalf("past job was not immediately eligible: %+v", claimed)
	}
	if err := s.Complete(ctx, claimed, json.RawMessage(`{"sum":42}`)); err != nil {
		t.Fatal(err)
	}
	claimed = claimEventually(t, s, "second")
	if claimed.ID != id || claimed.StartedAt == nil || claimed.StartedAt.Before(future) {
		t.Fatalf("scheduled claim: %+v", claimed)
	}
	if err := s.Fail(ctx, claimed, "test retry"); err != nil {
		t.Fatal(err)
	}
	queued := owned(t, s, id)
	if queued.Status != "QUEUED" || !queued.ScheduledAt.Equal(future) || !queued.NextRunAt.After(future) {
		t.Fatalf("retry did not keep original schedule and independent eligibility: %+v", queued)
	}
	cancelled := requestJSON[jobs.Job](t, "POST", base+"/jobs/"+id+"/cancel", "", "", 200)
	if cancelled.Status != "CANCELLED" || cancelled.AttemptCount != 1 || cancelled.LastError != nil ||
		cancelled.CancelRequestedAt == nil || cancelled.CompletedAt == nil || !cancelled.CompletedAt.Equal(*cancelled.CancelRequestedAt) {
		t.Fatalf("cancelled retry: %+v", cancelled)
	}
	if replay := submitAt(offset, "scheduled", 202); replay["job_id"] != id || replay["status"] != "CANCELLED" {
		t.Fatalf("cancelled replay: %+v", replay)
	}
	history := requestJSON[map[string][]jobs.Attempt](t, "GET", base+"/jobs/"+id+"/attempts", "", "", 200)["attempts"]
	if len(history) != 1 || history[0].Status != "FAILED" || history[0].LastError == nil || *history[0].LastError != "test retry" {
		t.Fatalf("history: %+v", history)
	}

	type page struct {
		Jobs []jobs.Job
		Next string `json:"next_cursor"`
	}
	query := base + "/jobs?limit=1&q=add_&type=add_numbers"
	first := requestJSON[page](t, "GET", query, "", "", 200)
	if len(first.Jobs) != 1 || first.Jobs[0].ID != pastID || first.Next == "" {
		t.Fatalf("first page: %+v", first)
	}
	submitAt(future, "", 202)
	second := requestJSON[page](t, "GET", query+"&cursor="+url.QueryEscape(first.Next), "", "", 200)
	if len(second.Jobs) != 1 || second.Jobs[0].ID != id || second.Next != "" {
		t.Fatalf("second page: %+v", second)
	}
	empty := requestJSON[page](t, "GET", query+"&status=FAILED", "", "", 200)
	if empty.Jobs == nil || len(empty.Jobs) != 0 {
		t.Fatalf("empty page: %+v", empty)
	}
	for _, query := range []string{"limit=101", "limit=1&limit=2", "cursor=bad", "q=%00", "status=scheduled", "offset=1"} {
		requestJSON[map[string]string](t, "GET", base+"/jobs?"+query, "", "", 400)
	}
	requestJSON[map[string]string](t, "GET", base+"/jobs/missing/attempts", "", "", 404)
	requestJSON[map[string]string](t, "POST", base+"/jobs/missing/cancel", "", "", 404)
	requestJSON[map[string]string](t, "POST", base+"/jobs/"+pastID+"/cancel", "", "", 409)
	requestJSON[map[string]string](t, "POST", base+"/jobs/"+id+"/retry", "", "", 409)
	if _, err := pool.Exec(ctx, "INSERT INTO workers(id,heartbeat_at) VALUES ('old', clock_timestamp()-interval '31 seconds')"); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	workers := requestJSON[map[string][]jobs.WorkerStatus](t, "GET", base+"/workers", "", "", 200)["workers"]
	if len(workers) != 2 || workers[0].ID != "new" || workers[0].Status != "ONLINE" || workers[1].ID != "old" || workers[1].Status != "STALE" {
		t.Fatalf("worker status: %+v", workers)
	}
}

func TestCancellationSerializesWithClaimAndCompletion(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	for range 12 {
		j := mustSubmit(t, s, "add_numbers", 2, 300)
		start := make(chan struct{})
		type claimResult struct {
			job jobs.Job
			err error
		}
		claimed := make(chan claimResult, 1)
		go func() {
			<-start
			job, err := s.Claim(ctx, "racing-worker")
			claimed <- claimResult{job, err}
		}()
		close(start)
		cancelled, err := s.Cancel(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		out := <-claimed
		switch cancelled.Status {
		case "CANCELLED":
			if !errors.Is(out.err, jobs.ErrNotFound) || cancelled.AttemptCount != 0 {
				t.Fatalf("claim after queued cancellation: %+v / %+v", cancelled, out)
			}
		case "RUNNING":
			if out.err != nil || out.job.ID != j.ID || cancelled.CancelRequestedAt == nil {
				t.Fatalf("claim before cancellation: %+v / %+v", cancelled, out)
			}
			if err := s.Complete(ctx, out.job, json.RawMessage(`{"sum":42}`)); err != nil {
				t.Fatal(err)
			}
			assertCancelledAttempt(t, s, j.ID)
		default:
			t.Fatalf("cancel returned unexpected state: %+v", cancelled)
		}

		j = mustSubmit(t, s, "add_numbers", 2, 300)
		owned := mustClaim(t, s, "racing-worker")
		start = make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-start
			done <- s.Complete(ctx, owned, json.RawMessage(`{"sum":42}`))
		}()
		close(start)
		pending, err := s.Cancel(ctx, j.ID)
		if completeErr := <-done; completeErr != nil {
			t.Fatal(completeErr)
		}
		if err == nil && pending.Status == "RUNNING" {
			assertCancelledAttempt(t, s, j.ID)
		} else if !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("invalid completion/cancel race: %+v %v", pending, err)
		} else {
			await(t, s, j.ID, "COMPLETED")
		}
	}
}

func assertCancelledAttempt(t *testing.T, s *store.Store, id string) {
	t.Helper()
	j := await(t, s, id, "CANCELLED")
	a, err := s.Attempts(context.Background(), id)
	if err != nil || len(a) != 1 || a[0].Status != "CANCELLED" || a[0].CompletedAt == nil ||
		j.CancelRequestedAt == nil || j.CompletedAt.Before(*j.CancelRequestedAt) ||
		!a[0].CompletedAt.Equal(*j.CompletedAt) || a[0].Result != nil || a[0].LastError != nil ||
		j.LeaseOwner != "" || j.Result != nil || j.LastError != nil {
		t.Fatalf("invalid cancellation: job=%+v attempts=%+v error=%v", j, a, err)
	}
}

func TestCancellationSignalsHandlerAndRecoversNoncooperativeAttempt(t *testing.T) {
	for _, cooperates := range []bool{true, false} {
		t.Run(fmt.Sprintf("cooperative=%v", cooperates), func(t *testing.T) {
			pool, _ := database(t)
			lease := 450 * time.Millisecond
			s := store.NewWithLease(pool, lease)
			ctx := context.Background()
			started, signal, release := make(chan struct{}), make(chan error, 1), make(chan struct{})
			j := mustSubmit(t, s, "test", 3, 30)
			stop := runTestWorker(t, s, lease, functionHandler(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
				close(started)
				<-ctx.Done()
				signal <- context.Cause(ctx)
				if !cooperates {
					<-release
				}
				return json.RawMessage(`{"late":true}`), nil
			}))
			defer stop()
			defer close(release)
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not start")
			}
			server := httptest.NewServer(api.New(s, tasks.DefaultRegistry(), logger()))
			defer server.Close()
			cancelURL := server.URL + "/api/v1/jobs/" + j.ID + "/cancel"
			pending := requestJSON[jobs.Job](t, "POST", cancelURL, "", "", 202)
			if pending.Status != "RUNNING" || pending.CancelRequestedAt == nil {
				t.Fatalf("expected racing attempt to be marked: %+v", pending)
			}
			select {
			case err := <-signal:
				if !errors.Is(err, jobs.ErrCancellationRequested) {
					t.Fatalf("unexpected context cause: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("running handler did not receive cancellation")
			}
			assertCancelledAttempt(t, s, j.ID)
			repeat := requestJSON[jobs.Job](t, "POST", cancelURL, "", "", 200)
			if !repeat.CancelRequestedAt.Equal(*pending.CancelRequestedAt) {
				t.Fatalf("repeat changed request timestamp: %+v", repeat)
			}
			requireStale(t, s.Complete(ctx, pending, json.RawMessage(`{"stale":true}`)))
			requireStale(t, s.Renew(ctx, pending))
			_, err := s.Retry(ctx, j.ID)
			requireStale(t, err)
		})
	}
}

func TestCancellationRacesWithExpiredLeaseRecovery(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	for range 12 {
		j := mustSubmit(t, s, "add_numbers", 1, 300)
		old := mustClaim(t, s, "old-owner")
		expire(t, pool, j.ID)
		requireStale(t, s.Fail(ctx, old, "late timeout"))
		start, recovered := make(chan struct{}), make(chan error, 1)
		go func() {
			<-start
			recovered <- s.Recover(ctx)
		}()
		close(start)
		reply, err := s.Cancel(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := <-recovered; err != nil {
			t.Fatal(err)
		}
		switch reply.Status {
		case "RUNNING":
			// The racing pass may have skipped the row while cancellation held its lock.
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			assertCancelledAttempt(t, s, j.ID)
		case "CANCELLED":
			j = await(t, s, j.ID, "CANCELLED")
			attempts, err := s.Attempts(ctx, j.ID)
			if err != nil || len(attempts) != 1 || attempts[0].Status != "FAILED" ||
				attempts[0].LastError == nil || *attempts[0].LastError != "lease expired" {
				t.Fatalf("recovery before cancellation should preserve failure: %+v %v", attempts, err)
			}
		default:
			t.Fatalf("unexpected cancellation status: %+v", reply)
		}
		requireStale(t, s.Complete(ctx, old, json.RawMessage(`{"sum":42}`)))
		if _, err := s.Claim(ctx, "replacement"); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("cancelled job reclaimed: %v", err)
		}
	}
}

func TestCancellationWhileTimedOutHandlerIsReturning(t *testing.T) {
	pool, _ := database(t)
	s := store.New(pool)
	ctx := context.Background()
	j := mustSubmit(t, s, "test", 1, 1)
	timedOut, release := make(chan error, 1), make(chan struct{})
	stop := runTestWorker(t, s, store.DefaultLease, functionHandler(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		timedOut <- ctx.Err()
		<-release
		return nil, ctx.Err()
	}))
	defer stop()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	select {
	case err := <-timedOut:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler timed out without reporting deadline")
	}
	pending, err := s.Cancel(ctx, j.ID)
	if err != nil || pending.Status != "RUNNING" || pending.CancelRequestedAt == nil {
		t.Fatalf("cancel while timed-out handler is running: %+v %v", pending, err)
	}
	close(release)
	assertCancelledAttempt(t, s, j.ID)
}
