//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/metrics"
	"github.com/boscostix/JobFlow/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func checkScrape(t *testing.T, handler http.Handler, expected ...string) string {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body := response.Body.String()
	if response.Code != http.StatusOK {
		t.Fatalf("scrape status %d: %s", response.Code, body)
	}
	for _, line := range expected {
		if !strings.Contains("\n"+body, "\n"+line+"\n") {
			t.Errorf("missing %q in scrape:\n%s", line, body)
		}
	}
	return body
}

func TestQueueMetricsUseEligibilityAtScrapeAndClaim(t *testing.T) {
	p, _ := database(t)
	ctx := context.Background()
	m := metrics.NewWorker()
	s := store.New(p).WithObservability(logger(), m)
	queue := metrics.QueueHandler(s.QueueDepth, logger())
	// Seed a job whose original hour-long schedule recently became eligible.
	const readyID = "job_ready"
	execSQL(t, p, `INSERT INTO jobs (id, task_type, payload, created_at, scheduled_at, next_run_at)
	VALUES ('job_ready','add_numbers','{"a":1,"b":2}', statement_timestamp()-interval '1 hour',
	statement_timestamp()-interval '5 seconds', statement_timestamp()-interval '5 seconds')`)
	future := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	delayed, err := s.Submit(ctx, "add_numbers", json.RawMessage(`{"a":1,"b":2}`), jobs.Options{TimeoutSeconds: 300, ScheduledAt: &future})
	if err != nil {
		t.Fatal(err)
	}
	checkScrape(t, queue, `jobflow_queue_depth{eligibility="eligible"} 1`, `jobflow_queue_depth{eligibility="delayed"} 1`)
	got := mustClaim(t, s, "claimant")
	if got.ID != readyID {
		t.Fatalf("claimed %s, expected %s", got.ID, readyID)
	}
	checkScrape(t, queue, `jobflow_queue_depth{eligibility="eligible"} 0`, `jobflow_queue_depth{eligibility="delayed"} 1`)
	if _, err := s.Cancel(ctx, delayed.ID); err != nil {
		t.Fatal(err)
	}
	checkScrape(t, queue, `jobflow_queue_depth{eligibility="eligible"} 0`, `jobflow_queue_depth{eligibility="delayed"} 0`)
	checkScrape(t, m.Handler(),
		`jobflow_queue_wait_seconds_count 1`,
		`jobflow_queue_wait_seconds_bucket{le="2.5"} 0`,
		`jobflow_queue_wait_seconds_bucket{le="60"} 1`,
		`jobflow_attempt_outcomes_total{outcome="cancelled"} 0`)
}

func TestTransientHandlerFailureRecordsRetryThenSuccess(t *testing.T) {
	p, _ := database(t)
	m := metrics.NewWorker()
	s := store.New(p).WithObservability(logger(), m)
	j := mustSubmit(t, s, "test", 1, 300)
	h := functionHandler(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		current, err := s.Get(ctx, j.ID)
		if err != nil {
			return nil, err
		}
		if current.AttemptCount == 1 {
			return nil, errors.New("transient test failure")
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	runTestWorker(t, s, store.DefaultLease, h)
	got := await(t, s, j.ID, "COMPLETED")
	history, err := s.Attempts(context.Background(), j.ID)
	if err != nil || len(history) != 2 || history[0].Status != "FAILED" || history[0].LastError == nil ||
		*history[0].LastError != "transient test failure" || history[0].CompletedAt == nil ||
		history[1].StartedAt.Sub(*history[0].CompletedAt) < 490*time.Millisecond ||
		history[1].Status != "COMPLETED" || history[1].FencingToken <= history[0].FencingToken ||
		got.AttemptCount != 2 || !bytes.Equal(got.Result, history[1].Result) {
		t.Fatalf("job %+v, history %+v, error %v", got, history, err)
	}
	// A scrape can briefly lag a database reader between commit and observation.
	deadline := time.Now().Add(time.Second)
	for {
		body := checkScrape(t, m.Handler())
		if strings.Contains(body, `jobflow_attempt_outcomes_total{outcome="completed"} 1`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completion metric did not arrive")
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkScrape(t, m.Handler(),
		`jobflow_queue_wait_seconds_count 2`,
		`jobflow_attempt_outcomes_total{outcome="failed"} 1`,
		`jobflow_automatic_retries_total{cause="failure"} 1`,
		`jobflow_lease_recoveries_total 0`)
}

func TestRecoverySignalsFollowCommitAndFencing(t *testing.T) {
	p, _ := database(t)
	ctx := context.Background()
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	ownerMetrics, recoveryMetrics := metrics.NewWorker(), metrics.NewWorker()
	owner := store.New(p).WithObservability(log, ownerMetrics)
	recoverer := store.New(p).WithObservability(log, recoveryMetrics)
	original := mustSubmit(t, owner, "add_numbers", 1, 300)
	old := mustClaim(t, owner, "old-owner")
	expire(t, p, original.ID)
	execSQL(t, p, `CREATE FUNCTION reject_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN RAISE EXCEPTION 'history unavailable'; END $$;
	CREATE TRIGGER reject_outcome BEFORE UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION reject_outcome();`)
	if err := recoverer.Recover(ctx); err == nil {
		t.Fatal("expected history update to roll back")
	}
	checkScrape(t, recoveryMetrics.Handler(),
		`jobflow_lease_recoveries_total 0`,
		`jobflow_attempt_outcomes_total{outcome="failed"} 0`,
		`jobflow_automatic_retries_total{cause="lease_expired"} 0`)
	if strings.Contains(logs.String(), "expired lease recovered") {
		t.Fatal("rollback logged as a committed recovery")
	}
	execSQL(t, p, "DROP TRIGGER reject_outcome ON job_attempts")
	if err := recoverer.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	next := claimEventually(t, recoverer, "new-owner")
	if err := recoverer.Complete(ctx, next, json.RawMessage(`{"sum": 3}`)); err != nil {
		t.Fatal(err)
	}
	requireStale(t, owner.Complete(ctx, old, json.RawMessage(`{"sum": 99}`)))
	checkScrape(t, ownerMetrics.Handler(), `jobflow_attempt_outcomes_total{outcome="completed"} 0`)
	checkScrape(t, recoveryMetrics.Handler(),
		`jobflow_queue_wait_seconds_count 1`,
		`jobflow_lease_recoveries_total 1`,
		`jobflow_attempt_outcomes_total{outcome="failed"} 1`,
		`jobflow_attempt_outcomes_total{outcome="completed"} 1`,
		`jobflow_automatic_retries_total{cause="lease_expired"} 1`)
	got := owned(t, owner, original.ID)
	if got.Status != "COMPLETED" || !bytes.Equal(got.Result, []byte(`{"sum": 3}`)) || got.FencingToken != next.FencingToken {
		t.Fatalf("stale outcome changed job: %+v", got)
	}
	var recovered, completed int
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		switch event["msg"] {
		case "expired lease recovered":
			recovered++
			checkAttemptLog(t, event, old, "QUEUED")
		case "attempt outcome committed":
			completed++
			checkAttemptLog(t, event, next, "COMPLETED")
		}
	}
	if recovered != 1 || completed != 1 {
		t.Fatal(logs.String())
	}
}

func execSQL(t *testing.T, p *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := p.Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}

func checkAttemptLog(t *testing.T, event map[string]any, attempt jobs.Job, status string) {
	t.Helper()
	if event["job_id"] != attempt.ID || event["worker_id"] != attempt.LeaseOwner ||
		event["attempt_number"] != float64(attempt.AttemptCount) ||
		event["fencing_token"] != float64(attempt.FencingToken) || event["job_status"] != status {
		t.Errorf("unexpected committed event: %+v", event)
	}
}

func TestCancelledAttemptReportsCommittedOutcome(t *testing.T) {
	p, _ := database(t)
	ctx := context.Background()
	var logs bytes.Buffer
	m := metrics.NewWorker()
	s := store.New(p).WithObservability(slog.New(slog.NewJSONHandler(&logs, nil)), m)
	j := mustSubmit(t, s, "add_numbers", 1, 300)
	attempt := mustClaim(t, s, "cancel-owner")
	for range 2 {
		if _, err := s.Cancel(ctx, j.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Complete(ctx, attempt, json.RawMessage(`{"sum":3}`)); err != nil {
		t.Fatal(err)
	}
	checkScrape(t, m.Handler(),
		`jobflow_attempt_outcomes_total{outcome="cancelled"} 1`,
		`jobflow_attempt_outcomes_total{outcome="completed"} 0`,
		`jobflow_automatic_retries_total{cause="failure"} 0`)
	var requests, outcomes int
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		switch event["msg"] {
		case "job cancellation recorded":
			requests++
			checkAttemptLog(t, event, attempt, "RUNNING")
		case "attempt outcome committed":
			outcomes++
			checkAttemptLog(t, event, attempt, "CANCELLED")
		}
	}
	if requests != 1 || outcomes != 1 {
		t.Fatal(logs.String())
	}
}
