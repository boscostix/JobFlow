package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"time"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/store/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultLease = 10 * time.Second

type Store struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	lease   time.Duration
}

func New(pool *pgxpool.Pool) *Store { return NewWithLease(pool, DefaultLease) }
func NewWithLease(pool *pgxpool.Pool, lease time.Duration) *Store {
	return &Store{pool, db.New(pool), lease}
}
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func text(s string) pgtype.Text                 { return pgtype.Text{String: s, Valid: true} }
func (s *Store) Create(ctx context.Context, task string, payload json.RawMessage) (jobs.Job, error) {
	return s.Submit(ctx, task, payload, jobs.Options{TimeoutSeconds: 300})
}
func (s *Store) Submit(ctx context.Context, task string, payload json.RawMessage, o jobs.Options) (jobs.Job, error) {
	if err := o.Validate(); err != nil {
		return jobs.Job{}, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return jobs.Job{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, err
	}
	defer tx.Rollback(context.Background())
	q := s.queries.WithTx(tx)
	j, err := q.CreateJob(ctx, db.CreateJobParams{ID: "job_" + hex.EncodeToString(id[:]), TaskType: task, Payload: payload, MaxRetries: o.MaxRetries, TimeoutSeconds: o.TimeoutSeconds})
	if err != nil {
		return jobs.Job{}, err
	}
	if o.IdempotencyKey != "" {
		n, err := q.InsertIdempotencyKey(ctx, db.InsertIdempotencyKeyParams{Key: o.IdempotencyKey, JobID: j.ID})
		if err != nil {
			return jobs.Job{}, err
		}
		if n == 0 {
			original, err := q.GetIdempotentJob(ctx, o.IdempotencyKey)
			if err != nil {
				return jobs.Job{}, err
			}
			same, err := q.EquivalentSubmission(ctx, db.EquivalentSubmissionParams{ID: original.ID, TaskType: task, Column3: payload, MaxRetries: o.MaxRetries, TimeoutSeconds: o.TimeoutSeconds})
			if err != nil {
				return jobs.Job{}, err
			}
			if !same.Valid || !same.Bool {
				return jobs.Job{}, jobs.ErrIdempotencyConflict
			}
			// Rolling back removes the unused candidate, never the original key/job.
			return convert(original), nil
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, err
	}
	return convert(j), nil
}
func (s *Store) Get(ctx context.Context, id string) (jobs.Job, error) {
	j, err := s.queries.GetJob(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return convert(j), err
}
func (s *Store) Claim(ctx context.Context, workerID string) (jobs.Job, error) {
	if workerID == "" || s.lease < 3*time.Millisecond {
		return jobs.Job{}, fmt.Errorf("worker ID and lease >= 3ms required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, err
	}
	defer tx.Rollback(context.Background())
	q := s.queries.WithTx(tx)
	j, err := q.ClaimJob(ctx, db.ClaimJobParams{LeaseOwner: text(workerID), LeaseMs: s.lease.Milliseconds()})
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, err
	}
	n, err := q.CreateAttempt(ctx, j.ID)
	if err != nil {
		return jobs.Job{}, err
	}
	if n != 1 {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, err
	}
	return convert(j), nil
}

// RetryDelay uses equal jitter in [exponential/2, exponential], capped at 60s.
// The failed attempt number is relative to the current manual retry cycle.
func RetryDelay(attempt int32) time.Duration {
	ceiling := time.Second
	for i := int32(1); i < attempt && ceiling < 60*time.Second; i++ {
		ceiling *= 2
	}
	if ceiling > 60*time.Second {
		ceiling = 60 * time.Second
	}
	half := ceiling / 2
	return half + time.Duration(mathrand.Int64N(int64(half)+1))
}
func (s *Store) Complete(ctx context.Context, j jobs.Job, result json.RawMessage) error {
	if !json.Valid(result) {
		return fmt.Errorf("handler result must be valid JSON")
	}
	return s.finish(ctx, j, result, "", false)
}
func (s *Store) Fail(ctx context.Context, j jobs.Job, message string) error {
	return s.finish(ctx, j, nil, message, false)
}
func (s *Store) finish(ctx context.Context, owned jobs.Job, result json.RawMessage, message string, expired bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	q := s.queries.WithTx(tx)
	j, err := q.LockOwned(ctx, owned.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ErrInvalidTransition
	}
	if err != nil {
		return err
	}
	if j.Status != "RUNNING" || j.LeaseOwner.String != owned.LeaseOwner || j.FencingToken != owned.FencingToken {
		return jobs.ErrInvalidTransition
	}
	if err = s.finishLocked(ctx, q, j, result, message, expired); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) finishLocked(ctx context.Context, q *db.Queries, j db.Job, result json.RawMessage, message string, expired bool) error {
	status, attemptStatus := "COMPLETED", "COMPLETED"
	var lastError pgtype.Text
	var delay time.Duration
	if result == nil {
		lastError = text(message)
		status, attemptStatus = "DEAD_LETTER", "FAILED"
		if j.AttemptCount-j.RetryStart <= j.MaxRetries {
			status = "QUEUED"
			delay = RetryDelay(j.AttemptCount - j.RetryStart)
		}
	}
	n, err := q.FinishJob(ctx, db.FinishJobParams{ID: j.ID, Owner: j.LeaseOwner, Token: j.FencingToken, NewStatus: status, Result: result, LastError: lastError, DelayMs: delay.Milliseconds(), Expired: expired})
	if err != nil {
		return err
	}
	if n != 1 {
		return jobs.ErrInvalidTransition
	}
	n, err = q.FinishAttempt(ctx, db.FinishAttemptParams{JobID: j.ID, FencingToken: j.FencingToken, Status: attemptStatus, Result: result, LastError: lastError})
	if err != nil {
		return err
	}
	if n != 1 {
		return jobs.ErrInvalidTransition
	}
	return nil
}

// Lock first so lease validity is evaluated after any row-lock wait. An UPDATE
// predicate alone can be evaluated before waiting if the lock holder never
// changes the row, allowing an expired lease to be revived.
func (s *Store) Renew(ctx context.Context, j jobs.Job) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	q := s.queries.WithTx(tx)
	if _, err = q.LockOwned(ctx, j.ID); errors.Is(err, pgx.ErrNoRows) {
		return jobs.ErrInvalidTransition
	} else if err != nil {
		return err
	}
	n, err := q.RenewLease(ctx, db.RenewLeaseParams{ID: j.ID, Owner: text(j.LeaseOwner), Token: j.FencingToken, LeaseMs: s.lease.Milliseconds()})
	if err != nil {
		return err
	}
	if n != 1 {
		return jobs.ErrInvalidTransition
	}
	return tx.Commit(ctx)
}

// Recover processes a bounded batch; any live worker can recover any expired job.
// Worker heartbeats are diagnostic, never an ownership authority.
func (s *Store) Recover(ctx context.Context) error {
	for range 100 {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		q := s.queries.WithTx(tx)
		j, err := q.LockExpired(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			tx.Rollback(context.Background())
			return nil
		}
		if err == nil {
			err = s.finishLocked(ctx, q, j, nil, "lease expired", true)
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		tx.Rollback(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) Heartbeat(ctx context.Context, id string) error { return s.queries.Heartbeat(ctx, id) }
func (s *Store) Retry(ctx context.Context, id string) (jobs.Job, error) {
	j, err := s.queries.ManualRetry(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, e := s.Get(ctx, id); e != nil {
			return jobs.Job{}, e
		}
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	return convert(j), err
}
func convert(j db.Job) jobs.Job {
	out := jobs.Job{ID: j.ID, TaskType: j.TaskType, Payload: j.Payload, Status: j.Status, CreatedAt: j.CreatedAt.Time, Result: j.Result, AttemptCount: j.AttemptCount, MaxRetries: j.MaxRetries, TimeoutSeconds: j.TimeoutSeconds, NextRunAt: j.NextRunAt.Time, LeaseOwner: j.LeaseOwner.String, FencingToken: j.FencingToken}
	if j.StartedAt.Valid {
		t := j.StartedAt.Time
		out.StartedAt = &t
	}
	if j.CompletedAt.Valid {
		t := j.CompletedAt.Time
		out.CompletedAt = &t
	}
	if j.LeaseExpiresAt.Valid {
		t := j.LeaseExpiresAt.Time
		out.LeaseExpiresAt = &t
	}
	if j.LastError.Valid {
		v := j.LastError.String
		out.LastError = &v
	}
	return out
}
