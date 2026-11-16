package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/store/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func New(pool *pgxpool.Pool) *Store             { return &Store{pool: pool, queries: db.New(pool)} }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Create(ctx context.Context, task string, payload json.RawMessage) (jobs.Job, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return jobs.Job{}, err
	}
	j, err := s.queries.CreateJob(ctx, db.CreateJobParams{ID: "job_" + hex.EncodeToString(id[:]), TaskType: task, Payload: payload})
	return convert(j), err
}
func (s *Store) Get(ctx context.Context, id string) (jobs.Job, error) {
	j, err := s.queries.GetJob(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return convert(j), err
}

// Claim preserves SKIP LOCKED claiming and commits its attempt record in the
// same transaction. Execution starts only after commit, outside queue row locks.
func (s *Store) Claim(ctx context.Context, workerID string) (jobs.Job, error) {
	if workerID == "" {
		return jobs.Job{}, fmt.Errorf("worker ID is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, err
	}
	defer tx.Rollback(context.Background())
	q := s.queries.WithTx(tx)
	j, err := q.ClaimJob(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, err
	}
	n, err := q.CreateAttempt(ctx, db.CreateAttemptParams{ID: j.ID, WorkerID: workerID})
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

// finish commits the job and attempt outcome together or rolls back both.
func (s *Store) finish(ctx context.Context, id string, update func(*db.Queries) (int64, error)) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	q := s.queries.WithTx(tx)
	n, err := update(q)
	if err != nil {
		return err
	}
	if n != 1 {
		return jobs.ErrInvalidTransition
	}
	n, err = q.FinishAttempt(ctx, id)
	if err != nil {
		return err
	}
	if n != 1 {
		return jobs.ErrInvalidTransition
	}
	return tx.Commit(ctx)
}
func (s *Store) Complete(ctx context.Context, id string, result json.RawMessage) error {
	if !json.Valid(result) {
		return fmt.Errorf("handler result must be valid JSON")
	}
	return s.finish(ctx, id, func(q *db.Queries) (int64, error) {
		return q.CompleteJob(ctx, db.CompleteJobParams{ID: id, Result: result})
	})
}
func (s *Store) Fail(ctx context.Context, id string, message string) error {
	return s.finish(ctx, id, func(q *db.Queries) (int64, error) {
		return q.FailJob(ctx, db.FailJobParams{ID: id, LastError: pgtype.Text{String: message, Valid: true}})
	})
}
func convert(j db.Job) jobs.Job {
	out := jobs.Job{ID: j.ID, TaskType: j.TaskType, Payload: j.Payload, Status: j.Status, CreatedAt: j.CreatedAt.Time, Result: j.Result}
	if j.StartedAt.Valid {
		t := j.StartedAt.Time
		out.StartedAt = &t
	}
	if j.CompletedAt.Valid {
		t := j.CompletedAt.Time
		out.CompletedAt = &t
	}
	if j.LastError.Valid {
		v := j.LastError.String
		out.LastError = &v
	}
	return out
}
