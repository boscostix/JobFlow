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

// Claim uses one atomic SQL statement. Its implicit transaction is committed
// before returning the job, so handler execution holds no queue row lock.
func (s *Store) Claim(ctx context.Context) (jobs.Job, error) {
	j, err := s.queries.ClaimJob(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return convert(j), err
}
func (s *Store) Complete(ctx context.Context, id string, result json.RawMessage) error {
	if !json.Valid(result) {
		return fmt.Errorf("handler result must be valid JSON")
	}
	n, err := s.queries.CompleteJob(ctx, db.CompleteJobParams{ID: id, Result: result})
	if err != nil {
		return err
	}
	if n != 1 {
		return jobs.ErrInvalidTransition
	}
	return nil
}
func (s *Store) Fail(ctx context.Context, id string, message string) error {
	n, err := s.queries.FailJob(ctx, db.FailJobParams{ID: id, LastError: pgtype.Text{String: message, Valid: true}})
	if err != nil {
		return err
	}
	if n != 1 {
		return jobs.ErrInvalidTransition
	}
	return nil
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
