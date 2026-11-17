package store

import (
	"context"
	"errors"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/store/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) Cancel(ctx context.Context, id string) (jobs.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, err
	}
	defer tx.Rollback(context.Background())
	q := s.queries.WithTx(tx)
	j, err := q.LockOwned(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, err
	}
	if j.CancelRequestedAt.Valid {
		return convert(j), nil
	}
	if j.Status != "QUEUED" && j.Status != "RUNNING" {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	j, err = q.RequestCancellation(ctx, id)
	if err != nil {
		return jobs.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, err
	}
	return convert(j), nil
}

func (s *Store) List(ctx context.Context, options jobs.ListOptions) ([]jobs.Job, error) {
	rows, err := s.queries.ListJobs(ctx, db.ListJobsParams{
		Status: options.Status, TaskType: options.TaskType, Search: options.Search,
		BeforeTime: pgtype.Timestamptz{Time: options.Before, Valid: options.BeforeID != ""},
		BeforeID:   options.BeforeID, PageSize: options.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]jobs.Job, 0, len(rows))
	for _, row := range rows {
		out = append(out, convert(row))
	}
	return out, nil
}

func (s *Store) Attempts(ctx context.Context, id string) ([]jobs.Attempt, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return nil, err
	}
	rows, err := s.queries.ListAttempts(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]jobs.Attempt, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobs.Attempt{
			JobID: row.JobID, Number: row.AttemptNumber, WorkerID: row.WorkerID,
			FencingToken: row.FencingToken, Status: row.Status, StartedAt: row.StartedAt.Time.UTC(),
			CompletedAt: optionalTime(row.CompletedAt), Result: row.Result, LastError: optionalText(row.LastError),
		})
	}
	return out, nil
}

func (s *Store) Workers(ctx context.Context) ([]jobs.WorkerStatus, error) {
	rows, err := s.queries.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]jobs.WorkerStatus, 0, len(rows))
	for _, row := range rows {
		out = append(out, jobs.WorkerStatus{
			ID: row.ID, StartedAt: row.StartedAt.Time.UTC(),
			HeartbeatAt: row.HeartbeatAt.Time.UTC(), Status: row.Status,
		})
	}
	return out, nil
}
