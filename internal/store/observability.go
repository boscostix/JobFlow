package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/boscostix/JobFlow/internal/metrics"
	"github.com/boscostix/JobFlow/internal/store/db"
)

// WithObservability returns a copy configured to report committed transitions.
func (s *Store) WithObservability(log *slog.Logger, workerMetrics *metrics.Worker) *Store {
	configured := *s
	configured.log = log
	configured.metrics = workerMetrics
	return &configured
}

func (s *Store) QueueDepth(ctx context.Context) (eligible, delayed int64, err error) {
	depth, err := s.queries.QueueDepth(ctx)
	return depth.Eligible, depth.Delayed, err
}

func (s *Store) finished(j db.Job, status string, delay time.Duration, expired bool) {
	if s.metrics != nil {
		s.metrics.Finish(status, expired)
	}
	if s.log != nil {
		event := "attempt outcome committed"
		if expired {
			event = "expired lease recovered"
		}
		s.jobLogger(j).Info(event, "job_status", status, "retry_delay_ms", delay.Milliseconds())
	}
}

func (s *Store) jobLogger(j db.Job) *slog.Logger {
	log := s.log.With("job_id", j.ID, "task_type", j.TaskType)
	if j.AttemptCount > 0 {
		log = log.With("attempt_number", j.AttemptCount, "fencing_token", j.FencingToken)
	}
	if j.LeaseOwner.Valid {
		log = log.With("worker_id", j.LeaseOwner.String)
	}
	return log
}
