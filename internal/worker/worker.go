package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/tasks"
)

type Store interface {
	Claim(context.Context) (jobs.Job, error)
	Complete(context.Context, string, json.RawMessage) error
	Fail(context.Context, string, string) error
}
type Worker struct {
	Store        Store
	Registry     tasks.Registry
	Log          *slog.Logger
	PollInterval time.Duration
}

// Run stops claiming when ctx is cancelled. An in-flight handler uses a separate
// context so a normal shutdown can finish it; the executable bounds shutdown.
func (w Worker) Run(ctx context.Context) error {
	if w.PollInterval <= 0 {
		return errors.New("poll interval must be positive")
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		job, err := w.Store.Claim(claimCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !errors.Is(err, jobs.ErrNotFound) {
				w.Log.Error("claim failed", "error", err)
			}
		} else {
			if err := w.execute(context.Background(), job); err != nil {
				return err
			}
			continue
		}
		timer := time.NewTimer(w.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
func (w Worker) execute(ctx context.Context, j jobs.Job) error {
	w.Log.Info("job started", "job_id", j.ID, "task_type", j.TaskType)
	h, ok := w.Registry[j.TaskType]
	var result json.RawMessage
	var err error
	if !ok {
		err = fmt.Errorf("unknown task type %q", j.TaskType)
	} else {
		result, err = invoke(ctx, h, j.Payload)
	}
	if err == nil && !json.Valid(result) {
		err = errors.New("handler returned invalid JSON")
	}
	persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var saveErr error
	if err != nil {
		saveErr = w.Store.Fail(persistCtx, j.ID, err.Error())
		w.Log.Error("job failed", "job_id", j.ID, "error", err)
	} else {
		saveErr = w.Store.Complete(persistCtx, j.ID, result)
		if saveErr == nil {
			w.Log.Info("job completed", "job_id", j.ID)
		}
	}
	if saveErr != nil {
		return fmt.Errorf("persist outcome for %s (job may remain RUNNING; handler will not be rerun): %w", j.ID, saveErr)
	}
	return nil
}
func invoke(ctx context.Context, h tasks.Handler, payload json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return h.Execute(ctx, payload)
}
