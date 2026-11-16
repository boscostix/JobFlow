package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/boscostix/JobFlow/internal/jobs"
	"github.com/boscostix/JobFlow/internal/tasks"
)

type Store interface {
	Claim(context.Context, string) (jobs.Job, error)
	Complete(context.Context, jobs.Job, json.RawMessage) error
	Fail(context.Context, jobs.Job, string) error

	Renew(context.Context, jobs.Job) error
	Recover(context.Context) error
	Heartbeat(context.Context, string) error
}
type Worker struct {
	LeaseDuration time.Duration
	ID            string
	Concurrency   int
	Store         Store
	Registry      tasks.Registry
	Log           *slog.Logger
	PollInterval  time.Duration
}

// Run starts a fixed number of slots. Each slot claims only when it can execute,
// and retains its slot until the outcome is persisted. Shutdown (or an outcome
// write failure) stops new claims and drains all in-flight work. The executable
// bounds signal-driven shutdown; handler contexts remain live during draining.
func (w Worker) Run(ctx context.Context) error {
	if w.PollInterval <= 0 {
		return errors.New("poll interval must be positive")
	}
	if w.Concurrency < 1 || w.Concurrency > 256 {
		return errors.New("concurrency must be between 1 and 256")
	}
	if w.ID == "" {
		return errors.New("worker ID is required")
	}
	if w.LeaseDuration == 0 {
		w.LeaseDuration = 10 * time.Second
	}
	if w.LeaseDuration < 3*time.Millisecond {
		return errors.New("lease must be at least 3ms")
	}
	// Keep maintenance alive while in-flight handlers drain after shutdown.
	maintenanceCtx, endMaintenance := context.WithCancel(context.Background())
	maintenanceDone := make(chan struct{})
	go func() { defer close(maintenanceDone); w.maintain(maintenanceCtx) }()
	defer func() { endMaintenance(); <-maintenanceDone }()
	claimCtx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for range w.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.runSlot(claimCtx); err != nil {
				once.Do(func() { firstErr = err; stop() })
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (w Worker) runSlot(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		job, err := w.Store.Claim(claimCtx, w.ID)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !errors.Is(err, jobs.ErrNotFound) {
				w.Log.Error("claim failed", "worker_id", w.ID, "error", err)
			}
		} else {
			// A claim committed just before cancellation still needs its outcome saved.
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
func (w Worker) maintain(ctx context.Context) {
	s := w.Store
	ticker := time.NewTicker(w.LeaseDuration / 3)
	defer ticker.Stop()
	for {
		opCtx, cancel := context.WithTimeout(ctx, w.LeaseDuration/3)
		if err := s.Heartbeat(opCtx, w.ID); err != nil && ctx.Err() == nil {
			w.Log.Error("heartbeat failed", "error", err)
		}
		cancel()
		opCtx, cancel = context.WithTimeout(ctx, w.LeaseDuration/3)
		if err := s.Recover(opCtx); err != nil && ctx.Err() == nil {
			w.Log.Error("recovery failed", "error", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (w Worker) execute(ctx context.Context, j jobs.Job) error {
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}
	ctx, cancel := context.WithTimeout(tasks.WithExecution(ctx, tasks.Execution{JobID: j.ID, FencingToken: j.FencingToken}), timeout)
	defer cancel()
	// Execute synchronously: a noncooperative handler retains its slot. Once its
	// deadline passes renewal stops, allowing another process to recover the job.
	renewCtx, stopRenew := context.WithCancel(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		s := w.Store
		lease := w.LeaseDuration
		if lease == 0 {
			lease = 10 * time.Second
		}
		ticker := time.NewTicker(lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				opCtx, done := context.WithTimeout(renewCtx, lease/3)
				err := s.Renew(opCtx, j)
				done()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	w.Log.Info("job started", "worker_id", w.ID, "job_id", j.ID, "task_type", j.TaskType)
	h, ok := w.Registry[j.TaskType]
	var result json.RawMessage
	var err error
	if !ok {
		err = fmt.Errorf("unknown task type %q", j.TaskType)
	} else {
		result, err = invoke(ctx, h, j.Payload)
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	stopRenew()
	<-renewed
	if err == nil && !json.Valid(result) {
		err = errors.New("handler returned invalid JSON")
	}
	persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var saveErr error
	if err != nil {
		saveErr = w.Store.Fail(persistCtx, j, err.Error())
		w.Log.Error("job failed", "worker_id", w.ID, "job_id", j.ID, "error", err)
	} else {
		saveErr = w.Store.Complete(persistCtx, j, result)
		if saveErr == nil {
			w.Log.Info("job completed", "worker_id", w.ID, "job_id", j.ID)
		}
	}
	if errors.Is(saveErr, jobs.ErrInvalidTransition) {
		w.Log.Info("attempt no longer owns job", "job_id", j.ID, "fencing_token", j.FencingToken)
		return nil
	}
	if saveErr != nil {
		return fmt.Errorf("persist outcome for %s (lease recovery will resolve abandoned state): %w", j.ID, saveErr)
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
