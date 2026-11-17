package jobs

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrNotFound = errors.New("job not found")
var ErrCancellationRequested = errors.New("job cancellation requested")
var ErrInvalidTransition = errors.New("invalid job transition")

var ErrIdempotencyConflict = errors.New("idempotency key conflicts with original submission")

type Options struct {
	MaxRetries     int32
	TimeoutSeconds int32
	IdempotencyKey string
	ScheduledAt    *time.Time
}

func (o Options) Validate() error {
	if o.MaxRetries < 0 || o.MaxRetries > 100 || o.TimeoutSeconds < 1 || o.TimeoutSeconds > 86400 {
		return errors.New("max_retries must be 0–100 and timeout_seconds must be 1–86400")
	}
	if o.ScheduledAt != nil {
		t := o.ScheduledAt.UTC()
		if t.Year() < 1 || t.Year() > 9999 || t.Nanosecond()%1000 != 0 {
			return errors.New("scheduled_at must be within UTC years 0001–9999 with microsecond precision")
		}
	}
	return nil
}

type Job struct {
	ScheduledAt       *time.Time      `json:"scheduled_at,omitempty"`
	CancelRequestedAt *time.Time      `json:"cancel_requested_at,omitempty"`
	AttemptCount      int32           `json:"attempt_count"`
	MaxRetries        int32           `json:"max_retries"`
	TimeoutSeconds    int32           `json:"timeout_seconds"`
	NextRunAt         time.Time       `json:"next_run_at"`
	LeaseOwner        string          `json:"lease_owner,omitempty"`
	LeaseExpiresAt    *time.Time      `json:"lease_expires_at,omitempty"`
	FencingToken      int64           `json:"fencing_token"`
	ID                string          `json:"id"`
	TaskType          string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Status            string          `json:"status"`
	CreatedAt         time.Time       `json:"created_at"`
	StartedAt         *time.Time      `json:"started_at,omitempty"`
	CompletedAt       *time.Time      `json:"completed_at,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
	LastError         *string         `json:"last_error,omitempty"`
}

type ListOptions struct {
	Status, TaskType, Search string
	Before                   time.Time
	BeforeID                 string
	Limit                    int32
}

type Attempt struct {
	JobID        string          `json:"job_id"`
	Number       int32           `json:"attempt_number"`
	WorkerID     string          `json:"worker_id"`
	FencingToken int64           `json:"fencing_token"`
	Status       string          `json:"status"`
	StartedAt    time.Time       `json:"started_at"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	LastError    *string         `json:"last_error,omitempty"`
}

type WorkerStatus struct {
	ID          string    `json:"id"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	Status      string    `json:"status"`
}
