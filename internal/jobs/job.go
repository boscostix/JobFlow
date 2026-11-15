package jobs

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrNotFound = errors.New("job not found")
var ErrInvalidTransition = errors.New("invalid job transition")

type Job struct {
	ID          string          `json:"id"`
	TaskType    string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
}
