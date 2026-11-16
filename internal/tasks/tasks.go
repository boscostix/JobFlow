// Package tasks defines the handlers supported by both the API and worker.
package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

type Handler interface {
	Validate(json.RawMessage) error
	Execute(context.Context, json.RawMessage) (json.RawMessage, error)
}

type Registry map[string]Handler

func DefaultRegistry() Registry { return Registry{"add_numbers": AddNumbers{}} }

// Decode accepts one object with exact field names and no duplicate keys.
// Exact names keep validation consistent with PostgreSQL JSONB normalization.
func Decode(raw []byte, dst any, allowedFields ...string) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("expected a JSON object")
	}
	seen := make(map[string]bool)
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("expected a field name")
		}
		allowed := false
		for _, name := range allowedFields {
			if key == name {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("unknown field %q", key)
		}
		if seen[key] {
			return fmt.Errorf("duplicate field %q", key)
		}
		seen[key] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return json.Unmarshal(raw, dst)
}

type AddNumbers struct{}
type numbers struct {
	A *int64 `json:"a"`
	B *int64 `json:"b"`
}

func parse(raw json.RawMessage) (numbers, error) {
	var n numbers
	if err := Decode(raw, &n, "a", "b"); err != nil {
		return n, fmt.Errorf("payload requires integer a and b: %w", err)
	}
	if n.A == nil || n.B == nil {
		return n, errors.New("payload requires integer a and b")
	}
	return n, nil
}
func (AddNumbers) Validate(raw json.RawMessage) error { _, err := parse(raw); return err }
func (AddNumbers) Execute(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n, err := parse(raw)
	if err != nil {
		return nil, err
	}
	a, b := *n.A, *n.B
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return nil, errors.New("integer addition overflow")
	}
	return json.Marshal(struct {
		Sum int64 `json:"sum"`
	}{a + b})
}

// Execution exposes the stable job ID for handler-side effect deduplication.
// The fencing token only protects external effects if the external system enforces it.
type Execution struct {
	JobID        string
	FencingToken int64
}
type executionKey struct{}

func WithExecution(ctx context.Context, e Execution) context.Context {
	return context.WithValue(ctx, executionKey{}, e)
}
func ExecutionFromContext(ctx context.Context) (Execution, bool) {
	e, ok := ctx.Value(executionKey{}).(Execution)
	return e, ok
}
