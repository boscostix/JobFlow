package tasks

import (
	"context"
	"encoding/json"
	"testing"
)

func TestAddNumbers(t *testing.T) {
	h := AddNumbers{}
	for _, raw := range []string{`{"a":null,"A":1,"b":2}`, `{"a":1,"A":2,"b":3}`, `{"A":1,"b":2}`, `{"a":1,"a":2,"b":3}`, `null`, `{}`, `{"a":1}`, `{"a":1,"b":null}`, `{"a":1.2,"b":2}`, `{"a":1,"b":2,"c":3}`, `{"a":1,"b":2} {}`, `{"a":9223372036854775808,"b":0}`} {
		if err := h.Validate(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	out, err := h.Execute(context.Background(), json.RawMessage(`{"a":20,"b":22}`))
	if err != nil || string(out) != `{"sum":42}` {
		t.Fatalf("result=%s err=%v", out, err)
	}
	for _, raw := range []string{`{"a":9223372036854775807,"b":1}`, `{"a":-9223372036854775808,"b":-1}`} {
		if _, err := h.Execute(context.Background(), json.RawMessage(raw)); err == nil {
			t.Errorf("overflow accepted: %s", raw)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Execute(ctx, json.RawMessage(`{"a":1,"b":2}`)); err == nil {
		t.Fatal("ignored cancellation")
	}
}
