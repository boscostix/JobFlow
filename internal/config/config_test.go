package config

import "testing"

func TestConcurrency(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{{"", 4}, {"1", 1}, {"256", 256}, {"0", 0}, {"-1", 0}, {"257", 0}, {"four", 0}} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("WORKER_CONCURRENCY", tc.value)
			n, err := Concurrency()
			if tc.want == 0 {
				if err == nil {
					t.Fatal("invalid concurrency accepted")
				}
				return
			}
			if err != nil || n != tc.want {
				t.Fatalf("n=%d err=%v", n, err)
			}
		})
	}
}
