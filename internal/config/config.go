package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func DatabaseURL() (string, error) {
	v := os.Getenv("DATABASE_URL")
	if v == "" {
		return "", fmt.Errorf("DATABASE_URL is required")
	}
	return v, nil
}
func Duration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return d, nil
}

// Concurrency bounds the number of execution slots allocated by each process.
func Concurrency() (int, error) {
	n, err := strconv.Atoi(Env("WORKER_CONCURRENCY", "4"))
	if err != nil || n < 1 || n > 256 {
		return 0, fmt.Errorf("WORKER_CONCURRENCY must be between 1 and 256")
	}
	return n, nil
}
