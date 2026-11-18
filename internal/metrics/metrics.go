package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Worker holds process-local observations of committed claims and outcomes.
type Worker struct {
	registry *prometheus.Registry
	wait     prometheus.Histogram
	outcomes *prometheus.CounterVec
	retries  *prometheus.CounterVec
	recovery prometheus.Counter
}

func NewWorker() *Worker {
	m := &Worker{
		registry: prometheus.NewRegistry(),
		wait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "jobflow_queue_wait_seconds",
			Help:    "Time from eligibility to claim for committed attempts.",
			Buckets: []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
		}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobflow_attempt_outcomes_total", Help: "Committed attempt outcomes by persisted attempt status.",
		}, []string{"outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobflow_automatic_retries_total", Help: "Committed automatic requeues by cause.",
		}, []string{"cause"}),
		recovery: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobflow_lease_recoveries_total", Help: "Expired leases resolved by a committed transition.",
		}),
	}
	for _, outcome := range []string{"completed", "failed", "cancelled"} {
		m.outcomes.WithLabelValues(outcome)
	}
	for _, cause := range []string{"failure", "lease_expired"} {
		m.retries.WithLabelValues(cause)
	}
	m.registry.MustRegister(m.wait, m.outcomes, m.retries, m.recovery)
	return m
}

func (m *Worker) Claim(wait time.Duration) {
	m.wait.Observe(max(0, wait.Seconds()))
}

// Finish is called only for committed RUNNING attempt transitions.
func (m *Worker) Finish(jobStatus string, expired bool) {
	switch jobStatus {
	case "COMPLETED":
		m.outcomes.WithLabelValues("completed").Inc()
	case "CANCELLED":
		m.outcomes.WithLabelValues("cancelled").Inc()
	case "QUEUED", "DEAD_LETTER":
		m.outcomes.WithLabelValues("failed").Inc()
	}
	if expired {
		m.recovery.Inc()
	}
	if jobStatus == "QUEUED" {
		cause := "failure"
		if expired {
			cause = "lease_expired"
		}
		m.retries.WithLabelValues(cause).Inc()
	}
}

func (m *Worker) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{MaxRequestsInFlight: 1})
}

func QueueHandler(depth func(context.Context) (int64, int64, error), log *slog.Logger) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(queueCollector{
		depth: depth,
		log:   log,
		desc:  prometheus.NewDesc("jobflow_queue_depth", "Current queued jobs by eligibility at scrape time.", []string{"eligibility"}, nil),
	})
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{MaxRequestsInFlight: 1})
}

type queueCollector struct {
	depth func(context.Context) (int64, int64, error)
	desc  *prometheus.Desc
	log   *slog.Logger
}

func (c queueCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	eligible, delayed, err := c.depth(ctx)
	if err != nil {
		c.log.Error("queue depth scrape failed", "error", err)
		ch <- prometheus.NewInvalidMetric(c.desc, errors.New("queue depth unavailable"))
		return
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(eligible), "eligible")
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(delayed), "delayed")
}
