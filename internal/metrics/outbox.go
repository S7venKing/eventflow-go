package metrics

import "github.com/prometheus/client_golang/prometheus"

type OutboxMetrics struct {
	Published prometheus.Counter
	Failed    prometheus.Counter
	Closed    prometheus.Counter
	Pending   prometheus.Gauge
}

func NewOutboxMetrics(
	registerer prometheus.Registerer,
) *OutboxMetrics {
	m := &OutboxMetrics{
		Published: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "outbox_events_published_total",
				Help: "Total number of outbox events successfully published.",
			},
		),

		Failed: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "outbox_events_failed_total",
				Help: "Total number of outbox event publish failures.",
			},
		),

		Closed: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "outbox_events_closed_total",
				Help: "Total number of outbox events moved to CLOSE.",
			},
		),

		Pending: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "outbox_pending_events",
				Help: "Current number of pending outbox events.",
			},
		),
	}

	registerer.MustRegister(
		m.Published,
		m.Failed,
		m.Closed,
		m.Pending,
	)

	return m
}
