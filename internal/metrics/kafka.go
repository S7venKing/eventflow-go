package metrics

import "github.com/prometheus/client_golang/prometheus"

// KafkaMetrics covers the producer side only. Labels stay low-cardinality
// on purpose: topic and outcome, never event ids.
type KafkaMetrics struct {
	Published *prometheus.CounterVec
	Failed    *prometheus.CounterVec
	Duration  *prometheus.HistogramVec
}

const (
	KafkaStatusSuccess = "success"
	KafkaStatusFailure = "failure"
)

func NewKafkaMetrics(
	registerer prometheus.Registerer,
) *KafkaMetrics {
	m := &KafkaMetrics{
		Published: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_publish_total",
				Help: "Total number of messages acknowledged by Kafka.",
			},
			[]string{"topic"},
		),

		Failed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_publish_failed_total",
				Help: "Total number of Kafka publish attempts that failed.",
			},
			[]string{"topic"},
		),

		Duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "kafka_publish_duration_seconds",
				Help: "Time from handing a message to the producer until Kafka acknowledged or rejected it.",
				Buckets: []float64{
					0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
					0.1, 0.25, 0.5, 1, 2.5, 5, 10,
				},
			},
			[]string{"topic", "status"},
		),
	}

	registerer.MustRegister(
		m.Published,
		m.Failed,
		m.Duration,
	)

	return m
}
