package config

import (
	"testing"
	"time"
)

// setRequiredEnv sets everything Load needs besides the outbox variables,
// so each test only has to declare the part it exercises.
func setRequiredEnv(t *testing.T) {
	t.Helper()

	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5452/eventflow")
	t.Setenv("DB_MAX_CONNS", "10")
	t.Setenv("DB_MIN_CONNS", "2")
	t.Setenv("DB_MAX_CONN_LIFETIME", "1h")
	t.Setenv("DB_MAX_CONN_IDLE_TIME", "30m")
}

// ========================================
// Outbox config loading
// ========================================

func TestLoadOutboxDefaults(t *testing.T) {
	setRequiredEnv(t)

	t.Setenv("OUTBOX_WORKERS", "")
	t.Setenv("OUTBOX_BATCH_SIZE", "")
	t.Setenv("OUTBOX_INTERVAL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Outbox.Workers != defaultOutboxWorkers {
		t.Errorf(
			"Workers = %d, want %d",
			cfg.Outbox.Workers,
			defaultOutboxWorkers,
		)
	}

	if cfg.Outbox.BatchSize != defaultOutboxBatchSize {
		t.Errorf(
			"BatchSize = %d, want %d",
			cfg.Outbox.BatchSize,
			defaultOutboxBatchSize,
		)
	}

	if cfg.Outbox.Interval != defaultOutboxInterval {
		t.Errorf(
			"Interval = %s, want %s",
			cfg.Outbox.Interval,
			defaultOutboxInterval,
		)
	}

	if cfg.Outbox.StaleTimeout != defaultOutboxStaleTimeout {
		t.Errorf(
			"StaleTimeout = %s, want %s",
			cfg.Outbox.StaleTimeout,
			defaultOutboxStaleTimeout,
		)
	}

	if cfg.Outbox.PublishFailureRate != 0 {
		t.Errorf(
			"PublishFailureRate = %f, want 0",
			cfg.Outbox.PublishFailureRate,
		)
	}

	if cfg.Outbox.Publisher != OutboxPublisherKafka {
		t.Errorf(
			"Publisher = %q, want %q",
			cfg.Outbox.Publisher,
			OutboxPublisherKafka,
		)
	}
}

func TestLoadOutboxFromEnvironment(t *testing.T) {
	setRequiredEnv(t)

	t.Setenv("OUTBOX_WORKERS", "8")
	t.Setenv("OUTBOX_BATCH_SIZE", "10")
	t.Setenv("OUTBOX_INTERVAL", "250ms")
	t.Setenv("OUTBOX_STALE_TIMEOUT", "90s")
	t.Setenv("PUBLISH_FAILURE_RATE", "0.25")
	t.Setenv("OUTBOX_PUBLISHER", "log")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Outbox.Workers != 8 {
		t.Errorf("Workers = %d, want 8", cfg.Outbox.Workers)
	}

	if cfg.Outbox.BatchSize != 10 {
		t.Errorf("BatchSize = %d, want 10", cfg.Outbox.BatchSize)
	}

	if cfg.Outbox.Interval != 250*time.Millisecond {
		t.Errorf(
			"Interval = %s, want 250ms",
			cfg.Outbox.Interval,
		)
	}

	if cfg.Outbox.StaleTimeout != 90*time.Second {
		t.Errorf(
			"StaleTimeout = %s, want 90s",
			cfg.Outbox.StaleTimeout,
		)
	}

	if cfg.Outbox.PublishFailureRate != 0.25 {
		t.Errorf(
			"PublishFailureRate = %f, want 0.25",
			cfg.Outbox.PublishFailureRate,
		)
	}

	if cfg.Outbox.Publisher != OutboxPublisherLog {
		t.Errorf(
			"Publisher = %q, want %q",
			cfg.Outbox.Publisher,
			OutboxPublisherLog,
		)
	}

	if err := cfg.Outbox.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestLoadOutboxRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{"zero workers", "OUTBOX_WORKERS", "0"},
		{"negative workers", "OUTBOX_WORKERS", "-1"},
		{"non numeric workers", "OUTBOX_WORKERS", "many"},
		{"zero batch size", "OUTBOX_BATCH_SIZE", "0"},
		{"negative batch size", "OUTBOX_BATCH_SIZE", "-10"},
		{"non numeric batch size", "OUTBOX_BATCH_SIZE", "ten"},
		{"zero interval", "OUTBOX_INTERVAL", "0s"},
		{"negative interval", "OUTBOX_INTERVAL", "-1s"},
		{"unparsable interval", "OUTBOX_INTERVAL", "5"},
		{"zero stale timeout", "OUTBOX_STALE_TIMEOUT", "0s"},
		{"negative stale timeout", "OUTBOX_STALE_TIMEOUT", "-1m"},
		{"unparsable stale timeout", "OUTBOX_STALE_TIMEOUT", "soon"},
		{"negative failure rate", "PUBLISH_FAILURE_RATE", "-0.1"},
		{"failure rate of one", "PUBLISH_FAILURE_RATE", "1"},
		{"failure rate above one", "PUBLISH_FAILURE_RATE", "1.5"},
		{"non numeric failure rate", "PUBLISH_FAILURE_RATE", "often"},
		{"unknown publisher", "OUTBOX_PUBLISHER", "rabbitmq"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tc.key, tc.value)

			if _, err := Load(); err == nil {
				t.Fatalf(
					"Load succeeded with %s=%q, want error",
					tc.key,
					tc.value,
				)
			}
		})
	}
}

// ========================================
// Outbox config validation
// ========================================

func TestOutboxConfigValidate(t *testing.T) {
	valid := OutboxConfig{
		Workers:      4,
		BatchSize:    10,
		Interval:     time.Second,
		Publisher:    OutboxPublisherKafka,
		StaleTimeout: time.Minute,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	logPublisher := valid
	logPublisher.Publisher = OutboxPublisherLog

	if err := logPublisher.Validate(); err != nil {
		t.Fatalf("log publisher rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(c *OutboxConfig)
	}{
		{"no workers", func(c *OutboxConfig) { c.Workers = 0 }},
		{"no batch size", func(c *OutboxConfig) { c.BatchSize = 0 }},
		{"no interval", func(c *OutboxConfig) { c.Interval = 0 }},
		{"no stale timeout", func(c *OutboxConfig) { c.StaleTimeout = 0 }},
		{"failure rate at one", func(c *OutboxConfig) { c.PublishFailureRate = 1 }},
		{"empty publisher", func(c *OutboxConfig) { c.Publisher = "" }},
		{"unknown publisher", func(c *OutboxConfig) { c.Publisher = "sqs" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)

			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate() = nil, want error")
			}
		})
	}
}

// ========================================
// Kafka config
// ========================================

func TestLoadKafkaDefaults(t *testing.T) {
	setRequiredEnv(t)

	for _, key := range []string{
		"KAFKA_BROKERS",
		"KAFKA_TOPIC",
		"KAFKA_CLIENT_ID",
		"KAFKA_WRITE_TIMEOUT",
		"KAFKA_MAX_ATTEMPTS",
		"KAFKA_BATCH_TIMEOUT",
	} {
		t.Setenv(key, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Kafka.Brokers) != 1 ||
		cfg.Kafka.Brokers[0] != defaultKafkaBrokers {
		t.Errorf(
			"Brokers = %v, want [%s]",
			cfg.Kafka.Brokers,
			defaultKafkaBrokers,
		)
	}

	if cfg.Kafka.Topic != defaultKafkaTopic {
		t.Errorf("Topic = %q, want %q", cfg.Kafka.Topic, defaultKafkaTopic)
	}

	if cfg.Kafka.ClientID != defaultKafkaClientID {
		t.Errorf(
			"ClientID = %q, want %q",
			cfg.Kafka.ClientID,
			defaultKafkaClientID,
		)
	}

	if cfg.Kafka.WriteTimeout != defaultKafkaWriteTimeout {
		t.Errorf(
			"WriteTimeout = %s, want %s",
			cfg.Kafka.WriteTimeout,
			defaultKafkaWriteTimeout,
		)
	}

	if cfg.Kafka.MaxAttempts != defaultKafkaMaxAttempts {
		t.Errorf(
			"MaxAttempts = %d, want %d",
			cfg.Kafka.MaxAttempts,
			defaultKafkaMaxAttempts,
		)
	}

	if cfg.Kafka.BatchTimeout != defaultKafkaBatchTimeout {
		t.Errorf(
			"BatchTimeout = %s, want %s",
			cfg.Kafka.BatchTimeout,
			defaultKafkaBatchTimeout,
		)
	}
}

func TestLoadKafkaFromEnvironment(t *testing.T) {
	setRequiredEnv(t)

	// Spaces and a trailing comma must not produce empty broker entries.
	t.Setenv("KAFKA_BROKERS", "kafka:9092, kafka-2:9092,")
	t.Setenv("KAFKA_TOPIC", "eventflow.staging")
	t.Setenv("KAFKA_CLIENT_ID", "eventflow-staging")
	t.Setenv("KAFKA_WRITE_TIMEOUT", "3s")
	t.Setenv("KAFKA_MAX_ATTEMPTS", "7")
	t.Setenv("KAFKA_BATCH_TIMEOUT", "25ms")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	wantBrokers := []string{"kafka:9092", "kafka-2:9092"}

	if len(cfg.Kafka.Brokers) != len(wantBrokers) {
		t.Fatalf(
			"Brokers = %v, want %v",
			cfg.Kafka.Brokers,
			wantBrokers,
		)
	}

	for i := range wantBrokers {
		if cfg.Kafka.Brokers[i] != wantBrokers[i] {
			t.Errorf(
				"Brokers[%d] = %q, want %q",
				i,
				cfg.Kafka.Brokers[i],
				wantBrokers[i],
			)
		}
	}

	if cfg.Kafka.Topic != "eventflow.staging" {
		t.Errorf("Topic = %q, want eventflow.staging", cfg.Kafka.Topic)
	}

	if cfg.Kafka.ClientID != "eventflow-staging" {
		t.Errorf("ClientID = %q, want eventflow-staging", cfg.Kafka.ClientID)
	}

	if cfg.Kafka.WriteTimeout != 3*time.Second {
		t.Errorf("WriteTimeout = %s, want 3s", cfg.Kafka.WriteTimeout)
	}

	if cfg.Kafka.MaxAttempts != 7 {
		t.Errorf("MaxAttempts = %d, want 7", cfg.Kafka.MaxAttempts)
	}

	if cfg.Kafka.BatchTimeout != 25*time.Millisecond {
		t.Errorf("BatchTimeout = %s, want 25ms", cfg.Kafka.BatchTimeout)
	}
}

func TestLoadKafkaRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{"only separators in brokers", "KAFKA_BROKERS", " , "},
		{"zero write timeout", "KAFKA_WRITE_TIMEOUT", "0s"},
		{"unparsable write timeout", "KAFKA_WRITE_TIMEOUT", "later"},
		{"zero attempts", "KAFKA_MAX_ATTEMPTS", "0"},
		{"non numeric attempts", "KAFKA_MAX_ATTEMPTS", "some"},
		{"negative batch timeout", "KAFKA_BATCH_TIMEOUT", "-1ms"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tc.key, tc.value)

			if _, err := Load(); err == nil {
				t.Fatalf(
					"Load succeeded with %s=%q, want error",
					tc.key,
					tc.value,
				)
			}
		})
	}
}

func TestKafkaConfigValidate(t *testing.T) {
	valid := KafkaConfig{
		Brokers:      []string{"kafka:9092"},
		Topic:        "eventflow.events",
		ClientID:     "eventflow-api",
		WriteTimeout: time.Second,
		MaxAttempts:  3,
		BatchTimeout: time.Millisecond,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(c *KafkaConfig)
	}{
		{"no brokers", func(c *KafkaConfig) { c.Brokers = nil }},
		{"empty broker", func(c *KafkaConfig) { c.Brokers = []string{"kafka:9092", ""} }},
		{"no topic", func(c *KafkaConfig) { c.Topic = "" }},
		{"no client id", func(c *KafkaConfig) { c.ClientID = "" }},
		{"no write timeout", func(c *KafkaConfig) { c.WriteTimeout = 0 }},
		{"no attempts", func(c *KafkaConfig) { c.MaxAttempts = 0 }},
		{"no batch timeout", func(c *KafkaConfig) { c.BatchTimeout = 0 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			cfg.Brokers = append([]string(nil), valid.Brokers...)
			tc.mutate(&cfg)

			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate() = nil, want error")
			}
		})
	}
}
