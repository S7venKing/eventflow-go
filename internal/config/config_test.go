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
}

func TestLoadOutboxFromEnvironment(t *testing.T) {
	setRequiredEnv(t)

	t.Setenv("OUTBOX_WORKERS", "8")
	t.Setenv("OUTBOX_BATCH_SIZE", "10")
	t.Setenv("OUTBOX_INTERVAL", "250ms")

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
		Workers:   4,
		BatchSize: 10,
		Interval:  time.Second,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	cases := []struct {
		name string
		cfg  OutboxConfig
	}{
		{
			name: "no workers",
			cfg: OutboxConfig{
				Workers:   0,
				BatchSize: 10,
				Interval:  time.Second,
			},
		},
		{
			name: "no batch size",
			cfg: OutboxConfig{
				Workers:   1,
				BatchSize: 0,
				Interval:  time.Second,
			},
		},
		{
			name: "no interval",
			cfg: OutboxConfig{
				Workers:   1,
				BatchSize: 10,
				Interval:  0,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil {
				t.Errorf("Validate() = nil, want error")
			}
		})
	}
}
