package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Database        DatabaseConfig
	Outbox          OutboxConfig
	ShutdownTimeout time.Duration
}

type DatabaseConfig struct {
	URL             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// OutboxConfig controls how many outbox workers run and how hard each one
// polls. Every worker shares the same repository, pool, publisher and
// metrics, and competes for events through ClaimPending, so these values
// change throughput only, never the outbox state machine.
type OutboxConfig struct {
	Workers   int
	BatchSize int
	Interval  time.Duration

	// StaleTimeout is how long an event may sit in PROCESSING before a
	// worker reclaims it back to PENDING. It must comfortably exceed the
	// longest legitimate publish (including the shutdown drain window),
	// or a slow worker's events get reclaimed and published twice.
	StaleTimeout time.Duration

	// PublishFailureRate injects transient publish failures at this
	// per-attempt probability. 0 disables injection; anything above it
	// is a chaos knob for exercising retry and recovery, never a
	// production setting.
	PublishFailureRate float64
}

const (
	defaultOutboxWorkers      = 1
	defaultOutboxBatchSize    = 100
	defaultOutboxInterval     = 5 * time.Second
	defaultOutboxStaleTimeout = 5 * time.Minute
)

func (c OutboxConfig) Validate() error {
	if c.Workers <= 0 {
		return fmt.Errorf(
			"OUTBOX_WORKERS must be greater than 0",
		)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf(
			"OUTBOX_BATCH_SIZE must be greater than 0",
		)
	}

	if c.Interval <= 0 {
		return fmt.Errorf(
			"OUTBOX_INTERVAL must be greater than 0",
		)
	}

	if c.StaleTimeout <= 0 {
		return fmt.Errorf(
			"OUTBOX_STALE_TIMEOUT must be greater than 0",
		)
	}

	if c.PublishFailureRate < 0 || c.PublishFailureRate >= 1 {
		return fmt.Errorf(
			"PUBLISH_FAILURE_RATE must be in [0, 1)",
		)
	}

	return nil
}

func (c DatabaseConfig) Validate() error {
	if c.URL == "" {
		return fmt.Errorf(
			"DATABASE_URL is required",
		)
	}

	if c.MaxConns <= 0 {
		return fmt.Errorf(
			"DB_MAX_CONNS must be greater than 0",
		)
	}

	if c.MinConns < 0 {
		return fmt.Errorf(
			"DB_MIN_CONNS must be greater than or equal to 0",
		)
	}

	if c.MinConns > c.MaxConns {
		return fmt.Errorf(
			"DB_MIN_CONNS cannot be greater than DB_MAX_CONNS",
		)
	}

	if c.MaxConnLifetime <= 0 {
		return fmt.Errorf(
			"DB_MAX_CONN_LIFETIME must be greater than 0",
		)
	}

	if c.MaxConnIdleTime <= 0 {
		return fmt.Errorf(
			"DB_MAX_CONN_IDLE_TIME must be greater than 0",
		)
	}

	return nil
}

func Load() (Config, error) {
	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		return Config{}, fmt.Errorf(
			"DATABASE_URL is required",
		)
	}

	maxConns, err := getInt32("DB_MAX_CONNS")
	if err != nil {
		return Config{}, err
	}

	minConns, err := getInt32("DB_MIN_CONNS")
	if err != nil {
		return Config{}, err
	}

	maxLifetime, err := getDuration(
		"DB_MAX_CONN_LIFETIME",
	)
	if err != nil {
		return Config{}, err
	}

	maxIdleTime, err := getDuration(
		"DB_MAX_CONN_IDLE_TIME",
	)
	if err != nil {
		return Config{}, err
	}

	shutdownTimeout, err := getDurationWithDefault(
		"SHUTDOWN_TIMEOUT",
		30*time.Second,
	)
	if err != nil {
		return Config{}, err
	}

	outbox, err := loadOutbox()
	if err != nil {
		return Config{}, err
	}

	return Config{
		Database: DatabaseConfig{
			URL:             databaseURL,
			MaxConns:        maxConns,
			MinConns:        minConns,
			MaxConnLifetime: maxLifetime,
			MaxConnIdleTime: maxIdleTime,
		},
		Outbox:          outbox,
		ShutdownTimeout: shutdownTimeout,
	}, nil
}

func loadOutbox() (OutboxConfig, error) {
	workers, err := getIntWithDefault(
		"OUTBOX_WORKERS",
		defaultOutboxWorkers,
	)
	if err != nil {
		return OutboxConfig{}, err
	}

	batchSize, err := getIntWithDefault(
		"OUTBOX_BATCH_SIZE",
		defaultOutboxBatchSize,
	)
	if err != nil {
		return OutboxConfig{}, err
	}

	interval, err := getDurationWithDefault(
		"OUTBOX_INTERVAL",
		defaultOutboxInterval,
	)
	if err != nil {
		return OutboxConfig{}, err
	}

	staleTimeout, err := getDurationWithDefault(
		"OUTBOX_STALE_TIMEOUT",
		defaultOutboxStaleTimeout,
	)
	if err != nil {
		return OutboxConfig{}, err
	}

	failureRate, err := getFloatWithDefault(
		"PUBLISH_FAILURE_RATE",
		0,
	)
	if err != nil {
		return OutboxConfig{}, err
	}

	if failureRate < 0 || failureRate >= 1 {
		return OutboxConfig{}, fmt.Errorf(
			"PUBLISH_FAILURE_RATE must be in [0, 1)",
		)
	}

	return OutboxConfig{
		Workers:            workers,
		BatchSize:          batchSize,
		Interval:           interval,
		StaleTimeout:       staleTimeout,
		PublishFailureRate: failureRate,
	}, nil
}

func getFloatWithDefault(
	key string,
	fallback float64,
) (float64, error) {
	value := os.Getenv(key)

	if value == "" {
		return fallback, nil
	}

	result, err := strconv.ParseFloat(value, 64)

	if err != nil {
		return 0, fmt.Errorf(
			"%s must be a number: %w",
			key,
			err,
		)
	}

	return result, nil
}

func getInt32(key string) (int32, error) {
	value := os.Getenv(key)

	if value == "" {
		return 0, fmt.Errorf(
			"%s is required",
			key,
		)
	}

	result, err := strconv.ParseInt(
		value,
		10,
		32,
	)

	if err != nil {
		return 0, fmt.Errorf(
			"%s must be an integer: %w",
			key,
			err,
		)
	}

	return int32(result), nil
}

func getIntWithDefault(
	key string,
	fallback int,
) (int, error) {
	value := os.Getenv(key)

	if value == "" {
		return fallback, nil
	}

	result, err := strconv.Atoi(value)

	if err != nil {
		return 0, fmt.Errorf(
			"%s must be an integer: %w",
			key,
			err,
		)
	}

	if result <= 0 {
		return 0, fmt.Errorf(
			"%s must be greater than 0",
			key,
		)
	}

	return result, nil
}

func getDurationWithDefault(
	key string,
	fallback time.Duration,
) (time.Duration, error) {
	value := os.Getenv(key)

	if value == "" {
		return fallback, nil
	}

	result, err := time.ParseDuration(value)

	if err != nil {
		return 0, fmt.Errorf(
			"%s must be a valid duration: %w",
			key,
			err,
		)
	}

	if result <= 0 {
		return 0, fmt.Errorf(
			"%s must be greater than 0",
			key,
		)
	}

	return result, nil
}

func getDuration(key string) (time.Duration, error) {
	value := os.Getenv(key)

	if value == "" {
		return 0, fmt.Errorf(
			"%s is required",
			key,
		)
	}

	result, err := time.ParseDuration(value)

	if err != nil {
		return 0, fmt.Errorf(
			"%s must be a valid duration: %w",
			key,
			err,
		)
	}

	return result, nil
}
