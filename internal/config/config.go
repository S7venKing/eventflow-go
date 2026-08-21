package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Database        DatabaseConfig
	ShutdownTimeout time.Duration
}

type DatabaseConfig struct {
	URL             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
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

	return Config{
		Database: DatabaseConfig{
			URL:             databaseURL,
			MaxConns:        maxConns,
			MinConns:        minConns,
			MaxConnLifetime: maxLifetime,
			MaxConnIdleTime: maxIdleTime,
		},
		ShutdownTimeout: shutdownTimeout,
	}, nil
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
