package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

func New(
	ctx context.Context,
	dsn string,
) (*DB, error) {
	pool, err := pgxpool.New(
		ctx,
		dsn,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"create postgres pool: %w",
			err,
		)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf(
			"ping postgres: %w",
			err,
		)
	}

	return &DB{
		Pool: pool,
	}, nil
}

func (db *DB) Close() {
	db.Pool.Close()
}
