package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func (db *DB) Migrate(ctx context.Context) error {
	if db == nil || db.Pool == nil {
		return fmt.Errorf("database pool is nil")
	}

	migrationDir, err := findMigrationDir()
	if err != nil {
		return err
	}

	files, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf(
			"read migration directory: %w",
			err,
		)
	}

	migrationFiles := make([]string, 0)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		migrationFiles = append(
			migrationFiles,
			file.Name(),
		)
	}

	sort.Strings(migrationFiles)

	for _, fileName := range migrationFiles {
		if err := db.runMigration(
			ctx,
			migrationDir,
			fileName,
		); err != nil {
			return err
		}
	}

	return nil
}

func findMigrationDir() (string, error) {
	candidates := []string{
		"migrations",
		filepath.Join("..", "..", "migrations"),
		"/app/migrations",
		"/src/migrations",
	}

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)

		if err != nil {
			continue
		}

		if info.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf(
		"migration directory not found (tried: %v)",
		candidates,
	)
}

func (db *DB) runMigration(
	ctx context.Context,
	migrationDir string,
	fileName string,
) error {
	path := filepath.Join(
		migrationDir,
		fileName,
	)

	sqlContent, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf(
			"read migration %s: %w",
			fileName,
			err,
		)
	}

	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin migration %s: %w",
			fileName,
			err,
		)
	}

	defer tx.Rollback(ctx)

	if _, err := tx.Exec(
		ctx,
		string(sqlContent),
	); err != nil {
		return fmt.Errorf(
			"run migration %s: %w",
			fileName,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit migration %s: %w",
			fileName,
			err,
		)
	}

	return nil
}
