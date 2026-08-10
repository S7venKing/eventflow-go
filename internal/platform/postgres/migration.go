package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (db *DB) Migrate(ctx context.Context) error {
	candidates := []string{
		"migrations",
		filepath.Join("..", "..", "migrations"),
		"/app/migrations",
		"/src/migrations",
	}

	migrationDir := ""
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			migrationDir = candidate
			break
		}
	}

	if migrationDir == "" {
		return fmt.Errorf("migration directory not found (tried: %v)", candidates)
	}

	files, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("read migration directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		path := filepath.Join(migrationDir, file.Name())
		sqlContent, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file.Name(), err)
		}

		if _, err := db.Pool.Exec(ctx, string(sqlContent)); err != nil {
			return fmt.Errorf("run migration %s: %w", file.Name(), err)
		}
	}

	return nil
}
