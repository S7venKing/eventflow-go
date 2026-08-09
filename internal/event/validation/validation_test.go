package validation_test

import (
	"testing"

	"github.com/s7venking/pulse/internal/event/validation"
)

func TestRequiredString(t *testing.T) {
	properties := map[string]any{
		"name": "Binh",
	}

	if err := validation.RequiredString(
		properties,
		"name",
	); err != nil {
		t.Fatalf(
			"unexpected error: %v",
			err,
		)
	}
}

func TestRequiredString_Missing(t *testing.T) {
	properties := map[string]any{}

	if err := validation.RequiredString(
		properties,
		"name",
	); err == nil {
		t.Fatal("expected error")
	}
}

func TestRequiredString_WrongType(t *testing.T) {
	properties := map[string]any{
		"name": 123,
	}

	if err := validation.RequiredString(
		properties,
		"name",
	); err == nil {
		t.Fatal("expected error")
	}
}

func TestNumber(t *testing.T) {
	properties := map[string]any{
		"amount": 1500000,
	}

	value, err := validation.Number(
		properties,
		"amount",
	)

	if err != nil {
		t.Fatalf(
			"unexpected error: %v",
			err,
		)
	}

	if value != 1500000 {
		t.Fatalf(
			"expected 1500000, got %v",
			value,
		)
	}
}
