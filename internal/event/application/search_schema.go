package application

import (
	"fmt"

	"github.com/s7venking/pulse/internal/event/domain"
	"github.com/s7venking/pulse/internal/event/validation"
)

type SearchSchema struct{}

func NewSearchSchema() SearchSchema {
	return SearchSchema{}
}

func (SearchSchema) Type() string {
	return "search"
}

func (SearchSchema) Version() int {
	return 1
}

func (SearchSchema) Validate(
	properties map[string]any,
) error {
	if properties == nil {
		return ErrPropertiesRequired
	}

	if err := validation.RequiredString(
		properties,
		"keyword",
	); err != nil {
		return err
	}

	resultCount, err := validation.Number(
		properties,
		"result_count",
	)

	if err != nil {
		return err
	}

	if resultCount < 0 {
		return fmt.Errorf(
			"result_count must be non-negative",
		)
	}

	if resultCount != float64(int64(resultCount)) {
		return fmt.Errorf(
			"result_count must be an integer",
		)
	}

	return nil
}

var _ domain.EventSchema = SearchSchema{}
