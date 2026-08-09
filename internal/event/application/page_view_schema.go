package application

import (
	"errors"

	"github.com/s7venking/pulse/internal/event/domain"
)

type PageViewSchema struct{}

func NewPageViewSchema() PageViewSchema {
	return PageViewSchema{}
}

func (PageViewSchema) Type() string {
	return "page_view"
}

func (PageViewSchema) Version() int {
	return 1
}

func (PageViewSchema) Validate(
	properties map[string]any,
) error {

	if properties == nil {
		return errors.New("properties is required")
	}

	page, ok := properties["page"]

	if !ok {
		return errors.New("page is required")
	}

	if _, ok := page.(string); !ok {
		return errors.New("page must be a string")
	}

	return nil
}

var _ domain.EventSchema = PageViewSchema{}
