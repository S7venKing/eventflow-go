package application

import "github.com/s7venking/eventflow/internal/event/domain"

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

func (PageViewSchema) Fields() []domain.FieldDefinition {
	return []domain.FieldDefinition{
		{
			Name:     "page",
			Type:     domain.FieldTypeString,
			Required: true,
		},
		{
			Name:     "device",
			Type:     domain.FieldTypeString,
			Required: false,
		},
		{
			Name:     "referrer",
			Type:     domain.FieldTypeString,
			Required: false,
		},
	}
}

var _ domain.EventSchema = PageViewSchema{}
