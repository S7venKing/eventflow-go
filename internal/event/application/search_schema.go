package application

import "github.com/s7venking/pulse/internal/event/domain"

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

func (SearchSchema) Fields() []domain.FieldDefinition {
	return []domain.FieldDefinition{
		{
			Name:     "keyword",
			Type:     domain.FieldTypeString,
			Required: true,
		},
		{
			Name:     "result_count",
			Type:     domain.FieldTypeNumber,
			Required: true,
		},
	}
}

var _ domain.EventSchema = SearchSchema{}
