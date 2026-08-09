package application

import "github.com/s7venking/eventflow/internal/event/domain"

type PurchaseSchema struct{}

func NewPurchaseSchema() PurchaseSchema {
	return PurchaseSchema{}
}

func (PurchaseSchema) Type() string {
	return "purchase"
}

func (PurchaseSchema) Version() int {
	return 1
}

func (PurchaseSchema) Fields() []domain.FieldDefinition {
	return []domain.FieldDefinition{
		{
			Name:     "order_id",
			Type:     domain.FieldTypeString,
			Required: true,
		},
		{
			Name:     "amount",
			Type:     domain.FieldTypeNumber,
			Required: true,
		},
		{
			Name:     "currency",
			Type:     domain.FieldTypeString,
			Required: true,
		},
	}
}

var _ domain.EventSchema = PurchaseSchema{}
