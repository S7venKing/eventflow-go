package application

import (
	"github.com/s7venking/pulse/internal/event/domain"
	"github.com/s7venking/pulse/internal/event/validation"
)

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

func (PurchaseSchema) Validate(
	properties map[string]any,
) error {
	if properties == nil {
		return ErrPropertiesRequired
	}

	if err := validation.RequiredString(
		properties,
		"order_id",
	); err != nil {
		return err
	}

	if err := validation.RequiredNumber(
		properties,
		"amount",
	); err != nil {
		return err
	}

	if err := validation.RequiredString(
		properties,
		"currency",
	); err != nil {
		return err
	}

	return nil
}

var _ domain.EventSchema = PurchaseSchema{}
