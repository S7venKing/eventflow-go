package application

import (
	"fmt"

	"github.com/s7venking/eventflow/internal/event/domain"
)

type Validator struct{}

func NewValidator() *Validator {
	return &Validator{}
}

func (v *Validator) Validate(
	schema domain.EventSchema,
	properties map[string]any,
) error {
	if properties == nil {
		return fmt.Errorf(
			"%w",
			ErrPropertiesRequired,
		)
	}

	for _, field := range schema.Fields() {
		value, exists := properties[field.Name]

		if !exists {
			if field.Required {
				return fmt.Errorf(
					"%w: %s is required",
					ErrInvalidEventSchema,
					field.Name,
				)
			}

			continue
		}

		if err := validateType(
			field,
			value,
		); err != nil {
			return fmt.Errorf(
				"%w: %w",
				ErrInvalidEventSchema,
				err,
			)
		}
	}

	return nil
}

func validateType(
	field domain.FieldDefinition,
	value any,
) error {
	switch field.Type {
	case domain.FieldTypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf(
				"%s must be a string",
				field.Name,
			)
		}

	case domain.FieldTypeNumber:
		switch value.(type) {
		case int:
		case int32:
		case int64:
		case float32:
		case float64:
		default:
			return fmt.Errorf(
				"%s must be a number",
				field.Name,
			)
		}

	case domain.FieldTypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf(
				"%s must be a boolean",
				field.Name,
			)
		}

	default:
		return fmt.Errorf(
			"unsupported field type: %s",
			field.Type,
		)
	}

	return nil
}
