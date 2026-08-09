package validation

import "fmt"

func RequiredNumber(
	properties map[string]any,
	field string,
) error {
	value, ok := properties[field]

	if !ok {
		return fmt.Errorf(
			"%s is required",
			field,
		)
	}

	switch value.(type) {
	case int:
		return nil

	case int32:
		return nil

	case int64:
		return nil

	case float32:
		return nil

	case float64:
		return nil

	default:
		return fmt.Errorf(
			"%s must be a number",
			field,
		)
	}
}

func Number(
	properties map[string]any,
	field string,
) (float64, error) {
	value, ok := properties[field]

	if !ok {
		return 0, fmt.Errorf(
			"%s is required",
			field,
		)
	}

	switch value := value.(type) {
	case int:
		return float64(value), nil

	case int32:
		return float64(value), nil

	case int64:
		return float64(value), nil

	case float32:
		return float64(value), nil

	case float64:
		return value, nil

	default:
		return 0, fmt.Errorf(
			"%s must be a number",
			field,
		)
	}
}
