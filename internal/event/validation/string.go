package validation

import "fmt"

func RequiredString(
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

	if _, ok := value.(string); !ok {
		return fmt.Errorf(
			"%s must be a string",
			field,
		)
	}

	return nil
}
