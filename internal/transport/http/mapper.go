package http

import (
	"fmt"
	"reflect"
)

// Map copies exported fields from src to dst when names match and types are assignable or convertible.
// dst must be a non-nil pointer to a struct. src must be a struct or pointer to struct.
func Map(dst any, src any) error {
	dv := reflect.ValueOf(dst)
	if dv.Kind() != reflect.Pointer || dv.IsNil() {
		return fmt.Errorf("dst must be a non-nil pointer to a struct")
	}

	sv := reflect.ValueOf(src)
	if sv.Kind() == reflect.Pointer {
		if sv.IsNil() {
			return fmt.Errorf("src pointer is nil")
		}
		sv = sv.Elem()
	}

	if sv.Kind() != reflect.Struct {
		return fmt.Errorf("src must be a struct or pointer to struct")
	}

	dv = dv.Elem()
	if dv.Kind() != reflect.Struct {
		return fmt.Errorf("dst must point to a struct")
	}

	dstType := dv.Type()

	for i := 0; i < dstType.NumField(); i++ {
		df := dstType.Field(i)
		if !df.IsExported() {
			continue
		}

		sf := sv.FieldByName(df.Name)
		if !sf.IsValid() {
			continue
		}

		if !dv.Field(i).CanSet() {
			continue
		}

		if sf.Type().AssignableTo(df.Type) {
			dv.Field(i).Set(sf)
			continue
		}

		if sf.Type().ConvertibleTo(df.Type) {
			dv.Field(i).Set(sf.Convert(df.Type))
		}
	}

	return nil
}
