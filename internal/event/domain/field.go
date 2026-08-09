package domain

type FieldType string

const (
	FieldTypeString FieldType = "string"
	FieldTypeNumber FieldType = "number"
	FieldTypeBool   FieldType = "bool"
)

type FieldDefinition struct {
	Name     string
	Type     FieldType
	Required bool
}
