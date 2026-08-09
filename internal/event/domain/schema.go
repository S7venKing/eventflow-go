package domain

type EventSchema interface {
	Type() string
	Version() int
	Fields() []FieldDefinition
}

type SchemaRegistry interface {
	Get(eventType string, version int) (EventSchema, bool)
}
