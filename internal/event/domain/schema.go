package domain

type EventSchema interface {
	Type() string
	Version() int
	Validate(properties map[string]any) error
}

type SchemaRegistry interface {
	Get(eventType string, version int) (EventSchema, bool)
}
