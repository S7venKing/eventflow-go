package domain

import "fmt"

type InMemorySchemaRegistry struct {
	schemas map[string]EventSchema
}

func NewInMemorySchemaRegistry() *InMemorySchemaRegistry {
	return &InMemorySchemaRegistry{
		schemas: make(map[string]EventSchema),
	}
}

func schemaKey(eventType string, version int) string {
	return fmt.Sprintf("%s:v%d", eventType, version)
}

func (r *InMemorySchemaRegistry) RegisterSchema(
	schema EventSchema,
) {
	key := schemaKey(
		schema.Type(),
		schema.Version(),
	)

	r.schemas[key] = schema
}

func (r *InMemorySchemaRegistry) Get(
	eventType string,
	version int,
) (EventSchema, bool) {

	key := schemaKey(eventType, version)

	schema, ok := r.schemas[key]

	return schema, ok
}
