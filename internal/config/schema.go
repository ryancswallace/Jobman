package config

import (
	"reflect"
	"strings"
)

const (
	schemaTypeKey = "type"
	schemaInteger = "integer"
	schemaString  = "string"
)

// JSONSchema describes native YAML field shapes. Validate remains authoritative
// for semantic references, filesystem rules, bounded durations, and policy.
func JSONSchema() map[string]any {
	schema := schemaType(reflect.TypeFor[Config]())
	schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	schema["title"] = "Jobman configuration schema 1"
	schema["description"] = "Validate semantic constraints with jobman config validate --isolated PATH."
	if properties, ok := schema["properties"].(map[string]any); ok {
		properties["schema_version"] = map[string]any{"const": SchemaVersion}
	}
	return schema
}

func schemaType(kind reflect.Type) map[string]any {
	if kind.Kind() == reflect.Pointer {
		return schemaType(kind.Elem())
	}
	if scalar := scalarSchema(kind); scalar != nil {
		return scalar
	}
	switch kind.Kind() {
	case reflect.Struct:
		properties := make(map[string]any)
		for index := range kind.NumField() {
			field := kind.Field(index)
			name := strings.Split(field.Tag.Get("yaml"), ",")[0]
			if name != "" && name != "-" {
				properties[name] = schemaType(field.Type)
			}
		}
		return map[string]any{schemaTypeKey: "object", "properties": properties, "additionalProperties": false}
	case reflect.Map:
		return map[string]any{schemaTypeKey: "object", "additionalProperties": schemaType(kind.Elem())}
	case reflect.Slice:
		return map[string]any{schemaTypeKey: "array", "items": schemaType(kind.Elem())}
	case reflect.Bool:
		return map[string]any{schemaTypeKey: "boolean"}
	case reflect.Int, reflect.Int64, reflect.Uint64, reflect.Uint32:
		return map[string]any{schemaTypeKey: schemaInteger}
	case reflect.Float64:
		return map[string]any{schemaTypeKey: "number"}
	default:
		return map[string]any{schemaTypeKey: schemaString}
	}
}

func scalarSchema(kind reflect.Type) map[string]any {
	switch kind {
	case reflect.TypeFor[Duration](), reflect.TypeFor[DurationLimit]():
		return map[string]any{schemaTypeKey: schemaString}
	case reflect.TypeFor[SecretRef]():
		return map[string]any{schemaTypeKey: schemaString, "pattern": "^(env:|file:)"}
	case reflect.TypeFor[IntegerLimit](), reflect.TypeFor[SlotLimit]():
		return map[string]any{"anyOf": []any{
			map[string]any{schemaTypeKey: schemaInteger, "minimum": 0}, map[string]any{"const": Unlimited},
		}}
	case reflect.TypeFor[ByteLimit]():
		return map[string]any{"anyOf": []any{
			map[string]any{schemaTypeKey: schemaInteger, "minimum": 0}, map[string]any{schemaTypeKey: schemaString},
		}}
	default:
		return nil
	}
}
