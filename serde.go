package kprocessor

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Deserializer converts some raw data from byte array to a specific Go type.
type Deserializer[T any] func(T, []byte) error

// Serializer converts a specific Go type into byte array
// using some specific codec/format.
type Serializer[T any] func(T) ([]byte, error)

// Serde can be used to serialize and deserialize a Go type into and from
// a specific codec/format.
type Serde[T any] struct {
	Serializer[T]
	Deserializer[T]
}

// JSONSerializer returns a Serializer instance that uses JSON
// as codec.
func JSONSerializer[T any]() Serializer[T] {
	return func(t T) ([]byte, error) {
		return json.Marshal(t)
	}
}

// JSONDeserializer returns a Deserializer instance that uses JSON
// as codec.
func JSONDeserializer[T any]() Deserializer[T] {
	return func(t T, data []byte) error {
		return json.Unmarshal(data, t)
	}
}

// ProtoSerializer returns a Serializer instance that uses Protobuf
// as codec.
func ProtoSerializer[T proto.Message]() Serializer[T] {
	return func(t T) ([]byte, error) {
		return proto.Marshal(t)
	}
}

// ProtoSerializer returns a Deserializer instance that uses Protobuf
// as codec.
func ProtoDeserializer[T proto.Message]() Deserializer[T] {
	return func(t T, data []byte) error {
		return proto.Unmarshal(data, t)
	}
}

// ProtoJSONSerializer returns a Serializer instance
// that uses Protobuf JSON as codec.
func ProtoJSONSerializer[T proto.Message]() Serializer[T] {
	return func(t T) ([]byte, error) {
		return protojson.Marshal(t)
	}
}

// ProtoJSONDeserializer returns a Deserializer instance
// that uses Protobuf JSON as codec.
func ProtoJSONDeserializer[T proto.Message]() Deserializer[T] {
	return func(t T, data []byte) error {
		return protojson.Unmarshal(data, t)
	}
}
