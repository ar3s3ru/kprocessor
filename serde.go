package kprocessor

import (
<<<<<<< HEAD
=======
	"encoding/json"

>>>>>>> d433e78 (feat: implement OrderedConsumer and TypedProcessor)
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

<<<<<<< HEAD
type Deserializer[T any] func([]byte) (T, error)

type Serializer[T any] func(T) ([]byte, error)

=======
// Deserializer converts some raw data from byte array to a specific Go type.
type Deserializer[T any] func(T, []byte) error

// Serializer converts a specific Go type into byte array
// using some specific codec/format.
type Serializer[T any] func(T) ([]byte, error)

// Serde can be used to serialize and deserialize a Go type into and from
// a specific codec/format.
>>>>>>> d433e78 (feat: implement OrderedConsumer and TypedProcessor)
type Serde[T any] struct {
	Serializer[T]
	Deserializer[T]
}

<<<<<<< HEAD
=======
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
>>>>>>> d433e78 (feat: implement OrderedConsumer and TypedProcessor)
func ProtoSerializer[T proto.Message]() Serializer[T] {
	return func(t T) ([]byte, error) {
		return proto.Marshal(t)
	}
}

<<<<<<< HEAD
func ProtoDeserializer[T proto.Message]() Deserializer[T] {
	return func(data []byte) (T, error) {
		var t T

		if err := proto.Unmarshal(data, t); err != nil {
			return t, err
		}

		return t, nil
	}
}

=======
// ProtoSerializer returns a Deserializer instance that uses Protobuf
// as codec.
func ProtoDeserializer[T proto.Message]() Deserializer[T] {
	return func(t T, data []byte) error {
		return proto.Unmarshal(data, t)
	}
}

// ProtoJSONSerializer returns a Serializer instance
// that uses Protobuf JSON as codec.
>>>>>>> d433e78 (feat: implement OrderedConsumer and TypedProcessor)
func ProtoJSONSerializer[T proto.Message]() Serializer[T] {
	return func(t T) ([]byte, error) {
		return protojson.Marshal(t)
	}
}

<<<<<<< HEAD
func ProtoJSONDeserializer[T proto.Message]() Deserializer[T] {
	return func(data []byte) (T, error) {
		var t T

		if err := protojson.Unmarshal(data, t); err != nil {
			return t, err
		}

		return t, nil
=======
// ProtoJSONDeserializer returns a Deserializer instance
// that uses Protobuf JSON as codec.
func ProtoJSONDeserializer[T proto.Message]() Deserializer[T] {
	return func(t T, data []byte) error {
		return protojson.Unmarshal(data, t)
>>>>>>> d433e78 (feat: implement OrderedConsumer and TypedProcessor)
	}
}
