package kprocessor

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Deserializer[T any] func([]byte) (T, error)

type Serializer[T any] func(T) ([]byte, error)

type Serde[T any] struct {
	Serializer[T]
	Deserializer[T]
}

func ProtoSerializer[T proto.Message]() Serializer[T] {
	return func(t T) ([]byte, error) {
		return proto.Marshal(t)
	}
}

func ProtoDeserializer[T proto.Message]() Deserializer[T] {
	return func(data []byte) (T, error) {
		var t T

		if err := proto.Unmarshal(data, t); err != nil {
			return t, err
		}

		return t, nil
	}
}

func ProtoJSONSerializer[T proto.Message]() Serializer[T] {
	return func(t T) ([]byte, error) {
		return protojson.Marshal(t)
	}
}

func ProtoJSONDeserializer[T proto.Message]() Deserializer[T] {
	return func(data []byte) (T, error) {
		var t T

		if err := protojson.Unmarshal(data, t); err != nil {
			return t, err
		}

		return t, nil
	}
}
