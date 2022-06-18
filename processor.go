package kprocessor

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Processor is a component that processes Kafka messages.
type Processor interface {
	// Process performs the necessary logic to handle a Kafka message.
	//
	// Processing can fail, hence why an error can be returned as output parameter.
	//
	// The method should handle context cancellation appropriately
	// to avoid unnecessary work if the Kafka consumer/client gets closed.
	Process(ctx context.Context, record *kgo.Record) error
}

// ProcessorFunc is a functional implementation of the Processor interface.
type ProcessorFunc func(ctx context.Context, record *kgo.Record) error

// Process executes the function behind this type.
func (fn ProcessorFunc) Process(ctx context.Context, record *kgo.Record) error {
	return fn(ctx, record)
}
