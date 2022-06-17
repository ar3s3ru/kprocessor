package kprocessor

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Processor interface {
	Process(ctx context.Context, record *kgo.Record) error
}

type ProcessorFunc func(ctx context.Context, record *kgo.Record) error

func (fn ProcessorFunc) Process(ctx context.Context, record *kgo.Record) error {
	return fn(ctx, record)
}
