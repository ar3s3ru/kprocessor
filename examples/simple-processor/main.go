package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ar3s3ru/kprocessor"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mustBeNotEmpty(s string) string {
	if len(s) == 0 {
		panic("provided string should not be empty")
	}

	return s
}

func main() {
	logger, err := zap.NewProduction()
	must(err)

	consumer := &kprocessor.OrderedConsumer{
		Topic:     mustBeNotEmpty(os.Getenv("KAFKA_GROUP_TOPIC")),
		GroupID:   mustBeNotEmpty(os.Getenv("KAFKA_GROUP_ID")),
		ClientID:  mustBeNotEmpty(os.Getenv("KAFKA_CLIENT_ID")),
		BatchSize: 1024,
		Logger:    kzap.New(logger),
		Processor: kprocessor.ProcessorFunc(func(ctx context.Context, record *kgo.Record) error {
			simulateWorkDuration := time.Duration(rand.Int63n(50)) * time.Millisecond
			<-time.After(simulateWorkDuration)

			logger.Info("Message received",
				zap.Any("record", record),
				zap.Duration("workDuration", simulateWorkDuration),
			)

			return nil
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return consumer.Run(ctx,
			kgo.SeedBrokers(mustBeNotEmpty(os.Getenv("KAFKA_BROKER"))),
		)
	})

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	<-exit
	logger.Info("Signal received, shutting down")
	cancel()

	must(group.Wait())
}
