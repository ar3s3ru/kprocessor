package kprocessor

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// DefaultBatchSize represents the default size for messages
// requested to the Kafka broker before consumption.
const DefaultBatchSize = 128

// OrderedConsumer is a Kafka consumer implementation that processes
// messages, through a given Processor instance, by the same order
// as given by the Kafka broker.
//
// Check out the OrderedConsumer.Run function documentation for more
// information.
type OrderedConsumer struct {
	// Topic is the name of the topic name to consume from.
	Topic string

	// GroupID is the Consumer Group identifier for this Kafka consumer.
	GroupID string

	// ClientID is the identifier for this Kafka consumer instance.
	ClientID string

	// BatchSize is the size of messages requested each time to the Kafka
	// broker for consumption. By default, the number used is 128.
	BatchSize int

	// Processor is the component that processes the messages received
	// from the Kafka topic.
	Processor Processor

	// Logger is used to print operational information when OrderedConsumer.Run
	// is executed.
	Logger kgo.Logger
}

func (c *OrderedConsumer) log(level kgo.LogLevel, msg string, keyAndVals ...any) {
	if c.Logger == nil {
		return
	}

	c.Logger.Log(level, msg, keyAndVals...)
}

type partitionKey struct {
	topic     string
	partition int32
}

func listOf(records map[partitionKey]*kgo.Record) []*kgo.Record {
	var result []*kgo.Record

	for _, record := range records {
		result = append(result, record)
	}

	return result
}

func (c *OrderedConsumer) consumeFetches(ctx context.Context, fetches kgo.Fetches) ([]*kgo.Record, error) {
	mx := new(sync.Mutex)
	recordsToCommit := make(map[partitionKey]*kgo.Record)

	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			group, ctx := errgroup.WithContext(ctx)
			done := make(chan struct{})

			for _, partitions := range topic.Partitions {
				partitions := partitions

				group.Go(func() error {
					partitionKey := partitionKey{
						topic:     topic.Topic,
						partition: partitions.Partition,
					}

					c.log(kgo.LogLevelDebug, "start worker for group partition",
						"topic", topic.Topic,
						"partition", partitions.Partition,
						"highWaterMark", partitions.HighWatermark,
						"err", partitions.Err,
					)

					for _, record := range partitions.Records {
						// If the context has been cancelled, avoid processing the other
						// records in the batch.
						//
						// The first goroutine in the group that consumes the cancellation message
						// will "steal" it from the other consumers. To broadcast the cancellation
						// to the rest of the group, a "done" channel is used instead.
						select {
						case <-ctx.Done():
							close(done)
							return ctx.Err()
						case <-done:
							return nil
						default:
						}

						if err := c.Processor.Process(ctx, record); err != nil {
							return err
						}

						mx.Lock()
						recordsToCommit[partitionKey] = record
						mx.Unlock()
					}

					return nil
				})
			}

			// NOTE: even in case of an error, return the records that have been
			// successfully processed thus far so they can be committed.
			if err := group.Wait(); err != nil {
				return listOf(recordsToCommit), err
			}
		}
	}

	return listOf(recordsToCommit), nil
}

func (c *OrderedConsumer) run(ctx context.Context, client *kgo.Client) (err error) {
	defer func() {
		c.log(kgo.LogLevelDebug, "Stopping consumer", "err", err)
	}()

	batchSize := DefaultBatchSize

	if c.BatchSize != 0 {
		batchSize = c.BatchSize
	}

	for {
		// If the context has been cancelled on a new fetches iteration,
		// then avoid processing it and just return to the caller.
		select {
		case <-ctx.Done():
			return fmt.Errorf("kprocessor.Consumer.run: context done, %w", ctx.Err())
		default:
		}

		fetches := client.PollRecords(ctx, batchSize)

		if fetches.IsClientClosed() {
			return nil
		}

		c.log(kgo.LogLevelDebug, "fetched new messages batch")

		if err := fetches.Err(); err != nil {
			return fmt.Errorf("kprocessor.Consumer.run: error found in the fetches, %w", err)
		}

		// Here, the forwarded error can only happen from message consumption.
		// We need to commit records regardless.
		recordsToCommit, err := c.consumeFetches(ctx, fetches)

		c.log(kgo.LogLevelDebug, "marking records to commit")
		{
			client.MarkCommitRecords(recordsToCommit...)
		}
		c.log(kgo.LogLevelDebug, "finished committing records")

		if err != nil {
			return fmt.Errorf("kprocessor.Consumer.run: error while consuming batches, %w", err)
		}

		c.log(kgo.LogLevelDebug, "allow rebalance after consuming messages batch")
		{
			// NOTE: allow rebalance to occur on this Consumer Group member, as messages
			// have finished consumption.
			client.AllowRebalance()
		}
		c.log(kgo.LogLevelDebug, "client rebalance completed")
	}
}

// Run starts the Kafka consumer, and blocks until either the context gets cancelled,
// the Kafka client returns an error or the Processor fails processing.
//
// In case of an error, the Consumer will always try to commit the processed messages
// up to receiving an error.
//
// Message processing is parallelized over all partitions of the same topic
// assigned to the Consumer Group member, to maximize performance in light of heavy i/o
// operations performed by the given Processor.
func (c *OrderedConsumer) Run(ctx context.Context, options ...kgo.Opt) error {
	client, err := kgo.NewClient(append(options,
		kgo.ClientID(c.ClientID),
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.Topic),
		kgo.BlockRebalanceOnPoll(),
		kgo.WithLogger(c.Logger),
		kgo.AutoCommitMarks(),
	)...)

	if err != nil {
		return fmt.Errorf("kprocessor.Consumer: failed to create new kafka client, %w", err)
	}

	// Close client to make sure messages in the auto-commit buffer are committed
	// before returning from this method.
	defer func() {
		c.log(kgo.LogLevelDebug, "Closing kafka client")

		// NOTE: we need to allow rebalance one last time, in case the Consumer has been
		// stopped due to context cancellation, thus avoiding the call to allow rebalancing.
		// Not doing so would block the client indefinitely from closing.
		client.AllowRebalance()
		client.Close()

		c.log(kgo.LogLevelDebug, "Kafka client closed")
	}()

	return c.run(ctx, client)
}
