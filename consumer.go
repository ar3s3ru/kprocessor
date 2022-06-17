package kafkagoprocessor

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const defaultBatchSize = 128

type Consumer struct {
	Topic     string
	GroupID   string
	ClientID  string
	Processor Processor
}

func (c *Consumer) Run(ctx context.Context, options ...kgo.Opt) error {
	client, err := kgo.NewClient(append(options,
		kgo.ClientID(c.ClientID),
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.Topic),
	))

	if err != nil {
		return fmt.Errorf("Consumer: failed to create new kafka client, %w", err)
	}

	defer client.Close()

	return c.run(ctx, client)
}

func (c *Consumer) run(ctx context.Context, client *kgo.Client) error {
	for {
		fetches := client.PollFetches(ctx)

		if err := fetches.Err(); err != nil {
			return fmt.Errorf("Consumer.run: error found in the fetches: %w", err)
		}

		// Here, the forwarded error can only happen from message consumption.
		// We need to commit records regardless.
		recordsToCommit, err := c.consumeBatch(ctx, fetches)

		client.MarkCommitRecords(recordsToCommit...)

		if err != nil {
			return fmt.Errorf("Consumer.run: error while consuming batches: %w", err)
		}
	}
}

func (c *Consumer) consumeBatch(ctx context.Context, fetches kgo.Fetches) ([]*kgo.Record, error) {
	mx := new(sync.Mutex)
	var toCommit []*kgo.Record

	for _, fetch := range fetches {
		group, ctx := errgroup.WithContext(ctx)

		if len(fetch.Topics) != 1 {
			return nil, fmt.Errorf("Consumer.consumeBatch: batch has messages from more than one topic, topics: %#v", fetch.Topics)
		}

		topic := fetch.Topics[0]

		for _, partitions := range topic.Partitions {
			partitions := partitions
			group.Go(func() error {
				for _, record := range partitions.Records {
					if err := c.Processor.Process(ctx, record); err != nil {
						return err
					}

					mx.Lock()
					toCommit = append(toCommit, record)
					mx.Unlock()
				}

				return nil
			})
		}

		if err := group.Wait(); err != nil {
			return toCommit, err
		}
	}

	return toCommit, nil
}
