package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// EnsureTopic creates topic if it does not exist yet. docker compose runs
// the equivalent through the kafka-init service; this is for tests and
// host-side tooling that talk to a broker directly.
func EnsureTopic(
	ctx context.Context,
	brokers []string,
	topic string,
	partitions int,
	timeout time.Duration,
) error {
	client := &kafkago.Client{
		Addr:    kafkago.TCP(brokers...),
		Timeout: timeout,
	}

	response, err := client.CreateTopics(
		ctx,
		&kafkago.CreateTopicsRequest{
			Topics: []kafkago.TopicConfig{
				{
					Topic:             topic,
					NumPartitions:     partitions,
					ReplicationFactor: 1,
				},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}

	topicErr := response.Errors[topic]

	if topicErr != nil &&
		!errors.Is(topicErr, kafkago.TopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w", topic, topicErr)
	}

	return nil
}

// WaitTopicReady polls metadata until every partition of topic has a
// leader. Right after creation the broker can briefly answer produces
// with "leader not available"; waiting here keeps that startup noise out
// of tests and benchmarks.
func WaitTopicReady(
	ctx context.Context,
	brokers []string,
	topic string,
	timeout time.Duration,
) error {
	client := &kafkago.Client{
		Addr:    kafkago.TCP(brokers...),
		Timeout: 5 * time.Second,
	}

	deadline := time.Now().Add(timeout)

	for {
		ready, err := topicReady(ctx, client, topic)

		if err == nil && ready {
			return nil
		}

		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf(
					"topic %s not ready within %s: %w",
					topic,
					timeout,
					err,
				)
			}

			return fmt.Errorf(
				"topic %s not ready within %s",
				topic,
				timeout,
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func topicReady(
	ctx context.Context,
	client *kafkago.Client,
	topic string,
) (bool, error) {
	response, err := client.Metadata(
		ctx,
		&kafkago.MetadataRequest{
			Topics: []string{topic},
		},
	)
	if err != nil {
		return false, err
	}

	for _, t := range response.Topics {
		if t.Name != topic {
			continue
		}

		if t.Error != nil {
			return false, t.Error
		}

		if len(t.Partitions) == 0 {
			return false, nil
		}

		for _, partition := range t.Partitions {
			if partition.Error != nil || partition.Leader.ID < 0 {
				return false, nil
			}
		}

		return true, nil
	}

	return false, nil
}

// DeleteTopic removes a topic; tests use it to clean up what they created.
func DeleteTopic(
	ctx context.Context,
	brokers []string,
	topic string,
	timeout time.Duration,
) error {
	client := &kafkago.Client{
		Addr:    kafkago.TCP(brokers...),
		Timeout: timeout,
	}

	response, err := client.DeleteTopics(
		ctx,
		&kafkago.DeleteTopicsRequest{
			Topics: []string{topic},
		},
	)
	if err != nil {
		return fmt.Errorf("delete topic %s: %w", topic, err)
	}

	if topicErr := response.Errors[topic]; topicErr != nil {
		return fmt.Errorf("delete topic %s: %w", topic, topicErr)
	}

	return nil
}
