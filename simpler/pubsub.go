// Package simpler provides a high-level interface for interacting with Google Cloud Pub/Sub.
package simpler

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
)

// PubSubClient handles Google Cloud Pub/Sub operations.
type PubSubClient struct {
	client *pubsub.Client
}

// NewPubSubClient creates a new PubSubGateway instance.
func NewPubSubClient(ctx context.Context, projectID string) (*PubSubClient, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating pubsub client: %w", err)
	}

	return &PubSubClient{
		client: client,
	}, nil
}

// PublishMessage publishes a message to the configured topic.
func (c *PubSubClient) PublishMessage(ctx context.Context, topicID string, object any) error {
	topic := c.client.Topic(topicID)

	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("error marshaling message: %w", err)
	}

	msg := &pubsub.Message{
		Data: data,
	}

	result := topic.Publish(ctx, msg)

	// Wait for the publish to complete
	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("publishing message to topic %s: %w", topicID, err)
	}

	return nil
}
