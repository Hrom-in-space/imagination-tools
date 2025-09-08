// Package econ provides helper types and functions for working with
// CloudEvents that wrap Google Cloud Pub/Sub messages.
// It helps decode the CloudEvent data payload into your own Go structs.
package econ

import (
	"encoding/json"
	"fmt"

	"github.com/cloudevents/sdk-go/v2/event"
)

// PubsubMessage represents the inner Pub/Sub message payload as defined by
// google.events.cloud.pubsub.v1.PubsubMessage (fields included here are
// limited to what we currently need for decoding business payloads).
// When unmarshaled from JSON, Data is automatically base64-decoded.
type PubsubMessage struct {
	Attributes map[string]string `json:"attributes"`
	Data       []byte            `json:"data"`
}

// MessagePublishedData is the CloudEvent data wrapper defined by
// google.events.cloud.pubsub.v1.MessagePublishedData, containing the
// published PubsubMessage. Only the `message` field is modeled here.
type MessagePublishedData struct {
	Message PubsubMessage `json:"message"`
}

// EventToStruct extracts the Pub/Sub message from a CloudEvent and
// unmarshals its JSON data into v.
// v must be a pointer to a struct or other JSON-decodable value.
// Returns an error if the CloudEvent payload cannot be parsed or if
// the data cannot be unmarshaled into v.
func EventToStruct(e event.Event, v any) error {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("failed to parse pubsub message wrapper: %w", err)
	}

	if err := json.Unmarshal(msg.Message.Data, v); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}
