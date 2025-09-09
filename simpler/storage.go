// Package simpler provides a high-level interface for interacting with Google Cloud Storage.
package simpler

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

type StorageClient struct {
	client *storage.Client
}

// NewStorageClient creates a new StorageGateway instance.
func NewStorageClient(ctx context.Context) (*StorageClient, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating storage client: %w", err)
	}

	return &StorageClient{
		client: client,
	}, nil
}

// UploadFile uploads a file to the specified bucket.
func (c *StorageClient) UploadFile(ctx context.Context, bucket string, name string, content io.Reader) error {
	obj := c.client.Bucket(bucket).Object(name)
	wc := obj.NewWriter(ctx)

	if _, err := io.Copy(wc, content); err != nil {
		return fmt.Errorf("copying file to artifacts bucket: %w", err)
	}

	if err := wc.Close(); err != nil {
		return fmt.Errorf("closing bucket writer: %w", err)
	}

	return nil
}
