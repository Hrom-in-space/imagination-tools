// Package simpler provides a high-level interface for interacting with Google Cloud Storage.
package simpler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/hamba/avro/v2"
)

const (
	schemaRefKey = "schema_ref"
)

type StorageClient interface {
	// UploadFile uploads a file to the specified bucket.
	UploadFile(ctx context.Context, bucket string, name string, content io.Reader) error

	// UploadJSONSchematized uploads a JSON-serializable object to the specified bucket.
	// It validates the object against its Avro schema before upload and stores the schema
	// reference in the object's metadata for later validation during download.
	UploadJSONSchematized(ctx context.Context, bucket string, name string, object SchemaProvider) error

	// DownloadFile downloads a file from the specified bucket and returns its contents as bytes.
	DownloadFile(ctx context.Context, bucket, name string) ([]byte, error)

	// DownloadJSONSchematized downloads a JSON-serializable object from the specified bucket.
	// It validates that the stored schema reference matches the expected schema and
	// validates the downloaded object against the schema after unmarshaling.
	DownloadJSONSchematized(ctx context.Context, bucket, name string, object SchemaProvider) error
}

type storageClient struct {
	client *storage.Client
}

var _ StorageClient = (*storageClient)(nil)

// NewStorageClient creates a new StorageGateway instance.
func NewStorageClient(ctx context.Context) (StorageClient, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating storage client: %w", err)
	}

	return &storageClient{
		client: client,
	}, nil
}

type SchemaProvider interface {
	Schema() avro.Schema
}

// upload writes the provided content to the specified GCS object in bucket/name.
// If contentType is non-empty, it is set on the object. If metadata is non-nil,
// its key-value pairs are attached as user-defined object metadata.
// The function copies all bytes from content and ensures the writer is closed,
// returning any encountered write/close errors wrapped with context.
func (c *storageClient) upload(ctx context.Context, bucket, name string, content io.Reader, contentType string, metadata map[string]string) error {
	obj := c.client.Bucket(bucket).Object(name)
	wc := obj.NewWriter(ctx)

	if contentType != "" {
		wc.ContentType = contentType
	}
	if metadata != nil {
		wc.Metadata = metadata
	}

	if _, err := io.Copy(wc, content); err != nil {
		return fmt.Errorf("copying file to artifacts bucket: %w", err)
	}

	if err := wc.Close(); err != nil {
		return fmt.Errorf("closing bucket writer: %w", err)
	}

	return nil
}

// UploadJSONSchematized uploads a JSON-serializable object to the specified bucket.
// It also validates the object against the provided Avro schema.
// And add in bucket meta schema name
func (c *storageClient) UploadJSONSchematized(ctx context.Context, bucket string, name string, object SchemaProvider) error {
	// validate
	err := avro.NewEncoderForSchema(object.Schema(), io.Discard).Encode(object)
	if err != nil {
		return fmt.Errorf("validating object against schema: %w", err)
	}

	// to JSON
	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("marshaling object: %w", err)
	}

	return c.upload(ctx, bucket, name, bytes.NewReader(data), "application/json", map[string]string{
		schemaRefKey: object.(avro.NamedSchema).Name(),
	})
}

// UploadFile uploads a file to the specified bucket.
func (c *storageClient) UploadFile(ctx context.Context, bucket string, name string, content io.Reader) error {
	return c.upload(ctx, bucket, name, content, "", nil)
}

// readObject reads the full contents of the given GCS object and returns the bytes.
// It centralizes opening/closing the reader and wraps read errors with context.
// Callers should prefer this helper from Download* methods.
func (c *storageClient) readObject(ctx context.Context, obj *storage.ObjectHandle) ([]byte, error) {
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating reader: %w", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}
	return data, nil
}

// DownloadFile downloads a file from the specified bucket and returns its contents as bytes.
func (c *storageClient) DownloadFile(ctx context.Context, bucket, name string) ([]byte, error) {
	obj := c.client.Bucket(bucket).Object(name)
	return c.readObject(ctx, obj)
}

// DownloadJSONSchematized downloads a JSON-serializable object from the specified bucket.
// Also validate the object against the provided Avro schema.
func (c *storageClient) DownloadJSONSchematized(ctx context.Context, bucket, name string, object SchemaProvider) error {
	obj := c.client.Bucket(bucket).Object(name)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("getting attrs: %w", err)
	}

	schemaRef := attrs.Metadata[schemaRefKey]
	ns, ok := object.Schema().(avro.NamedSchema)
	if !ok || schemaRef != ns.Name() {
		return fmt.Errorf("schema mismatch or missing schema_ref: have=%q want=%q", schemaRef, ns.Name())
	}

	data, err := c.readObject(ctx, obj)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, object); err != nil {
		return fmt.Errorf("json: %w", err)
	}

	// validate schema
	if err := avro.NewEncoderForSchema(object.Schema(), io.Discard).Encode(object); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	return nil
}
