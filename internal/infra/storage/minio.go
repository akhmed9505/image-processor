// Package storage implements private MinIO object storage.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrObjectNotFound identifies an absent object without hiding the S3 error.
var ErrObjectNotFound = errors.New("storage object not found")

// ObjectInfo describes a stored object; ETag is not necessarily a content hash.
// ContentType is populated by Get and Stat.
type ObjectInfo struct {
	Key, ContentType, ETag string
	Size                   int64
	LastModified           time.Time
}

// Page contains at most the requested number of objects and a key cursor.
type Page struct {
	Objects    []ObjectInfo
	NextCursor string
}

// Storage streams objects from a private bucket.
type Storage struct {
	client    *minio.Client
	bucket    string
	transport *http.Transport
}

// New checks access and creates the bucket if absent; existing public policies are rejected.
func New(ctx context.Context, cfg config.MinIO) (*Storage, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	transport, err := minio.DefaultTransport(cfg.Secure)
	if err != nil {
		return nil, fmt.Errorf("create storage transport: %w", err)
	}
	ready := false
	defer func() {
		if !ready {
			transport.CloseIdleConnections()
		}
	}()
	client, err := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: cfg.Secure, Transport: transport})
	if err != nil {
		return nil, fmt.Errorf("create storage client: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check storage bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil && minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
			return nil, fmt.Errorf("create storage bucket: %w", err)
		}
	}
	policy, err := client.GetBucketPolicy(ctx, cfg.Bucket)
	if err != nil && minio.ToErrorResponse(err).Code != "NoSuchBucketPolicy" {
		return nil, fmt.Errorf("check bucket privacy: %w", err)
	}
	if policy != "" {
		return nil, errors.New("storage bucket must have no public access policy")
	}
	ready = true
	return &Storage{client, cfg.Bucket, transport}, nil
}

// Close releases idle HTTP connections; callers must close outstanding readers.
func (s *Storage) Close() { s.transport.CloseIdleConnections() }

// Put streams data; size may be -1 for an unknown length. It does not close reader.
func (s *Storage) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	if reader == nil || size < -1 {
		return errors.New("invalid storage upload input")
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, size, minio.PutObjectOptions{ContentType: contentType, PartSize: 8 * 1024 * 1024, NumThreads: 1})
	return storageError("put object", err)
}

// Get opens a stream and metadata. The caller must close the stream.
func (s *Storage) Get(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	body, info, _, err := (minio.Core{Client: s.client}).GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, ObjectInfo{}, storageError("get object", err)
	}
	return body, objectInfo(info), nil
}

// OpenOriginal opens the processing input, translating absence to the service contract.
func (s *Storage) OpenOriginal(ctx context.Context, key string) (io.ReadCloser, error) {
	body, _, err := s.Get(ctx, key)
	if errors.Is(err, ErrObjectNotFound) {
		return nil, fmt.Errorf("%w: %w", processor.ErrOriginalNotFound, err)
	}
	return body, err
}

// Stat returns metadata or ErrObjectNotFound.
func (s *Storage) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return ObjectInfo{}, storageError("stat object", err)
	}
	return objectInfo(info), nil
}

// Delete is idempotent for absent objects; bucket and transport errors are preserved.
func (s *Storage) Delete(ctx context.Context, key string) error {
	return storageError("delete object", s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}))
}

// List scans recursively by prefix; an empty NextCursor ends the pass.
func (s *Storage) List(ctx context.Context, prefix, afterKey string, limit int) (Page, error) {
	if limit < 1 || limit > 1000 {
		return Page{}, errors.New("storage list limit must be between 1 and 1000")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	page := Page{Objects: make([]ObjectInfo, 0, limit)}
	for info := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, StartAfter: afterKey, Recursive: true, MaxKeys: limit}) {
		if info.Err != nil {
			return Page{}, storageError("list objects", info.Err)
		}
		if len(page.Objects) == limit {
			page.NextCursor = page.Objects[limit-1].Key
			return page, nil
		}
		page.Objects = append(page.Objects, objectInfo(info))
	}
	if err := ctx.Err(); err != nil {
		return Page{}, fmt.Errorf("list objects: %w", err)
	}
	return page, nil
}

func objectInfo(info minio.ObjectInfo) ObjectInfo {
	return ObjectInfo{info.Key, info.ContentType, info.ETag, info.Size, info.LastModified}
}
func storageError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return fmt.Errorf("%s: %w: %w", operation, ErrObjectNotFound, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
