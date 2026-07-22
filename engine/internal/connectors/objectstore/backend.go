package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/minio/minio-go/v7"
)

// objectInfo is the backend-agnostic listing entry for one object.
type objectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// errStopList lets a list callback halt iteration cleanly (e.g. after the first
// object), without surfacing as an error.
var errStopList = errors.New("stop listing")

// backend abstracts an object store behind the three operations the connector
// needs, so one connector serves both S3-compatible stores and Azure Blob.
type backend interface {
	// exists reports whether the target bucket/container is reachable.
	exists(ctx context.Context) (bool, error)
	// list calls fn for each object under the prefix (directory placeholders
	// excluded); returning errStopList halts iteration cleanly.
	list(ctx context.Context, fn func(objectInfo) error) error
	// open returns a reader over one object's bytes.
	open(ctx context.Context, key string) (io.ReadCloser, error)
}

// --- S3-compatible backend (Amazon S3, GCS interop, MinIO) via minio-go ---

type s3Backend struct {
	client *minio.Client
	bucket string
	prefix string
}

func (b *s3Backend) exists(ctx context.Context) (bool, error) {
	return b.client.BucketExists(ctx, b.bucket)
}

func (b *s3Backend) list(ctx context.Context, fn func(objectInfo) error) error {
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for o := range b.client.ListObjects(listCtx, b.bucket, minio.ListObjectsOptions{Prefix: b.prefix, Recursive: true}) {
		if o.Err != nil {
			return fmt.Errorf("list objects: %w", o.Err)
		}
		if strings.HasSuffix(o.Key, "/") {
			continue
		}
		if err := fn(objectInfo{Key: o.Key, Size: o.Size, LastModified: o.LastModified}); err != nil {
			if errors.Is(err, errStopList) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (b *s3Backend) open(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := b.client.GetObject(ctx, b.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// --- Azure Blob backend via the azblob SDK ---

type azureBackend struct {
	client    *azblob.Client
	container string
	prefix    string
}

func (b *azureBackend) exists(ctx context.Context) (bool, error) {
	_, err := b.client.ServiceClient().NewContainerClient(b.container).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *azureBackend) list(ctx context.Context, fn func(objectInfo) error) error {
	var prefix *string
	if b.prefix != "" {
		prefix = &b.prefix
	}
	pager := b.client.NewListBlobsFlatPager(b.container, &azblob.ListBlobsFlatOptions{Prefix: prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list blobs: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil || strings.HasSuffix(*item.Name, "/") {
				continue
			}
			info := objectInfo{Key: *item.Name}
			if p := item.Properties; p != nil {
				if p.ContentLength != nil {
					info.Size = *p.ContentLength
				}
				if p.LastModified != nil {
					info.LastModified = *p.LastModified
				}
			}
			if err := fn(info); err != nil {
				if errors.Is(err, errStopList) {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (b *azureBackend) open(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := b.client.DownloadStream(ctx, b.container, key, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
