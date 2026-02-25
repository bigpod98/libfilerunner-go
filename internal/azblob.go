package internal

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type azureBlobAPI interface {
	ListBlobNames(ctx context.Context, prefix string) ([]string, error)
	CopyBlob(ctx context.Context, srcKey, dstKey string) error
	DeleteBlob(ctx context.Context, key string) error
	BlobExists(ctx context.Context, key string) (bool, error)
	OpenBlob(ctx context.Context, key string) (io.ReadCloser, error)
}

type azureBlobClient struct {
	container string
	client    *azblob.Client
}

func (c *azureBlobClient) ListBlobNames(ctx context.Context, prefix string) ([]string, error) {
	pager := c.client.NewListBlobsFlatPager(c.container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	keys := make([]string, 0)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil {
				continue
			}
			keys = append(keys, *item.Name)
		}
	}

	return keys, nil
}

func (c *azureBlobClient) CopyBlob(ctx context.Context, srcKey, dstKey string) error {
	containerClient := c.client.ServiceClient().NewContainerClient(c.container)
	srcURL := containerClient.NewBlobClient(srcKey).URL()
	_, err := containerClient.NewBlobClient(dstKey).CopyFromURL(ctx, srcURL, nil)
	return err
}

func (c *azureBlobClient) DeleteBlob(ctx context.Context, key string) error {
	_, err := c.client.DeleteBlob(ctx, c.container, key, nil)
	return err
}

func (c *azureBlobClient) BlobExists(ctx context.Context, key string) (bool, error) {
	containerClient := c.client.ServiceClient().NewContainerClient(c.container)
	_, err := containerClient.NewBlobClient(key).GetProperties(ctx, nil)
	if err == nil {
		return true, nil
	}
	if isAzureNotFoundError(err) {
		return false, nil
	}
	return false, err
}

func (c *azureBlobClient) OpenBlob(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := c.client.DownloadStream(ctx, c.container, key, nil)
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// AzureBlobBackend implements a file-queue workflow on top of Azure Blob prefixes.
// Blobs are claimed by copying from InputPrefix to InProgressPrefix, then deleting the source.
type AzureBlobBackend struct {
	Container        string
	InputPrefix      string
	InProgressPrefix string
	FailedPrefix     string
	client           azureBlobAPI
}

// ClaimedAzureBlob represents a blob currently parked in the in-progress prefix.
type ClaimedAzureBlob struct {
	container string
	name      string
	key       string
	client    azureBlobAPI
}

func NewAzureBlobBackend(accountURL, container, inputPrefix, inProgressPrefix, failedPrefix string) (*AzureBlobBackend, error) {
	if strings.TrimSpace(accountURL) == "" {
		return nil, errors.New("account URL is required")
	}
	if strings.TrimSpace(container) == "" {
		return nil, errors.New("container is required")
	}
	if strings.TrimSpace(inputPrefix) == "" {
		return nil, errors.New("input prefix is required")
	}
	if strings.TrimSpace(inProgressPrefix) == "" {
		return nil, errors.New("in-progress prefix is required")
	}
	if strings.TrimSpace(failedPrefix) == "" {
		return nil, errors.New("failed prefix is required")
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := azblob.NewClient(accountURL, cred, nil)
	if err != nil {
		return nil, err
	}

	return NewAzureBlobBackendFromClient(
		&azureBlobClient{container: container, client: client},
		container,
		inputPrefix,
		inProgressPrefix,
		failedPrefix,
	)
}

func NewAzureBlobBackendFromClient(client azureBlobAPI, container, inputPrefix, inProgressPrefix, failedPrefix string) (*AzureBlobBackend, error) {
	if client == nil {
		return nil, errors.New("azure blob client is required")
	}
	if strings.TrimSpace(container) == "" {
		return nil, errors.New("container is required")
	}
	if strings.TrimSpace(inputPrefix) == "" {
		return nil, errors.New("input prefix is required")
	}
	if strings.TrimSpace(inProgressPrefix) == "" {
		return nil, errors.New("in-progress prefix is required")
	}
	if strings.TrimSpace(failedPrefix) == "" {
		return nil, errors.New("failed prefix is required")
	}

	return &AzureBlobBackend{
		Container:        container,
		InputPrefix:      normalizePrefix(inputPrefix),
		InProgressPrefix: normalizePrefix(inProgressPrefix),
		FailedPrefix:     normalizePrefix(failedPrefix),
		client:           client,
	}, nil
}

// ClaimNext claims the next blob from input to in-progress and returns a handle.
// Selection is lexicographic by blob name and ignores key placeholders ending with '/'.
func (b *AzureBlobBackend) ClaimNext(ctx context.Context) (*ClaimedAzureBlob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	keys, err := b.listInputKeys(ctx)
	if err != nil {
		return nil, err
	}

	for _, srcKey := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		suffix := strings.TrimPrefix(srcKey, b.InputPrefix)
		if suffix == "" {
			continue
		}

		dstBase := b.InProgressPrefix + suffix
		dstKey, err := b.uniqueDestinationKey(ctx, dstBase)
		if err != nil {
			return nil, err
		}

		if err := b.copyBlob(ctx, srcKey, dstKey); err != nil {
			if isAzureNotFoundError(err) {
				// Source vanished between listing and claim; try the next one.
				continue
			}
			return nil, err
		}

		if err := b.deleteBlob(ctx, srcKey); err != nil {
			return nil, err
		}

		return &ClaimedAzureBlob{
			container: b.Container,
			name:      path.Base(dstKey),
			key:       dstKey,
			client:    b.client,
		}, nil
	}

	return nil, ErrNoFileAvailable
}

func (o *ClaimedAzureBlob) Name() string {
	return o.name
}

func (o *ClaimedAzureBlob) Path() string {
	return o.key
}

func (o *ClaimedAzureBlob) Open() (io.ReadCloser, error) {
	return o.client.OpenBlob(context.Background(), o.key)
}

func (o *ClaimedAzureBlob) Delete() error {
	return o.client.DeleteBlob(context.Background(), o.key)
}

// MoveToFailed moves the in-progress blob into the failed prefix.
// If the target blob already exists, a unique suffix is added.
func (o *ClaimedAzureBlob) MoveToFailed(failedPrefix string) (string, error) {
	b := &AzureBlobBackend{
		Container:    o.container,
		FailedPrefix: normalizePrefix(failedPrefix),
		client:       o.client,
	}

	base := b.FailedPrefix + path.Base(o.name)
	dstKey, err := b.uniqueDestinationKey(context.Background(), base)
	if err != nil {
		return "", err
	}

	if err := b.copyBlob(context.Background(), o.key, dstKey); err != nil {
		return "", err
	}
	if err := b.deleteBlob(context.Background(), o.key); err != nil {
		return "", err
	}

	o.key = dstKey
	o.name = path.Base(dstKey)
	return dstKey, nil
}

func (b *AzureBlobBackend) listInputKeys(ctx context.Context) ([]string, error) {
	keys, err := b.client.ListBlobNames(ctx, b.InputPrefix)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == b.InputPrefix || strings.HasSuffix(key, "/") {
			continue
		}
		filtered = append(filtered, key)
	}

	sort.Strings(filtered)
	return filtered, nil
}

func (b *AzureBlobBackend) uniqueDestinationKey(ctx context.Context, baseKey string) (string, error) {
	exists, err := b.objectExists(ctx, baseKey)
	if err != nil {
		return "", err
	}
	if !exists {
		return baseKey, nil
	}

	dir, file := path.Split(baseKey)
	ext := path.Ext(file)
	stem := strings.TrimSuffix(file, ext)

	for i := 1; ; i++ {
		candidate := dir + stem + "_" + itoa(i) + ext
		exists, err := b.objectExists(ctx, candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
}

func (b *AzureBlobBackend) objectExists(ctx context.Context, key string) (bool, error) {
	return b.client.BlobExists(ctx, key)
}

func (b *AzureBlobBackend) copyBlob(ctx context.Context, srcKey, dstKey string) error {
	return b.client.CopyBlob(ctx, srcKey, dstKey)
}

func (b *AzureBlobBackend) deleteBlob(ctx context.Context, key string) error {
	return b.client.DeleteBlob(ctx, key)
}

func isAzureNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusNotFound
	}

	return false
}
