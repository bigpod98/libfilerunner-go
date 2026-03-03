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
	ClaimDirs        bool
	client           azureBlobAPI
}

// ClaimedAzureBlob represents a blob currently parked in the in-progress prefix.
type ClaimedAzureBlob struct {
	container string
	name      string
	key       string
	isDir     bool
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
	if err := validateNonOverlappingPrefixes(inputPrefix, inProgressPrefix, failedPrefix); err != nil {
		return nil, err
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

	if b.ClaimDirs {
		return b.claimNextDirectory(ctx, keys)
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

func (b *AzureBlobBackend) claimNextDirectory(ctx context.Context, keys []string) (*ClaimedAzureBlob, error) {
	dirSuffixes := firstLevelDirectorySuffixes(keys, b.InputPrefix)
	for _, dirSuffix := range dirSuffixes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		srcPrefix := b.InputPrefix + dirSuffix
		dstPrefix, err := b.uniqueDestinationPrefix(ctx, b.InProgressPrefix+dirSuffix)
		if err != nil {
			return nil, err
		}

		claimedAny, err := b.copyThenDeletePrefix(ctx, srcPrefix, dstPrefix)
		if err != nil {
			return nil, err
		}
		if !claimedAny {
			continue
		}

		trimmed := strings.TrimSuffix(dstPrefix, "/")
		return &ClaimedAzureBlob{
			container: b.Container,
			name:      path.Base(trimmed),
			key:       dstPrefix,
			isDir:     true,
			client:    b.client,
		}, nil
	}

	return nil, ErrNoFileAvailable
}

func (b *AzureBlobBackend) CompleteClaim(ctx context.Context, inProgressKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(inProgressKey) == "" {
		return errors.New("in-progress key is required")
	}
	if !strings.HasPrefix(inProgressKey, b.InProgressPrefix) {
		return errors.New("in-progress key is not in the configured in-progress prefix")
	}
	if b.ClaimDirs {
		return b.deleteAllWithPrefix(ctx, ensureTrailingSlash(inProgressKey))
	}
	return b.deleteBlob(ctx, inProgressKey)
}

func (b *AzureBlobBackend) FailClaim(ctx context.Context, inProgressKey string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if strings.TrimSpace(inProgressKey) == "" {
		return "", errors.New("in-progress key is required")
	}
	if !strings.HasPrefix(inProgressKey, b.InProgressPrefix) {
		return "", errors.New("in-progress key is not in the configured in-progress prefix")
	}
	if b.ClaimDirs {
		srcPrefix := ensureTrailingSlash(inProgressKey)
		baseName := path.Base(strings.TrimSuffix(srcPrefix, "/"))
		dstPrefix, err := b.uniqueDestinationPrefix(ctx, b.FailedPrefix+baseName+"/")
		if err != nil {
			return "", err
		}
		if _, err := b.copyThenDeletePrefix(ctx, srcPrefix, dstPrefix); err != nil {
			return "", err
		}
		return dstPrefix, nil
	}

	claimed := &ClaimedAzureBlob{
		container: b.Container,
		name:      path.Base(inProgressKey),
		key:       inProgressKey,
		client:    b.client,
	}
	return claimed.MoveToFailed(ctx, b.FailedPrefix)
}

func (o *ClaimedAzureBlob) Name() string {
	return o.name
}

func (o *ClaimedAzureBlob) Path() string {
	return o.key
}

func (o *ClaimedAzureBlob) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.isDir {
		return nil, errors.New("cannot open directory claim")
	}
	return o.client.OpenBlob(ctx, o.key)
}

func (o *ClaimedAzureBlob) Delete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.isDir {
		b := &AzureBlobBackend{client: o.client}
		return b.deleteAllWithPrefix(ctx, ensureTrailingSlash(o.key))
	}
	return o.client.DeleteBlob(ctx, o.key)
}

// MoveToFailed moves the in-progress blob into the failed prefix.
// If the target blob already exists, a unique suffix is added.
func (o *ClaimedAzureBlob) MoveToFailed(ctx context.Context, failedPrefix string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	b := &AzureBlobBackend{
		Container:    o.container,
		FailedPrefix: normalizePrefix(failedPrefix),
		client:       o.client,
	}

	if o.isDir {
		basePrefix := b.FailedPrefix + path.Base(strings.TrimSuffix(o.key, "/")) + "/"
		dstPrefix, err := b.uniqueDestinationPrefix(ctx, basePrefix)
		if err != nil {
			return "", err
		}
		if _, err := b.copyThenDeletePrefix(ctx, ensureTrailingSlash(o.key), dstPrefix); err != nil {
			return "", err
		}
		o.key = dstPrefix
		o.name = path.Base(strings.TrimSuffix(dstPrefix, "/"))
		return dstPrefix, nil
	}

	base := b.FailedPrefix + path.Base(o.name)
	dstKey, err := b.uniqueDestinationKey(ctx, base)
	if err != nil {
		return "", err
	}

	if err := b.copyBlob(ctx, o.key, dstKey); err != nil {
		return "", err
	}
	if err := b.deleteBlob(ctx, o.key); err != nil {
		return "", err
	}

	o.key = dstKey
	o.name = path.Base(dstKey)
	return dstKey, nil
}

func (b *AzureBlobBackend) listInputKeys(ctx context.Context) ([]string, error) {
	return b.listKeysWithPrefix(ctx, b.InputPrefix)
}

func (b *AzureBlobBackend) listKeysWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	keys, err := b.client.ListBlobNames(ctx, prefix)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == prefix || strings.HasSuffix(key, "/") {
			continue
		}
		filtered = append(filtered, key)
	}

	sort.Strings(filtered)
	return filtered, nil
}

func (b *AzureBlobBackend) copyThenDeletePrefix(ctx context.Context, srcPrefix, dstPrefix string) (bool, error) {
	keys, err := b.listKeysWithPrefix(ctx, srcPrefix)
	if err != nil {
		return false, err
	}
	if len(keys) == 0 {
		return false, nil
	}

	for _, srcKey := range keys {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		rel := strings.TrimPrefix(srcKey, srcPrefix)
		dstKey := dstPrefix + rel
		if err := b.copyBlob(ctx, srcKey, dstKey); err != nil {
			if isAzureNotFoundError(err) {
				return false, nil
			}
			return false, err
		}
		if err := b.deleteBlob(ctx, srcKey); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (b *AzureBlobBackend) deleteAllWithPrefix(ctx context.Context, prefix string) error {
	keys, err := b.listKeysWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.deleteBlob(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (b *AzureBlobBackend) uniqueDestinationPrefix(ctx context.Context, basePrefix string) (string, error) {
	basePrefix = ensureTrailingSlash(basePrefix)
	exists, err := b.prefixExists(ctx, basePrefix)
	if err != nil {
		return "", err
	}
	if !exists {
		return basePrefix, nil
	}

	trimmed := strings.TrimSuffix(basePrefix, "/")
	dir, file := path.Split(trimmed)
	for i := 1; ; i++ {
		candidate := dir + file + "_" + itoa(i) + "/"
		exists, err := b.prefixExists(ctx, candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
}

func (b *AzureBlobBackend) prefixExists(ctx context.Context, prefix string) (bool, error) {
	keys, err := b.listKeysWithPrefix(ctx, prefix)
	if err != nil {
		return false, err
	}
	return len(keys) > 0, nil
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
