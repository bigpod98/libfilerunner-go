package internal

import (
	"context"
	"errors"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type s3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3Backend implements a file-queue workflow on top of S3 prefixes.
// Objects are claimed by copying from InputPrefix to InProgressPrefix, then deleting the source.
type S3Backend struct {
	Bucket           string
	InputPrefix      string
	InProgressPrefix string
	FailedPrefix     string
	ClaimDirs        bool
	client           s3API
}

// ClaimedS3Object represents an object currently parked in the in-progress prefix.
type ClaimedS3Object struct {
	bucket string
	name   string
	key    string
	isDir  bool
	client s3API
}

func NewS3Backend(region, bucket, inputPrefix, inProgressPrefix, failedPrefix string) (*S3Backend, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, errors.New("bucket is required")
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

	loadOptions := []func(*config.LoadOptions) error{}
	if strings.TrimSpace(region) != "" {
		loadOptions = append(loadOptions, config.WithRegion(region))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, err
	}

	return NewS3BackendFromClient(
		s3.NewFromConfig(awsCfg),
		bucket,
		inputPrefix,
		inProgressPrefix,
		failedPrefix,
	)
}

func NewS3BackendFromClient(client s3API, bucket, inputPrefix, inProgressPrefix, failedPrefix string) (*S3Backend, error) {
	if client == nil {
		return nil, errors.New("s3 client is required")
	}
	if strings.TrimSpace(bucket) == "" {
		return nil, errors.New("bucket is required")
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

	return &S3Backend{
		Bucket:           bucket,
		InputPrefix:      normalizePrefix(inputPrefix),
		InProgressPrefix: normalizePrefix(inProgressPrefix),
		FailedPrefix:     normalizePrefix(failedPrefix),
		client:           client,
	}, nil
}

// ClaimNext claims the next object from input to in-progress and returns a handle.
// Selection is lexicographic by object key and ignores key placeholders ending with '/'.
func (b *S3Backend) ClaimNext(ctx context.Context) (*ClaimedS3Object, error) {
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

		if err := b.copyObject(ctx, srcKey, dstKey); err != nil {
			if isNotFoundError(err) {
				// Source vanished between listing and claim; try the next one.
				continue
			}
			return nil, err
		}

		if err := b.deleteObject(ctx, srcKey); err != nil {
			return nil, err
		}

		return &ClaimedS3Object{
			bucket: b.Bucket,
			name:   path.Base(dstKey),
			key:    dstKey,
			client: b.client,
		}, nil
	}

	return nil, ErrNoFileAvailable
}

func (b *S3Backend) claimNextDirectory(ctx context.Context, keys []string) (*ClaimedS3Object, error) {
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
		return &ClaimedS3Object{
			bucket: b.Bucket,
			name:   path.Base(trimmed),
			key:    dstPrefix,
			isDir:  true,
			client: b.client,
		}, nil
	}

	return nil, ErrNoFileAvailable
}

func (b *S3Backend) CompleteClaim(ctx context.Context, inProgressKey string) error {
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
	return b.deleteObject(ctx, inProgressKey)
}

func (b *S3Backend) FailClaim(ctx context.Context, inProgressKey string) (string, error) {
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

	claimed := &ClaimedS3Object{
		bucket: b.Bucket,
		name:   path.Base(inProgressKey),
		key:    inProgressKey,
		client: b.client,
	}
	return claimed.MoveToFailed(ctx, b.FailedPrefix)
}

func (o *ClaimedS3Object) Name() string {
	return o.name
}

func (o *ClaimedS3Object) Path() string {
	return o.key
}

func (o *ClaimedS3Object) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.isDir {
		return nil, errors.New("cannot open directory claim")
	}
	res, err := o.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
	})
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (o *ClaimedS3Object) Delete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.isDir {
		b := &S3Backend{Bucket: o.bucket, client: o.client}
		return b.deleteAllWithPrefix(ctx, ensureTrailingSlash(o.key))
	}
	_, err := o.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
	})
	return err
}

// MoveToFailed moves the in-progress object into the failed prefix.
// If the target object already exists, a unique suffix is added.
func (o *ClaimedS3Object) MoveToFailed(ctx context.Context, failedPrefix string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	b := &S3Backend{
		Bucket:       o.bucket,
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

	if err := b.copyObject(ctx, o.key, dstKey); err != nil {
		return "", err
	}
	if err := b.deleteObject(ctx, o.key); err != nil {
		return "", err
	}

	o.key = dstKey
	o.name = path.Base(dstKey)
	return dstKey, nil
}

func (b *S3Backend) listInputKeys(ctx context.Context) ([]string, error) {
	return b.listKeysWithPrefix(ctx, b.InputPrefix)
}

func (b *S3Backend) listKeysWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	keys := make([]string, 0)
	var token *string

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			key := *obj.Key
			if key == prefix || strings.HasSuffix(key, "/") {
				continue
			}
			keys = append(keys, key)
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}

	sort.Strings(keys)
	return keys, nil
}

func (b *S3Backend) copyThenDeletePrefix(ctx context.Context, srcPrefix, dstPrefix string) (bool, error) {
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
		if err := b.copyObject(ctx, srcKey, dstKey); err != nil {
			if isNotFoundError(err) {
				return false, nil
			}
			return false, err
		}
		if err := b.deleteObject(ctx, srcKey); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (b *S3Backend) deleteAllWithPrefix(ctx context.Context, prefix string) error {
	keys, err := b.listKeysWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.deleteObject(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (b *S3Backend) uniqueDestinationPrefix(ctx context.Context, basePrefix string) (string, error) {
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

func (b *S3Backend) prefixExists(ctx context.Context, prefix string) (bool, error) {
	keys, err := b.listKeysWithPrefix(ctx, prefix)
	if err != nil {
		return false, err
	}
	return len(keys) > 0, nil
}

func firstLevelDirectorySuffixes(keys []string, inputPrefix string) []string {
	seen := make(map[string]struct{})
	dirs := make([]string, 0)
	for _, key := range keys {
		suffix := strings.TrimPrefix(key, inputPrefix)
		idx := strings.Index(suffix, "/")
		if idx <= 0 {
			continue
		}
		dir := suffix[:idx+1]
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	return dirs
}

func ensureTrailingSlash(key string) string {
	if strings.HasSuffix(key, "/") {
		return key
	}
	return key + "/"
}

func (b *S3Backend) uniqueDestinationKey(ctx context.Context, baseKey string) (string, error) {
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

func (b *S3Backend) objectExists(ctx context.Context, key string) (bool, error) {
	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	if isNotFoundError(err) {
		return false, nil
	}
	return false, err
}

func (b *S3Backend) copyObject(ctx context.Context, srcKey, dstKey string) error {
	copySource := b.Bucket + "/" + srcKey
	_, err := b.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(b.Bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(dstKey),
	})
	return err
}

func (b *S3Backend) deleteObject(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(key),
	})
	return err
}

func normalizePrefix(prefix string) string {
	trimmed := strings.TrimSpace(prefix)
	trimmed = strings.TrimLeft(trimmed, "/")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NotFound" || code == "NoSuchKey" || code == "NoSuchBucket"
	}

	return false
}
