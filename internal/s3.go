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
	client           s3API
}

// ClaimedS3Object represents an object currently parked in the in-progress prefix.
type ClaimedS3Object struct {
	bucket string
	name   string
	key    string
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

func (o *ClaimedS3Object) Name() string {
	return o.name
}

func (o *ClaimedS3Object) Path() string {
	return o.key
}

func (o *ClaimedS3Object) Open() (io.ReadCloser, error) {
	res, err := o.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
	})
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (o *ClaimedS3Object) Delete() error {
	_, err := o.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
	})
	return err
}

// MoveToFailed moves the in-progress object into the failed prefix.
// If the target object already exists, a unique suffix is added.
func (o *ClaimedS3Object) MoveToFailed(failedPrefix string) (string, error) {
	b := &S3Backend{
		Bucket:       o.bucket,
		FailedPrefix: normalizePrefix(failedPrefix),
		client:       o.client,
	}

	base := b.FailedPrefix + path.Base(o.name)
	dstKey, err := b.uniqueDestinationKey(context.Background(), base)
	if err != nil {
		return "", err
	}

	if err := b.copyObject(context.Background(), o.key, dstKey); err != nil {
		return "", err
	}
	if err := b.deleteObject(context.Background(), o.key); err != nil {
		return "", err
	}

	o.key = dstKey
	o.name = path.Base(dstKey)
	return dstKey, nil
}

func (b *S3Backend) listInputKeys(ctx context.Context) ([]string, error) {
	keys := make([]string, 0)
	var token *string

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.Bucket),
			Prefix:            aws.String(b.InputPrefix),
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
			if key == b.InputPrefix || strings.HasSuffix(key, "/") {
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
