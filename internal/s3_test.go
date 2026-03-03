package internal

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestS3BackendClaimNext_PicksLexicographicallyAndMovesToInProgress(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"input/b.txt":    []byte("b"),
		"input/a.txt":    []byte("a"),
		"input/subdir/":  []byte(""),
		"other/file.txt": []byte("x"),
	})

	backend, err := NewS3BackendFromClient(client, "bucket", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewS3BackendFromClient() error = %v", err)
	}

	claimed, err := backend.ClaimNext(context.Background())
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}

	if got, want := claimed.Name(), "a.txt"; got != want {
		t.Fatalf("claimed.Name() = %q, want %q", got, want)
	}
	if got, want := claimed.Path(), "in-progress/a.txt"; got != want {
		t.Fatalf("claimed.Path() = %q, want %q", got, want)
	}

	if client.has("input/a.txt") {
		t.Fatalf("expected input/a.txt to be moved")
	}
	if !client.has("in-progress/a.txt") {
		t.Fatalf("expected in-progress/a.txt to exist")
	}
}

func TestS3BackendClaimNext_NoFileAvailable(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"other/file.txt": []byte("x"),
	})

	backend, err := NewS3BackendFromClient(client, "bucket", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewS3BackendFromClient() error = %v", err)
	}

	_, err = backend.ClaimNext(context.Background())
	if err != ErrNoFileAvailable {
		t.Fatalf("ClaimNext() error = %v, want %v", err, ErrNoFileAvailable)
	}
}

func TestS3BackendClaimNext_DirectoryTargetModeClaimsDirectoryPrefix(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"input/dir-a/part1.bin": []byte("a1"),
		"input/dir-a/part2.bin": []byte("a2"),
		"input/single.txt":      []byte("single"),
	})

	backend, err := NewS3BackendFromClient(client, "bucket", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewS3BackendFromClient() error = %v", err)
	}
	backend.ClaimDirs = true

	claimed, err := backend.ClaimNext(context.Background())
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}

	if got, want := claimed.Path(), "in-progress/dir-a/"; got != want {
		t.Fatalf("claimed.Path() = %q, want %q", got, want)
	}
	if !client.has("in-progress/dir-a/part1.bin") || !client.has("in-progress/dir-a/part2.bin") {
		t.Fatalf("expected directory objects moved to in-progress")
	}
	if client.has("input/dir-a/part1.bin") || client.has("input/dir-a/part2.bin") {
		t.Fatalf("expected source directory objects removed from input")
	}
	if !client.has("input/single.txt") {
		t.Fatalf("expected root file to remain in input in directory-target mode")
	}
}

func TestS3BackendClaimNext_ConcurrentClaimersSingleWinner(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"input/job.txt": []byte("payload"),
	})

	backend, err := NewS3BackendFromClient(client, "bucket", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewS3BackendFromClient() error = %v", err)
	}

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, results[idx] = backend.ClaimNext(context.Background())
		}(i)
	}
	wg.Wait()

	successes := 0
	noFile := 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrNoFileAvailable):
			noFile++
		default:
			t.Fatalf("unexpected claim error = %v", err)
		}
	}
	if successes != 1 || noFile != 1 {
		t.Fatalf("claim outcomes success=%d no-file=%d, want 1 and 1", successes, noFile)
	}
}

func TestClaimedS3ObjectMoveToFailed_AddsUniqueSuffixOnCollision(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"in-progress/job.txt": []byte("payload"),
		"failed/job.txt":      []byte("existing"),
	})

	claimed := &ClaimedS3Object{
		bucket: "bucket",
		name:   "job.txt",
		key:    "in-progress/job.txt",
		client: client,
	}

	dst, err := claimed.MoveToFailed(context.Background(), "failed")
	if err != nil {
		t.Fatalf("MoveToFailed() error = %v", err)
	}

	if got, want := dst, "failed/job_1.txt"; got != want {
		t.Fatalf("moved key = %q, want %q", got, want)
	}
	if !client.has("failed/job_1.txt") {
		t.Fatalf("expected failed/job_1.txt to exist")
	}
	if client.has("in-progress/job.txt") {
		t.Fatalf("expected in-progress/job.txt to be removed")
	}
}

func TestClaimedS3ObjectOpenAndDelete(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"in-progress/job.txt": []byte("hello"),
	})

	claimed := &ClaimedS3Object{
		bucket: "bucket",
		name:   "job.txt",
		key:    "in-progress/job.txt",
		client: client,
	}

	rc, err := claimed.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got, want := string(b), "hello"; got != want {
		t.Fatalf("open data = %q, want %q", got, want)
	}

	if err := claimed.Delete(context.Background()); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if client.has("in-progress/job.txt") {
		t.Fatalf("expected object to be deleted")
	}
}

func TestClaimedS3ObjectOperations_RespectCanceledContext(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"in-progress/job.txt": []byte("hello"),
	})

	claimed := &ClaimedS3Object{
		bucket: "bucket",
		name:   "job.txt",
		key:    "in-progress/job.txt",
		client: client,
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := claimed.Open(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v, want %v", err, context.Canceled)
	}
	if err := claimed.Delete(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() error = %v, want %v", err, context.Canceled)
	}
	if _, err := claimed.MoveToFailed(canceledCtx, "failed"); !errors.Is(err, context.Canceled) {
		t.Fatalf("MoveToFailed() error = %v, want %v", err, context.Canceled)
	}

	if !client.has("in-progress/job.txt") {
		t.Fatalf("expected source object to remain")
	}
}

func TestS3BackendCompleteAndFailClaim(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"in-progress/a.txt": []byte("a"),
		"in-progress/b.txt": []byte("b"),
	})

	backend, err := NewS3BackendFromClient(client, "bucket", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewS3BackendFromClient() error = %v", err)
	}

	if err := backend.CompleteClaim(context.Background(), "in-progress/a.txt"); err != nil {
		t.Fatalf("CompleteClaim() error = %v", err)
	}
	if client.has("in-progress/a.txt") {
		t.Fatalf("expected completed object removed")
	}

	failedKey, err := backend.FailClaim(context.Background(), "in-progress/b.txt")
	if err != nil {
		t.Fatalf("FailClaim() error = %v", err)
	}
	if got, want := failedKey, "failed/b.txt"; got != want {
		t.Fatalf("failed key = %q, want %q", got, want)
	}
	if !client.has("failed/b.txt") {
		t.Fatalf("expected failed/b.txt to exist")
	}
}

func TestS3BackendCompleteAndFailClaim_DirectoryTargetMode(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{
		"in-progress/dir-a/part1.bin": []byte("a1"),
		"in-progress/dir-b/part1.bin": []byte("b1"),
	})

	backend, err := NewS3BackendFromClient(client, "bucket", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewS3BackendFromClient() error = %v", err)
	}
	backend.ClaimDirs = true

	if err := backend.CompleteClaim(context.Background(), "in-progress/dir-a/"); err != nil {
		t.Fatalf("CompleteClaim() error = %v", err)
	}
	if client.has("in-progress/dir-a/part1.bin") {
		t.Fatalf("expected completed directory objects removed")
	}

	failedKey, err := backend.FailClaim(context.Background(), "in-progress/dir-b/")
	if err != nil {
		t.Fatalf("FailClaim() error = %v", err)
	}
	if got, want := failedKey, "failed/dir-b/"; got != want {
		t.Fatalf("failed key = %q, want %q", got, want)
	}
	if !client.has("failed/dir-b/part1.bin") {
		t.Fatalf("expected failed directory objects to exist")
	}
}

type mockS3Client struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMockS3Client(objects map[string][]byte) *mockS3Client {
	cp := make(map[string][]byte, len(objects))
	for k, v := range objects {
		cp[k] = append([]byte(nil), v...)
	}
	return &mockS3Client{objects: cp}
}

func (m *mockS3Client) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	prefix := aws.ToString(params.Prefix)
	keys := make([]string, 0)
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	contents := make([]s3types.Object, 0, len(keys))
	for _, key := range keys {
		k := key
		etag := m.etagForLocked(key)
		contents = append(contents, s3types.Object{Key: &k, ETag: &etag})
	}

	out := &s3.ListObjectsV2Output{Contents: make([]s3types.Object, 0, len(contents))}
	for _, c := range contents {
		out.Contents = append(out.Contents, s3types.Object{Key: c.Key})
	}
	return out, nil
}

func (m *mockS3Client) CopyObject(_ context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	copySource := aws.ToString(params.CopySource)
	idx := strings.Index(copySource, "/")
	if idx < 0 || idx == len(copySource)-1 {
		return nil, mockAPIError{code: "InvalidArgument", message: "invalid copy source"}
	}
	srcKey := copySource[idx+1:]
	b, ok := m.objects[srcKey]
	if !ok {
		return nil, mockAPIError{code: "NoSuchKey", message: "source not found"}
	}
	if params.CopySourceIfMatch != nil {
		if got, want := m.etagForLocked(srcKey), aws.ToString(params.CopySourceIfMatch); got != want {
			return nil, mockAPIError{code: "PreconditionFailed", message: "source etag mismatch"}
		}
	}

	dst := aws.ToString(params.Key)
	if params.IfNoneMatch != nil && aws.ToString(params.IfNoneMatch) == "*" {
		if _, exists := m.objects[dst]; exists {
			return nil, mockAPIError{code: "PreconditionFailed", message: "destination already exists"}
		}
	}
	m.objects[dst] = append([]byte(nil), b...)
	return &s3.CopyObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := aws.ToString(params.Key)
	if params.IfMatch != nil {
		if _, exists := m.objects[key]; !exists {
			return nil, mockAPIError{code: "NoSuchKey", message: "object not found"}
		}
		if got, want := m.etagForLocked(key), aws.ToString(params.IfMatch); got != want {
			return nil, mockAPIError{code: "PreconditionFailed", message: "etag mismatch"}
		}
	}
	delete(m.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.objects[aws.ToString(params.Key)]; ok {
		return &s3.HeadObjectOutput{}, nil
	}
	return nil, mockAPIError{code: "NotFound", message: "object not found"}
}

func (m *mockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.objects[aws.ToString(params.Key)]
	if !ok {
		return nil, mockAPIError{code: "NoSuchKey", message: "object not found"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(append([]byte(nil), b...)))}, nil
}

func (m *mockS3Client) has(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok
}

func (m *mockS3Client) etagForLocked(key string) string {
	b := m.objects[key]
	sum := md5.Sum(b)
	return "\"" + hex.EncodeToString(sum[:]) + "\""
}

type mockAPIError struct {
	code    string
	message string
}

func (e mockAPIError) Error() string {
	return e.code + ": " + e.message
}

func (e mockAPIError) ErrorCode() string {
	return e.code
}

func (e mockAPIError) ErrorMessage() string {
	return e.message
}

func (e mockAPIError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}
