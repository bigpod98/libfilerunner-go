package internal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

func TestAzureBlobBackendClaimNext_PicksLexicographicallyAndMovesToInProgress(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"input/b.txt":    []byte("b"),
		"input/a.txt":    []byte("a"),
		"input/subdir/":  []byte(""),
		"other/file.txt": []byte("x"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
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

func TestAzureBlobBackendClaimNext_NoFileAvailable(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"other/file.txt": []byte("x"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
	}

	_, err = backend.ClaimNext(context.Background())
	if err != ErrNoFileAvailable {
		t.Fatalf("ClaimNext() error = %v, want %v", err, ErrNoFileAvailable)
	}
}

func TestAzureBlobBackendClaimNext_DirectoryTargetModeClaimsDirectoryPrefix(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"input/dir-a/part1.bin": []byte("a1"),
		"input/dir-a/part2.bin": []byte("a2"),
		"input/single.txt":      []byte("single"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
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
		t.Fatalf("expected directory blobs moved to in-progress")
	}
	if client.has("input/dir-a/part1.bin") || client.has("input/dir-a/part2.bin") {
		t.Fatalf("expected source directory blobs removed from input")
	}
	if !client.has("input/single.txt") {
		t.Fatalf("expected root blob to remain in input in directory-target mode")
	}
}

func TestAzureBlobBackendClaimNext_ConcurrentClaimersSingleWinner(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"input/job.txt": []byte("payload"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
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

func TestAzureBlobBackendListQueueItemNames(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"input/a.txt": []byte("a"),
		"input/b.txt": []byte("b"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
	}

	names, err := backend.ListQueueItemNames(context.Background())
	if err != nil {
		t.Fatalf("ListQueueItemNames() error = %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("len(names) = %d, want 2", len(names))
	}
}

func TestAzureBlobBackendListQueueItemNames_DirectoryMode(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"input/dir-a/part1.bin": []byte("a1"),
		"input/dir-b/part1.bin": []byte("b1"),
		"input/single.txt":      []byte("single"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
	}
	backend.ClaimDirs = true

	names, err := backend.ListQueueItemNames(context.Background())
	if err != nil {
		t.Fatalf("ListQueueItemNames() error = %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("len(names) = %d, want 2", len(names))
	}
}

func TestAzureBlobBackendListInProgressItemNames(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"in-progress/a.txt": []byte("a"),
		"in-progress/b.txt": []byte("b"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
	}

	names, err := backend.ListInProgressItemNames(context.Background())
	if err != nil {
		t.Fatalf("ListInProgressItemNames() error = %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("len(names) = %d, want 2", len(names))
	}
}

func TestClaimedAzureBlobMoveToFailed_AddsUniqueSuffixOnCollision(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"in-progress/job.txt": []byte("payload"),
		"failed/job.txt":      []byte("existing"),
	})

	claimed := &ClaimedAzureBlob{
		container: "container",
		name:      "job.txt",
		key:       "in-progress/job.txt",
		client:    client,
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

func TestClaimedAzureBlobOpenAndDelete(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"in-progress/job.txt": []byte("hello"),
	})

	claimed := &ClaimedAzureBlob{
		container: "container",
		name:      "job.txt",
		key:       "in-progress/job.txt",
		client:    client,
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
		t.Fatalf("expected blob to be deleted")
	}
}

func TestClaimedAzureBlobOperations_RespectCanceledContext(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"in-progress/job.txt": []byte("hello"),
	})

	claimed := &ClaimedAzureBlob{
		container: "container",
		name:      "job.txt",
		key:       "in-progress/job.txt",
		client:    client,
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
		t.Fatalf("expected source blob to remain")
	}
}

func TestAzureBlobBackendCompleteAndFailClaim(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"in-progress/a.txt": []byte("a"),
		"in-progress/b.txt": []byte("b"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
	}

	if err := backend.CompleteClaim(context.Background(), "in-progress/a.txt"); err != nil {
		t.Fatalf("CompleteClaim() error = %v", err)
	}
	if client.has("in-progress/a.txt") {
		t.Fatalf("expected completed blob removed")
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

func TestAzureBlobBackendCompleteAndFailClaim_DirectoryTargetMode(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{
		"in-progress/dir-a/part1.bin": []byte("a1"),
		"in-progress/dir-b/part1.bin": []byte("b1"),
	})

	backend, err := NewAzureBlobBackendFromClient(client, "container", "input", "in-progress", "failed")
	if err != nil {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v", err)
	}
	backend.ClaimDirs = true

	if err := backend.CompleteClaim(context.Background(), "in-progress/dir-a/"); err != nil {
		t.Fatalf("CompleteClaim() error = %v", err)
	}
	if client.has("in-progress/dir-a/part1.bin") {
		t.Fatalf("expected completed directory blobs removed")
	}

	failedKey, err := backend.FailClaim(context.Background(), "in-progress/dir-b/")
	if err != nil {
		t.Fatalf("FailClaim() error = %v", err)
	}
	if got, want := failedKey, "failed/dir-b/"; got != want {
		t.Fatalf("failed key = %q, want %q", got, want)
	}
	if !client.has("failed/dir-b/part1.bin") {
		t.Fatalf("expected failed directory blobs to exist")
	}
}

type mockAzureBlobClient struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMockAzureBlobClient(objects map[string][]byte) *mockAzureBlobClient {
	cp := make(map[string][]byte, len(objects))
	for k, v := range objects {
		cp[k] = append([]byte(nil), v...)
	}
	return &mockAzureBlobClient{objects: cp}
}

func (m *mockAzureBlobClient) ListBlobNames(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys := make([]string, 0)
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *mockAzureBlobClient) CopyBlob(_ context.Context, srcKey, dstKey string, destinationMustNotExist bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.objects[srcKey]
	if !ok {
		return &azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"}
	}
	if destinationMustNotExist {
		if _, exists := m.objects[dstKey]; exists {
			return &azcore.ResponseError{StatusCode: 412, ErrorCode: "ConditionNotMet"}
		}
	}
	m.objects[dstKey] = append([]byte(nil), b...)
	return nil
}

func (m *mockAzureBlobClient) DeleteBlob(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, key)
	return nil
}

func (m *mockAzureBlobClient) BlobExists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.objects[key]; ok {
		return true, nil
	}
	return false, nil
}

func (m *mockAzureBlobClient) OpenBlob(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.objects[key]
	if !ok {
		return nil, &azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"}
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), b...))), nil
}

func (m *mockAzureBlobClient) has(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok
}
