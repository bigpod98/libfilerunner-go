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

func (m *mockAzureBlobClient) CopyBlob(_ context.Context, srcKey, dstKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.objects[srcKey]
	if !ok {
		return &azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"}
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
