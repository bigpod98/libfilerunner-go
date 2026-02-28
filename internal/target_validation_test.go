package internal

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestNewDirectoryBackend_RejectsOverlappingTargets(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	_, err := NewDirectoryBackend(
		filepath.Join(root, "queue"),
		filepath.Join(root, "queue", "in-progress"),
		filepath.Join(root, "failed"),
	)
	if err == nil || !strings.Contains(err.Error(), "must not overlap") {
		t.Fatalf("NewDirectoryBackend() error = %v, want overlap validation error", err)
	}
}

func TestNewS3BackendFromClient_RejectsOverlappingPrefixes(t *testing.T) {
	t.Parallel()

	client := newMockS3Client(map[string][]byte{})
	_, err := NewS3BackendFromClient(client, "bucket", "queue", "queue/in-progress", "failed")
	if err == nil || !strings.Contains(err.Error(), "must not overlap") {
		t.Fatalf("NewS3BackendFromClient() error = %v, want overlap validation error", err)
	}
}

func TestNewAzureBlobBackendFromClient_RejectsOverlappingPrefixes(t *testing.T) {
	t.Parallel()

	client := newMockAzureBlobClient(map[string][]byte{})
	_, err := NewAzureBlobBackendFromClient(client, "container", "queue", "in-progress", "in-progress/failed")
	if err == nil || !strings.Contains(err.Error(), "must not overlap") {
		t.Fatalf("NewAzureBlobBackendFromClient() error = %v, want overlap validation error", err)
	}
}
