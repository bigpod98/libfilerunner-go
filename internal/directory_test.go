package internal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryBackendClaimNext_PicksLexicographicallyAndMovesToInProgress(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	inputDir := filepath.Join(root, "input")
	inProgressDir := filepath.Join(root, "in-progress")
	failedDir := filepath.Join(root, "failed")

	backend, err := NewDirectoryBackend(inputDir, inProgressDir, failedDir, false)
	if err != nil {
		t.Fatalf("NewDirectoryBackend() error = %v", err)
	}
	if err := backend.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	mustWriteFile(t, filepath.Join(inputDir, "b.txt"), "b")
	mustWriteFile(t, filepath.Join(inputDir, "a.txt"), "a")
	if err := os.Mkdir(filepath.Join(inputDir, "subdir"), 0o755); err != nil {
		t.Fatalf("Mkdir(subdir) error = %v", err)
	}

	claimed, err := backend.ClaimNext(context.Background())
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}

	if got, want := claimed.Name(), "a.txt"; got != want {
		t.Fatalf("claimed.Name() = %q, want %q", got, want)
	}
	if got := claimed.Path(); filepath.Dir(got) != inProgressDir {
		t.Fatalf("claimed.Path() dir = %q, want %q", filepath.Dir(got), inProgressDir)
	}

	if _, err := os.Stat(filepath.Join(inputDir, "a.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected input/a.txt to be moved, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(inProgressDir, "a.txt")); err != nil {
		t.Fatalf("expected in-progress/a.txt to exist, stat err = %v", err)
	}
}

func TestDirectoryBackendClaimNext_NoFileAvailable(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	backend, err := NewDirectoryBackend(
		filepath.Join(root, "input"),
		filepath.Join(root, "in-progress"),
		filepath.Join(root, "failed"),
		false,
	)
	if err != nil {
		t.Fatalf("NewDirectoryBackend() error = %v", err)
	}
	if err := backend.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	_, err = backend.ClaimNext(context.Background())
	if !errors.Is(err, ErrNoFileAvailable) {
		t.Fatalf("ClaimNext() error = %v, want %v", err, ErrNoFileAvailable)
	}
}

func TestDirectoryBackendClaimNext_DirectoryTargetModeClaimsDirectories(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	inputDir := filepath.Join(root, "input")
	inProgressDir := filepath.Join(root, "in-progress")
	failedDir := filepath.Join(root, "failed")

	backend, err := NewDirectoryBackend(inputDir, inProgressDir, failedDir, true)
	if err != nil {
		t.Fatalf("NewDirectoryBackend() error = %v", err)
	}
	if err := backend.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.Mkdir(filepath.Join(inputDir, "a-dir"), 0o755); err != nil {
		t.Fatalf("Mkdir(a-dir) error = %v", err)
	}
	mustWriteFile(t, filepath.Join(inputDir, "a-dir", "part1.bin"), "a")
	mustWriteFile(t, filepath.Join(inputDir, "single.txt"), "x")

	claimed, err := backend.ClaimNext(context.Background())
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}

	if got, want := claimed.Name(), "a-dir"; got != want {
		t.Fatalf("claimed.Name() = %q, want %q", got, want)
	}
	if got := claimed.Path(); filepath.Dir(got) != inProgressDir {
		t.Fatalf("claimed.Path() dir = %q, want %q", filepath.Dir(got), inProgressDir)
	}

	if _, err := os.Stat(filepath.Join(inputDir, "a-dir")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected input/a-dir to be moved, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(inProgressDir, "a-dir", "part1.bin")); err != nil {
		t.Fatalf("expected in-progress/a-dir/part1.bin to exist, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(inputDir, "single.txt")); err != nil {
		t.Fatalf("expected input/single.txt to remain, stat err = %v", err)
	}
}

func TestClaimedFileMoveToFailed_AddsUniqueSuffixOnCollision(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	inProgressDir := filepath.Join(root, "in-progress")
	failedDir := filepath.Join(root, "failed")
	if err := os.MkdirAll(inProgressDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(in-progress) error = %v", err)
	}
	if err := os.MkdirAll(failedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(failed) error = %v", err)
	}

	inProgressPath := filepath.Join(inProgressDir, "job.txt")
	mustWriteFile(t, inProgressPath, "payload")
	mustWriteFile(t, filepath.Join(failedDir, "job.txt"), "existing")

	claimed := &ClaimedFile{name: "job.txt", path: inProgressPath}
	dst, err := claimed.MoveToFailed(context.Background(), failedDir)
	if err != nil {
		t.Fatalf("MoveToFailed() error = %v", err)
	}

	if got, want := filepath.Base(dst), "job_1.txt"; got != want {
		t.Fatalf("moved file = %q, want %q", got, want)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("expected failed file to exist, stat err = %v", err)
	}
	if _, err := os.Stat(inProgressPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected original in-progress file removed, stat err = %v", err)
	}
}

func TestClaimedFileOperations_RespectCanceledContext(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	inProgressDir := filepath.Join(root, "in-progress")
	failedDir := filepath.Join(root, "failed")
	if err := os.MkdirAll(inProgressDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(in-progress) error = %v", err)
	}
	if err := os.MkdirAll(failedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(failed) error = %v", err)
	}

	inProgressPath := filepath.Join(inProgressDir, "job.txt")
	mustWriteFile(t, inProgressPath, "payload")

	claimed := &ClaimedFile{name: "job.txt", path: inProgressPath}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := claimed.Open(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v, want %v", err, context.Canceled)
	}
	if err := claimed.Delete(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() error = %v, want %v", err, context.Canceled)
	}
	if _, err := claimed.MoveToFailed(canceledCtx, failedDir); !errors.Is(err, context.Canceled) {
		t.Fatalf("MoveToFailed() error = %v, want %v", err, context.Canceled)
	}

	if _, err := os.Stat(inProgressPath); err != nil {
		t.Fatalf("expected source file to remain, stat err = %v", err)
	}
}

func TestDirectoryBackendCompleteAndFailClaim(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	inputDir := filepath.Join(root, "input")
	inProgressDir := filepath.Join(root, "in-progress")
	failedDir := filepath.Join(root, "failed")

	backend, err := NewDirectoryBackend(inputDir, inProgressDir, failedDir, false)
	if err != nil {
		t.Fatalf("NewDirectoryBackend() error = %v", err)
	}
	if err := backend.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	inProgressA := filepath.Join(inProgressDir, "a.txt")
	mustWriteFile(t, inProgressA, "a")
	if err := backend.CompleteClaim(context.Background(), inProgressA); err != nil {
		t.Fatalf("CompleteClaim() error = %v", err)
	}
	if _, err := os.Stat(inProgressA); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected completed file removed, stat err = %v", err)
	}

	inProgressB := filepath.Join(inProgressDir, "b.txt")
	mustWriteFile(t, inProgressB, "b")
	failedPath, err := backend.FailClaim(context.Background(), inProgressB)
	if err != nil {
		t.Fatalf("FailClaim() error = %v", err)
	}
	if failedPath == "" {
		t.Fatalf("FailClaim() path is empty")
	}
	if _, err := os.Stat(failedPath); err != nil {
		t.Fatalf("expected failed file to exist, stat err = %v", err)
	}
}

func TestDirectoryBackendCompleteAndFailClaim_DirectoryTargetMode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	inputDir := filepath.Join(root, "input")
	inProgressDir := filepath.Join(root, "in-progress")
	failedDir := filepath.Join(root, "failed")

	backend, err := NewDirectoryBackend(inputDir, inProgressDir, failedDir, true)
	if err != nil {
		t.Fatalf("NewDirectoryBackend() error = %v", err)
	}
	if err := backend.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	dirA := filepath.Join(inProgressDir, "dir-a")
	if err := os.MkdirAll(dirA, 0o755); err != nil {
		t.Fatalf("MkdirAll(dir-a) error = %v", err)
	}
	mustWriteFile(t, filepath.Join(dirA, "part1.bin"), "a1")
	if err := backend.CompleteClaim(context.Background(), dirA); err != nil {
		t.Fatalf("CompleteClaim() error = %v", err)
	}
	if _, err := os.Stat(dirA); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected completed directory removed, stat err = %v", err)
	}

	dirB := filepath.Join(inProgressDir, "dir-b")
	if err := os.MkdirAll(dirB, 0o755); err != nil {
		t.Fatalf("MkdirAll(dir-b) error = %v", err)
	}
	mustWriteFile(t, filepath.Join(dirB, "part1.bin"), "b1")
	failedPath, err := backend.FailClaim(context.Background(), dirB)
	if err != nil {
		t.Fatalf("FailClaim() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(failedPath, "part1.bin")); err != nil {
		t.Fatalf("expected failed directory contents to exist, stat err = %v", err)
	}
}

func mustWriteFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}
