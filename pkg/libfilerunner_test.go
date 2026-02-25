package libfilerunner_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	libfilerunner "github.com/bigpod98/libfilerunner-go/pkg"
)

func TestDirectoryRunnerRunOnce_SuccessDeletesFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	inputFile := filepath.Join(dirs.input, "job.txt")
	if err := os.WriteFile(inputFile, []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	var seenName string
	var seenData string
	res, err := runner.RunOnce(context.Background(), func(ctx context.Context, job libfilerunner.FileJob) error {
		seenName = job.Name
		f, err := job.Open()
		if err != nil {
			return err
		}
		defer f.Close()

		b, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		seenData = string(b)
		return nil
	})
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if !res.Found || !res.Processed {
		t.Fatalf("result = %+v, want Found=true Processed=true", res)
	}
	if res.FileName != "job.txt" {
		t.Fatalf("result.FileName = %q, want %q", res.FileName, "job.txt")
	}
	if res.FailedPath != "" {
		t.Fatalf("result.FailedPath = %q, want empty", res.FailedPath)
	}
	if res.HandlerErr != nil {
		t.Fatalf("result.HandlerErr = %v, want nil", res.HandlerErr)
	}
	if seenName != "job.txt" || seenData != "hello" {
		t.Fatalf("handler saw name=%q data=%q, want name=%q data=%q", seenName, seenData, "job.txt", "hello")
	}

	assertNotExists(t, filepath.Join(dirs.input, "job.txt"))
	assertNotExists(t, filepath.Join(dirs.inProgress, "job.txt"))
	assertNotExists(t, filepath.Join(dirs.failed, "job.txt"))
}

func TestDirectoryRunnerRunOnce_HandlerFailureMovesToFailed(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.input, "job.txt"), []byte("bad"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	wantErr := errors.New("processor failed")
	res, err := runner.RunOnce(context.Background(), func(ctx context.Context, job libfilerunner.FileJob) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunOnce() error = %v, want %v", err, wantErr)
	}

	if !res.Found || !res.Processed {
		t.Fatalf("result = %+v, want Found=true Processed=true", res)
	}
	if !errors.Is(res.HandlerErr, wantErr) {
		t.Fatalf("result.HandlerErr = %v, want %v", res.HandlerErr, wantErr)
	}
	if res.FailedPath == "" {
		t.Fatalf("result.FailedPath is empty, want failed path")
	}

	assertNotExists(t, filepath.Join(dirs.input, "job.txt"))
	assertNotExists(t, filepath.Join(dirs.inProgress, "job.txt"))
	if _, statErr := os.Stat(res.FailedPath); statErr != nil {
		t.Fatalf("expected failed file to exist at %q, stat err = %v", res.FailedPath, statErr)
	}
}

func TestDirectoryRunnerRunOnce_NoFileReturnsEmptyResult(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	called := false
	res, err := runner.RunOnce(context.Background(), func(ctx context.Context, job libfilerunner.FileJob) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if called {
		t.Fatalf("handler was called, want not called")
	}
	if res.Found || res.Processed || res.FileName != "" {
		t.Fatalf("result = %+v, want empty no-file result", res)
	}
}

type dirsConfig struct {
	input      string
	inProgress string
	failed     string
}

func queueDirs(root string) dirsConfig {
	return dirsConfig{
		input:      filepath.Join(root, "input"),
		inProgress: filepath.Join(root, "in-progress"),
		failed:     filepath.Join(root, "failed"),
	}
}

func mustNewRunner(t *testing.T, dirs dirsConfig) *libfilerunner.DirectoryRunner {
	t.Helper()

	runner, err := libfilerunner.NewDirectoryRunner(libfilerunner.DirectoryConfig{
		InputDir:      dirs.input,
		InProgressDir: dirs.inProgress,
		FailedDir:     dirs.failed,
	})
	if err != nil {
		t.Fatalf("NewDirectoryRunner() error = %v", err)
	}
	return runner
}

func assertNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %q not to exist, stat err = %v", path, err)
	}
}
