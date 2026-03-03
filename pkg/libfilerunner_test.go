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

func TestDirectoryRunnerRunOnceOrchestration_ClaimCanceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := runner.RunOnceOrchestration(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnceOrchestration() error = %v, want %v", err, context.Canceled)
	}
}

func TestDirectoryRunnerQueue_ReturnsCountAndNames(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.input, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile(a) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dirs.input, "b.txt"), []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile(b) error = %v", err)
	}

	queue, err := runner.Queue(context.Background())
	if err != nil {
		t.Fatalf("Queue() error = %v", err)
	}
	if queue.Count != 2 {
		t.Fatalf("queue.Count = %d, want 2", queue.Count)
	}
	if len(queue.Names) != 2 {
		t.Fatalf("len(queue.Names) = %d, want 2", len(queue.Names))
	}
}

func TestDirectoryRunnerInProgress_ReturnsCountAndNames(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.inProgress, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile(a) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dirs.inProgress, "b.txt"), []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile(b) error = %v", err)
	}

	queue, err := runner.InProgress(context.Background())
	if err != nil {
		t.Fatalf("InProgress() error = %v", err)
	}
	if queue.Count != 2 {
		t.Fatalf("queue.Count = %d, want 2", queue.Count)
	}
	if len(queue.Names) != 2 {
		t.Fatalf("len(queue.Names) = %d, want 2", len(queue.Names))
	}
}

func TestDirectoryRunnerRunOnceOrchestration_ClaimsWithoutFinalizing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.input, "job.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	res, err := runner.RunOnceOrchestration(context.Background())
	if err != nil {
		t.Fatalf("RunOnceOrchestration() error = %v", err)
	}

	if !res.Found {
		t.Fatalf("result = %+v, want Found=true", res)
	}
	if res.Processed {
		t.Fatalf("result.Processed = true, want false")
	}
	if res.FileName != "job.txt" {
		t.Fatalf("result.FileName = %q, want %q", res.FileName, "job.txt")
	}
	if res.InProgress == "" {
		t.Fatalf("result.InProgress is empty, want claimed path")
	}

	assertNotExists(t, filepath.Join(dirs.input, "job.txt"))
	if _, statErr := os.Stat(res.InProgress); statErr != nil {
		t.Fatalf("expected in-progress file to exist at %q, stat err = %v", res.InProgress, statErr)
	}
	assertNotExists(t, filepath.Join(dirs.failed, "job.txt"))
}

func TestDirectoryRunnerRunOrchestration_ClaimsAllAvailableFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	for _, name := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(dirs.input, name), []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", name, err)
		}
	}

	runRes, err := runner.RunOrchestration(context.Background())
	if err != nil {
		t.Fatalf("RunOrchestration() error = %v", err)
	}

	if runRes.FoundCount != 2 {
		t.Fatalf("runRes.FoundCount = %d, want 2", runRes.FoundCount)
	}
	if runRes.ProcessedCount != 0 {
		t.Fatalf("runRes.ProcessedCount = %d, want 0", runRes.ProcessedCount)
	}
	if !runRes.Last.Found || runRes.Last.Processed {
		t.Fatalf("runRes.Last = %+v, want Found=true Processed=false", runRes.Last)
	}

	inProgressEntries, err := os.ReadDir(dirs.inProgress)
	if err != nil {
		t.Fatalf("ReadDir(inProgress) error = %v", err)
	}
	if len(inProgressEntries) != 2 {
		t.Fatalf("in-progress entry count = %d, want 2", len(inProgressEntries))
	}

	assertNotExists(t, filepath.Join(dirs.input, "a.txt"))
	assertNotExists(t, filepath.Join(dirs.input, "b.txt"))
}

func TestDirectoryRunnerRun_ProcessesUntilQueueEmpty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	for _, name := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(dirs.input, name), []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", name, err)
		}
	}

	handlerCalls := 0
	runRes, err := runner.Run(context.Background(), func(ctx context.Context, job libfilerunner.FileJob) error {
		handlerCalls++
		f, err := job.Open()
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.ReadAll(f)
		return err
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if handlerCalls != 2 {
		t.Fatalf("handlerCalls = %d, want 2", handlerCalls)
	}
	if runRes.FoundCount != 2 || runRes.ProcessedCount != 2 {
		t.Fatalf("runRes = %+v, want FoundCount=2 ProcessedCount=2", runRes)
	}

	assertNotExists(t, filepath.Join(dirs.input, "a.txt"))
	assertNotExists(t, filepath.Join(dirs.input, "b.txt"))
	assertNotExists(t, filepath.Join(dirs.inProgress, "a.txt"))
	assertNotExists(t, filepath.Join(dirs.inProgress, "b.txt"))
	assertNotExists(t, filepath.Join(dirs.failed, "a.txt"))
	assertNotExists(t, filepath.Join(dirs.failed, "b.txt"))
}

func TestDirectoryRunnerRun_RespectsConfiguredBatchSize(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunnerWithBatch(t, dirs, 1)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	for _, name := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(dirs.input, name), []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", name, err)
		}
	}

	handlerCalls := 0
	runRes, err := runner.Run(context.Background(), func(ctx context.Context, job libfilerunner.FileJob) error {
		handlerCalls++
		return nil
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if handlerCalls != 1 {
		t.Fatalf("handlerCalls = %d, want 1", handlerCalls)
	}
	if runRes.FoundCount != 1 || runRes.ProcessedCount != 1 {
		t.Fatalf("runRes = %+v, want FoundCount=1 ProcessedCount=1", runRes)
	}

	inputEntries, err := os.ReadDir(dirs.input)
	if err != nil {
		t.Fatalf("ReadDir(input) error = %v", err)
	}
	if len(inputEntries) != 1 {
		t.Fatalf("remaining input entry count = %d, want 1", len(inputEntries))
	}
}

func TestDirectoryRunnerCompleted_RemovesClaimedInProgressFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.input, "job.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	claimed, err := runner.RunOnceOrchestration(context.Background())
	if err != nil {
		t.Fatalf("RunOnceOrchestration() error = %v", err)
	}
	if !claimed.Found {
		t.Fatalf("claimed result = %+v, want Found=true", claimed)
	}

	if err := runner.Completed(context.Background(), claimed.InProgress); err != nil {
		t.Fatalf("Completed() error = %v", err)
	}

	assertNotExists(t, claimed.InProgress)
	assertNotExists(t, filepath.Join(dirs.failed, "job.txt"))
}

func TestDirectoryRunnerFailed_MovesClaimedInProgressFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.input, "job.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	claimed, err := runner.RunOnceOrchestration(context.Background())
	if err != nil {
		t.Fatalf("RunOnceOrchestration() error = %v", err)
	}

	failedPath, err := runner.Failed(context.Background(), claimed.InProgress)
	if err != nil {
		t.Fatalf("Failed() error = %v", err)
	}
	if failedPath == "" {
		t.Fatalf("Failed() path is empty, want failed path")
	}

	assertNotExists(t, claimed.InProgress)
	if _, statErr := os.Stat(failedPath); statErr != nil {
		t.Fatalf("expected failed file to exist at %q, stat err = %v", failedPath, statErr)
	}
}

func TestDirectoryRunnerFailed_FinalizeCanceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.input, "job.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	claimed, err := runner.RunOnceOrchestration(context.Background())
	if err != nil {
		t.Fatalf("RunOnceOrchestration() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = runner.Failed(ctx, claimed.InProgress)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Failed() error = %v, want %v", err, context.Canceled)
	}
}

func TestDirectoryRunnerRunOnce_HandlerFailureAndFinalizeFailureReturnsCombinedError(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner := mustNewRunner(t, dirs)
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(dirs.input, "job.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	handlerErr := errors.New("process failed")
	_, err := runner.RunOnce(context.Background(), func(ctx context.Context, job libfilerunner.FileJob) error {
		if removeErr := os.Remove(job.Path); removeErr != nil {
			t.Fatalf("Remove(job.Path) error = %v", removeErr)
		}
		return handlerErr
	})
	if err == nil {
		t.Fatalf("RunOnce() error = nil, want combined handler/finalize error")
	}
	if !errors.Is(err, handlerErr) {
		t.Fatalf("RunOnce() error = %v, want to wrap %v", err, handlerErr)
	}
}

func TestDirectoryRunnerRunOnceOrchestration_DirectoryTargetModeClaimsDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirs := queueDirs(root)
	runner, err := libfilerunner.NewDirectoryRunner(libfilerunner.DirectoryConfig{
		InputDir:      dirs.input,
		InProgressDir: dirs.inProgress,
		FailedDir:     dirs.failed,
		SelectTarget:  libfilerunner.SelectTargetDirectories,
	})
	if err != nil {
		t.Fatalf("NewDirectoryRunner() error = %v", err)
	}
	if err := runner.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	if err := os.Mkdir(filepath.Join(dirs.input, "job-dir"), 0o755); err != nil {
		t.Fatalf("Mkdir(job-dir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dirs.input, "job-dir", "part1.bin"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(part1) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dirs.input, "single.txt"), []byte("single"), 0o644); err != nil {
		t.Fatalf("WriteFile(single) error = %v", err)
	}

	res, err := runner.RunOnceOrchestration(context.Background())
	if err != nil {
		t.Fatalf("RunOnceOrchestration() error = %v", err)
	}
	if !res.Found || res.Processed {
		t.Fatalf("result = %+v, want Found=true Processed=false", res)
	}
	if got, want := res.FileName, "job-dir"; got != want {
		t.Fatalf("result.FileName = %q, want %q", got, want)
	}

	if _, err := os.Stat(filepath.Join(res.InProgress, "part1.bin")); err != nil {
		t.Fatalf("expected claimed directory contents in %q, stat err = %v", res.InProgress, err)
	}
	if _, err := os.Stat(filepath.Join(dirs.input, "single.txt")); err != nil {
		t.Fatalf("expected input/single.txt to remain, stat err = %v", err)
	}
}

func TestNewDirectoryRunner_RejectsInvalidSelectTarget(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	_, err := libfilerunner.NewDirectoryRunner(libfilerunner.DirectoryConfig{
		InputDir:      filepath.Join(root, "input"),
		InProgressDir: filepath.Join(root, "in-progress"),
		FailedDir:     filepath.Join(root, "failed"),
		SelectTarget:  libfilerunner.SelectTarget("invalid"),
	})
	if err == nil {
		t.Fatalf("NewDirectoryRunner() error = nil, want invalid select-target error")
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

func mustNewRunnerWithBatch(t *testing.T, dirs dirsConfig, batchSize int) *libfilerunner.DirectoryRunner {
	t.Helper()

	runner, err := libfilerunner.NewDirectoryRunner(libfilerunner.DirectoryConfig{
		InputDir:      dirs.input,
		InProgressDir: dirs.inProgress,
		FailedDir:     dirs.failed,
		BatchSize:     batchSize,
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
