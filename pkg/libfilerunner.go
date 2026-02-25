package libfilerunner

import (
	"context"
	"errors"
	"fmt"
	"io"

	libinternal "github.com/bigpod98/libfilerunner-go/internal"
)

// DirectoryConfig configures the V1 local-directory queue flow.
type DirectoryConfig struct {
	InputDir      string
	InProgressDir string
	FailedDir     string
}

// S3Config configures the V1 S3 queue flow.
type S3Config struct {
	Region           string
	Bucket           string
	InputPrefix      string
	InProgressPrefix string
	FailedPrefix     string
}

// AzureBlobConfig configures the V1 Azure Blob queue flow.
type AzureBlobConfig struct {
	AccountURL       string
	Container        string
	InputPrefix      string
	InProgressPrefix string
	FailedPrefix     string
}

// FileJob is the file handed to the consumer while it is in the in-progress directory.
type FileJob struct {
	Name string
	Path string
	open func() (io.ReadCloser, error)
}

func (j FileJob) Open() (io.ReadCloser, error) {
	if j.open == nil {
		return nil, errors.New("job open function is not configured")
	}
	return j.open()
}

// Handler processes a single claimed file.
// Returning an error moves the file to the failed directory.
type Handler func(ctx context.Context, job FileJob) error

// RunOnceResult describes a single poll cycle outcome.
type RunOnceResult struct {
	Found      bool
	Processed  bool
	FileName   string
	InProgress string
	FailedPath string
	HandlerErr error
}

// DirectoryRunner implements a V1 single-poll queue processor using local directories.
type DirectoryRunner struct {
	backend *libinternal.DirectoryBackend
}

// S3Runner implements a V1 single-poll queue processor using S3 prefixes.
type S3Runner struct {
	backend *libinternal.S3Backend
}

// AzureBlobRunner implements a V1 single-poll queue processor using Azure Blob prefixes.
type AzureBlobRunner struct {
	backend *libinternal.AzureBlobBackend
}

func NewDirectoryRunner(cfg DirectoryConfig) (*DirectoryRunner, error) {
	backend, err := libinternal.NewDirectoryBackend(cfg.InputDir, cfg.InProgressDir, cfg.FailedDir)
	if err != nil {
		return nil, err
	}
	return &DirectoryRunner{backend: backend}, nil
}

func NewS3Runner(cfg S3Config) (*S3Runner, error) {
	backend, err := libinternal.NewS3Backend(
		cfg.Region,
		cfg.Bucket,
		cfg.InputPrefix,
		cfg.InProgressPrefix,
		cfg.FailedPrefix,
	)
	if err != nil {
		return nil, err
	}
	return &S3Runner{backend: backend}, nil
}

func NewAzureBlobRunner(cfg AzureBlobConfig) (*AzureBlobRunner, error) {
	backend, err := libinternal.NewAzureBlobBackend(
		cfg.AccountURL,
		cfg.Container,
		cfg.InputPrefix,
		cfg.InProgressPrefix,
		cfg.FailedPrefix,
	)
	if err != nil {
		return nil, err
	}
	return &AzureBlobRunner{backend: backend}, nil
}

// EnsureDirectories creates the configured directories if they do not exist.
func (r *DirectoryRunner) EnsureDirectories() error {
	return r.backend.EnsureDirectories()
}

// RunOnce claims one file (if present), invokes the handler, then deletes or fails it.
//
// Success path: input -> in-progress -> delete
// Failure path: input -> in-progress -> failed
func (r *DirectoryRunner) RunOnce(ctx context.Context, handler Handler) (RunOnceResult, error) {
	var result RunOnceResult

	if handler == nil {
		return result, errors.New("handler is required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	claimed, err := r.backend.ClaimNext(ctx)
	if err != nil {
		if errors.Is(err, libinternal.ErrNoFileAvailable) {
			return result, nil
		}
		return result, err
	}

	result.Found = true
	result.Processed = true
	result.FileName = claimed.Name()
	result.InProgress = claimed.Path()

	job := FileJob{
		Name: claimed.Name(),
		Path: claimed.Path(),
		open: claimed.Open,
	}

	if handlerErr := handler(ctx, job); handlerErr != nil {
		result.HandlerErr = handlerErr
		failedPath, moveErr := claimed.MoveToFailed(r.backend.FailedDir)
		if moveErr != nil {
			return result, fmt.Errorf("handler failed: %w; additionally failed to move file to failed directory: %v", handlerErr, moveErr)
		}
		result.FailedPath = failedPath
		return result, handlerErr
	}

	if err := claimed.Delete(); err != nil {
		return result, err
	}

	return result, nil
}

// RunOnce claims one object (if present), invokes the handler, then deletes or fails it.
//
// Success path: input prefix -> in-progress prefix -> delete
// Failure path: input prefix -> in-progress prefix -> failed prefix
func (r *S3Runner) RunOnce(ctx context.Context, handler Handler) (RunOnceResult, error) {
	var result RunOnceResult

	if handler == nil {
		return result, errors.New("handler is required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	claimed, err := r.backend.ClaimNext(ctx)
	if err != nil {
		if errors.Is(err, libinternal.ErrNoFileAvailable) {
			return result, nil
		}
		return result, err
	}

	result.Found = true
	result.Processed = true
	result.FileName = claimed.Name()
	result.InProgress = claimed.Path()

	job := FileJob{
		Name: claimed.Name(),
		Path: claimed.Path(),
		open: claimed.Open,
	}

	if handlerErr := handler(ctx, job); handlerErr != nil {
		result.HandlerErr = handlerErr
		failedPath, moveErr := claimed.MoveToFailed(r.backend.FailedPrefix)
		if moveErr != nil {
			return result, fmt.Errorf("handler failed: %w; additionally failed to move file to failed directory: %v", handlerErr, moveErr)
		}
		result.FailedPath = failedPath
		return result, handlerErr
	}

	if err := claimed.Delete(); err != nil {
		return result, err
	}

	return result, nil
}

// RunOnce claims one blob (if present), invokes the handler, then deletes or fails it.
//
// Success path: input prefix -> in-progress prefix -> delete
// Failure path: input prefix -> in-progress prefix -> failed prefix
func (r *AzureBlobRunner) RunOnce(ctx context.Context, handler Handler) (RunOnceResult, error) {
	var result RunOnceResult

	if handler == nil {
		return result, errors.New("handler is required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	claimed, err := r.backend.ClaimNext(ctx)
	if err != nil {
		if errors.Is(err, libinternal.ErrNoFileAvailable) {
			return result, nil
		}
		return result, err
	}

	result.Found = true
	result.Processed = true
	result.FileName = claimed.Name()
	result.InProgress = claimed.Path()

	job := FileJob{
		Name: claimed.Name(),
		Path: claimed.Path(),
		open: claimed.Open,
	}

	if handlerErr := handler(ctx, job); handlerErr != nil {
		result.HandlerErr = handlerErr
		failedPath, moveErr := claimed.MoveToFailed(r.backend.FailedPrefix)
		if moveErr != nil {
			return result, fmt.Errorf("handler failed: %w; additionally failed to move file to failed directory: %v", handlerErr, moveErr)
		}
		result.FailedPath = failedPath
		return result, handlerErr
	}

	if err := claimed.Delete(); err != nil {
		return result, err
	}

	return result, nil
}
