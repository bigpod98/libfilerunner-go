package libfilerunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	libinternal "github.com/bigpod98/libfilerunner-go/internal"
)

// DirectoryConfig configures the V1 local-directory queue flow.
type DirectoryConfig struct {
	InputDir      string
	InProgressDir string
	FailedDir     string
	BatchSize     int
	// SelectTarget chooses whether each claim is a single file or a directory unit.
	SelectTarget SelectTarget
}

// S3Config configures the V1 S3 queue flow.
type S3Config struct {
	Region           string
	Bucket           string
	InputPrefix      string
	InProgressPrefix string
	FailedPrefix     string
	BatchSize        int
	// SelectTarget chooses whether each claim is a single object or a directory-like prefix unit.
	SelectTarget SelectTarget
}

// AzureBlobConfig configures the V1 Azure Blob queue flow.
type AzureBlobConfig struct {
	AccountURL       string
	Container        string
	InputPrefix      string
	InProgressPrefix string
	FailedPrefix     string
	BatchSize        int
	// SelectTarget chooses whether each claim is a single blob or a directory-like prefix unit.
	SelectTarget SelectTarget
}

// SelectTarget controls what each claim unit represents.
type SelectTarget string

const (
	// SelectTargetFiles claims one file/object/blob per unit of work.
	SelectTargetFiles SelectTarget = "files"
	// SelectTargetDirectories claims one directory/prefix per unit of work.
	SelectTargetDirectories SelectTarget = "directories"
)

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

// RunResult aggregates outcomes across repeated RunOnce/RunOnceOrchestration calls.
type RunResult struct {
	FoundCount     int
	ProcessedCount int
	Last           RunOnceResult
}

// DirectoryRunner implements a V1 single-poll queue processor using local directories.
type DirectoryRunner struct {
	backend   *libinternal.DirectoryBackend
	batchSize int
}

// S3Runner implements a V1 single-poll queue processor using S3 prefixes.
type S3Runner struct {
	backend   *libinternal.S3Backend
	batchSize int
}

// AzureBlobRunner implements a V1 single-poll queue processor using Azure Blob prefixes.
type AzureBlobRunner struct {
	backend   *libinternal.AzureBlobBackend
	batchSize int
}

func NewDirectoryRunner(cfg DirectoryConfig) (*DirectoryRunner, error) {
	claimDirs, err := normalizeSelectTarget(cfg.SelectTarget)
	if err != nil {
		return nil, err
	}

	backend, err := libinternal.NewDirectoryBackend(cfg.InputDir, cfg.InProgressDir, cfg.FailedDir, claimDirs)
	if err != nil {
		return nil, err
	}
	return &DirectoryRunner{backend: backend, batchSize: normalizeBatchSize(cfg.BatchSize)}, nil
}

func NewS3Runner(cfg S3Config) (*S3Runner, error) {
	claimDirs, err := normalizeSelectTarget(cfg.SelectTarget)
	if err != nil {
		return nil, err
	}

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
	backend.ClaimDirs = claimDirs
	return &S3Runner{backend: backend, batchSize: normalizeBatchSize(cfg.BatchSize)}, nil
}

func NewAzureBlobRunner(cfg AzureBlobConfig) (*AzureBlobRunner, error) {
	claimDirs, err := normalizeSelectTarget(cfg.SelectTarget)
	if err != nil {
		return nil, err
	}

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
	backend.ClaimDirs = claimDirs
	return &AzureBlobRunner{backend: backend, batchSize: normalizeBatchSize(cfg.BatchSize)}, nil
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
	return runOnceWithHandler(ctx, handler, r.backend.FailedDir, func(ctx context.Context) (claimedItem, error) {
		claimed, err := r.backend.ClaimNext(ctx)
		if err != nil {
			return nil, err
		}
		return claimed, nil
	})
}

// RunOnceOrchestration claims one file (if present) and leaves it in in-progress.
//
// This variant does not invoke a handler and does not move/delete the claimed file.
func (r *DirectoryRunner) RunOnceOrchestration(ctx context.Context) (RunOnceResult, error) {
	return runOnceOrchestration(ctx, func(ctx context.Context) (claimedItem, error) {
		claimed, err := r.backend.ClaimNext(ctx)
		if err != nil {
			return nil, err
		}
		return claimed, nil
	})
}

// Run processes files repeatedly until no file is available or an error occurs.
func (r *DirectoryRunner) Run(ctx context.Context, handler Handler) (RunResult, error) {
	return runRepeatedly(ctx, r.batchSize, func(ctx context.Context) (RunOnceResult, error) {
		return r.RunOnce(ctx, handler)
	})
}

// RunOrchestration repeatedly claims files until no file is available or an error occurs.
func (r *DirectoryRunner) RunOrchestration(ctx context.Context) (RunResult, error) {
	return runRepeatedly(ctx, r.batchSize, r.RunOnceOrchestration)
}

// Completed marks a previously claimed in-progress file as completed by deleting it.
func (r *DirectoryRunner) Completed(ctx context.Context, inProgress string) error {
	return r.backend.CompleteClaim(ctx, inProgress)
}

// Failed marks a previously claimed in-progress file as failed by moving it to the failed directory.
func (r *DirectoryRunner) Failed(ctx context.Context, inProgress string) (string, error) {
	return r.backend.FailClaim(ctx, inProgress)
}

// RunOnce claims one object (if present), invokes the handler, then deletes or fails it.
//
// Success path: input prefix -> in-progress prefix -> delete
// Failure path: input prefix -> in-progress prefix -> failed prefix
func (r *S3Runner) RunOnce(ctx context.Context, handler Handler) (RunOnceResult, error) {
	return runOnceWithHandler(ctx, handler, r.backend.FailedPrefix, func(ctx context.Context) (claimedItem, error) {
		claimed, err := r.backend.ClaimNext(ctx)
		if err != nil {
			return nil, err
		}
		return claimed, nil
	})
}

// RunOnceOrchestration claims one object (if present) and leaves it in in-progress.
//
// This variant does not invoke a handler and does not move/delete the claimed object.
func (r *S3Runner) RunOnceOrchestration(ctx context.Context) (RunOnceResult, error) {
	return runOnceOrchestration(ctx, func(ctx context.Context) (claimedItem, error) {
		claimed, err := r.backend.ClaimNext(ctx)
		if err != nil {
			return nil, err
		}
		return claimed, nil
	})
}

// Run processes objects repeatedly until no object is available or an error occurs.
func (r *S3Runner) Run(ctx context.Context, handler Handler) (RunResult, error) {
	return runRepeatedly(ctx, r.batchSize, func(ctx context.Context) (RunOnceResult, error) {
		return r.RunOnce(ctx, handler)
	})
}

// RunOrchestration repeatedly claims objects until no object is available or an error occurs.
func (r *S3Runner) RunOrchestration(ctx context.Context) (RunResult, error) {
	return runRepeatedly(ctx, r.batchSize, r.RunOnceOrchestration)
}

// Completed marks a previously claimed in-progress object as completed by deleting it.
func (r *S3Runner) Completed(ctx context.Context, inProgress string) error {
	return r.backend.CompleteClaim(ctx, inProgress)
}

// Failed marks a previously claimed in-progress object as failed by moving it to the failed prefix.
func (r *S3Runner) Failed(ctx context.Context, inProgress string) (string, error) {
	return r.backend.FailClaim(ctx, inProgress)
}

// RunOnce claims one blob (if present), invokes the handler, then deletes or fails it.
//
// Success path: input prefix -> in-progress prefix -> delete
// Failure path: input prefix -> in-progress prefix -> failed prefix
func (r *AzureBlobRunner) RunOnce(ctx context.Context, handler Handler) (RunOnceResult, error) {
	return runOnceWithHandler(ctx, handler, r.backend.FailedPrefix, func(ctx context.Context) (claimedItem, error) {
		claimed, err := r.backend.ClaimNext(ctx)
		if err != nil {
			return nil, err
		}
		return claimed, nil
	})
}

// RunOnceOrchestration claims one blob (if present) and leaves it in in-progress.
//
// This variant does not invoke a handler and does not move/delete the claimed blob.
func (r *AzureBlobRunner) RunOnceOrchestration(ctx context.Context) (RunOnceResult, error) {
	return runOnceOrchestration(ctx, func(ctx context.Context) (claimedItem, error) {
		claimed, err := r.backend.ClaimNext(ctx)
		if err != nil {
			return nil, err
		}
		return claimed, nil
	})
}

// Run processes blobs repeatedly until no blob is available or an error occurs.
func (r *AzureBlobRunner) Run(ctx context.Context, handler Handler) (RunResult, error) {
	return runRepeatedly(ctx, r.batchSize, func(ctx context.Context) (RunOnceResult, error) {
		return r.RunOnce(ctx, handler)
	})
}

// RunOrchestration repeatedly claims blobs until no blob is available or an error occurs.
func (r *AzureBlobRunner) RunOrchestration(ctx context.Context) (RunResult, error) {
	return runRepeatedly(ctx, r.batchSize, r.RunOnceOrchestration)
}

// Completed marks a previously claimed in-progress blob as completed by deleting it.
func (r *AzureBlobRunner) Completed(ctx context.Context, inProgress string) error {
	return r.backend.CompleteClaim(ctx, inProgress)
}

// Failed marks a previously claimed in-progress blob as failed by moving it to the failed prefix.
func (r *AzureBlobRunner) Failed(ctx context.Context, inProgress string) (string, error) {
	return r.backend.FailClaim(ctx, inProgress)
}

type claimedItem interface {
	Name() string
	Path() string
	Open(ctx context.Context) (io.ReadCloser, error)
	Delete(ctx context.Context) error
	MoveToFailed(ctx context.Context, failedTarget string) (string, error)
}

func runOnceWithHandler(ctx context.Context, handler Handler, failedTarget string, claim func(ctx context.Context) (claimedItem, error)) (RunOnceResult, error) {
	var result RunOnceResult

	if handler == nil {
		return result, errors.New("handler is required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	claimed, err := claim(ctx)
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
		open: func() (io.ReadCloser, error) {
			return claimed.Open(ctx)
		},
	}

	if handlerErr := handler(ctx, job); handlerErr != nil {
		result.HandlerErr = handlerErr
		failedPath, moveErr := claimed.MoveToFailed(ctx, failedTarget)
		if moveErr != nil {
			return result, fmt.Errorf("handler failed: %w; additionally failed to move claimed item to failed target: %v", handlerErr, moveErr)
		}
		result.FailedPath = failedPath
		return result, handlerErr
	}

	if err := claimed.Delete(ctx); err != nil {
		return result, err
	}

	return result, nil
}

func runOnceOrchestration(ctx context.Context, claim func(ctx context.Context) (claimedItem, error)) (RunOnceResult, error) {
	var result RunOnceResult
	if err := ctx.Err(); err != nil {
		return result, err
	}

	claimed, err := claim(ctx)
	if err != nil {
		if errors.Is(err, libinternal.ErrNoFileAvailable) {
			return result, nil
		}
		return result, err
	}

	result.Found = true
	result.FileName = claimed.Name()
	result.InProgress = claimed.Path()
	result.Processed = false

	return result, nil
}

func runRepeatedly(ctx context.Context, batchSize int, runOnce func(context.Context) (RunOnceResult, error)) (RunResult, error) {
	var aggregate RunResult

	for {
		if batchSize > 0 && aggregate.FoundCount >= batchSize {
			return aggregate, nil
		}

		if err := ctx.Err(); err != nil {
			return aggregate, err
		}

		result, err := runOnce(ctx)
		if err != nil {
			return aggregate, err
		}
		if !result.Found {
			return aggregate, nil
		}

		aggregate.FoundCount++
		if result.Processed {
			aggregate.ProcessedCount++
		}
		aggregate.Last = result
	}
}

func normalizeBatchSize(configured int) int {
	if configured < 0 {
		return 0
	}
	return configured
}

func normalizeSelectTarget(target SelectTarget) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(string(target))) {
	case "", string(SelectTargetFiles):
		return false, nil
	case string(SelectTargetDirectories):
		return true, nil
	default:
		return false, errors.New("select target must be either files or directories")
	}
}
