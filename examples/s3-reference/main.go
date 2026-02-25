package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	libfilerunner "github.com/bigpod98/libfilerunner-go/pkg"
)

func main() {
	cfg, err := loadConfigFromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	runner, err := libfilerunner.NewS3Runner(cfg)
	if err != nil {
		log.Fatalf("create S3 runner: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pollInterval := 2 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			log.Printf("shutting down: %v", err)
			return
		}

		result, runErr := runner.RunOnce(ctx, func(ctx context.Context, job libfilerunner.FileJob) error {
			f, err := job.Open()
			if err != nil {
				return err
			}
			defer f.Close()

			body, err := io.ReadAll(f)
			if err != nil {
				return err
			}

			log.Printf("processed object=%s bytes=%d", job.Path, len(body))
			return nil
		})

		switch {
		case runErr == nil && !result.Found:
			log.Printf("no objects found in %s", cfg.InputPrefix)
		case runErr == nil:
			log.Printf("completed object=%s", result.FileName)
		case errors.Is(runErr, context.Canceled), errors.Is(runErr, context.DeadlineExceeded):
			log.Printf("stopping: %v", runErr)
			return
		default:
			log.Printf("run failed object=%s failed_path=%s err=%v", result.FileName, result.FailedPath, runErr)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func loadConfigFromEnv() (libfilerunner.S3Config, error) {
	cfg := libfilerunner.S3Config{
		Region:           os.Getenv("AWS_REGION"),
		Bucket:           os.Getenv("LIBFILERUNNER_S3_BUCKET"),
		InputPrefix:      os.Getenv("LIBFILERUNNER_S3_INPUT_PREFIX"),
		InProgressPrefix: os.Getenv("LIBFILERUNNER_S3_INPROGRESS_PREFIX"),
		FailedPrefix:     os.Getenv("LIBFILERUNNER_S3_FAILED_PREFIX"),
	}

	if cfg.Bucket == "" {
		return cfg, fmt.Errorf("LIBFILERUNNER_S3_BUCKET is required")
	}
	if cfg.InputPrefix == "" {
		return cfg, fmt.Errorf("LIBFILERUNNER_S3_INPUT_PREFIX is required")
	}
	if cfg.InProgressPrefix == "" {
		return cfg, fmt.Errorf("LIBFILERUNNER_S3_INPROGRESS_PREFIX is required")
	}
	if cfg.FailedPrefix == "" {
		return cfg, fmt.Errorf("LIBFILERUNNER_S3_FAILED_PREFIX is required")
	}

	return cfg, nil
}
