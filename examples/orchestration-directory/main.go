package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"

	libfilerunner "github.com/bigpod98/libfilerunner-go/pkg"
)

func main() {
	root := mustEnv("LIBFILERUNNER_QUEUE_ROOT")
	runner, err := libfilerunner.NewDirectoryRunner(libfilerunner.DirectoryConfig{
		InputDir:      filepath.Join(root, "input"),
		InProgressDir: filepath.Join(root, "in-progress"),
		FailedDir:     filepath.Join(root, "failed"),
	})
	if err != nil {
		log.Fatalf("NewDirectoryRunner() error: %v", err)
	}
	if err := runner.EnsureDirectories(); err != nil {
		log.Fatalf("EnsureDirectories() error: %v", err)
	}

	ctx := context.Background()
	for {
		queue, err := runner.Queue(ctx)
		if err != nil {
			log.Fatalf("Queue() error: %v", err)
		}
		inProgress, err := runner.InProgress(ctx)
		if err != nil {
			log.Fatalf("InProgress() error: %v", err)
		}
		log.Printf("snapshot queue=%d in-progress=%d", queue.Count, inProgress.Count)

		claim, err := runner.RunOnceOrchestration(ctx)
		if err != nil {
			log.Fatalf("RunOnceOrchestration() error: %v", err)
		}
		if !claim.Found {
			log.Println("no work found")
			return
		}

		processErr := processClaim(ctx, claim.InProgress)
		if processErr == nil {
			err = runner.Completed(ctx, claim.InProgress)
			if err != nil {
				log.Fatalf("Completed(%q) error: %v", claim.InProgress, err)
			}
			log.Printf("completed claim=%s", claim.InProgress)
			continue
		}

		failedPath, failErr := runner.Failed(ctx, claim.InProgress)
		if failErr != nil {
			log.Fatalf("Failed(%q) error after process failure (%v): %v", claim.InProgress, processErr, failErr)
		}
		log.Printf("failed claim=%s moved=%s process_err=%v", claim.InProgress, failedPath, processErr)
	}
}

func processClaim(ctx context.Context, claimedPath string) error {
	f, err := os.Open(claimedPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.ReadAll(f)
	if err != nil {
		return err
	}

	if filepath.Ext(claimedPath) == ".bad" {
		return errors.New("simulated processor failure for .bad input")
	}
	return nil
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s is required", key)
	}
	return v
}
