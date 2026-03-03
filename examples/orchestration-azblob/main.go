package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"

	libfilerunner "github.com/bigpod98/libfilerunner-go/pkg"
)

func main() {
	cfg := libfilerunner.AzureBlobConfig{
		AccountURL:       mustEnv("LIBFILERUNNER_AZURE_ACCOUNT_URL"),
		Container:        mustEnv("LIBFILERUNNER_AZURE_CONTAINER"),
		InputPrefix:      mustEnv("LIBFILERUNNER_AZURE_INPUT_PREFIX"),
		InProgressPrefix: mustEnv("LIBFILERUNNER_AZURE_INPROGRESS_PREFIX"),
		FailedPrefix:     mustEnv("LIBFILERUNNER_AZURE_FAILED_PREFIX"),
	}
	runner, err := libfilerunner.NewAzureBlobRunner(cfg)
	if err != nil {
		log.Fatalf("NewAzureBlobRunner() error: %v", err)
	}

	ctx := context.Background()
	for {
		claim, err := runner.RunOnceOrchestration(ctx)
		if err != nil {
			log.Fatalf("RunOnceOrchestration() error: %v", err)
		}
		if !claim.Found {
			log.Println("no work found")
			return
		}

		processErr := processClaim(claim.InProgress)
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

func processClaim(inProgressPath string) error {
	if strings.HasSuffix(inProgressPath, ".bad") {
		return errors.New("simulated processor failure for .bad blob")
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
