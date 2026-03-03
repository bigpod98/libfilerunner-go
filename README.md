# libfilerunner-go

`libfilerunner-go` provides queue-style file processing for local directories, S3, and Azure Blob.

## Queue Targets

Configure claim target type up-front with `SelectTarget`:

- `SelectTargetFiles` (default): claim one file/object/blob per unit of work.
- `SelectTargetDirectories`: claim one directory/prefix per unit of work.

All runners support `BatchSize` to cap how many claims `Run`/`RunOrchestration` process per call.

## Orchestration Lifecycle

Use the orchestration API when claiming and finalizing are split across systems.

### Recommended call order

1. `RunOnceOrchestration(ctx)` or `RunOrchestration(ctx)` to claim work and move it to in-progress.
2. Perform processing externally.
3. On success: `Completed(ctx, inProgressPath)`.
4. On failure: `Failed(ctx, inProgressPath)`.

### API summary

All runners (`DirectoryRunner`, `S3Runner`, `AzureBlobRunner`) expose:

- `RunOnce(ctx, handler)` - handler-coupled flow: claim -> process -> complete/fail.
- `Run(ctx, handler)` - repeats `RunOnce` until queue empty or `BatchSize` reached.
- `RunOnceOrchestration(ctx)` - claim only (no processing/finalization).
- `RunOrchestration(ctx)` - repeat claim-only flow until queue empty or `BatchSize` reached.
- `Completed(ctx, inProgressPath)` - finalize success.
- `Failed(ctx, inProgressPath)` - finalize failure and move to failed target.
- `Queue(ctx)` - returns current claimable item count and names for dashboards.
- `InProgress(ctx)` - returns current in-progress item count and names for dashboards.

### Dashboard snapshot example

```go
queue, err := runner.Queue(ctx)
if err != nil {
    return err
}

inProgress, err := runner.InProgress(ctx)
if err != nil {
    return err
}

log.Printf("queue=%d in_progress=%d", queue.Count, inProgress.Count)
log.Printf("queue items: %v", queue.Names)
log.Printf("in-progress items: %v", inProgress.Names)
```

## Migration Notes (handler-coupled -> orchestration)

Existing style:

```go
result, err := runner.RunOnce(ctx, handler)
```

Orchestration style:

```go
claim, err := runner.RunOnceOrchestration(ctx)
if err != nil || !claim.Found {
    return
}

if processErr := process(claim.InProgress); processErr == nil {
    _ = runner.Completed(ctx, claim.InProgress)
} else {
    _, _ = runner.Failed(ctx, claim.InProgress)
}
```

Behavior differences:

- `RunOnce` returns handler errors directly and internally finalizes success/failure.
- `RunOnceOrchestration` never calls a handler and leaves the claim in-progress until explicit check-in.
- `Completed`/`Failed` are idempotent from a caller perspective only when underlying storage semantics allow it; callers should dedupe retries.

## Orchestration Examples

- Local directories: `examples/orchestration-directory/main.go`
- S3: `examples/orchestration-s3/main.go`
- Azure Blob: `examples/orchestration-azblob/main.go`
