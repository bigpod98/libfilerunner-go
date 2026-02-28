# TODO - v1.1.0

Track planned work for the `v1.1.0` release here.

## Development Flow (single ordered list)

- [x] Add config validation to prevent overlapping input/in-progress/failed targets
- [x] Propagate caller `context.Context` through open/delete/fail operations (avoid internal `context.Background()` in runtime paths)
- [ ] Add orchestration-oriented variants of `RunOnce` and `Run` for coordinating remote workers
  - Note: this step only pulls the file and places it in the working bucket, then stops until the next method is called
- [ ] Add task check-in methods for worker status reporting (`Completed` and `Failed`)
- [ ] Support select-work targets as either files or directories (configured up front per check)
  - Note: treat file and directory targets uniformly so projects can submit whole directories when multiple files are needed
  - Reason: binary-heavy workflows (for example audio/video + sidecar metadata files) need related files in the same directory to travel together
- [ ] Make number of claimed items per run configurable
  - Recommendation: set default batch size at checker/runner configuration time for predictable orchestration behavior
  - Optional follow-up: allow per-call override on `RunOnce`/`Run` only if a workflow needs temporary burst control
- [ ] Harden remote claim behavior against concurrent workers (reduce duplicate-claim race windows)
- [ ] Improve large-queue claim performance (avoid full list+sort when only next item(s) are needed)
- [ ] Add tests for cancellation during claim/finalize and partial-failure scenarios
- [ ] Add tests for concurrent claimers across backends
- [ ] Publish orchestration-focused examples for directory, S3, and Azure Blob (claim -> process -> complete/fail)
- [ ] Add API docs for orchestration lifecycle methods and recommended call order
- [ ] Add migration notes from handler-coupled `RunOnce` to orchestration flow
