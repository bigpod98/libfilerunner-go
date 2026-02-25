# S3 Reference Implementation

This example runs the S3 queue flow in a polling loop.

## Behavior

- Claims one object from input prefix to in-progress prefix
- Calls the handler
- On success, deletes the in-progress object
- On handler error, moves object to failed prefix

## Required environment

- `LIBFILERUNNER_S3_BUCKET`
- `LIBFILERUNNER_S3_INPUT_PREFIX`
- `LIBFILERUNNER_S3_INPROGRESS_PREFIX`
- `LIBFILERUNNER_S3_FAILED_PREFIX`

Optional:

- `AWS_REGION`

The AWS SDK default credential chain is used.

## Run

```bash
export LIBFILERUNNER_S3_BUCKET="my-bucket"
export LIBFILERUNNER_S3_INPUT_PREFIX="queue/input"
export LIBFILERUNNER_S3_INPROGRESS_PREFIX="queue/in-progress"
export LIBFILERUNNER_S3_FAILED_PREFIX="queue/failed"
export AWS_REGION="us-east-1"

go run ./examples/s3-reference
```

Or use the helper script:

```bash
export LIBFILERUNNER_S3_BUCKET="my-bucket"
./examples/s3-reference/run.sh
```
