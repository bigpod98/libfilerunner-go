#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${LIBFILERUNNER_S3_BUCKET:-}" ]]; then
  echo "LIBFILERUNNER_S3_BUCKET is required"
  exit 1
fi

export AWS_REGION="${AWS_REGION:-us-east-1}"
export LIBFILERUNNER_S3_INPUT_PREFIX="${LIBFILERUNNER_S3_INPUT_PREFIX:-queue/input}"
export LIBFILERUNNER_S3_INPROGRESS_PREFIX="${LIBFILERUNNER_S3_INPROGRESS_PREFIX:-queue/in-progress}"
export LIBFILERUNNER_S3_FAILED_PREFIX="${LIBFILERUNNER_S3_FAILED_PREFIX:-queue/failed}"

go run ./examples/s3-reference
