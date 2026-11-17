#!/usr/bin/env bash
# Lints proto/ and generates gen/pb/{go,python,ts}, using the Bazel-built
# toolchain (see buf.sh), then runs gazelle on the Go and Python output.
#
# Usage: tools/proto/gen.sh [extra `buf generate` args]
set -euo pipefail

cd "$(dirname "$0")/../.."

# Hand-written files inside the output directories, kept across regeneration.
KEEP=(
  gen/pb/python/BUILD.bazel
)

tools/proto/buf.sh lint

# Clear previous output (including gazelle's BUILD files, which it recreates),
# so stubs for deleted protos don't linger.
prune=()
for keep in "${KEEP[@]}"; do
  prune+=(! -path "$keep")
done
for dir in gen/pb/go gen/pb/python gen/pb/ts; do
  [ -d "$dir" ] || continue
  find "$dir" -type f "${prune[@]}" -delete
  find "$dir" -mindepth 1 -type d -empty -delete
done

tools/proto/buf.sh generate "$@"

bazel run //:gazelle -- gen/pb
