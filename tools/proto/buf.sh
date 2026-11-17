#!/usr/bin/env bash
# Runs buf from the repo root with the Bazel-built buf and protoc plugins (see
# tools/proto/BUILD.bazel) on a throwaway PATH that holds nothing else, so no
# locally installed tool or Buf remote plugin takes part.
#
# Usage: tools/proto/buf.sh <buf args>, e.g. `tools/proto/buf.sh dep update`.
set -euo pipefail

cd "$(dirname "$0")/../.."

TOOLS=(
  buf
  protoc-gen-go
  protoc-gen-connect-go
  protoc-gen-es
  protoc-gen-py
  protoc-gen-connectrpc
)
TARGETS=()
for tool in "${TOOLS[@]}"; do
  TARGETS+=("//tools/proto:$tool")
done

bazel build "${TARGETS[@]}"

execroot="$(bazel info execution_root)"
bin_dir="$(mktemp -d)"
trap 'rm -rf "$bin_dir"' EXIT

# One wrapper per tool, named after it, exec'ing the Bazel-built executable.
# Launchers for js/py binaries find their runfiles next to that path.
for target in "${TARGETS[@]}"; do
  path="$(bazel cquery "$target" --output=starlark \
    --starlark:expr='target.files_to_run.executable.path' 2>/dev/null)"
  name="${target##*:}"
  printf '#!/bin/sh\nexec "%s" "$@"\n' "$execroot/$path" >"$bin_dir/$name"
  chmod +x "$bin_dir/$name"
done

# /usr/bin and /bin stay for the launchers' shells and coreutils.
export PATH="$bin_dir:/usr/bin:/bin"
# js_binary refuses to run from the execroot without it; "." is the documented
# value for running outside a build action.
export BAZEL_BINDIR=.

buf "$@"
