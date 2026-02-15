#!/usr/bin/env bash
set -euo pipefail

REPO="us-east1-docker.pkg.dev/homelab-464022/homelab-repo"
TAG=$(git rev-parse --short HEAD)

build_and_push() {
    local name=$1
    local dockerfile=$2
    local image="${REPO}/${name}:${TAG}"

    echo "Building ${image}..."
    docker build -t "${image}" -f "${dockerfile}" .

    if gcloud artifacts docker images describe "${image}" &>/dev/null; then
        echo "Image ${image} already exists in Artifact Registry, skipping push."
    else
        echo "Pushing ${image}..."
        docker push "${image}"
    fi

    echo "  ${image}"
}

build_and_push "durable-execution" "Dockerfile"
build_and_push "durable-execution-migrations" "Dockerfile.migrations"

echo ""
echo "Image tag: ${TAG}"
