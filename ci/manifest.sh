#!/bin/bash
set -e

create_manifest() {
  docker buildx imagetools create -t "${IMAGE_NAME}:$1" \
    "${IMAGE_NAME}:$1-amd64" \
    "${IMAGE_NAME}:$1-arm64"
}

create_manifest "${GITHUB_SHA}"
create_manifest "latest"

if [[ "${GITHUB_REF}" == refs/tags/v* ]]; then
  VERSION="${GITHUB_REF#refs/tags/v}"
  create_manifest "${VERSION}"
fi
