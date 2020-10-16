set -euo pipefail
RS_CHANGED_COUNT=$(git diff --numstat HEAD^..HEAD -- '*.rs' | wc -l)
DOCKERFILE_CHANGED=$(git diff --numstat HEAD^..HEAD -- 'Dockerfile' | wc -l)
DOCKER_CI_CHANGED=$(git diff --numstat HEAD^..HEAD -- 'docker-image.yml' | wc -l)

if [ '$RS_CHANGED_COUNT' != "0" ] || [ '$DOCKERFILE_CHANGED' != "0" ] || [ '$DOCKER_CI_CHANGED' != "0" ]; then
    echo "The docker image has to be built"
    echo "BUILD=true" >>$GITHUB_ENV
fi
