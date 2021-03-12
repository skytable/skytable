# This script checks if any source files were modified in a push event
# If source files were indeed modified, then this script sets `BUILD=true`

set -euo pipefail

SRC_CHANGED_COUNT=$(git diff --numstat HEAD^..HEAD -- '*.rs' '*.yml' '*.toml' 'Dockerfile' | wc -l)

if [ $SRC_CHANGED_COUNT != "0" ]; then
    echo "The docker image has to be built"
    echo "BUILD=true" >>$GITHUB_ENV
fi
