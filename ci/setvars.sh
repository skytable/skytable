# This script sets a couple of environment variables
# If *.md files are modified, then there is no need for running the expensive
# build steps, instead we just set the `IS_MD_FILE` variable to true.
# Similarly, if the `actions.jsonc` file is modified, the documentation needs to be
# be updated
set -euo pipefail
git checkout next
FILES_CHANGED=$(git diff --numstat HEAD^..HEAD | wc -l)
ACTIONS_CHANGED_COUNT=$(git diff --numstat HEAD^..HEAD -- 'actions.jsonc' | wc -l)
MD_CHANGED_COUNT=$(git diff --numstat HEAD^..HEAD -- '*.md' | wc -l)
if [ ${FILES_CHANGED} = ${MD_CHANGED_COUNT} ]; then
    # This push just modifies markdown files
    echo "This push only modifies markdown files"
    echo "IS_MD_FILE=true" >>$GITHUB_ENV
elif [ ${ACTIONS_CHANGED_COUNT} = "1" ]; then
    # This push changes the actions documentation
    echo "This push modifies the actions.jsonc file"
    echo "IS_ACTIONS_DOC=true" >>$GITHUB_ENV
fi
