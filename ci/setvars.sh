# This script sets a couple of environment variables
# If *.md files are modified, then there is no need for running the expensive
# build steps, instead we just set the `IS_MD_FILE` variable to true.
# Similarly, if the `actiondoc.yml` file is modified, the documentation needs to be
# be updated
set -euo pipefail
FILES_CHANGED=$(git diff --numstat HEAD^..HEAD | wc -l)
ACTIONS_CHANGED_COUNT=$(git diff --numstat HEAD^..HEAD -- 'actiondoc.yml' | wc -l)
if [ ${ACTIONS_CHANGED_COUNT} = "1" ]; then
    # This push changes the actions documentation
    echo "This push modifies the actiondoc.yml file"
    echo "IS_ACTIONS_DOC=true" >>$GITHUB_ENV
fi
