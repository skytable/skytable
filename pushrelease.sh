# This is the script that is used for pushing the latest tag to `remote`
lr=`git describe --tags --abbrev=0 --match v*`
git push origin $lr