{
    git config --global user.email "$BOT_MAIL"
    git config --global user.name "$BOT_USER"
    C_USER=$(git show -s --format='%an' HEAD)
    C_MAIL=$(git show -s --format='%ae' HEAD)
    cd ..
    git clone https://github.com/Skybase/docs.git
    cd docs
    chmod +x builddoc.sh
    sh ./builddoc.sh
    git add . && git commit -m "Updated docs from upstream" -m "Triggered by ${GITHUB_SHA}" --author "$C_USER <$C_MAIL>"
    eval '${BOT_API}'
} >>/dev/null
