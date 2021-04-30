cd artifacts
shopt -s dotglob
find * -prune -type d | while IFS= read -r d; do
    echo "Zipping $d into $d.zip"
    zip $d.zip -r $d
done
