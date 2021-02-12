# This is a simple script which creates a release build and
# moves the release builds into my $HOME/bin folder
cargo build --release
cp -f target/release/sdb target/release/skysh target/release/sky-bench $HOME/bin
echo 'Done!'
