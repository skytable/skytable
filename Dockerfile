#
# The Dockerfile for the TerrabaseDB server tdb
#

FROM dockerfile/ubuntu

RUN \
    cd /tmp &&
    apt install git
    curl https://sh.rustup.rs -sSf | sh -s -- -y
    git clone https://github.com/terrabasedb/terrabase.git
    cd terrabase
    git fetch --tags
    lt=$(git describe --tags `git rev-list --tags --max-count=1`)
    git checkout $lt
    cargo build --release -p tdb
    cp -f target/release/tdb /usr/local/bin

VOLUME ["/data"]

WORKDIR /data

CMD ["tdb"]

EXPOSE 2003