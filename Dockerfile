#
# The Dockerfile for the TerrabaseDB server tdb
#

FROM ubuntu:20.04
ENV TZ=america/central
RUN \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ >/etc/timezone && \
    apt-get update && apt-get install git curl -y && \
    cd /tmp && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    git clone https://github.com/terrabasedb/terrabase.git && \
    cd terrabase && \
    git fetch --tags && \
    lt=$(git describe --tags $(git rev-list --tags --max-count=1)) && \
    git checkout $lt && \
    cargo build --release -p tdb && \
    apt-get remove rustc git curl -y && \
    apt-get autoremove -y && \
    cp -f target/release/tdb /usr/local/bin

VOLUME ["/data"]

WORKDIR /data

CMD ["tdb"]

EXPOSE 2003

ARG DEBIAN_FRONTEND=noninteractive
