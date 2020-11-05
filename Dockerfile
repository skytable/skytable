#
# The Dockerfile for the TerrabaseDB server tdb
#

FROM ubuntu:20.04
ENV TZ=america/central
RUN \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ >/etc/timezone && \
    apt-get update && apt-get install git curl build-essential -y && \
    cd /tmp && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    git clone https://github.com/terrabasedb/terrabasedb.git && \
    cd terrabasedb && \
    git checkout next && \
    $HOME/.cargo/bin/cargo build --release -p tdb && \
    apt-get remove git curl -y && \
    apt-get autoremove -y && \
    $HOME/.cargo/bin/rustup self uninstall -y && \
    cp -f target/release/tdb /usr/local/bin

CMD ["tdb", "-h", "0.0.0.0", "-p", "2003"]

EXPOSE 2003/tcp

ARG DEBIAN_FRONTEND=noninteractive
