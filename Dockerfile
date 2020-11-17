#
# The Dockerfile for the TerrabaseDB server tdb
#

FROM rust:latest
RUN \
    apt-get update && apt-get install git curl -y && \
    cd /tmp && \
    git clone https://github.com/terrabasedb/terrabasedb.git && \
    cd terrabasedb && \
    git checkout next && \
    cargo test --release -p tdb && \
    cargo build --release -p tdb && \
    apt-get remove git curl -y && \
    apt-get autoremove -y && \
    cp -f target/release/tdb /usr/local/bin

CMD ["tdb", "-h", "0.0.0.0", "-p", "2003"]

EXPOSE 2003/tcp
