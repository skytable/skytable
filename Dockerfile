#
# The Dockerfile for the TerrabaseDB server tdb
#

FROM debian:stable

COPY target/release/tdb /usr/local/bin

CMD ["tdb", "-h", "0.0.0.0", "-p", "2003"]

EXPOSE 2003/tcp
