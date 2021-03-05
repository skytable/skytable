#
# The Dockerfile for the Skytable server sdb
#

FROM debian:stable

COPY target/release/sdb /usr/local/bin

CMD ["sdb", "-h", "0.0.0.0", "-p", "2003"]

EXPOSE 2003/tcp
