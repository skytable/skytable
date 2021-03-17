#
# The Dockerfile for the Skytable server sdb
#

FROM debian:stable

COPY target/release/skyd /usr/local/bin

CMD ["skyd", "-h", "0.0.0.0", "-p", "2003"]

EXPOSE 2003/tcp
