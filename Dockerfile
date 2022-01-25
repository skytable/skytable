#
# The Dockerfile for the Skytable server sdb
#

FROM debian:stable

COPY target/release/skyd /usr/local/bin
RUN mkdir /etc/skytable
COPY examples/config-files/docker.toml /etc/skytable/skyd.toml

CMD ["skyd", "-c", "/etc/skytable/skyd.toml"]

EXPOSE 2003/tcp
