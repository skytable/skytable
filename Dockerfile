#
# The Dockerfile for the Skytable server sdb
#

FROM debian:stable

COPY target/release/skyd /usr/local/bin
COPY target/release/skysh /usr/local/bin
RUN mkdir /etc/skytable
RUN mkdir /var/lib/skytable
COPY examples/config-files/docker.toml /etc/skytable/skyd.toml
WORKDIR /var/lib/skytable
CMD ["skyd", "-c", "/etc/skytable/skyd.toml"]
EXPOSE 2003/tcp
