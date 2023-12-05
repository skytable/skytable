# The Dockerfile for the Skytable server sdb

FROM debian:stable
# Copy the necessary binaries
COPY target/release/skyd /usr/local/bin
COPY target/release/skysh /usr/local/bin
# Create necessary directories
RUN mkdir /var/lib/skytable
COPY examples/config-files/dpkg/config.yaml /var/lib/skytable/config.yaml
COPY pkg/docker/start-server.sh /usr/local/bin/start-server.sh
WORKDIR /var/lib/skytable
# Install uuidgen for generating a random password
RUN apt-get update && apt-get install -y uuid-runtime
RUN chmod +x /usr/local/bin/start-server.sh
ENTRYPOINT ["/usr/local/bin/start-server.sh"]
EXPOSE 2003/tcp
