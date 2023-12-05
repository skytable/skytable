#!/bin/bash

CONFIG_FILE="/var/lib/skytable/config.yaml"
PASSWORD_MARKER="rootpass"
IP_MARKER="127.0.0.1"

generate_password() {
    uuidgen | cut -c -16
}

sed -i "s/$IP_MARKER/0.0.0.0/g" "$CONFIG_FILE"

if grep -q "$PASSWORD_MARKER" "$CONFIG_FILE"; then
    # Password not set, generate a new one
    PASSWORD=$(generate_password)
    sed -i "s/$PASSWORD_MARKER/$PASSWORD/g" "$CONFIG_FILE"
    echo "Generated Password: $PASSWORD"
else
    echo "Using existing password in config file"
fi

exec skyd --config "$CONFIG_FILE"
