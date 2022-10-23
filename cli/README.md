# Skytable Shell

This directory contains the source code for the Skytable Shell. When compiled, it produces a single binary `skysh`, short for **Sky**table **s**hell. The Skytable shell can be used to connect to a Skytable instance, and it provides some nice to have shell features like history, terminal interactions and more.

By default, `skysh` connects to the `default:default` entity for an instance running on `127.0.0.1` (localhost) on port `2003`. You can change these connection settings use command-line parameters.

# Usage

The below terminal interactions show a few ways in which `skysh` can be used:

```shell
# Use skysh to run one-off expressions
skysh -e "SET x 100" "GET x"
```

## License

All files in this directory are distributed under the [AGPL-3.0 License](../LICENSE).
