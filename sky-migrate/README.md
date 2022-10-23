# Skytable migration tool

## Introduction

The Skytable migration tool can be used to perform migrations between database versions. The basic
idea is: "read all data from the older machine and send it over to the newer one".

For migrating versions from 0.6 to 0.7, the database administrator has to launch the tool in the
old data directory of the old instance and then pass the new instance host/port information. The
tool will then read the data from the directory (this was possible because 0.6 used a very simple
disk format than newer versions). This approach however has the advantage of not having to start
the database server for the migration to happen.

## How To Use

To upgrade, one needs to simply run:
```shell
# Here `<lastpath>` is the path to the last installation's data directory and
# `<host>` and `<port>` is the hostname and port for the new server instance
sky-migrate --prevdir <lastpath> --new <host>:<port>
```

## License

All files in this directory are distributed under the [AGPL-3.0 License](../LICENSE).
