# Skytable migration tool

The Skytable migration tool can be used to perform migrations between database versions. The basic idea is:
"read all data from the older machine and send it over to the newer one."

For migrating versions from 0.6 to 0.7, the database administrator has to launch the tool in the old data directory of the old instance and then pass the new instance host/port information. The tool will then read the data from the directory (this was possible because 0.6 used a very simple disk format than newer versions). This approach however has the advantage of not having to start the database server for the migration to happen.

## License

All files in this directory are distributed under the [AGPL-3.0 License](../LICENSE).
