# Changelog

All changes in this project will be noted in this file.

## Version 0.6.0 [2021-05-27]

> Breaking changes!

* Dropped support for Terrapipe 1.0 (reached EOL)
* Added support for Skyhash 1.0 (see [#147](https://github.com/skytable/skytable/pull/147))
* Fixed persistence bugs (see [#151](https://github.com/skytable/skytable/pull/151))
* Fixed bugs in background services (see [#152](https://github.com/skytable/skytable/pull/152))
* Make BGSAVE recoverable (see [#153](https://github.com/skytable/skytable/pull/153))
* Added `lskeys` action (see [#155](https://github.com/skytable/skytable/pull/155))
* Added `compat` module (see [#158](https://github.com/skytable/skytable/pull/158))
* Added backward compatibility for storage formats all the way upto 0.3.0. See [this wiki article](https://github.com/skytable/skytable/wiki/Disk-storage-formats) for more information

### Upgrading existing clients

As Terrapipe 1.0 has reached EOL, all clients have to be upgraded to use the [Skyhash 1.0 Protocol](https://docs.skytable.io/protocol/skyhash).

### Upgrading existing datasets

Please refer to [this wiki article](https://github.com/skytable/skytable/wiki/Disk-storage-formats).

### Improvements in the new protocol (Skyhash)

* Upto 40% lower bandwidth requirements
* Upto 30% faster queries
* Support for recursive arrays
* More robust and well tested than Terrapipe

### Internal codebase improvements

* BGSAVE is now tested by the test suite
* `skysh` and `sky-bench` both use the [Rust client driver for Skytable](https://github.com/skytable/client-rust).

## Version 0.5.3 [2021-05-13]

> No breaking changes

Fix persistence (see [#150](https://github.com/skytable/skytable/issues/150))

## Version 0.5.2 [2021-05-07]

> No breaking changes

* `sky-bench` is less agressive and runs sanity test before benchmarking
* `skyd` now locks the data file (the `data.bin` file)
* The data directory structure has been changed (see #144) (all files are now stored in ./data/*)
* Fixed 'Connection Forcibly Closed' errors on Windows (see #110)
* Add support for line-editing and keyboard shortcuts on `skysh` (see #142)
* Fixed problems while parsing snapshots in the snapshot directory (see #144)
* The old data directory structure has been deprecated (see #144)
* Official support for 32-bit platforms (see #139)
* Tier-2 Support for Linux musl (x86_64) (see #136, #135)


## Version 0.5.1 [2021-03-17]

> No breaking changes

* Built-in TLS/SSL support
* Custom host/port settings in `sky-bench`
* Mock keys can be created with `sky-bench`
* Security patch for VE/S/00001
* Escaping for spaces in `skysh`
* `tdb` is now called `skyd` (short for 'Skytable Daemon')

## Version 0.5.0 [2020-11-19]

> This release introduces breaking changes!

* Command line configuration added to `tdb`
* ⚠ Positional arguments in `tsh` and `tdb-bench` have been removed
* `MKSNAP` now has an _enhanced version_ which enables snapshots to be created even if they're disabled on the server side
* If `BGSAVE` fails, no more writes will be accepted on the server side. All commands that try to modify data will return an internal server error
* `tdb-bench` now provides JSON output with the `--json` flag
* The `Dockerfile` was fixed to use command line arguments instead of the configuration file which caused problems
* The `enabled` key under the `snapshots` key in the configuration file has been removed

### Upgrading

* Users who ran `tsh` like `tsh 172.17.0.1 2003` will now need to run: 

``` shell
tsh -h 172.17.0.1 -p 2003
```

* Users who ran `tdb-bench` like `tdb-bench 10 100000 4` will now need to run:

``` shell
tdb-bench -c 10 -q 100000 -s 4
```

* To enable snapshots, you just have to add the key: there is no need for the `enabled` key. To disable snapshots, you just have to omit the `snapshot` key (block) from your configuration file

## Version 0.4.5 [2020-10-29]

> No breaking changes

This release adds support for automated snapshots, while also adding the `MKSNAP` action for doing the same remotely.

## Version 0.4.4 [2020-10-03]

> No breaking changes

This release adds the following actions: `KEYLEN` and `USET`

## Version 0.4.3 [2020-09-25]

> No breaking changes

This release adds the following actions:
`SSET` , `SUPDATE` , `SDEL` , `DBSIZE` and `FLUSHDB`

## Version 0.4.2 [2020-09-19]

> No breaking changes

This release adds `BGSAVE` for automated background saving (see [#11](https://github.com/skytable/skytable/issues/21))

## Version 0.4.1 [2020-09-06]

> No breaking changes

This release adds support for configuration files

## Version 0.4.0 [2020-08-30]

> This release introduces breaking changes

Changes:

* Actions added: `MSET` , `MGET` , `MUPDATE`
* Terrapipe 1.0
* Improved terminal output

Fixes:

* Explicit handling for incomplete responses in `tsh`

### Migrating existing clients

The Terrapipe protocol was revised and promoted to 1.0. This will cause all existing client implementations to break, since the protocol has changed fundamentally. The clients have to implement the [latest spec](https://terrabasedb.github.io/docs/Protocols/terrapipe).

## Version 0.3.2 [2020-08-07]

> No breaking changes

The `tsh` component printed the wrong version number. This has been fixed.

## Version 0.3.1 [2020-08-05]

> This release introduces breaking changes

This release fixes #7, #8. It also adds several under-the-hood optimizations greatly improving query performance.

### Migrating existing clients

The only significant change in the protocol is the new metalayout format: `#a#b#c` instead of the previously proposed `a#b#c#` .

### Disk storage format

The disk storage format was changed rendering existing binary data files incompatible. However, if you have any existing data - which is important, open an issue - because we'll be able to provide a tool that can help you easily migrate your existing datasets - with a one line command - so - no worries!

## Version 0.3.0 [2020-07-28] (⚠ EOL)

> No breaking changes.

This version enables persistence for stored data

## Version 0.2.0 [2020-07-27] (⚠ EOL)

> This release introduces breaking changes

This release implements the latest version of the Terrapipe protocol.

### Migrating existing clients

All clients have to reimplement the Terrapipe protocol to match the [latest spec](https://terrabasedb.github.io/docs/Protocols/unsupported-tp).

## Version 0.1.0 [2020-07-17] (⚠ EOL)

This release provides an experimental client and server implementation.
