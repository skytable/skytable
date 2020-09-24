# Changelog

All changes in this project will be noted in this file.

## Version 0.4.3 [2020-09-25]

> No breaking changes

This release adds the following actions:
`SSET` , `SUPDATE` , `SDEL` , `DBSIZE` and `FLUSHDB`

## Version 0.4.2 [2020-09-19]

> No breaking changes

This release adds `BGSAVE` for automated background saving (see [#11](https://github.com/terrabasedb/terrabasedb/issues/21))

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

The Terrapipe protocol was revised and promoted to 1.0. This will cause all existing client implementations to break, since the protocol has changed fundamentally. The clients have to implement the [latest spec](https://git.io/JJZ8Z).

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

## Version 0.3.0 [2020-07-28]

> No breaking changes

This version enables persistence for stored data

## Version 0.2.0 [2020-07-27]

> This release introduces breaking changes

This release implements the latest version of the Terrapipe protocol.

### Migrating existing clients

All clients have to reimplement the Terrapipe protocol to match the [latest spec](https://git.io/JJZ8Z).

## Version 0.1.0 [2020-07-17]

This release provides an experimental client and server implementation.
