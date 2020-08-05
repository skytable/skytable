# Changelog

All changes in this project will be noted in this file.

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
