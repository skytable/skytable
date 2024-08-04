# Changelog

All changes in this project will be noted in this file.

## Version 0.8.4

### Additions

- Server:
  - Added `TRUNCATE MODEL` query (and no, I still have no idea why this wasn't present in the first place. There are some things in the universe that no one knows the answer to. This is one of those things.)
    > Also note: `TRUNCATE MODEL` is root only right now.
  - Simple lists can now be defined using simple list syntax
    - For example, instead of `create model spc.mdl(uid: username, notes: list { type: string })` you will now be able to simply use `create model mymodel(uid: username, notes: [string])` instead.
    - Similarly, instead of `alter model spc.mdl add notes { type: list { type: string } }` you can use `alter model spc.mdl add notes { type: [string] }`

### Fixes

- Server:
  - Improved diagnostic messages (and output formatting)
  - Fixed memory leaks across multiple routines (oarticularly startup routines)
  - Fixed potential segfaults (note: these are *potential segfaults* as we were not able to actually reproduce it anywhere despite heavy permutation
  testing, but the fixes were made out of an abundance of caution)
- CLI:
  - Upgraded client driver to fix loading of large blob/string fetches from the database

## Version 0.8.3

### Additions

- Server:
  - Added the `backup` subcommand to create unattended manifest-based backups
  - Added the `restore` subcommand to enable unattended restore for manifest-based backups
- Benchmark tool:
  - Supports *full spectrum latency analysis* and offers advanced insight into executed queries:
    - Latency distributions
    - Mean/Min/Max/Stdev statistics
    - Details about executed workload and its tasks with clear descriptions of what was executed
  - Switch to using `workload` based benchmarks

### Fixes

- Server:
  - Fixed error messages
  - Fixed a bug where an empty GNS won't detect a missing data directory which could cause issues with DDL transactions
- Benchmark tool:
  - Running a SIGINT now gracefully terminates the workload

## Version 0.8.2

### Additions

- Server:
  - Skyhash/2: Restored support for pipelines
  - Enable online (runtime) recovery of transactional failures due to disk errors
  - Added BlueQL shorthands:
    - `INSERT`: `INS`
    - `SELECT`: `SEL`
    - `UPDATE`: `UPD`
    - `DELETE`: `DEL`
  - Added new `UPSERT` (shorthand: `UPS`) query
  - Auto-compaction of journal(s) on boot
  - Allow manual compaction with `skyd compact` subcommand
- CLI:
  - Enable setting custom history file location using the `SKYSH_HISTORY_FILE` environment variable
  - Correctly format rows as CSV when using `--eval`

### Fixes

- Server:
  - Fixed an issue where an incorrect handshake with multiple errors caused the client connection
    to be terminated without yielding an error
  - Fixed SE bug that resulted in unsafe cleanup of journals when multiple failures occur in sequence
  - Fixed SE memory management bug in delta diff algorithm: In rare cases, a crash might occur on startup (*only during startup* and *not* at runtime)
  - Fixed DCL command `sysctl alter user` not working properly (preventing change of any data)
- CLI:
  - Fixed transient history file location. History is now always saved to $HOME/.sky_history
  - Fixed an issue where queries were not correctly parameterized when using `--eval`

### Platform notes

- 32-bit Windows (MSVC) has been downgraded to a Tier-2 platform and will likely be deprecated in the future
- Linux ARM64 is now a Tier-1 platform
- macOS ARM64 is a now a Tier-1X platform

## Version 0.8.1

### Additions

- Added support for manual repair with the `skyd repair` command

### Fixes

- **Server**:
  - Fixed migration from v1 SE (released with v0.8.0-beta) to v2 SE (released in v0.8.0)
  - Fixed health reporting
  - Fixed a connection crash (not a server-wide crash) at the pre-connection stage when authentication data
    was sent incorrectly
- **CLI**:
  - Fixed `--eval` output. All server errors are now correctly written to `stderr`
  - Guard against empty passwords

## Version 0.8.0

> This is the first release of Skytable Octave, and it changes the query API entirely making all previous versions incompatible 
> excluding the data files which are automatically upgraded per our backwards compatibility guarantees

### Additions

#### BlueQL query language
- DDL:
  - `space`s are the equivalent of the `keyspace` from previous versions
  - `model`s are the equivalent of `table`s from previous version
  - The following queries were added:
    - `CREATE SPACE [IF NOT EXISTS] ...`
    - `CREATE MODEL [IF NOT EXISTS] ...`
      - Nested lists are now supported
      - Type definitions are now supported
      - Multiple fields are now supported
    - `ALTER SPACE ...`
    - `ALTER MODEL ...`
    - `DROP SPACE [IF EXISTS] ...`
    - `DROP MODEL [IF EXISTS] ...`
    - `USE <space>`:
      - works just like SQL
      - **does not work with DDL queries**: the reason it works in this way is to prevent accidental deletes
    - `INSPECT ...`:
      - `INSPECT global`: can be used to inspect the global state, seeing all spaces currently present on the system, users and other information. Some information is limited to the root account only (as JSON)
      - `INSPECT space <space>`: can be used to inspect a single space, returning a list of models and relevant information (as JSON)
      - `INSPECT model <model>`: can be used to inspect a single model, returning declaration and relevant information (as JSON)
- DML:
  - **All actions removed**: All the prior `SET`, `GET` and other actions have been removed in favor of the new query language
  - The following queries were added:
    - `INSERT INTO <space>.<model>(col, col2, col3, ...)`
      - Insert queries can use several ways of insertion including:
        - a standard order of declared columns like this:
          ```sql
          INSERT INTO myspace.mymodel(col1, col2, col3, ...)
          ```
        - using a map:
          ```sql
          INSERT INTO myspace.mymodel { col1: val1, col2: val2, col4: val4, col3: val3 }
          ```
      - Inserts can also make use of function calls during inserts:
        - It can be called like this: `INSERT INTO myspace.mymodel(@uuidstr, ...)`
        - The following functions are available:
          - `@uuidstr`: returns a string with a randomly generated v4 UUID
          - `@uuidbin`: same as `@uuidstr` but returns it as a blob
          - `@timesec`: returns a 64-bit integer with the current time in seconds
    - `SELECT [ALL] field1, field2, ... FROM <space>.<model> WHERE <primary_key_column> = <value> [LIMIT n]`
    - New data manipulation via `UPDATE` allows arithmetic operations, string manipulation and more! Examples:
      - `UPDATE <space>.<model> SET col_num += 1 WHERE <primary_key_column> = <value>`
      - `UPDATE <space>.<model> SET mystring += " last part of string" WHERE ...`
    - `DELETE FROM <space>.<model> WHERE <primary_key_column> = <value>`
- DCL:
  - `SYSCTL CREATE USER <name> WITH { password: <password> }`
  - `SYSCTL ALTER USER <name> WITH { password: <new password> }`
  - `SYSCTL DROP USER <name>`

#### Fractal engine

- The fractal engine is the start of the development of advanced internal state management in Skytable
- Effectively balances performance and reliability tasks

#### Skyhash 2 protocol
- The `Skyhash-2` protocol now uses a multi-stage connection sequence for improved security and performance
- Seamless auth
- More types
- Fewer retransmissions

#### Storage engines
- **New `deltax` storage engine for data**:
  - The `deltax` storage engine monitors the database for changes and only records the changes in an append only file
  - The interval can be adjusted per requirements of reliability
  - Faster and hugely more reliable than the previous engine
- **New DDL ACID transactions with with the `logx` engine**:
  - DDL queries are now fully transactional which means that if they are executed, you can be sure that they were complete and synced to disk
  - This largely improves reliability

#### New shell

- The new client shell easily authenticates based on the Skyhash-2 protocol
- More reliable

#### Benchmark tool

- New benchmark engines to enable more efficient and real-world load testing
- New benchmark engines use far lesser memory
- New engines can handle midway benchmark crashes

### Breaking changes

- `skyd`:
  - **The entire query API has changed as actions have been removed**
  - **Spaces and models**: replace keyspaces and models respectively
  - **Configuration**:
    - The configuration system now uses YAML instead of TOML for better readability
    - The configuration options have changed across CLI, ENV and the config file
    - Authentication **must be enabled** irrespective of `dev`/`prod` mode
- `sky-bench`:
  - New benchmark engines are completely different from the previous engines (see above)
  - Configuration options have changed because of how the new Skytable engine works
- `skysh`:
  - Configuration options have changed because of how the new Skytable engine works
- `sky-migrate`: **This tool is deprecated and has been removed**. The Skytable engine will now automatically manage data upgrades. (please see [this issue](https://github.com/skytable/skytable/issues/320) for discussion on the same)

## Version 0.7.6

### Fixes

- `skyd`:
  - Fixed erroneous removal of `auth` system table during tree cleanup (see #276)
- `sky-bench`:
  - Fixed sample space calculation

### Breaking changes

- The `testkey` subcommand was removed. This was done due to the addition of different data types making this command
  redundant since it only works with simple key/value pairs

## Version 0.7.5

### Additions

- `sys` action for system information and metrics:
  - `info`: For static properties:
    - `version`: Server version
    - `protocol`: Protocol version string (Skyhash-x.y)
    - `protover`: Protocol version float
  - `metric`: For dynamic properties:
    - `health`: System health
    - `storage`: Bytes used for on-disk storage
- `INSPECT KEYSPACE` without arguments to inspect the current keyspace
- `INSPECT TABLE` without arguments to inspect the current table
- `AUTH WHOAMI` returns the AuthID of the currently logged in user

### Improvements

- Enable multiples values to be pushed into a list at once with `lmod push`
- (skyd) Improved error reporting infrastructure with more details and context

### Fixes

- skysh:
  - Added error code output for new error codes which otherwise printed "Unknown error"
  - Fixed handling of EOF keystroke
- skyd:
  - Respect logging level for all terminal output
  - Fixed LF check in protocol impl
  - Fixed new instance detection (now checks if data directory is empty or not)
  - Fixed panic resulting from corrupted metadata in `PARTMAP`
  - Fixed invalid pipeline response when action error is propagated from a single stage
  - Fixed bug where the preload wasn't flushed if a snapshot already flushed it before the
    save on termination routine
  - Fixed bug that prevented tree cleanup from working
  - Disallow `PRELOAD` and `PARTMAP` as entity names
  - Fixed edge case where time on host is incorrect, resulting in bad snapshot names which might
    ultimately lead to loss of snapshot data
  - Fixed remote snapshots with same names being overwritten
- (sky-bench) Fixed `testkey` causing RSTs

## Version 0.7.4

### Additions

- Token-based authentication:
  - `auth claim`
  - `auth login <username> <token>`
  - `auth logout`
  - `auth adduser <username>`
  - `auth deluser <username>`
  - `auth restore <username>`
  - `auth restore <origin key> <username>`
  - `auth listuser`
- Shell now supports multiple `--eval` expressions
- Partial entity syntax: `:table` can be used for referring to the current table. For example
  you can use `use :default` instead of `use default:default`

### Fixes

- Fixed snapshot option being silently ignored in configuration file
- Fixed snapshot engine init failure

## Version 0.7.3

### Additions

- Added `dev/prod` mode for making sure that the recommended production settings are used
- Added support for system native endian storage (backward compatible)
- Added `range` to `lget` to get subarrays

### Fixes

- Fixed infinite wait (loop) when sample space for key generation is not large enough
- Fixed infinite save-on-termination loop. Now loop uses a threshold and on 4 subsequent `SIGINT` signals,
  it will quit forcefully
- Removed `upgrade` subcommand from `skyd` which was removed in 0.7, but was erroneously accepted in the CLI
  parameters
- Restored ability to use `--restore <backupdir>` to restore data from previous snapshots which was silently
  ignored

## Version 0.7.2

### Additions

- `skyd`:
  - Pipelined queries are now supported:
    - Saves bandwidth
    - Reduces latency
  - The new non-null typed array type was added
  - The `whereami` action was added which lets one find out which entity the current connection
    is connected to
- `skysh`:
  - now shows the current entity in the prompt
  - handles special character strings more reliably
  - now supports splitting a command across multiple lines
  - now supports multi-line pastes
  - now supports comments (beginning with `#`)

## Version 0.7.1

### Additions

- Added the list data type
- Added actions for lists:
  - `LSET <list> <values> ...`
  - `LGET`
    - `LGET <list>`
    - `LGET <list> LEN`
    - `LGET <list> VALUEAT <idx>`
    - `LGET <list> LIMIT <limit>`
    - `LGET <list> FIRST`
    - `LGET <list> LAST`
  - `LMOD`
    - `LMOD <list> push <value>`
    - `LMOD <list> insert <index> <value>`
    - `LMOD <list> pop <optional index>`
    - `LMOD <list> remove <index>`
    - `LMOD <list> clear`
- Added creation of lists:
  - `CREATE TABLE <entity> keymap(type,list<type>)`
- Added compatibility of `DEL`, `EXISTS` and `LSKEYS` with lists
- Added support for configuration via environment variables

### Improvements

- Reduced memory usage in `sky-bench`
- Reduced allocations in Skyhash (`skyd`) protocol implementation
- Misc. fixes in internal structures (`skyd`)
- Improvements in printing of binary strings in `skysh`

### Fixes

- Fixed unexpected removal of single and double quotes from input strings in `skysh`
- Fixed incorrect removal of quotes from input strings in `skysh`

## Version 0.7.0

### Additions

- **Multiple keyspaces**:
  - Keyspaces hold multiple tables and determine the replication strategy
  - Keyspaces can be created with:
    ```sql
    CREATE KEYSPACE <name>
    ```
  - The `system` keyspace is reserved for the system while the `default` keyspace is
    the one available by default. The `system` or `default` keyspace cannot be removed
  - Keyspaces can be deleted with:
    ```sql
    DROP KEYSPACE <name>
    ```
  - If a keyspace still has tables, it cannot be dropped by using `DROP KEYSPACE <name>`, instead
    one needs to use `DROP KEYSPACE <name> force` to force drop the keyspace
- **Multiple tables**:
  - Tables hold the actual data. When you connect to the server, you are connected to the `default`
    table in the `default` keyspace. This table is non-removable
  - Tables can be created with:
    ```sql
    CREATE TABLE <entity> <model>(modelargs) <properties>
    ```
  - Tables can be dropped with:
    ```sql
    DROP TABLE <entity>
    ```
- **Entity groups**: While using DDL queries and inspection queries, we can use the _Fully Qualified Entity_ (FQE) syntax instead of the table name. For example, to `inspect` the `cyan` table in keyspace `supercyan`, one can simply run:
  ```sql
  INSPECT TABLE supercyan:cyan
  ```
  The syntax is:
  ```sql
  keyspace:table
  ```
  **Note**: Both keyspaces and tables are entities. The names of entities must:
  - Begin with an underscore (\_) or an ASCII alphabet
  - Not begin with a number (0-9)
  - Must have lesser than 64 characters
- **Keymap data model**:
  - To create a keymap table, run:
    ```sql
    CREATE TABLE <entity> keymap(<type>,<type>)
    ```
  - The following types were introduced:
    - `str`: A valid unicode string
    - `binstr`: A binary string
- **Volatile table property**:

  - To create a volatile table, irrespective of the data model, run:
    ```sql
    CREATE TABLE <entity> <model>(modelargs) volatile
    ```
  - Volatile tables always exist, but the data in them does not persist between restarts. This makes them extremely useful for caches

- **Inspection**:
  - Keyspaces can be inspected with:
    ```sql
    INSPECT KEYSPACE <name>
    ```
    This will list all the tables in the keyspace
  - Tables can be inspected with:
    ```sql
    INSPECT TABLE <entity>
    ```
    This will give information about the data model and other properties of the table
  - To list all keyspaces, this can be run:
    ```sql
    INSPECT KEYSPACES
    ```
- **Cyanstore 1A disk storage format**: Cyanstore (v1A) was a new storage format built for the multi-keyspace-table world. It efficiently stores and retrieves records, tables and keyspaces
- **Realtime keyspace/table switch**
  - To switch to a new keyspace in real-time, one needs to run:
    ```sql
    USE keyspace
    ```
  - To switch to a new table in real-time, one needs to run:
    ```sql
    USE keyspace:table
    ```
- **Entity respecting actions**:

  - **`FLUSHDB`**: To flush all the data in a specific table, run:
    ```sql
    FLUSHDB <entity>
    ```
  - **`DBSIZE`**: To see the number of entries in a specific table, run:
    ```sql
    DBSIZE <entity>
    ```
  - **`LSKEYS`**:
    - `LSKEYS` will return keys from the current table
    - `LSKEYS <count>` will return _count_ keys from the current table
    - `LSKEYS <entity>` will return keys from the given table
    - `LSKEYS <entity> <count>` will return _count_ keys from the given table

- **Snapshot isolation for strong actions**: This makes strong actions
  extremely reliable when compared to the earlier releases
- Non-interactive TLS private key passphrase input: Just save your password in some
  file and then pass `--tlspassin /path/to/passfile.txt`. You can do the same by using the
  `tlspassin` key under SSL in the configuration file
- `MPOP` now replaces `POP` to accept multiple keys while `POP` will accept a single key
  to follow the `MGET`/`GET` naming convention
- TLS port can now be set to a custom port via CLI arguments
- `sky-bench` can now run multiple times to get average values through the `--runs` option
- `HEYA` now does an echo with the second argument

### Fixes

- Zero length argument causing runtime panic in `skysh`
- `HEYA!` not reporting errors on incorrect number of arguments
- Panic on incorrect data type in `skyd`
- `sky-bench` no longer affects your personal data because it creates a random temporary table
  under the `default` keyspace
- `sky-bench`'s `testkey` subcommand causing key collisions
- Fix log output in `sky-bench` even if the `--json` flag was passed
- Use flocks to enable auto release of pid file, even if process is forcefully terminated
- Fixes [CVE-2021-37625](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-37625)

### Breaking

- All actions now accept the `AnyArray` type introduced in Skyhash 1.1
- `POP` now accepts one key while `MPOP` accepts multiple keys
- Disk storage format has changed
- The `upgrade` subcommand has been removed

## Version 0.6.4 [2021-08-05]

### Fixes

- Fixes [CVE-2021-37625](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-37625) (backport)

## Version 0.6.3 [2021-06-27]

### Additions

- The maximum number of clients can now be manually configured [see [#182](https://github.com/skytable/skytable/pull/182)]

### Fixes

- Ability to connect to the server over TLS has been restored in `skysh` [see [#181](https://github.com/skytable/skytable/pull/181)]

## Version 0.6.2 [2021-06-24]

### Fixes

- The save operation now automatically attempts to recover on failure during termination [see [#166](https://github.com/skytable/skytable/pull/166)]
- More than one process can no longer concurrently use the same data directory, preventing any possible data corruption [see [#169](https://github.com/skytable/skytable/pull/169), [#167](https://github.com/skytable/skytable/issues/167)]
- Fixed longstanding error in `sky-bench` component that caused key collisions
- Fixed bug in `sky-bench` that allowed passing 0 for the inputs
- Fixed handling of SIGTERM in `skyd` [see [#178](https://github.com/skytable/skytable/pull/178)]
- Fixed incorrect termination codes [see [#178](https://github.com/skytable/skytable/pull/178)]

### Additions

- Added the `POP` query [[see #173](https://github.com/skytable/skytable/pull/173)]
- Added stress testing for testing correctness under load [[see #175](https://github.com/skytable/skytable/pull/175)]

### Workflow

- Use Makefiles for builds [see [#172](https://github.com/skytable/skytable/pull/172), [#174](https://github.com/skytable/skytable/pull/174)]
- Tier-1 Support for ARM64 [see [#179](https://github.com/skytable/skytable/pull/179)]

## Version 0.6.1 [2021-06-07]

> No breaking changes

- Snapshotting failure will now poison the database (customizable through CLI options or the configuration file)
  [see [#160](https://github.com/skytable/skytable/pull/160)]
- Added file-locking on Solaris [see [#162](https://github.com/skytable/skytable/pull/162)]
- Fixed missing explicit `fsync` or `FlushFileBuffers` after writing data
- Optimized wait on snapshot busy-loop using `_mm_pause` (on x86/x86_64) or `__yield` (on aarch64/arm)

## Version 0.6.0 [2021-05-27]

> Breaking changes!

- ⚠ Dropped support for Terrapipe 1.0 (reached EOL)
- ⚠ New disk storage format with much faster reads and/or writes
- Added support for Skyhash 1.0 (see [#147](https://github.com/skytable/skytable/pull/147))
- Fixed persistence bugs (see [#151](https://github.com/skytable/skytable/pull/151))
- Fixed bugs in background services (see [#152](https://github.com/skytable/skytable/pull/152))
- Make BGSAVE recoverable (see [#153](https://github.com/skytable/skytable/pull/153))
- Added `lskeys` action (see [#155](https://github.com/skytable/skytable/pull/155))
- Added `compat` module (see [#158](https://github.com/skytable/skytable/pull/158))
- Added backward compatibility for storage formats all the way upto 0.3.0. See [this wiki article](https://github.com/skytable/skytable/wiki/Disk-storage-formats) for more information

### Upgrading existing clients

As Terrapipe 1.0 has reached EOL, all clients have to be upgraded to use the [Skyhash 1.0 Protocol](https://docs.skytable.io/protocol/skyhash).

### Upgrading existing datasets

Please refer to [this wiki article](https://github.com/skytable/skytable/wiki/Disk-storage-formats).

### Improvements in the new protocol (Skyhash)

- Upto 40% lower bandwidth requirements
- Upto 30% faster queries
- Support for recursive arrays
- More robust and well tested than Terrapipe

### Internal codebase improvements

- BGSAVE is now tested by the test suite
- `skysh` and `sky-bench` both use the [Rust client driver for Skytable](https://github.com/skytable/client-rust).

## Version 0.5.3 [2021-05-13]

> No breaking changes

Fix persistence (see [#150](https://github.com/skytable/skytable/issues/150))

## Version 0.5.2 [2021-05-07]

> No breaking changes

- `sky-bench` is less agressive and runs sanity test before benchmarking
- `skyd` now locks the data file (the `data.bin` file)
- The data directory structure has been changed (see #144) (all files are now stored in ./data/\*)
- Fixed 'Connection Forcibly Closed' errors on Windows (see #110)
- Add support for line-editing and keyboard shortcuts on `skysh` (see #142)
- Fixed problems while parsing snapshots in the snapshot directory (see #144)
- The old data directory structure has been deprecated (see #144)
- Official support for 32-bit platforms (see #139)
- Tier-2 Support for Linux musl (x86_64) (see #136, #135)

## Version 0.5.1 [2021-03-17]

> No breaking changes

- Built-in TLS/SSL support
- Custom host/port settings in `sky-bench`
- Mock keys can be created with `sky-bench`
- Security patch for VE/S/00001 [(CVE-2021-32814)](https://nvd.nist.gov/vuln/detail/CVE-2021-32814)
- Escaping for spaces in `skysh`
- `tdb` is now called `skyd` (short for 'Skytable Daemon')

## Version 0.5.0 [2020-11-19]

> This release introduces breaking changes!

- Command line configuration added to `tdb`
- ⚠ Positional arguments in `tsh` and `tdb-bench` have been removed
- `MKSNAP` now has an _enhanced version_ which enables snapshots to be created even if they're disabled on the server side
- If `BGSAVE` fails, no more writes will be accepted on the server side. All commands that try to modify data will return an internal server error
- `tdb-bench` now provides JSON output with the `--json` flag
- The `Dockerfile` was fixed to use command line arguments instead of the configuration file which caused problems
- The `enabled` key under the `snapshots` key in the configuration file has been removed

### Upgrading

- Users who ran `tsh` like `tsh 172.17.0.1 2003` will now need to run:

```shell
tsh -h 172.17.0.1 -p 2003
```

- Users who ran `tdb-bench` like `tdb-bench 10 100000 4` will now need to run:

```shell
tdb-bench -c 10 -q 100000 -s 4
```

- To enable snapshots, you just have to add the key: there is no need for the `enabled` key. To disable snapshots, you just have to omit the `snapshot` key (block) from your configuration file

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

- Actions added: `MSET` , `MGET` , `MUPDATE`
- Terrapipe 1.0
- Improved terminal output

Fixes:

- Explicit handling for incomplete responses in `tsh`

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
