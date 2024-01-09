# Contribution guidelines

Firstly, thank you for your interest in contributing to this project! This project is powered by the community
and relies on hackers across the globe to report bugs, send suggestions and contribute code to move this project forward.

## Ways to contribute

- **#1**: Report bugs
- **#2**: Give us ideas
- **#3**: Send patches

## Coding guidelines

### Special comments

- `FIXME(@<username>)` : Use this when you have made an implementation that can be improved in the future, such as improved efficiency
- `HACK(@<username>)` : Use this when the code you are using a temporary workaround
- `TODO(@<username>)` : Use this when you have kept something incomplete
- `UNSAFE(@<username>)` : Use this to explain why the `unsafe` block used is safe

### Comment style

We prefer to use a 'mixed' commenting style: a cross between the C and C++ styles.
Generally, if a comment can be explained in one-line, then follow the C++ commenting style.
In other cases, use the C style.

### Formatting

- All Rust code should be formatted using `rustfmt`
- Imports in Rust source files are to be merged into a single `use` block. For example, instead of:
  ```rust
  use devtimer::SimpleTimer;
  use devtimer::ComplexTimer;
  use serde::{Serialize, Deserialize};
  use crate::something;
  ```

  this mode of styling is preferred:

  ```rust
  use {
      devtimer::{SimpleTimer, ComplexTimer},
      serde::{Serialize, Deserialize},
      crate::something,
  };
  ```

### Parts of the project

The main parts (ignorning CI scripts, stress test suite, test harness and custom compiler macros) are:

- `cli`: interactive shell `skysh`
- `server`: server daemon `skyd`
- `sky-bench`: benchmark tool

The `pkg` folder contains scripts used to build packages for different targets. The `examples` folder
contains example configuration files.

### Branches

The `next` branch is the _kind of_ stable branch which contains the latest changes. However, for most purposes, you should always download sources from the tags.

Pushes are made directly to next if they don't change things significantly; for example, changes in 
documentation comments and general optimizations. If however the changes are huge, then they must be created 
on a separate branch, a pull request opened, the CI suite run and finally merged into next.

## Pull request guidelines

### Typo correction or doc updates

The creation of superflous merge requests is generally discouraged. Such examples include the creation of 
multiple PRs to correct single typos, update comments or update docs.

It would be far better if you could fix a considerable subset (if not all) of these issues in one pull request (it's fine to create multiple commits in the same PR).
This is because we don't want to utilize compute capacity or multiply our git history for changes
that do not have any behavioral impact. It is recommended that you open up a single PR and correct a substantial subset of these inconsistencies (if not all), while also adding `[skip ci]` or `[ci skip]` in your
commit messages to avoid triggering the workflows.

## Steps

1. Fork the repository
2. Make your changes and start a pull request
3. Sign the CLA (if you haven't signed it already)
4. One of the maintainers will review your patch and suggest changes if required
5. Once your patch is approved, it will be merged into the respective branch
6. Done, you're now a contributor 🎉!

## Development environment setup

Skytable uses a Makefile for builds and running the test suite along with a number of tools. To set these up:

1. Install the latest Rust toolchain (stable)
2. Install a C Compiler, Make, Perl and the libssl-dev package on platforms where they are required
3. On Windows:
   - You might need to perform additional steps to add `perl` to our `PATH` variable
   - You may also need to set up powershell if it isn't set up already as it is used by the test script

**Building**

1. For debug (unoptimized) builds:
   ```
   make build
   ```
2. For release (optimized) builds:
   ```
   make release
   ```

> **TIP**: You can explicitly specify a target triple by setting a `TARGET` environment variable

**Testing**

Testing is simple: just run this:

```
make test
```

> **NOTE**: Make sure port 2003 and 2004 are not used by any applications. Also, make sure your _own instance_ isn't running on any of these ports; if that is the case, you might end up losing data due to conflicting entity names! The test suite creates multiple spaces and some models within it to run all the tests.
