# Terrabase**DB** - The next-generation database

![GitHub Workflow Status](https://img.shields.io/github/workflow/status/terrabasedb/terrabase/Rust?style=flat-square) ![Status: Pre-Alpha](https://img.shields.io/badge/Status-Pre--alpha-critical?style=flat-square) ![Version: 0.1.0](https://img.shields.io/badge/Development-Actively%20Developed-32CD32?style=flat-square) ![GitHub release (latest by date)](https://img.shields.io/github/v/tag/terrabasedb/terrabase.svg?style=flat-square)

## What is TerrabaseDB?

TerrabaseDB (or TDB for short) is an effort to provide the best of key/value stores, document stores and columnar databases - **simplicity, flexibility and queryability at scale**. This project is currently in a <b>pre-alpha</b> stage and is undergoing rapid development.

## Status

As noted earlier, TerrabaseDB is pre-alpha software and the entire API is subject to major breaking changes, at the moment.

## Platforms
![Linux supported](https://img.shields.io/badge/Linux%20x86__64-supported%20✓-228B22?style=flat-square&logo=linux) ![macOS supported](https://img.shields.io/badge/macOS%20x86__64-supported%20✓-228B22?style=flat-square&logo=apple) ![Windows supported](https://img.shields.io/badge/Windows%20x86__64-supported%20✓-228B22?style=flat-square&logo=windows)

## Getting started

We have experimnetal client and server implementations for the database already.
The releases are uploaded in bundles, for example, `tdb-bundle-v0.2.0-x86_64-unknown-linux-gnu.zip`. Each bundle contains `tdb` and `tsh`, that is, the database server and the client command-line tool.

* Download a bundle for your platform from [releases](https://github.com/terrabasedb/terrabase/releases)
* Rename the files to tdb and tsh for ease
* Run `chmod +x tdb tsh`
* Start the database server by running `./tdb`
* Start the client by running `./tsh`
* You can run commands like `SET sayan 17` , `GET cat` , `UPDATE cat 100` or `DEL cat` !

## Goals

* Fast
* Designed to provide <b>safe flexibility</b>
* Multithreaded ✓
* Memory-safe ✓
* Resource friendly ✓
* Scalable
* Simplicity

## Versioning

This project strictly follows semver, however, since this project is currently in the development phase (0.x.y), the API may change unpredictably

## Contributing

**Yes!** - this project **needs you**! We want hackers, like you, from all across the globe to help us create the next-generation database. Read the guide [here](./CONTRIBUTING.md).

## Community

A project which is powered by the community believes in the power of community!
<html>
<a href="https://gitter.im/terrabasehq/community"><img src="https://img.shields.io/badge/CHAT%20ON-GITTER-ed1965?logo=gitter&style=for-the-badge"></img>
</a>
<a href="https://join.slack.com/t/terrabasedb/shared_invite/zt-fnkfgzf7-~WO~RzGUUvTiYV4iPAMiiQ"><img src="https://img.shields.io/badge/Discuss%20on-SLACK-4A154B?logo=slack&style=for-the-badge"></img>
</a><a href="https://discord.gg/QptWFdx"><img src="https://img.shields.io/badge/TALK-On%20Discord-7289DA?logo=discord&style=for-the-badge"></img>
</a>
</html>

## License

This project is licensed under the [AGPL-3.0 License](./LICENSE).
