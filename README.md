# Terrabase**DB** - The next-generation database

![Status: Pre-Alpha](https://img.shields.io/badge/Status-Pre--alpha-critical?style=flat-square)
![Version: 0.1.0](https://img.shields.io/badge/Development-Actively%20Developed-success?style=flat-square) ![GitHub release (latest by date)](https://img.shields.io/github/v/release/terrabasedb/terrabase?style=flat-square)

## What is TerrabaseDB?

TerrabaseDB (or TDB for short) is an effort to provide the best of key/value stores, document stores and columnar databases - **simplicity, flexibility and queryability at scale**. This project is currently in a <b>pre-alpha</b> stage and is undergoing rapid development.

## Status

As noted earlier, TerrabaseDB is pre-alpha software and the entire API is subject to major breaking changes, at the moment.

## Getting started

We have an experimental client and server implementation for the database already. You can download a pre-built binary for `x86_64-linux` in the releases section and try it out!

* First unzip the file
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

## Community

A project which is powered by the community believes in the power of community!
<html>
<a href="https://gitter.im/terrabasehq/community"><img src="https://img.shields.io/badge/CHAT%20ON-GITTER-ed1965?logo=gitter&style=for-the-badge"></img>
</a>
<a href="https://join.slack.com/t/terrabasedb/shared_invite/zt-fnkfgzf7-~WO~RzGUUvTiYV4iPAMiiQ"><img src="https://img.shields.io/badge/Discuss%20on-SLACK-4A154B?logo=slack&style=for-the-badge"></img>
</a><a href="https://discord.gg/QptWFdx"><img src="https://img.shields.io/badge/TALK-On%20Discord-7289DA?logo=discord&style=for-the-badge"></img>
</a>
</html>

## Contributing

Yes - this project needs you! We want hackers from all across the globe to help us create the next-generation database. Read the guide [here](./CONTRIBUTING.md).

## License

This project is licensed under the [AGPL-3.0 License](./LICENSE).
