<html>
<div align="center">
<img src="https://raw.githubusercontent.com/terrabasedb/docs/master/docs/assets/img/favicon.ico" height=64 width=64 style="float:left">
<h1>Terrabase<b>DB</b></h1><h3>The next-generation NoSQL database</h3>

![GitHub Workflow Status](https://img.shields.io/github/workflow/status/terrabasedb/terrabase/Tests?style=flat-square) ![Status: Pre-Alpha](https://img.shields.io/badge/status-pre--alpha-critical?style=flat-square) ![Version: 0.1.0](https://img.shields.io/badge/development-actively%20developed-32CD32?style=flat-square) ![GitHub release (latest by date)](https://img.shields.io/github/v/tag/terrabasedb/terrabase.svg?style=flat-square)

</div>
</html>

## What is TerrabaseDB?

TerrabaseDB (or TDB for short) is an effort to provide the best of key/value stores, document stores and columnar databases, that is, **simplicity, flexibility and queryability at scale**. TDB is curently in a pre-alpha stage, but can be used as a **performant** and **persistent key-value store**.

## Getting started

1. Download a bundle for your platform from [here ⬇️ ](https://github.com/terrabasedb/terrabase/releases)
2. Unzip the bundle
3. Make the files executable (run `chmod +x tdb tsh` on *nix systems)
4. First run `tdb` to start the database server and then run `tsh` to start the interactive shell
5. Run commands like: `SET foo bar` , `GET bar` , `UPDATE cat mitten` or `DEL proprietary` 🤪 on `tsh` !

## Supported actions

* `HEYA` - A ping message to check if everything's fine
* `GET` - Get a key
* `SET` - Set a key
* `UPDATE` - Update the value of a key which has already been created with `SET`
* `EXISTS` - Check if a key exists
* `DEL` - Delete a key

And, a lot more actions are coming soon!

## Clients 🔌

We're officially working on a [Python Driver](https://github.com/terrabasedb/python-driver) and we plan to support more languages along the way 🎉! You're free to write your own clients - all you need to do is implement the simple and performant [Terrapipe protocol spec](https://git.io/JJZ4Z).

## Community 👐

A project which is powered by the community believes in the power of community! If you get stuck anywhere - here are your options!
<html>
<a href="https://gitter.im/terrabasehq/community"><img src="https://img.shields.io/badge/chat%20on-gitter-ed1965?logo=gitter&style=flat-square"></img>
</a>
<a href="https://join.slack.com/t/terrabasedb/shared_invite/zt-fnkfgzf7-~WO~RzGUUvTiYV4iPAMiiQ"><img src="https://img.shields.io/badge/discuss%20on-slack-4A154B?logo=slack&style=flat-square"></img>
</a><a href="https://discord.gg/QptWFdx"><img src="https://img.shields.io/badge/talk-on%20discord-7289DA?logo=discord&style=flat-square"></img></a>
</html>

## Platforms 💻

![Linux supported](https://img.shields.io/badge/Linux%20x86__64-supported%20✓-228B22?style=flat-square&logo=linux) ![macOS supported](https://img.shields.io/badge/macOS%20x86__64-supported%20✓-228B22?style=flat-square&logo=apple) ![Windows supported](https://img.shields.io/badge/Windows%20x86__64-supported%20✓-228B22?style=flat-square&logo=windows)

## Versioning 

This project strictly follows semver, however, since this project is currently in the development phase (0.x.y), the API may change unpredictably

## Contributing

**Yes - we need you!** Be it a typo, a bizarre idea, a dirty bug🐞 or an amazing patch - you're welcome to contribute to TDB! Beginner friendly issues are marked with the ![easyfix](https://img.shields.io/badge/easyfix-C71585) label. Read the guide [here](./CONTRIBUTING.md).

.

## License

This project is licensed under the [AGPL-3.0 License](./LICENSE).
