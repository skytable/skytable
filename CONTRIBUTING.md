# Contribution guidelines

Firstly, thank you ❤️ for your interest in contributing to this project! This project is powered by the community
and relies on hackers across the globe 🌐 to report bugs, send suggestions 💡 and contribute code to move this project forward ⏩.

You can see a list of contributors **[here](./CONTRIBUTORS.md)**

## Ways to contribute

* **#1**: Report bugs 🐞
* **#2**: Give us ideas 💡
* **#3**: Send patches

## Coding guidelines 👩‍💻 👨‍💻 

### Conventions

* `FIXME(@<username>)` : Use this when you have made an implementation that can be improved in the future, such as improved efficiency
* `HACK(@<username>)` : Use this when the code you are using a temporary workaround
* `TODO(@<username>)` : Use this when you have kept something incomplete
* `UNSAFE(@<username>)` : Use this to explain why the `unsafe` block used is safe

### Formatting

* All Rust code should be formatted using `rustfmt`

### Parts of the project

The project root has three main directories:

* `cli` : This contains code for `tsh` which is the command-line client for TDB
* `libcore` : This contains function, structs, ... used by both the `cli` and the `server`
* `server` : This contains code for the main database server

### Branches

The `next` branch is the _kind of_ stable branch which contains the latest changes. However, for most purposes, you should always download the sources from the tags. Usually, when a feature is worked on, the work will be done on a separate branch, and then it will be merged into next.

## Steps

1. Fork the repository
2. Make your changes and start a pull request
3. Sign the CLA (if you haven't signed it already)
4. One of the maintainers will review your patch and suggest changes if required
5. Once your patch is approved, it will be merged into the respective branch
6. Done, you're now one of the [contributors](./CONTRIBUTORS.md) 🎉

## Testing locally

1. Install rust (stable)
2. Run `cargo build --verbose && cargo test --verbose`
3. That's it!
