# Skytable <img align="right" src="assets/logo.jpg" height="128" width="128" alt="Skytable Logo"/>
A modern NoSQL database, powered by BlueQL.<br/>
<p>
<a href="https://github.com/skytable/skytable/releases"><img src="https://img.shields.io/github/v/release/skytable/skytable?style=flat" alt="GitHub release (with filter)"></a> <a href="https://github.com/skytable/skytable/actions"><img src="https://img.shields.io/github/actions/workflow/status/skytable/skytable/test-push.yml?style=flat" alt="GitHub Workflow Status (with event)"></a> <a href="https://discord.gg/QptWFdx"><img src="https://img.shields.io/discord/729378001023926282?logo=discord&style=flat" alt="Discord"></a> <a href="https://docs.skytable.io"><img src="https://img.shields.io/badge/read%20the%20docs-here-blue?style=flat" alt="Docs"></a> <a href="https://github.com/skytable/skytable/discussions?style=flat"><img src="https://img.shields.io/badge/discuss-here-8A3324?style=flat&logo=github&labelColor=C34723" alt="Static Badge"></a>
</p>

## What is Skytable?

Skytable is a **modern NoSQL database** that focuses on **performance, flexibility and scalability**. Skytable
is a primarily in-memory, wide-column based database with support for additional data models<sup>*</sup> that 
uses its own storage engine (structured like n-ary records with optimized delayed-durability transactions) and 
enables querying with its own query language *BlueQL* that builds on top of SQL to improve security and flexibility.

Skytable is best-suited for applications that need to store large-scale data, need high-performance and low latencies.

> **You can read more about Skytable's architecture, including information on the clustering and HA implementation that we're currently working on, and limitations [on this page](https://docs.skytable.io/architecture).**

## Features

- **Spaces, models and more**: For flexible data definition
- **Powerful querying with BlueQL**: A modern query language based on SQL
- **Rich data modeling**: Use `model`s to define data with complex types, collections and more
- **Performant**: Heavily multithreaded, optimized write batching and more
- **Secure**: BlueQL is designed to strongly deter query injection pathways
- **Enforces best practices**: If you're building with Skytable today, the practices you'll learn here will let you easily take on the job of building performant systems, even outside Skytable

> Learn more about [Skytable's features here](https://docs.skytable.io).

## Getting started

1. **Set up Skytable on your machine**: You'll need to download a bundled release file [from the releases page](https://github.com/skytable/skytable/releases). Unzip the files and you're ready to go.
2. Start the database server: `./skyd --auth-root-password <password>` with your choice of a password for the `root` account. The `root` account is just like a `root` account on Unix based systems that has control over everything.
3. Start the interactive client REPL: `./skysh` and then enter your password.
4. You're ready to run queries!

> **For a more detailed guide on installation and deployment, [follow the guide here.](https://docs.skytable.io/installation)**


## Using Skytable

Skytable has `SPACE`s instead of `DATABASE`s due to signficant operational differences (and because `SPACE`s store a lot more than tabular data). 

**With the REPL started, follow this guide**:

1. Create a `space` and switch to it:
   ```sql
   CREATE SPACE myspace
   USE myspace
   ```
2. Create a `model`:
   ```sql
   CREATE MODEL myspace.mymodel(username: string, password: string, notes: list { type: string })
   ```
   The rough representation for this in Rust would be:
   ```rust
   pub struct MyModel {
    username: String,
    password: String,
    notes: Vec<String>,
   }
   ```
3. `INSERT` some data:
   ```sql
   INSERT INTO mymodel('sayan', 'pass123', [])
   ```
4. `UPDATE` some data:
   ```sql
   UPDATE mymodel SET notes += "my first note" WHERE username = 'sayan'
   ```
5. `SELECT` some data
   ```sql
   SELECT * FROM mymodel WHERE username = 'sayan'
   ```
6. Poke around! **And then make sure you [read the documentation learn BlueQL](https://docs.skytable.io/blueql/overview).**

> **For a complete guide on Skytable, it's architecture, BlueQL, queries and more we strongly recommend you to [read the documentation here.](https://docs.skytable.io)**
>
> While you're seeing strings and other values being used here, this is so because the REPL client smartly parameterizes queries behind the scenes. **BlueQL has mandatory parameterization**. (See below to see how the Rust client handles this)

## Find a client driver

You need a client driver to use Skytable in your programs. Officially, we maintain a regularly updated [Rust client driver](https://github.com/skytable/client-rust) which is liberally license under the Apache-2.0 license so that you can use it anywhere.

Using the Rust client driver, it's very straightforward to run queries thanks to Rust's powerful type system and macros:

```rust
use skytable::{Config, query};

fn main() {
    let mut db = Config::new_default("username", "password").connect().unwrap();
    let query = query!("select username, password from myspace.mymodel where username = ?", "sayan");
    let (username, password): (String, Vec<u8>) = db.query_parse(&query).unwrap();
    // do something with it!
}
```

> **You can find more information on client drivers on [this page](https://docs.skytable.io/libraries). If you want to help write a client driver for your language of choice, *we're here to support your work*. Please reach out to: hey@skytable.io or leave a message on our Discord server!**

## Getting help

We exclusively use [Discord](https://discord.gg/QptWFdx) for most real-time communications — you can chat with developers, maintainers, and our amazing users! Outside that, we recommend that you use our [GitHub Discussions page](https://github.com/skytable/skytable/discussions) for any questions or open a new issue if you think you've found a bug.

*We're here to help!*

## Contributing

Please read the [contributing guide here](./CONTRIBUTING.md).

## Acknowledgements

Please read the [acknowledgements](./ACKNOWLEDGEMENTS.txt) document.

## License

Skytable is distributed under the [AGPL-3.0 License](./LICENSE). **You may not use Skytable's logo for other projects.**
