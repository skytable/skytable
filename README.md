# Terrabase - The new in-memory database
Terrabase is a **new in-memory database** that aims to **fill the gap** between **key/value stores**, 
**document stores** and **columnar stores**, by providing the *best of three worlds* - **simplicity**, 
**flexibility** and **querying**. 

Right now, Terrabase is a complete work in progress and is under
rapid development, but, you can still have fun with it!

## Getting started

For now the only way to use terrabase is by building it from source. I will be adding instructions on
the same soon.

## Status

Terrabase, at this moment, can only parse arguments in REPL mode and report errors.
This is what you can do:
```shell
$./terrabase
Terrabase | Version 0.1.0
Copyright (c) 2020 Sayan Nandan
terrabase> set sayan 17
SET(
    KeyValue(
        "sayan",
        "17",
    ),
)
terrabase> get sayan
GET(
    "sayan",
)
terrabase> exit
Goodbye!
$
```

More functionality to follow soon!

## Contributing

YES - I need you! This is an open-source project and it relies on contributions from hackers 
across the globe. Again, I will be adding contributing instructions in a while.

## License

This project is licensed under the AGPL-3.0 license. This means, if you end up using this database 
with your app, without any modifications, then you don't have to do anything. However, if you plan
to provide access to this database over a network or you want to distribute a modified version, then
you'll have to open-source your code. This is not legal advice, but it's an easy-to-understand human
version of the AGPL license. You can read the long-form [here](https://opensource.org/licenses/AGPL-3.0).
