# Terrapipe

> Date: 2<sup>nd</sup> July, 2020<br>Copyright &copy; 2020 Sayan Nandan

Terrapipe is a protocol that is used by Terrabase for data transfer. It is an application layer
protocol that builds on top of TCP. Just like HTTP's request/response action, Terrapipe (i.e tp://)
also makes use of  a query/result action.
From now on, I will refer to Terrapipe as _TP_ or _tp_.

TP makes use of two packets:

1. **The `Q` uery packet**: This is sent by the client
2. **The `R` esult packet**: This is sent by the server

## The `Q` uery packet

The `Q` uery packet has the following structure:

``` 
TP <VERSION>/Q <QTYPE>/<LENGTH>
\n
--------------- DATA ----------
```

**Note:** The first line, is followed by a line break, and then the subsequent lines.

### Line 1: Meta frame

The first line is called the meta frame. The `<VALUES>` and their corresponding meanings are as follows:
- **`VERSION`**: The version of the protocol, in semver form, i.e `major.minor.patch`.
An example can be: `0.1.0`
- **`QTYPE`**: This is the type of query. It can have the following values:
    - `GET`: For `GET` operations
    - `SET`: For `SET` operations
    - `UPDATE`: For `UDPATE` operations
    - `DEL`: For `DEL` operations
- **`LENGTH`**: The number of bytes that are being transmitted. This is useful for preallocating buffers for copying the data.

#### Example meta frame
```
TP 0.1.0/Q GET/15
```

### Line 2: Line break
This is a line break that separates the meta frame from the data frame.

### Line 3: Data frame
The data frame doesn't have any defined format. It can be anything that can be transferred over TCP - that is, well, anything: letters, numbers or vaguely bytes.

## The `R`esult packet
The `R`esult packet has the following structure:
```
TP <VERSION>/R <QTYPE>/<RESPONSECODE>/<LENGTH>
\n
--------------------- DATA -------------------
```
**Note:** The first line is followed by a line break, and then the subsequent lines.

### Line 1: Meta frame
Just like the `Q`uery packet, the first line is called the meta frame.
The `<VALUES>` and their corresponding meanings are as follows:
- **`VERSION`**: The version of the protocol, in semver form, i.e `major.minor.patch`.
An example can be: `0.1.0`
- **`QTYPE`**: This is the type of query. It can have the following values:
    - `GET`: For responses to `GET` operations
    - `SET`: For responses to `SET` operations
    - `UPDATE`: For responses to `UPDATE` operations
    - `DEL`: For response to `DEL` operations
    
    This must match with the initial query packet.
- **`RESPONSECODE`**: This is the outcome of the query. It can have the following values:
    - 0: Okay
    - 1: Not found
    - 2: Method not allowed
    - 3: Server error
    - 4: Corrupt byte
    - 5: Protocol version mismatch
- **`LENGTH`**: The number of bytes that are being transmitted. This is useful for preallocating buffers for copying the data.

#### Example data frame
```
sayan is writing a protocol
```

## An example of a query/result
Let us assume a key called `sayan` with a value of '17' exists on the database.
Our client, uses `0.1.0` version of tp and sends a `GET` request for the key to our server which also uses version `0.1.0` of tp.

### The `Q`uery packet
```
TP 0.1.0/Q GET/5
\n
sayan
```

### The `R`esult packet
```
TP 0.1.0/R GET/0/2
\n
17
```
