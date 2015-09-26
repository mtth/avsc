# Avsc [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc)

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html).


## Example

```javascript
var avsc = require('avsc');

var type = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
  ]
});

var buf = type.encode({name: 'Ann', age: 25});
```


## Installation

```bash
$ npm install avsc
```


## Documentation

+ [API](https://github.com/mtth/avsc/wiki/API)


## Status

What's already there:

+ Parsing schemas.
+ Encoding, decoding, validating, and generating data.
+ Resolving schemas (a.k.a. "reader's schemas").
+ Reading container files.

Coming up:

+ Writing container files.
+ Sort order.
+ Canonical schemas and fingerprints.
+ Protocols.
