# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html).


## Features

+ Full Avro schema support, including recursive schemas, [sort
  order][sort-order], and [schema evolution][schema-evolution].
+ [Fast!](#performance) Typically twice as fast as JSON with much smaller
  encodings (varies per schema).
+ No dependencies, `avsc` even runs in the browser!


## Performance

Representative decoding throughput rates (higher is better):

![Throughput rate chart](etc/benchmarks/charts/coupons-decode-throughput-b219b06.png)

Libraries compared:

+ `node-avsc`, this package.
+ `node-json`, built-in JSON serializer.
+ [`node-pson`](https://www.npmjs.com/package/pson), an alternative to JSON.
+ [`node-avro-io`](https://www.npmjs.com/package/node-avro-io), most popular
  previously existing Avro implementation.

These rates are for decoding a [realistic record schema][coupon-schema],
modeled after a popular open-source API. Encoding rates are slightly lower but
ratios across libraries are similar. You can find the raw numbers and more
details on the [benchmarks page][benchmarks].


## Installation

```bash
$ npm install avsc
```

`avsc` is compatible with all versions of [node.js][] since `0.11` and major
browsers via [browserify][].


## Documentation

+ [Overview](https://github.com/mtth/avsc/wiki)
+ [API](https://github.com/mtth/avsc/wiki/API)
+ [Advanced usage](https://github.com/mtth/avsc/wiki/Advanced-usage)


## Examples

Inside a node.js module, or using browserify:

```javascript
var avsc = require('avsc');
```

+ Encode and decode objects:

  ```javascript
  var type = avsc.parse('./Person.avsc'); // Load schema from a file.
  var buf = type.toBuffer({name: 'Ann', age: 25}); // Serialize an object.
  var obj = type.fromBuffer(buf); // {name: 'Ann', age: 25}
  ```

+ Generate random instances from a schema:

  ```javascript
  var type = avsc.parse({ // Declare schema inline.
    name: 'Pet',
    type: 'record',
    fields: [
      {name: 'kind', type: {name: 'Kind', type: 'enum', symbols: ['CAT', 'DOG']}},
      {name: 'name', type: 'string'}
    ]
  });
  var pet = type.random(); // E.g. {kind: 'CAT', name: 'qwXlrew'}
  ```

+ Get a [readable stream][readable-stream] of decoded records from an Avro
  container file (not in the browser):

  ```javascript
  avsc.createFileDecoder('records.avro')
    .on('data', function (record) { /* Do something with the record. */ });
  ```


## Limitations

+ JavaScript doesn't natively support `long`s, so numbers larger than
  `Number.MAX_SAFE_INTEGER` (or smaller than the corresponding lower bound)
  might suffer a loss of precision.
+ Protocols aren't yet implemented.


[node.js]: https://nodejs.org/en/
[benchmarks]: https://github.com/mtth/avsc/wiki/Benchmarks
[schema-evolution]: https://github.com/mtth/avsc/wiki/Advanced-usage#schema-evolution
[sort-order]: https://avro.apache.org/docs/current/spec.html#order
[readable-stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
[browserify]: http://browserify.org/
[coupon-schema]: etc/benchmarks/schemas/Coupon.avsc
