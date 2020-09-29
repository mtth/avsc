# Avro types [![NPM version](https://img.shields.io/npm/v/@avro/types.svg)](https://www.npmjs.com/package/@avro/types)

Pure JavaScript implementation of the [Avro
specification](https://avro.apache.org/docs/current/spec.html).


## Features

+ Blazingly fast and compact serialization! Typically faster than JSON with
  much smaller encodings.
+ All the Avro goodness and more: [type inference][type-inference], [schema
  evolution][schema-evolution], ...
+ Support for [serializing arbitrary JavaScript objects][logical-types].
+ Unopinionated [64-bit integer compatibility][custom-long].


## Installation

```bash
$ npm install @avro/types
```

`@avro/types` is compatible with all versions of [node.js][] since `0.11`.


## Examples

Inside a node.js module, or using browserify:

```javascript
const {Type} = require('@avro/types');
```

+ Encode and decode values from a known schema:

  ```javascript
  const type = Type.forSchema({
    type: 'record',
    fields: [
      {name: 'kind', type: {type: 'enum', symbols: ['CAT', 'DOG']}},
      {name: 'name', type: 'string'}
    ]
  });

  const buf = type.toBuffer({kind: 'CAT', name: 'Albert'}); // Encoded buffer.
  const val = type.fromBuffer(buf); // = {kind: 'CAT', name: 'Albert'}
  ```

+ Infer a value's schema and encode similar values:

  ```javascript
  const type = Type.forValue({
    city: 'Cambridge',
    zipCodes: ['02138', '02139'],
    visits: 2
  });

  // We can use `type` to encode any values with the same structure:
  const bufs = [
    type.toBuffer({city: 'Seattle', zipCodes: ['98101'], visits: 3}),
    type.toBuffer({city: 'NYC', zipCodes: [], visits: 0})
  ];
  ```

[custom-long]: https://github.com/mtth/avsc/wiki/Advanced-usage#custom-long-types
[home]: https://github.com/mtth/avsc/wiki
[logical-types]: https://github.com/mtth/avsc/wiki/Advanced-usage#logical-types
[node.js]: https://nodejs.org/en/
[schema-evolution]: https://github.com/mtth/avsc/wiki/Advanced-usage#schema-evolution
[type-inference]: https://github.com/mtth/avsc/wiki/Advanced-usage#type-inference
