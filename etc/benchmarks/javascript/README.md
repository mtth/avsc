# JavaScript benchmarks

These benchmarks compare the relative performance of various JavaScript
serialization libraries.

## Quickstart

First install the required dependencies:

```bash
$ npm install
```

We can then run (for example) a benchmark comparing throughputs of Avro,
built-in JSON, MessagePack, and Protocol Buffers on a sample schema:

```bash
$ node . --avsc --json --msgpack-lite --protocol-buffers schemas/Person.proto:Person ../../schemas/Person.avsc
decode "Person"
avsc x 10,744,017 ops/sec ±4.00% (81 runs sampled)
json x 969,565 ops/sec ±2.74% (85 runs sampled)
msgpackLite x 367,800 ops/sec ±3.18% (82 runs sampled)
protocolBuffers x 4,633,326 ops/sec ±2.45% (84 runs sampled)
encode "Person"
avsc x 1,573,218 ops/sec ±2.75% (83 runs sampled)
json x 1,164,215 ops/sec ±0.99% (91 runs sampled)
msgpackLite x 205,621 ops/sec ±1.42% (85 runs sampled)
protocolBuffers x 1,489,432 ops/sec ±1.53% (91 runs sampled)
```

You can run `node . -h` to view the full list of available options.
