# JavaScript benchmarks

These benchmarks compare the relative performance of various JavaScript
serialization libraries.

## Quickstart

First install the required dependencies:

```bash
$ npm install
```

We can then run (for example) a benchmark comparing throughputs of Avro,
built-in JSON, MessagePack, SchemaPack, and Protocol Buffers on a sample
schema:

```bash
$ node . \
  --avsc \
  --flatbuffers=schemas/Coupon.bfbs \
  --json \
  --msgpack-lite \
  --protobufjs=schemas/Coupon.proto:Coupon \
  --protocol-buffers=schemas/Coupon.proto:Coupon \
  --schemapack=schemas/Coupon.schemapack.json \
  ../../schemas/Coupon.avsc
decode "Coupon"
avsc x 1,112,347 ops/sec ±0.67% (90 runs sampled)
flatbuffers x 726,951 ops/sec ±1.21% (89 runs sampled)
json x 245,524 ops/sec ±1.59% (87 runs sampled)
msgpackLite x 63,882 ops/sec ±1.36% (92 runs sampled)
protobufjs x 770,191 ops/sec ±1.10% (87 runs sampled)
protocolBuffers x 660,440 ops/sec ±1.69% (91 runs sampled)
schemapack x 820,796 ops/sec ±2.06% (91 runs sampled)
encode "Coupon"
avsc x 690,559 ops/sec ±1.82% (90 runs sampled)
flatbuffers x 213,089 ops/sec ±1.43% (61 runs sampled)
json x 324,065 ops/sec ±1.45% (91 runs sampled)
msgpackLite x 77,005 ops/sec ±4.45% (76 runs sampled)
protobufjs x 621,946 ops/sec ±1.39% (92 runs sampled)
protocolBuffers x 337,732 ops/sec ±1.07% (91 runs sampled)
schemapack x 596,313 ops/sec ±1.72% (87 runs sampled)
```

You can run `node . -h` to view the full list of available options.
