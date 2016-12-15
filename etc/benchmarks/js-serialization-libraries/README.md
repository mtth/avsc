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
  --json \
  --msgpack-lite \
  --protobufjs=schemas/Coupon.proto:Coupon \
  --protocol-buffers=schemas/Coupon.proto:Coupon \
  --schemapack=schemas/Coupon.schemapack.json \
  ../../schemas/Coupon.avsc
decode "Coupon"
avsc x 873,395 ops/sec ±6.06% (82 runs sampled)
json x 304,785 ops/sec ±1.71% (88 runs sampled)
msgpackLite x 59,397 ops/sec ±1.51% (88 runs sampled)
protobufjs x 654,275 ops/sec ±1.99% (85 runs sampled)
protocolBuffers x 466,255 ops/sec ±1.37% (91 runs sampled)
schemapack x 762,265 ops/sec ±2.16% (85 runs sampled)
encode "Coupon"
avsc x 503,947 ops/sec ±2.30% (84 runs sampled)
json x 97,496 ops/sec ±2.98% (81 runs sampled)
msgpackLite x 58,076 ops/sec ±4.41% (66 runs sampled)
protobufjs x 292,524 ops/sec ±6.64% (58 runs sampled)
protocolBuffers x 128,208 ops/sec ±9.22% (59 runs sampled)
schemapack x 235,291 ops/sec ±9.73% (59 runs sampled)
```

You can run `node . -h` to view the full list of available options.
