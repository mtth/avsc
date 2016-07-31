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
avsc x 988,276 ops/sec ±1.26% (86 runs sampled)
json x 273,586 ops/sec ±1.52% (87 runs sampled)
protobufjs x 71,768 ops/sec ±1.71% (86 runs sampled)
protocolBuffers x 443,517 ops/sec ±1.86% (82 runs sampled)
schemapack x 881,333 ops/sec ±1.49% (85 runs sampled)
encode "Coupon"
avsc x 522,615 ops/sec ±1.81% (85 runs sampled)
json x 140,532 ops/sec ±1.54% (87 runs sampled)
protobufjs x 22,312 ops/sec ±1.66% (85 runs sampled)
protocolBuffers x 386,598 ops/sec ±1.58% (88 runs sampled)
schemapack x 514,179 ops/sec ±1.28% (89 runs sampled)
```

You can run `node . -h` to view the full list of available options.
