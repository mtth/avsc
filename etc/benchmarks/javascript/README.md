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
avsc x 933,993 ops/sec ±1.62% (83 runs sampled)
json x 262,768 ops/sec ±3.10% (81 runs sampled)
msgpackLite x 74,712 ops/sec ±3.84% (85 runs sampled)
protobufjs x 76,688 ops/sec ±1.50% (86 runs sampled)
protocolBuffers x 445,859 ops/sec ±3.40% (85 runs sampled)
schemapack x 865,788 ops/sec ±1.46% (86 runs sampled)
encode "Coupon"
avsc x 523,396 ops/sec ±3.74% (78 runs sampled)
json x 51,622 ops/sec ±1.17% (89 runs sampled)
msgpackLite x 74,697 ops/sec ±5.94% (67 runs sampled)
protobufjs x 20,731 ops/sec ±2.56% (79 runs sampled)
protocolBuffers x 354,428 ops/sec ±3.27% (78 runs sampled)
schemapack x 497,618 ops/sec ±1.27% (85 runs sampled)
```

You can run `node . -h` to view the full list of available options.
