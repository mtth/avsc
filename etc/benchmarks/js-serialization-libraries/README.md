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
avsc x 1,074,539 ops/sec ±1.22% (87 runs sampled)
json x 304,586 ops/sec ±1.47% (88 runs sampled)
msgpackLite x 62,785 ops/sec ±1.33% (88 runs sampled)
protobufjs x 785,787 ops/sec ±1.19% (89 runs sampled)
protocolBuffers x 639,090 ops/sec ±1.67% (88 runs sampled)
schemapack x 840,587 ops/sec ±1.33% (91 runs sampled)
encode "Coupon"
avsc x 663,740 ops/sec ±1.69% (88 runs sampled)
json x 253,980 ops/sec ±1.41% (91 runs sampled)
msgpackLite x 70,431 ops/sec ±4.59% (77 runs sampled)
protobufjs x 564,237 ops/sec ±1.07% (89 runs sampled)
protocolBuffers x 337,082 ops/sec ±1.16% (88 runs sampled)
schemapack x 600,636 ops/sec ±1.57% (89 runs sampled)
```

You can run `node . -h` to view the full list of available options.
