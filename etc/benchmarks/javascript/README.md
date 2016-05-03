# JavaScript benchmarks

These benchmarks compare the relative performance of various JavaScript
serialization libraries.

## Quickstart

First install the required dependencies:

```bash
$ npm install
```

We can then run (for example) a benchmark comparing throughputs of Avro,
built-in JSON, and MessagePack on a sample schema:

```bash
$ node . --avro --json --msgpack ../../schemas/Coupon.avsc
decode "Coupon"
avro x 946,599 ops/sec ±1.27% (88 runs sampled)
json x 331,370 ops/sec ±1.75% (83 runs sampled)
msgpack x 103,387 ops/sec ±1.67% (85 runs sampled)
encode "Coupon"
avro x 642,643 ops/sec ±1.48% (86 runs sampled)
json x 100,664 ops/sec ±1.31% (90 runs sampled)
msgpack x 126,208 ops/sec ±3.08% (77 runs sampled)
```

You can run `node . -h` to view the full list of available options.
