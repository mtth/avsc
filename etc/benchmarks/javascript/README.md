# JavaScript benchmarks

These benchmarks compare the relative performance of various JavaScript
serialization libraries.

## Quickstart

First install the required dependencies:

```bash
$ npm install
```

Then run a sample benchmark comparing throughputs of Avro, built-in JSON, and
`msgpack-lite`:

```bash
$ node . ../../schemas/Coupon.avsc --avro --json --msgpack
decode "Coupon" #1
avro x 851,909 ops/sec ±3.34% (85 runs sampled)
json x 303,416 ops/sec ±2.61% (81 runs sampled)
msgpack x 101,048 ops/sec ±2.13% (86 runs sampled)
encode "Coupon" #1
avro x 521,903 ops/sec ±1.69% (87 runs sampled)
json x 92,348 ops/sec ±9.77% (77 runs sampled)
msgpack x 120,446 ops/sec ±3.81% (76 runs sampled)
```

You can run `node . -h` to view the full list of available options.
