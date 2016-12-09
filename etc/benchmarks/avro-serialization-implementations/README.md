# Avro benchmarks

These benchmarks compare the performance of various Avro implementations
(JavaScript, Java, Python, and Ruby). For historical reasons a few other
JavaScript libraries are also included (JSON, PSON), but refer to the
`etc/benchmarks/javascript` folder for such comparisons.

## Quickstart

To run all available benchmarks:

```bash
$ python tools/run.py >timings.json
```

Logging messages will be printed to standard error. Any benchmarks with missing
requirements will be skipped. The data is returned in a format suitable for
analysis (e.g. using `pandas`, see below).

For more options (e.g. choosing which schemas or libraries to benchmark):

```bash
$ python tools/run.py -h
```


## Requirements

+ Python packages to run the benchmarks driver:
  + `docopt`

+ NPM packages (installed via `npm install .`):
  + `pson`
  + `node-avro-io`

+ Java, Maven to build and run the Java benchmarks. Then run `npm run
  compile-java` to build the executable jar.


## Analysis

Assuming you have `pandas` installed:

```bash
$ python tools/analyze.py timings.json
```

The above command will print out the throughput rates (operations per second)
as well as the percentage compared to the max rate for each schema and library.
