# Benchmarks

## Quickstart

To run all available benchmarks:

```bash
$ python run.py >timings.json
```

Logging messages will be printed to standard error. Any benchmarks with missing
requirements will be skipped. The data is returned in a format suitable for
analysis (e.g. using `pandas`, see below).

For more options (e.g. choosing which schemas or libraries to benchmark):

```
$ python run.py -h
```


## Requirements

+ `docopt`; to run the benchmarks.

+ Java, Maven; to build and run the Java benchmarks.

+ `node-avro-io`; somewhat tricky to install on newer versions of Node. This
  branch seems to do the job:

  ```bash
  $ npm install git://github.com/mdlavin/node-avro-io.git#node-4.1-adoption
  ```


## Analysis

Assuming you have `pandas` installed:

```bash
$ python tools/analyze.py timings.json
```

The above command will print out the throughput rates (operations per second)
as well as the percentage compared to the max rate for each schema and library.
