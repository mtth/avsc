# Benchmarks

## Quickstart

To run all available benchmarks:

```bash
$ python __main__.py >timings.json
```

Logging messages will be printed to standard error. Any benchmarks with missing
requirements will be skipped. The data is returned in a format suitable for
analysis (e.g. using `pandas`).


## Requirements

+ Java, Maven

  To build and run the Java benchmarks.

+ `node-avro-io`

  Somewhat tricky to install on newer versions of Node. This branch seems to do
  the job:

  ```bash
  $ npm install git://github.com/mdlavin/node-avro-io.git#node-4.1-adoption
  ```
