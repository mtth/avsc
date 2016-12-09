# Server throughput benchmark

Comparing Avro v.s. JSON, running using Express.

## Load testing

```bash
$ ab -c 10 -n 10000 -p POST.avro -T 'avro/binary' localhost:8000/avro
```

```bash
$ ab -c 10 -n 10000 -p POST.json -T 'application/json' localhost:8000/json
```

## Single request

```bash
$ curl -i -X POST localhost:8000/avro -H 'content-type: avro/binary' --data-binary @POST.avro
```
