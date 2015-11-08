#!/usr/bin/env node

'use strict';

/**
 * This benchmark only works with the `Coupon` schema (the equivalent protobuf
 * message is below).
 *
 */

var avsc = require('../../../../lib'),
    ProtoBuf = require('protobufjs');


var dataPath = process.argv[2];
if (!dataPath) {
  process.exit(1);
}

var builder = ProtoBuf.newBuilder({})['import']({
    "package": null,
    "messages": [
        {
            "name": "Coupon",
            "fields": [
                {
                    "rule": "required",
                    "type": "string",
                    "name": "id",
                    "id": 1
                },
                {
                    "rule": "required",
                    "type": "string",
                    "name": "object",
                    "id": 2
                },
                {
                    "rule": "required",
                    "type": "bool",
                    "name": "livemode",
                    "id": 3
                },
                {
                    "rule": "required",
                    "type": "sint32",
                    "name": "created",
                    "id": 4
                },
                {
                    "rule": "required",
                    "type": "Duration",
                    "name": "duration",
                    "id": 5
                },
                {
                    "rule": "required",
                    "type": "bytes",
                    "name": "metadata",
                    "id": 6
                },
                {
                    "rule": "required",
                    "type": "sint32",
                    "name": "times_redeemed",
                    "id": 7
                },
                {
                    "rule": "required",
                    "type": "bool",
                    "name": "valid",
                    "id": 8
                },
                {
                    "rule": "required",
                    "type": "sint32",
                    "name": "amount_off",
                    "id": 9
                },
                {
                    "rule": "required",
                    "type": "Currency",
                    "name": "currency",
                    "id": 10
                },
                {
                    "rule": "required",
                    "type": "sint32",
                    "name": "duration_in_months",
                    "id": 11
                },
                {
                    "rule": "required",
                    "type": "sint32",
                    "name": "max_redemptions",
                    "id": 12
                },
                {
                    "rule": "required",
                    "type": "sint32",
                    "name": "percent_off",
                    "id": 13
                },
                {
                    "rule": "required",
                    "type": "sint32",
                    "name": "redeem_by",
                    "id": 14
                }
            ],
            "enums": [
                {
                    "name": "Duration",
                    "values": [
                        {
                            "name": "FOREVER",
                            "id": 0
                        },
                        {
                            "name": "ONCE",
                            "id": 1
                        },
                        {
                            "name": "REPEATING",
                            "id": 2
                        }
                    ]
                },
                {
                    "name": "Currency",
                    "values": [
                        {
                            "name": "DOLLAR",
                            "id": 0
                        },
                        {
                            "name": "EURO",
                            "id": 1
                        }
                    ]
                }
            ]
        }
    ]
}).build();

var Coupon = builder.Coupon;
var loops = 1;
var objs = [];

avsc.createFileDecoder(dataPath)
  .on('metadata', function (type) {
    var name = type.getName();
    if (name !== 'Coupon') {
      console.error('no equivalent protobuf message for: ' + name);
      process.exit(1);
    }
  })
  .on('data', function (record) {
    objs.push(new Coupon(record));
  })
  .on('end', function () {
    var i = 0;
    var n = 0;
    var time = process.hrtime();
    for (i = 0; i < loops; i++) {
      n += loop();
    }
    time = process.hrtime(time);
    if (n < 0) {
      console.error('no');
    }
    console.log(1000 * (time[0] + time[1] * 1e-9) / (objs.length * loops));
  });


function loop() {
  var n = 0;
  var i, l, buf;
  for (i = 0, l = objs.length; i < l; i++) {
    buf = objs[i].encode().toBuffer();
    n += buf[0] + buf.length;
  }
  return n;
}
