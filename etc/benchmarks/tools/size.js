/* jshint node: true */

'use strict';

/**
 * Compute average encoding size for a given schema.
 *
 */

var avsc = require('../../../lib'),
    ProtoBuf = require('protobufjs'),
    pson = require('pson'),
    msgpack = require('msgpack-lite'),
    util = require('util');

var COUNT = 10000;

var pPair = new pson.ProgressivePair([]);
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

var schemaPath = process.argv[2];
if (!schemaPath) {
  console.error('usage: node random.js SCHEMA');
  process.exit(1);
}

var type = avsc.parse(schemaPath);

var libs = ['avsc', 'json', 'pson', 'pbuf', 'msgpack'];
var sizes = {};
libs.forEach(function (lib) { sizes[lib] = 0; });

var count = COUNT;
var record;
while (count--) {
  pPair.include(record);
  record = type.random();
  sizes.avsc += type.toBuffer(record).length;
  sizes.json += JSON.stringify(record).length;
  sizes.pson += pPair.encode(record).toBuffer().length;
  sizes.msgpack += msgpack.encode(record).length;
  sizes.pbuf += (new Coupon(record)).encode().toBuffer().length;
}

var minSize = Infinity;
libs.forEach(function (lib) { minSize = Math.min(sizes[lib], minSize); });

libs.forEach(function (lib) {
  var size = sizes[lib];
  var ratio = size / minSize;
  console.log(util.format('%s\t%s\t%s'), lib, size / COUNT | 0, ratio);
});
