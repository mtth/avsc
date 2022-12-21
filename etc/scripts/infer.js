#!/usr/bin/env node

'use strict';

/**
 * Script to output a schema from one or more values.
 *
 * Usage:
 *
 *  ./infer [JSON]
 *
 * Arguments:
 *
 *  JSON          JSON-encoded value. If not specified, the script will read
 *                JSON values from stdin.
 *
 */

var avro = require('../../lib'),
    utils = require('../../lib/utils'),
    util = require('util');


var argv = process.argv;
switch (argv.length) {
  case 2:
    fromStdin();
    return;
  case 3:
    show(avro.Type.forValue(JSON.parse(argv[2])));
    return;
  default:
    console.error(util.format('usage: %s [JSON]', argv[1]));
    process.exit(1);
}

/**
 * Infer a type from a stream of serialized JSON values.
 *
 */
function fromStdin() {
  var type = null;
  var str = '';
  process.stdin
    .on('data', function (buf) {
      str += buf.toString();
      var pos;
      while ((pos = utils.jsonEnd(str)) >= 0) {
        var val = JSON.parse(str.slice(0, pos));
        if (type === null) {
          type = avro.Type.forValue(val);
        } else if (!type.isValid(val, {noUndeclaredFields: true})) {
          type = avro.Type.forTypes([type, avro.Type.forValue(val)]);
        }
        str = str.slice(pos);
      }
    })
    .on('end', function () {
      if (/[^\s]/.test(str)) {
        throw new Error('trailing data');
      }
      if (!type) {
        throw new Error('no value');
      }
      show(type);
    });
}

/**
 * Output a type's schema, including defaults.
 *
 */
function show(type) { console.log(type.schema({exportAttrs: true})); }
