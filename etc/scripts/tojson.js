#!/usr/bin/env node

/* jshint node: true */

'use strict';

/**
 * Print out JSON-strintified contents of an Avro container file.
 *
 * This is very similar to the `tojson` command of Avro tools.
 *
 */

var avsc = require('../../lib'),
    util = require('util');


var filePath = process.argv[2];
if (!filePath) {
  console.error(util.format('usage: %s PATH', process.argv[1]));
  process.exit(1);
}

var type;
avsc.createFileDecoder(filePath)
  .on('metadata', function (writerType) { type = writerType; })
  .on('data', function (val) { console.log(type.toString(val)); });
