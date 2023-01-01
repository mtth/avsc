#!/usr/bin/env node

'use strict';

/**
 * Assemble an IDL and print out the corresponding schema.
 *
 * Usage:
 *
 *  ./assemble PATH
 *
 * Arguments:
 *
 *  PATH          Path to IDL file.
 *
 */

var avro = require('../../lib'),
    util = require('util');


var fpath = process.argv[2];
if (!fpath) {
  console.error(util.format('usage: %s PATH', process.argv[1]));
  process.exit(1);
}

avro.assembleProtocol(fpath, function (err, attrs) {
  if (err) {
    console.trace(err.message);
    return;
  }
  console.log(JSON.stringify(attrs));
});
