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

let avro = require('../../lib');


let fpath = process.argv[2];
if (!fpath) {
  console.error(`usage: ${process.argv[1]} PATH'`);
  process.exit(1);
}

avro.assembleProtocol(fpath, (err, attrs) => {
  if (err) {
    console.trace(err.message);
    return;
  }
  console.log(JSON.stringify(attrs));
});
