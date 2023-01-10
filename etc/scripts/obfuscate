#!/usr/bin/env node

'use strict';

/**
 * Obfuscate a schema.
 *
 * Returns a new (canonical) schema with the names of records, fixed, enums,
 * fields, and symbols mangled.
 *
 * Usage:
 *
 *  ./obfuscate SCHEMA
 *
 * Arguments:
 *
 *  SCHEMA          Schema or a path to a file containing a schema.
 *
 */

let avro = require('../../lib'),
    utils = require('../../lib/utils');


let RANDOM = new utils.Lcg();

let fpath = process.argv[2];
if (!fpath) {
  console.error(`usage: ${process.argv[1]} SCHEMA`);
  process.exit(1);
}

let type = avro.Type.forSchema(fpath, {typeHook: function (attrs) {
  if (attrs.name) {
    attrs.name = RANDOM.nextString(8);
  }
  let fields, symbols;
  if ((fields = attrs.fields)) {
    fields.forEach((o) => { o.name = RANDOM.nextString(8); });
  } else if ((symbols = attrs.symbols)) {
    symbols.forEach((s, i) => { symbols[i] = RANDOM.nextString(8); });
  }
}});

console.log(type.schema());
