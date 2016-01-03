#!/usr/bin/env node

/* jshint node: true */

'use strict';

/**
 * Obfuscate a schema.
 *
 * Returns a new (canonical) schema with the names of records, fixed, enums,
 * fields, and symbols mangled.
 *
 *
 */

var avsc = require('../../lib'),
    utils = require('../../lib/utils'),
    util = require('util');


var RANDOM = new utils.Lcg();

var fpath = process.argv[2];
if (!fpath) {
  console.error(util.format('usage: %s PATH', process.argv[1]));
  process.exit(1);
}

var type = avsc.parse(fpath, {typeHook: function (attrs) {
  if (!attrs.name) {
    return; // Nothing to mangle.
  }
  attrs.name = RANDOM.nextString(8);
  var fields, symbols;
  if ((fields = attrs.fields)) {
    fields.forEach(function (o) { o.name = RANDOM.nextString(8); });
  } else if ((symbols = attrs.symbols)) {
    symbols.forEach(function (s, i) { symbols[i] = RANDOM.nextString(8); });
  }
}});

console.log(type.getSchema());
