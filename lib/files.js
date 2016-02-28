/* jshint node: true */

'use strict';

/**
 * Filesystem specifics.
 *
 * This module contains functions only used by node.js. It is shimmed by
 * another module when `avsc` is required from `browserify`.
 *
 */

var fs = require('fs');


/**
 * Try to load a schema.
 *
 * This method will attempt to load schemas from a file if the schema passed is
 * a string which isn't valid JSON and contains at least one slash.
 *
 */
function load(schema) {
  var obj;
  if (typeof schema == 'string' && schema !== 'null') {
    // This last predicate is to allow `avro.parse('null')` to work similarly
    // to `avro.parse('int')` and other primitives (null needs to be handled
    // separately since it is also a valid JSON identifier).
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      if (~schema.indexOf('/')) {
        // This can't be a valid name, so we interpret is as a filepath. This
        // makes is always feasible to read a file, independent of its name
        // (i.e. even if its name is valid JSON), by prefixing it with `./`.
        obj = JSON.parse(fs.readFileSync(schema));
      }
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return obj;
}

/**
 * Default file loading function for assembling IDLs.
 *
 */
function createImportHook() {
  var imports = {};
  return function (fpath, kind, cb) {
    if (imports[fpath]) {
      // Already imported, return nothing to avoid duplicating attributes.
      process.nextTick(cb);
      return;
    }
    imports[fpath] = true;
    fs.readFile(fpath, {encoding: 'utf8'}, cb);
  };
}


module.exports = {
  createImportHook: createImportHook,
  load: load
};
