/* jshint node: true */

'use strict';

/**
 * Schema loading utilities.
 *
 * In the future, this will include IDL parsing.
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
  if (typeof schema == 'string') {
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
 * Parse an IDL file and return the decoded JSON.
 *
 */
function translate(fpath, opts, cb) {
  opts = opts || {};

  fs.readFile(fpath, function (err, data) {
    if (err) {
      cb(err);
      return;
    }
    // TODO: actually decode IDL.
    cb(null, data);
  });
}


module.exports = {
  load: load,
  translate: translate
};
