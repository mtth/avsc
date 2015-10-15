/* jshint node: true */

'use strict';


var files = require('./files'),
    types = require('./types'),
    fs = require('fs');


/**
 * Parse a schema and return the corresponding type.
 *
 */
function parse(schema, opts) {
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
  return types.Type.fromSchema(obj, opts);
}


module.exports = {
  parse: parse,
  createFileDecoder: files.createFileDecoder,
  extractFileHeader: files.extractFileHeader,
  streams: files.streams,
  types: types
};
