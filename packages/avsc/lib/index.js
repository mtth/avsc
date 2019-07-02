/* jshint node: true */

'use strict';

var types = require('./types');
var streams = require('@avro/streams');
var path = require('path');
var fs = require('fs');

function parse() {
  var schema;
  if (typeof s == 'string' && ~s.indexOf(path.sep) && fs.existsSync(s)) {
    // Try interpreting `s` as path to a file contain a JSON schema or an IDL
    // protocol. Note that we add the second check to skip primitive references
    // (e.g. `"int"`, the most common use-case for `avro.parse`).
    var contents = fs.readFileSync(s, {encoding: 'utf8'});
    try {
      schema = JSON.parse(contents);
    } catch (err) {
      schema = idl.assembleProtocolSync(s, opts);
    }
  } else {
    schema = s;
  }
  if (typeof schema == 'string' && schema !== 'null') {
    // This last predicate is to allow `read('null')` to work similarly to
    // `read('int')` and other primitives (null needs to be handled separately
    // since it is also a valid JSON identifier).
    schema = JSON.parse(schema);
  }
  return types.Type.forSchema(schema);
}

module.exports = {
  Type: types.Type,
  assembleProtocol: idl.assembleProtocol,
  createFileDecoder: streams.createFileDecoder,
  createFileEncoder: streams.createFileEncoder,
  extractFileHeader: streams.extractFileHeader,
  parse,
  streams,
  types: types.builtins
};
