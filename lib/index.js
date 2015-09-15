/* jshint node: true */

'use strict';

var parse = require('./parse'),
    fs = require('fs');

/**
 * Convenience function to parse an Avro schema file (`.avsc` typically).
 *
 * @param path {String} Path to file.
 * @param registry Optional registry of type names.
 *
 */
function parseFile(path, registry) {

  return parse.parse(JSON.parse(fs.readFileSync(path)), undefined, registry);

}

module.exports = {
  parse: parse.parse,
  parseFile: parseFile,
  types: parse.types
};
