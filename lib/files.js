'use strict';

/**
 * Filesystem specifics.
 *
 * This module contains functions only used by node.js. It is shimmed by
 * another module when `avsc` is required from `browserify`.
 */

let fs = require('fs'),
    path = require('path'),
    specs = require('./specs');

/** Default (asynchronous) file loading function for assembling IDLs. */
function createImportHook() {
  let imports = {};
  return function ({path: fpath, importerPath}, cb) {
    fpath = path.resolve(path.dirname(importerPath), fpath);
    if (imports[fpath]) {
      // Already imported, return nothing to avoid duplicating attributes.
      process.nextTick(cb);
      return;
    }
    imports[fpath] = true;
    fs.readFile(fpath, {encoding: 'utf8'}, (err, data) => {
      if (err) return cb(err);
      return cb(null, {contents: data, path: fpath});
    });
  };
}

/**
 * Synchronous file loading function for assembling IDLs.
 *
 * This is only for internal use (inside `specs.parse`). The returned
 * hook should only be called on paths that are guaranteed to exist (where
 * `fs.readFileSync` will not throw, otherwise the calling `assemble` call will
 * throw rather than return the error to the callback).
 */
function createSyncImportHook() {
  let imports = {};
  return function ({path: fpath, importerPath}, cb) {
    fpath = path.resolve(path.dirname(importerPath), fpath);
    if (imports[fpath]) {
      cb();
    } else {
      imports[fpath] = true;
      cb(null, {
        contents: fs.readFileSync(fpath, {encoding: 'utf8'}),
        path: fpath
      });
    }
  };
}

/**
 * Convenience function to parse multiple inputs into protocols and schemas.
 *
 * It should cover most basic use-cases but has a few limitations:
 *
 * + It doesn't allow passing options to the parsing step.
 * + The protocol/type inference logic can be deceived.
 *
 * The parsing logic is as follows:
 *
 * + If `str` contains `path.sep` (on windows `\`, otherwise `/`) and is a path
 *   to an existing file, it will first be read as JSON, then as an IDL
 *   specification if JSON parsing failed. If either succeeds, the result is
 *   returned, otherwise the next steps are run using the file's content
 *   instead of the input path.
 * + If `str` is a valid JSON string, it is parsed then returned.
 * + If `str` is a valid IDL protocol specification, it is parsed and returned
 *   if no imports are present (and an error is thrown if there are any
 *   imports).
 * + If `str` is a valid IDL type specification, it is parsed and returned.
 * + If neither of the above cases apply, `str` is returned.
 */
function readSchemaFromPathOrString(str) {
  let schema;
  if (
    typeof str == 'string' &&
    str.indexOf('/') !== -1 &&
    fs.existsSync(str)
  ) {
    // Try interpreting `str` as path to a file contain a JSON schema or an IDL
    // protocol. Note that we add the second check to skip primitive references
    // (e.g. `"int"`, the most common use-case for `avro.parse`).
    let contents = fs.readFileSync(str, {encoding: 'utf8'});
    try {
      return JSON.parse(contents);
    } catch (err) {
      let opts = {importHook: createSyncImportHook()};
      specs.assembleProtocol(str, opts, (err, protocolSchema) => {
        schema = err ? contents : protocolSchema;
      });
    }
  } else {
    schema = str;
  }
  if (typeof schema != 'string' || schema === 'null') {
    // This last predicate is to allow `read('null')` to work similarly to
    // `read('int')` and other primitives (null needs to be handled separately
    // since it is also a valid JSON identifier).
    return schema;
  }
  try {
    return JSON.parse(schema);
  } catch (err) {
    try {
      return specs.readProtocol(schema);
    } catch (err) {
      try {
        return specs.readSchema(schema);
      } catch (err) {
        return schema;
      }
    }
  }
}

module.exports = {
  createImportHook,
  createSyncImportHook,
  // Proxy a few methods to better shim them for browserify.
  existsSync: fs.existsSync,
  readFileSync: fs.readFileSync,
  readSchemaFromPathOrString
};
