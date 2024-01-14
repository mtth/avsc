'use strict';

/**
 * Filesystem specifics.
 *
 * This module contains functions only used by node.js. It is shimmed by
 * another module when `avsc` is required from `browserify`.
 */

let fs = require('fs'),
    path = require('path');

/** Default (asynchronous) file loading function for assembling IDLs. */
function createImportHook() {
  let imports = {};
  return function ({path: fpath, parentPath}, cb) {
    fpath = path.resolve(path.dirname(parentPath), fpath);
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
  return function ({path: fpath, parentPath}, cb) {
    fpath = path.resolve(path.dirname(parentPath), fpath);
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


module.exports = {
  createImportHook,
  createSyncImportHook,
  // Proxy a few methods to better shim them for browserify.
  existsSync: fs.existsSync,
  readFileSync: fs.readFileSync
};
