'use strict';

/** Shim without file-system operations. */

function createError() { return new Error('unsupported in the browser'); }

function createImportHook() {
  return function (fpath, kind, cb) { cb(createError()); };
}

function createSyncImportHook() {
  return function () { throw createError(); };
}

function createRelativeResolveHook() {
  return function () { throw createError(); };
}

module.exports = {
  createImportHook: createImportHook,
  createSyncImportHook: createSyncImportHook,
  createRelativeResolveHook: createRelativeResolveHook,
  treatStringAsPath: () => false,
  // Proxy a few methods to better shim them for browserify.
  readFileSync: function () { throw createError(); }
};
