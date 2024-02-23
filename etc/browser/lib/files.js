'use strict';

/** Shim without file-system operations. */

function createError() { return new Error('unsupported in the browser'); }

function createImportHook() {
  return function (_, cb) { cb(createError()); };
}

function createSyncImportHook() {
  return function () { throw createError(); };
}

function tryReadFileSync() { return null; }

module.exports = {
  createImportHook,
  createSyncImportHook,
  tryReadFileSync,
};
