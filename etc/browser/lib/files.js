/* jshint node: true */

'use strict';

/** Shim without file-system operations. */

function createError() { return new Error('unsupported in the browser'); }


module.exports = {
  createImportHook: function (fpath, kind, cb) { cb(createError()); },
  createSyncImportHook: function () { throw createError(); },
  existsSync: function () { return false; },
  readFileSync: function () { throw createError(); }
};
