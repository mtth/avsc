/* jshint browserify: true */

'use strict';

/**
 * Shim to disable schema fingerprint computation.
 *
 */

function createHash() {
  throw new Error('schema fingerprinting not supported in the browser');
}


module.exports = {
  createHash: createHash
};
