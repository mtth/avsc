/* jshint browserify: true */

'use strict';

/**
 * Shim to give a more explicit error message when parsing files.
 *
 */

function unsupported() {
  throw new Error('parsing schema files is not supported in the browser');
}


module.exports = {
  readFileSync: unsupported
};
