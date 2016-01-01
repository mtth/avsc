/* jshint browserify: true */

'use strict';

/**
 * Shim to give a more explicit error message when parsing schemas from files.
 *
 */


module.exports = {
  readFileSync: function () {
    throw new Error('parsing schema files is not supported in the browser');
  }
};
