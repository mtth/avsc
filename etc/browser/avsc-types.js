'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-types')`.
 */

let types = require('../../lib/types');


module.exports = {
  Type: types.Type,
  types: types.builtins,
};
