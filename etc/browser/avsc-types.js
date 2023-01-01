'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-types')`.
 */

let types = require('../../lib/types');


/** Basic parse method, only supporting JSON parsing. */
function parse(any, opts) {
  let schema;
  if (typeof any == 'string') {
    try {
      schema = JSON.parse(any);
    } catch (err) {
      schema = any;
    }
  } else {
    schema = any;
  }
  return types.Type.forSchema(schema, opts);
}


module.exports = {
  Type: types.Type,
  parse,
  types: types.builtins,
  // Deprecated exports (not using `util.deprecate` since it causes stack
  // overflow errors in the browser).
  combine: types.Type.forTypes,
  infer: types.Type.forValue
};
