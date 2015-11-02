/* jshint node: true */

'use strict';

/**
 * Shim to ease transition from the previous JS validator.
 *
 * This API is identical to the previous one except for the following
 * differences:
 *
 * + The validator's `schema` property isn't available.
 * + Error messages might be worded differently.
 * + All types are now implemented (e.g. `bytes`).
 * + Namespaces are now correctly handled.
 * + Very large `long`s will not pass validation (safer since they might suffer
 *   precision loss).
 * + Extra fields in records are ignored (more efficient and leaves more
 *   flexibility to clients).
 *
 * See `test.js` in this folder for more information.
 *
 */

var protocols = require('../../lib/protocols'),
    schemas = require('../../lib/schemas'),
    util = require('util');


var WARNING = 'Validator API is now deprecated in favor of the Type API';

/* jshint -W021 */
Validator = util.deprecate(Validator, WARNING);
ProtocolValidator = util.deprecate(ProtocolValidator, WARNING);
/* jshint +W021 */


function Validator(schema, namespace, namedTypes) {
  var opts = {namespace: namespace, registry: namedTypes};
  this._type = schemas.createType(schema, opts);
  this.rawSchema = schema;
}

Validator.prototype.validate = function (obj) {
  return this._type.isValid(obj, {errorHook: errorHook});
};

Validator.validate = function (schema, obj) {
  var validator = new Validator(schema);
  return validator.validate(obj);
};


function ProtocolValidator(protocol) {
  this._protocol = new protocols.Protocol(protocol);
}

ProtocolValidator.prototype.validate = function (typeName, obj) {
  var type = this._protocol.getTypes()[typeName];
  if (!type) {
    throw new Error(util.format('unknown type: %s', typeName));
  }
  return type.isValid(obj, {errorHook: errorHook});
};

ProtocolValidator.validate = function (protocol, typeName, obj) {
  var validator = new ProtocolValidator(protocol);
  return validator.validate(typeName, obj);
};

// Helpers.

/**
 * Hook used to get information about errors.
 *
 * @param path {Array} Components of path to mismatch.
 * @param obj {...} Mismatched value.
 * @param type {Type} Expected type.
 *
 * This hook will throw an error on the first mismatch encounter, consistent
 * with the behavior of the previous validator.
 *
 */
function errorHook(path, obj, type) {
  var name = type.getSchema(true);
  throw new Error(util.format('invalid %s@%s: %j', name, path.join(), obj));
}


module.exports = {
  Validator: Validator,
  ProtocolValidator: ProtocolValidator
};
