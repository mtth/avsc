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
 * + Namespaced type names are now correctly supported.
 *
 */

var protocols = require('../../lib/protocols'),
    schemas = require('../../lib/schemas'),
    util = require('util');

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
