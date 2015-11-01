/* jshint node: true */

'use strict';

var schemas = require('./schemas');


var HANDSHAKE_REQUEST_TYPE = schemas.createType({
  name: 'org.apache.avro.ipc.HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string']},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', {type: 'map', values: 'bytes'}]}
  ]
});

var HANDSHAKE_RESPONSE_TYPE = schemas.createType({
  name: 'org.apache.avro.ipc.HandshakeResponse',
  type: 'record',
  fields: [
    {
      name: 'match',
      type: {
        name: 'HandshakeMatch',
        type: 'enum',
        symbols: ['BOTH', 'CLIENT', 'NONE']
      }
    },
    {name: 'serverProtocol', type: ['null', 'string']},
    {
      name: 'serverHash',
      type: ['null', {name: 'MD5', type: 'fixed', size: 16}]
    },
    {name: 'meta', type: ['null', {type: 'map', values: 'bytes'}]}
  ]
});


/**
 * An Avro protocol.
 *
 */
function Protocol(schema) {
  this._name = schemas.resolveNames(schema, undefined, 'protocol').name;

  if (!schema.messages) {
    throw new Error('missing protocol messages');
  }
  var opts = {namespace: schema.namespace};
  if (schema.types) {
    // Add all type definitions to `opts.registry`.
    schema.types.forEach(function (obj) { schemas.createType(obj, opts); });
  }
  this._messages = {};
  Object.keys(schema.messages).forEach(function (key) {
    this._messages[key] = new Message(schema.messages[key], opts);
  }, this);

  this._types = opts.registry;
}

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.getTypes = function () { return this._types; };

/**
 * An Avro message.
 *
 */
function Message(schema, opts) {
  this._params = schema.request.map(function (obj) {
    return {name: obj.name, type: schemas.createType(obj.type, opts)};
  });

  var response = schema.response;
  if (!response) {
    throw new Error('missing response');
  }
  this._responseType = schemas.createType(response, opts);

  var errors = schema.errors || [];
  errors.unshift('string');
  this._errorType = schemas.createType(errors, opts);

  this._oneWay = !!schema['one-way'];
  if (this._oneWay) {
    if (
      !(this._responseType instanceof schemas.types.NullType) ||
      errors.length > 1
    ) {
      throw new Error('unapplicable one-way parameter');
    }
  }
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Protocol: Protocol
};
