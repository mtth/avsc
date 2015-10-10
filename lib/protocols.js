/* jshint node: true */

// TODO: Split record and error namespaces.

'use strict';


var types = require('./types');


var HANDSHAKE_REQUEST_TYPE = types.Type.fromSchema({
  name: 'org.apache.avro.ipc.HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string']},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', {type: 'map', values: 'bytes'}]}
  ]
});

var HANDSHAKE_RESPONSE_TYPE = types.Type.fromSchema({
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
 * Protocols are event emitters. To send a message:
 *
 *  protocol.emit(name, params, cb);
 *
 */
function Protocol(schema) {
  var name = schema.protocol;
  if (!name) {
    throw new Error('missing protocol name');
  }
  var namespace = schema.namespace;
  if (!~name.indexOf('.') && namespace) {
    name = namespace + '.' + name;
  }
  this._name = name;

  if (!schema.messages) {
    throw new Error('missing protocol messages');
  }
  var keys = Object.keys(schema.messages);
  var opts = {
    namespace: namespace,
    registry: types.Type.getDefaultRegistry(),
    typeHook: function (schema) {
      if (schema.type === 'error') {
        schema.type = 'record';
      }
    }
  };
  if (schema.types) {
    // Register all type definitions.
    schema.types.forEach(function (obj) { types.Type.fromSchema(obj, opts); });
  }
  this._messages = {};
  var i, l, key;
  for (i = 0, l = keys.length; i < l; i++) {
    key = keys[i];
    this._messages[key] = new Message(schema.messages[key], opts);
  }
}

Protocol.prototype.toString = function () { return JSON.stringify(this); };


/**
 * An Avro message.
 *
 */
function Message(schema, opts) {
  this._params = schema.request.map(function (obj) {
    return {
      name: obj.name,
      type: types.Type.fromSchema(obj.type, opts)
    };
  });
  this._responseType = types.Type.fromSchema(schema.response, opts);
  var errors = schema.errors || [];
  errors.unshift('string');
  this._errorType = types.Type.fromSchema(errors, opts);
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Protocol: Protocol
};
