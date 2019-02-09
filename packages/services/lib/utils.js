/* jshint esversion: 6, node: true */

'use strict';

const {Type, LogicalType} = require('@avro/types');
const {DateTime} = require('luxon');

// Namespace used for all internal types declared here.
const NAMESPACE = 'org.apache.avro.ipc';

const ERROR_PREFIX = 'ERR_AVRO_';

class SystemError extends Error {
  constructor(code, cause) {
    if (!code.startsWith(ERROR_PREFIX)) {
      throw new Error(`bad error code: ${code}`);
    }
    super(cause ? cause.message : undefined);
    this.code = code;
    this.cause = cause || null;
  }

  get name() {
    return `SystemError [${this.code}]`;
  }

  static isSystemError(any) {
    return any &&
      typeof any.code == 'string' &&
      any.code.startsWith(ERROR_PREFIX);
  }

  static orCode(code, err) {
    if (SystemError.isSystemError(err)) {
      return err;
    }
    return new SystemError(code, err);
  }
}

class SystemErrorType extends LogicalType {
  _fromValue(val) {
    let obj;
    try {
      obj = JSON.parse(val);
    } catch (err) { // Possibly message from an incompatible server.
      obj = {code: 'ERR_AVRO_GENERIC', cause: new Error(val)};
    }
    return new SystemError(obj.code, obj.cause);
  }

  _toValue(any) {
    if (!SystemError.isSystemError(any)) {
      return undefined;
    }
    const obj = {code: any.code};
    if (any.cause) {
      obj.cause = Object.assign({message: any.cause.message}, any.cause);
    }
    return JSON.stringify(obj);
  }
}

class DateTimeType extends LogicalType {
  _fromValue(val) {
    return DateTime.fromMillis(val, {zone: this._zone});
  }

  _toValue(any) {
    return DateTime.isDateTime(any) ? any.toMillis() : undefined;
  }
}

class JsonType extends LogicalType {
  _fromValue(val) {
    return JSON.parse(val);
  }

  _toValue(any) {
    return JSON.stringify(any);
  }
}

// Various useful types. We instantiate options once, to share the registry.
const opts = {
  namespace: NAMESPACE,
  logicalTypes: {
    'datetime-millis': DateTimeType,
    'json': JsonType,
    'system-error': SystemErrorType,
  },
};

const stringType = Type.forSchema('string', opts);

const mapOfBytesType = Type.forSchema({type: 'map', values: 'bytes'}, opts);

const mapOfJsonType = Type.forSchema({
  type: 'map', values: {type: 'string', logicalType: 'json'},
}, opts);

const handshakeRequestType = Type.forSchema({
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], default: null},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', mapOfBytesType], default: null}
  ]
}, opts);

const handshakeResponseType = Type.forSchema({
  name: 'HandshakeResponse',
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
    {name: 'serverProtocol', type: ['null', 'string'], default: null},
    {name: 'serverHash', type: ['null', 'MD5'], default: null},
    {name: 'meta', type: ['null', mapOfBytesType], default: null}
  ]
}, opts);

const systemErrorType = Type.forSchema({
  type: 'string',
  logicalType: 'system-error',
}, opts);

const dateTimeType = Type.forSchema({
  type: 'long',
  logicalType: 'datetime-millis',
}, opts);

/**
 * Random ID generator, mostly useful for packets.
 *
 * We are using 31 bit IDs since this is what the Java Netty implementation
 * uses (see http://goto.mtth.xyz/avro-java-netty), hopefully there aren't ever
 * enough packets in flight for collisions to be an issue. (We could use 32
 * bits but the extra bit isn't worth the inconvenience of negative numbers or
 * additional logic to transform them.)
 */
function randomId() {
  return ((-1 >>> 1) * Math.random()) | 0;
}

module.exports = {
  NAMESPACE,
  SystemError,
  dateTimeType,
  handshakeRequestType,
  handshakeResponseType,
  mapOfBytesType,
  mapOfJsonType,
  randomId,
  stringType,
  systemErrorType,
};
