/* jshint esversion: 6, node: true */

'use strict';

const {Type, LogicalType} = require('@avro/types');
const {DateTime} = require('luxon');

// Namespace used for all internal types declared here.
const NAMESPACE = 'org.apache.avro.ipc';

class SystemError extends Error {
  constructor(code, cause) {
    super(cause ? cause.message : undefined);
    this.code = code;
    this.cause = cause || null;
  }

  get name() {
    return `SystemError [${this.code}]`;
  }

  get applicationCode() {
    const cause = this.cause;
    return this.code === 'ERR_APPLICATION' && cause ? cause.code : '';
  }

  wrap() {
    return stringType.wrap(this);
  }

  get _isAvroServicesSystemError() {
    return true;
  }

  static isSystemError(any) {
    return !!(any && any._isAvroServicesSystemError);
  }

  static orCode(code, err) {
    if (SystemError.isSystemError(err)) {
      return err;
    }
    return new SystemError(code, err);
  }

  static forPacket(pkt) {
    const body = pkt.body;
    if (!body[0] || body[1]) {
      return null;
    }
    try {
      const {value} = systemErrorType.decode(body, 2);
      return value;
    } catch (err) {
      return null;
    }
  }
}

class SystemErrorType extends LogicalType {
  _fromValue(val) {
    let obj;
    try {
      obj = JSON.parse(val);
    } catch (err) { // Possibly message from an incompatible server.
      obj = {code: 'ERR_UNKNOWN', cause: new Error(val)};
    }
    const err = new SystemError(obj.code, obj.cause);
    if (obj.cause && obj.cause.stack) {
      err.stack += `\n  Caused by ${obj.cause.stack}`;
    }
    return err;
  }

  _toValue(any) {
    if (!SystemError.isSystemError(any)) {
      return undefined;
    }
    const obj = {code: any.code};
    if (any.cause) {
      obj.cause = Object.assign({
        code: any.cause.code,
        message: any.cause.message,
        stack: any.cause.stack,
      }, any.cause);
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

// Various useful types. We instantiate options once, to share the registry.
const opts = {
  namespace: NAMESPACE,
  logicalTypes: {
    'datetime-millis': DateTimeType,
    'system-error': SystemErrorType,
  },
};

const stringType = Type.forSchema('string', opts);

const mapOfBytesType = Type.forSchema({type: 'map', values: 'bytes'}, opts);

const mapOfStringType = Type.forSchema({type: 'map', values: 'string'}, opts);

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

function handshakeRequest(clientSvc, serverHash, includePtcl, meta) {
  const handshake = {
    clientHash: Buffer.from(clientSvc.hash, 'binary'),
    serverHash: Buffer.from(serverHash, 'binary'),
    meta: meta || {},
  };
  if (includePtcl) {
    handshake.clientProtocol = JSON.stringify(clientSvc.protocol);
  }
  return handshake;
}

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
  handshakeRequest,
  handshakeResponseType,
  mapOfBytesType,
  mapOfStringType,
  randomId,
  stringType,
  systemErrorType,
};
