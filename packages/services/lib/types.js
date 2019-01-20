/* jshint esversion: 6, node: true */

'use strict';

const {Type, types: {LogicalType}} = require('avsc');
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

// Various useful types. We instantiate options once, to share the registry.
const opts = {
  namespace: NAMESPACE,
  logicalTypes: {
    'datetime-millis': DateTimeType,
    'system-error': SystemErrorType,
  },
};

const string = Type.forSchema('string', opts);

const mapOfBytes = Type.forSchema({type: 'map', values: 'bytes'}, opts);

const handshakeRequest = Type.forSchema({
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], default: null},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', mapOfBytes], default: null}
  ]
}, opts);

const handshakeResponse = Type.forSchema({
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
    {name: 'meta', type: ['null', mapOfBytes], default: null}
  ]
}, opts);

const systemError = Type.forSchema({
  type: 'string',
  logicalType: 'system-error',
}, opts);

const dateTime = Type.forSchema({
  type: 'long',
  logicalType: 'datetime-millis',
}, opts);

class Packet {
  constructor(id, svc, body, headers) {
    this.id = id;
    this.service = svc;
    this.body = body;
    this.headers = headers || {};
  }
}

/**
 * Random ID generator, mostly useful for packets.
 *
 * We are using 31 bit IDs since this is what the Java netty implementation
 * uses, hopefully there aren't ever enough packets in flight for collisions to
 * be an issue. (We could use 32 bits but the extra bit isn't worth the
 * inconvenience of negative numbers or additional logic to transform them.)
 * https://github.com/apache/avro/blob/5e8168a25494b04ef0aeaf6421a033d7192f5625/lang/java/ipc/src/main/java/org/apache/avro/ipc/NettyTransportCodec.java#L100
 */
function randomId() {
  return ((-1 >>> 1) * Math.random()) | 0;
}

module.exports = {
  NAMESPACE,
  Packet,
  SystemError,
  dateTime,
  handshakeRequest,
  handshakeResponse,
  mapOfBytes,
  randomId,
  string,
  systemError,
};
