/* jshint esversion: 6, node: true */

'use strict';

const {Type, types: {LogicalType}} = require('avsc');

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

  toPacket(id) {
    const buf = systemError.toBuffer(this);
    return new ResponsePacket(id, Buffer.concat([Buffer.from([1, 0]), buf]));
  }

  static isSystemError(any) {
    return any &&
      typeof any.code == 'string' &&
      any.code.startsWith(ERROR_PREFIX);
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

// Namespace used for all internal types declared here.
const NAMESPACE = 'org.apache.avro.ipc';

// Various useful types. We instantiate options once, to share the registry.
const opts = {
  namespace: NAMESPACE,
  logicalTypes: {'system-error': SystemErrorType},
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

class RequestPacket {
  constructor(id, msgName, body, headers) {
    this.id = id;
    this.messageName = msgName;
    this.body = body;
    this.headers = headers || {};
  }

  toPayload() {
    return Buffer.concat([
      mapOfBytes.toBuffer(this.headers),
      string.toBuffer(this.messageName),
      this.body,
    ]);
  }

  static fromPayload(id, buf) {
    const pkt = new RequestPacket(id);
    let obj;
    obj = mapOfBytes.decode(buf, 0);
    if (obj.offset < 0) {
      throw new Error('truncated request packet headers');
    }
    pkt.headers = obj.value;
    obj = string.decode(buf, obj.offset)
    if (obj.offset < 0) {
      throw new Error('truncated request packet message name');
    }
    pkt.messageName = obj.value;
    pkt.body = buf.slice(obj.offset);
    return pkt;
  }
}

class ResponsePacket {
  constructor(id, body, headers) {
    this.id = id;
    this.body = body;
    this.headers = headers || {};
  }

  toPayload() {
    return Buffer.concat([mapOfBytes.toBuffer(this.headers), this.body]);
  }

  static fromPayload(id, buf) {
    const pkt = new ResponsePacket(id);
    const {value: headers, offset} = mapOfBytes.decode(buf, 0);
    if (offset < 0) {
      throw new Error('truncated response packet headers');
    }
    pkt.headers = headers;
    pkt.body = buf.slice(offset);
    return pkt;
  }
}

const systemError = Type.forSchema({
  type: 'string',
  logicalType: 'system-error',
}, opts);

module.exports = {
  NAMESPACE,
  RequestPacket,
  ResponsePacket,
  SystemError,
  handshakeRequest,
  handshakeResponse,
  systemError,
};
