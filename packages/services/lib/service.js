/* jshint node: true */

'use strict';

/** This module implements Avro's IPC/RPC logic. */

const utils = require('./utils');

const {Type, WrappedUnionType} = require('@avro/types');
const crypto = require('crypto');

/** An Avro message, containing its request, response, etc. */
class Message {
  constructor(name, schema, opts) {
    opts = opts || {};

    if (!isValidName(name)) {
      throw new Error(`invalid message name: ${name}`);
    }
    this.name = name;

    // We use a record with a placeholder name here (the user might have set
    // `noAnonymousTypes`, so we can't use an anonymous one). We remove it from
    // the registry afterwards to avoid exposing it outside.
    if (!Array.isArray(schema.request)) {
      throw new Error(`invalid message request: ${name}`);
    }
    const recordName = `${utils.NAMESPACE}.Request`;
    try {
      this.request = Type.forSchema({
        name: recordName,
        type: 'record',
        namespace: opts.namespace || '', // Don't leak request namespace.
        fields: schema.request
      }, opts);
    } finally {
      delete opts.registry[recordName];
    }

    if (!schema.response) {
      throw new Error(`invalid message response: ${name}`);
    }
    this.response = Type.forSchema(schema.response, opts);

    if (schema.errors !== undefined && !Array.isArray(schema.errors)) {
      throw new Error(`invalid message errors: ${name}`);
    }
    this.error = new WrappedUnionType( // Errors are always wrapped.
      [utils.systemErrorType].concat(schema.errors || []),
      opts
    );

    this.oneWay = !!schema['one-way'];
    this.doc = schema.doc !== undefined ? '' + schema.doc : undefined;

    Object.freeze(this);
  }

  schema() {
    return Type.prototype.schema.call(this);
  }

  _attrs(opts) {
    const reqSchema = this.request._attrs(opts);
    const schema = {
      request: reqSchema.fields,
      response: this.response._attrs(opts)
    };
    const msgDoc = this.doc;
    if (msgDoc !== undefined) {
      schema.doc = msgDoc;
    }
    const errSchema = this.error._attrs(opts);
    if (errSchema.length > 1) {
      schema.errors = errSchema.slice(1);
    }
    if (this.oneWay) {
      schema['one-way'] = true;
    }
    return schema;
  }
}

/**
 * An Avro RPC service.
 *
 * This constructor shouldn't be called directly, but via the
 * `Service.forProtocol` method. This function performs little logic to better
 * support efficient copy.
 */
class Service {
  constructor(ptcl, opts) {
    opts = opts || {};

    const name = ptcl.protocol;
    if (!name) {
      throw new Error('missing protocol name');
    }
    if (ptcl.namespace !== undefined) {
      opts.namespace = ptcl.namespace;
    } else {
      const match = /^(.*)\.[^.]+$/.exec(name);
      if (match) {
        opts.namespace = match[1];
      }
    }
    this.name = qualify(name, opts.namespace);

    if (ptcl.types) {
      for (const schema of ptcl.types) {
        Type.forSchema(schema, opts); // Add to registry.
      }
    }
    this.messages = new Map();
    if (ptcl.messages) {
      for (const key of Object.keys(ptcl.messages)) {
        this.messages.set(key, new Message(key, ptcl.messages[key], opts));
      }
    }
    this.types = new Map();
    for (const key of Object.keys(opts.registry || {})) {
      this.types.set(key, opts.registry[key]);
    }

    this.protocol = ptcl;
    this.hash = getHash(JSON.stringify(ptcl)); // TODO: Canonicalize.
    this.doc = ptcl.doc ? '' + ptcl.doc : undefined;

    Object.freeze(this);
  }

  /** Returns all value constructors in the given namespace. */
  constructors(ns = '') {
    const obj = {};
    for (const [qualifiedName, type] of this.types) {
      const parts = qualifiedName.split('.');
      const name = parts.pop();
      // TODO: Rename `type.recordConstructor` on errors and records to
      // `valueConstructor`. This name extends more naturally to logical types.
      const ctor = type.recordConstructor || type.valueConstructor;
      if (ctor && parts.join('.') === ns) {
        obj[name] = ctor;
      }
    }
    return obj;
  }

  static compatible(clientSvc, serverSvc) {
    try {
      new Decoder(clientSvc, serverSvc);
    } catch (err) {
      return false;
    }
    return true;
  }
}

class Decoder {
  constructor(clientSvc, serverSvc) {
    if (clientSvc.name !== serverSvc.name) {
      throw new Error('protocol name mismatch');
    }
    const rs = new Map();
    for (const clientMsg of clientSvc.messages.values()) {
      const name = clientMsg.name;
      const serverMsg = serverSvc.messages.get(name);
      if (!serverMsg) {
        throw new Error(`missing server message: ${name}`);
      }
      if (serverMsg.oneWay !== clientMsg.oneWay) {
        throw new Error(`inconsistent one-way message: ${name}`);
      }
      addResolver(name + '?', serverMsg.request, clientMsg.request);
      addResolver(name + '*', clientMsg.error, serverMsg.error);
      addResolver(name + '!', clientMsg.response, serverMsg.response);
    }
    this._resolvers = rs;
    this._clientService = clientSvc;
    this._serverService = serverSvc;

    function addResolver(key, rtype, wtype) {
      if (!rtype.equals(wtype)) {
        rs.set(key, rtype.createResolver(wtype));
      }
    }
  }

  decodeRequest(name, buf) {
    const msg = this._serverService.messages.get(name);
    return msg.request.fromBuffer(buf, this._resolvers.get(name + '?'));
  }

  decodeError(name, buf) {
    const msg = this._clientService.messages.get(name);
    return msg.error.fromBuffer(buf, this._resolvers.get(name + '*'));
  }

  decodeResponse(name, buf) {
    const msg = this._clientService.messages.get(name);
    return msg.response.fromBuffer(buf, this._resolvers.get(name + '!'));
  }
}

function isValidName(name) { // TODO: Consolidate.
  try {
    Type.forSchema({name: 'UNUSED', type: 'enum', symbols: [name]});
    return true;
  } catch (err) {
    return false;
  }
}

function qualify(name, namespace) { // TODO: Consolidate.
  return Type.forSchema({name, type: 'record', fields: []}, {namespace}).name;
}

/** MD5 hash. */
function getHash(str) {
  var hash = crypto.createHash('md5');
  hash.end(str);
  return hash.read().toString('binary');
}

module.exports = {
  Decoder,
  Message,
  Service,
};
