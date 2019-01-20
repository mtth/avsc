/* jshint esversion: 6, node: true */

'use strict';

const {Context} = require('../context');
const {Service} = require('../service');
const types = require('../types');

const debug = require('debug');
const {EventEmitter} = require('events');
const stream = require('stream');

const {Packet, SystemError, randomId} = types;
const d = debug('avro:services:channels:netty');

// Service used to send discovery requests.
const discoveryService = new Service({protocol: 'avro.DiscoveryService'});

const DEADLINE_HEADER = 'avro.netty.deadline';

/** Bridge between a channel and netty connecion. */
// TODO: Add a `free` function to allow reusing the input streams?
class NettyClientBridge {
  constructor(readable, writable, opts) {
    if (!isStream(writable)) {
      opts = writable;
      writable = readable;
    }
    this._serverServices = new Map(); // Keyed by _remote_ hash.
    const serverSvcs = this._serverServices;
    const hashes = new Map(); // Client hash to server hash.
    const cbs = new Map();
    let draining = false;

    const encoder = new NettyEncoder(types.handshakeRequest);
    encoder
      .on('error', (err) => {
        d('Encoder error: %s', err);
        writable.emit('error', err);
      })
      .pipe(writable)
      .on('finish', onWritableFinish);

    if (readable !== writable) {
      writable.on('error', onWritableError);
    }

    const decoder = new NettyDecoder(types.handshakeResponse);
    readable
      .on('error', onReadableError)
      .on('end', onReadableEnd)
      .pipe(decoder)
      .on('data', ({handshake, id, packet}) => {
        let obj = cbs.get(id);
        if (!obj) {
          d('No callback for packet %s.', id);
          return;
        }
        const match = handshake.match;
        d('Response to packet %s has a handshake with match %s.', id, match);
        const clientSvc = obj.clientService;
        const clientHash = clientSvc.hash;
        let serverSvc;
        if (handshake.serverHash || handshake.serverProtocol) {
          const serverHash = handshake.serverHash.toString('binary');
          try {
            serverSvc = new Service(JSON.parse(handshake.serverProtocol));
          } catch (err) {
            destroy(err); // Fatal error.
            return;
          }
          // IMPORTANT: `serverHash` might be different from `serverSvc.hash`.
          // The remove server might use a different algorithm to compute it.
          serverSvcs.set(serverHash, serverSvc);
          hashes.set(clientHash, serverHash);
        } else {
          const serverHash = hashes.get(clientHash);
          serverSvc = serverHash ? serverSvcs.get(serverHash) : clientSvc;
        }
        if (match === 'NONE' && !obj.retried) {
          d('Retrying packet %s.', id);
          obj.retried = true;
          send(id, obj.packet, true);
          return;
        }
        const delay = Date.now() - obj.time;
        d('Received response to packet %s in %sms.', id, delay);
        const pres = new Packet(id, serverSvc, packet.body, packet.headers);
        obj.cb(null, pres);
      })
      .on('error', (err) => {
        d('Decoder error: %s', err);
        readable.emit('error', err);
      });

    this._channel = function (ctx, preq, cb) {
      if (ctx.cancelled) {
        return;
      }
      if (!readable) {
        cb(new Error('channel was destroyed'));
        return;
      }
      if (draining) {
        cb(new Error('channel started draining'));
        return;
      }
      if (!preq) {
        // TODO: Discovery RPC.
        cb(new Error('not yet supported'));
        return;
      }
      const id = preq.id;
      const headers = preq.headers;
      if (ctx.deadline) {
        headers[DEADLINE_HEADER] = types.dateTime.toBuffer(ctx.deadline);
      }
      const cleanup = ctx.onCancel(() => { cbs.delete(id); });
      const packet = new NettyPacket(preq.body, headers);
      const clientSvc = preq.service;
      cbs.set(id, {
        cb: (...args) => {
          if (cleanup()) {
            cbs.delete(id);
            cb(...args);
          }
        },
        clientService: clientSvc,
        packet, // For retries.
        retried: false,
        time: Date.now(),
      });
      send(id, packet, clientSvc, false);
    }

    function send(id, packet, clientSvc, includePtcl) {
      const serverHash = hashes.get(clientSvc.hash) || clientSvc.hash;
      let handshake = {
        clientHash: Buffer.from(clientSvc.hash, 'binary'),
        serverHash: Buffer.from(serverHash, 'binary'),
      };
      let suffix;
      if (includePtcl) {
        handshake.clientProtocol = JSON.stringify(clientSvc.protocol);
        suffix = 'with full handshake';
      } else {
        suffix = 'with light handshake';
      }
      d('Sending packet %s %s.', id, suffix);
      encoder.write({handshake, id, packet});
    }

    function onReadableEnd() {
      d('Readable end.');
      destroy();
    }

    function onReadableError(err) {
      d('Readable error: %s', err);
      destroy(err);
      emitIfSwallowed(this, err);
    }

    function onWritableError(err) {
      d('Writable error: %s', err);
      destroy(err);
      emitIfSwallowed(this, err);
    }

    function onWritableFinish() {
      d('Writable finished, starting to drain.');
      draining = true;
    }

    function destroy(err) {
      if (!readable) {
        return; // Already destroyed.
      }
      d('Destroying channel.');
      err = err || new Error('unavailable channel');
      readable
        .unpipe(decoder)
        .removeListener('error', onReadableError)
        .removeListener('end', onReadableEnd);
      encoder.unpipe(writable);
      writable
        .removeListener('error', onWritableError)
        .removeListener('finish', onWritableFinish);
      readable = null;
      writable = null;
      for (const obj of cbs.values()) {
        obj.cb(err);
      }
      cbs.clear();
    }
  }

  /** Cache of protocol hash to service. */
  get knownServices() {
    return this._serverServices;
  }

  get channel() {
    return this._channel;
  }
}

/**
 * Netty connection to channel bridge.
 *
 * Note that when a request packet omits its handshake, the previous packet's
 * service will be used. This will work correctly when only clients from a
 * single service connect to this bridge, but likely won't otherwise.
 */
class NettyServerBridge extends EventEmitter {
  constructor(chan) {
    super();
    this._clientServices = null;
    this._serverServices = null;
    this._channel = chan;

    chan(new Context(), null, (err, svcs) => {
      if (err) {
        this.emit('error', err);
        return;
      }
      this._clientServices = new Map(); // Keyed by hash.
      this._serverServices = new Map(); // Keyed by name.
      for (const svc of svcs) {
        this._clientServices.set(svc.hash, svc);
        this._serverServices.set(svc.name, svc);
      }
      d('Bridge ready to serve %s service(s).', this._serverServices.size);
      this.emit('ready');
    });
  }

  /** Cache of protocol hash to service, saves handshake requests. */
  get knownServices() {
    return this._clientServices;
  }

  /** Accept a connection. */
  accept(readable, writable) {
    if (!this._clientServices) {
      d('Server bridge is not ready yet, queuing acceptance call');
      this.once('ready', () => this.accept(readable, writable));
      return;
    }

    if (!writable) {
      writable = readable;
    }
    // We store the last client service seen for stateful connections.
    let clientSvc = null;

    const decoder = new NettyDecoder(types.handshakeRequest);
    readable
      .on('error', onReadableError)
      .on('end', onReadableEnd)
      .pipe(decoder)
      .on('data', ({handshake: hreq, id, packet}) => {
        if (!hreq && !clientSvc) {
          d('Dropping packet %s, no handshake or client service.', id);
          readable.emit('error', new Error('expected handshake'));
          return;
        }
        let hres = null;
        if (hreq) {
          hres = {match: 'BOTH'};
          const clientHash = hreq.clientHash.toString('binary');
          clientSvc = this._clientServices.get(clientHash);
          if (!clientSvc) {
            if (!hreq.clientProtocol) {
              d('No service for packet %s, responding with match NONE.', id);
              // A retry will be required.
              const err = new SystemError('ERR_AVRO_UNKNOWN_CLIENT_PROTOCOL');
              hres.match = 'NONE';
              if (this._serverServices.size === 1) {
                // If only one server is behind this channel, we can already
                // send its protocol, otherwise we will have to wait for the
                // client protocol's name to tell which one to send back.
                const serverSvc = Array.from(this._serverServices.value())[0];
                hres.serverProtocol = JSON.stringify(serverSvc.protocol);
                hres.serverHash = Buffer.from(serverSvc.hash, 'binary');
              }
              const res = NettyResponse.forSystemError(err, id);
              encoder.write({handshake: hres, response: res});
              return;
            }
            try {
              clientSvc = new Service(JSON.parse(hreq.clientProtocol));
            } catch (err) {
              d('Bad protocol in packet %s: %s', id, err);
              readable.emit('error', err);
              return;
            }
            d('Set client service from packet %s.', id);
            this._clientServices.set(clientHash, clientSvc);
          }
          const serverSvc = this._serverServices.get(clientSvc.name);
          if (!serverSvc) {
            d('Unknown server hash in packet %s, sending protocol.', id);
            hres.match = 'CLIENT';
            hres.serverProtocol = JSON.stringify(serverSvc.protocol);
            hres.serverHash = Buffer.from(serverSvc.hash, 'binary');
          }
        }
        let ctx;
        const deadlineBuf = packet.headers[DEADLINE_HEADER];
        if (deadlineBuf) {
          delete packet.headers[DEADLINE_HEADER];
          try {
            ctx = new Context(types.dateTime.fromBuffer(deadlineBuf));
          } catch (err) {
            d('Bad deadline in packet %s: %s', id, err);
            readable.emit('error', err);
            return;
          }
          if (ctx.cancelled) {
            d('Packet %s is past its deadline, dropping request.', id);
            return;
          }
          d('Packet %s has a timeout of %sms.', id, +ctx.remainingDuration);
        } else {
          ctx = new Context();
        }
        const preq = new Packet(id, clientSvc, packet.body, packet.headers);
        this._channel(ctx, preq, (err, pres) => {
          if (ctx.cancelled) {
            d('Packet %s was cancelled, dropping response.', id);
            return;
          }
          if (err) {
            d('Error while responding to packet %s: %s', id, err);
            err = new SystemError('ERR_AVRO_CHANNEL_FAILURE', err);
            const packet = NettyPacket.forSystemError(err);
            encoder.write({handshake: hres, id, packet});
            destroy();
            return;
          }
          const packet = new NettyPacket(pres.body, pres.headers);
          encoder.write({handshake: hres, id, packet});
        });
      })
      .on('error', (err) => {
        d('Server bridge decoder error: %s', err);
        readable.emit('error', err);
      });

    const encoder = new NettyEncoder(types.handshakeResponse);
    encoder
      .on('error', (err) => {
        d('Server bridge encoder error: %s', err);
        writable.emit('error', err);
      })
      .pipe(writable)
      .on('finish', onWritableFinish);

    if (readable !== writable) {
      writable.on('error', onWritableError);
    }

    function onReadableEnd() {
      d('Server bridge readable end.');
      destroy();
    }

    function onReadableError(err) {
      d('Server bridge readable error: %s', err);
      destroy();
      emitIfSwallowed(this, err);
    }

    function onWritableError(err) {
      d('Server bridge writable error: %s', err);
      destroy();
      emitIfSwallowed(this, err);
    }

    function onWritableFinish() {
      d('Server bridge writable finished.');
      destroy();
    }

    function destroy() {
      if (!readable) {
        return; // Already destroyed.
      }
      d('Destroying proxied channel.');
      readable
        .unpipe(decoder)
        .removeListener('error', onReadableError)
        .removeListener('end', onReadableEnd);
      encoder.unpipe(writable);
      writable
        .removeListener('error', onWritableError)
        .removeListener('finish', onWritableFinish);
      readable = null;
      writable = null;
    }
  }
}

/** Netty-compatible decoding stream. */
class NettyDecoder extends stream.Transform {
  constructor(handshakeType) {
    super({readableObjectMode: true});
    this._expectHandshake = true;
    this._handshakeType = handshakeType;
    this._id = undefined;
    this._frameCount = 0;
    this._buf = Buffer.alloc(0);
    this._bufs = [];
    this.on('finish', () => { this.push(null); });
  }

  _transform(buf, encoding, cb) {
    buf = Buffer.concat([this._buf, buf]);

    while (true) {
      if (this._id === undefined) {
        if (buf.length < 8) {
          this._buf = buf;
          cb();
          return;
        }
        this._id = buf.readInt32BE(0);
        this._frameCount = buf.readInt32BE(4);
        buf = buf.slice(8);
      }

      let frameLength;
      while (
        this._frameCount &&
        buf.length >= 4 &&
        buf.length >= (frameLength = buf.readInt32BE(0)) + 4
      ) {
        this._frameCount--;
        this._bufs.push(buf.slice(4, frameLength + 4));
        buf = buf.slice(frameLength + 4);
      }

      if (this._frameCount) {
        this._buf = buf;
        cb();
        return;
      } else {
        let obj;
        try {
          obj = this._decode(this._id, Buffer.concat(this._bufs));
        } catch (err) {
          this.emit('error', err);
          return;
        }
        this._bufs = [];
        this._id = undefined;
        this.push(obj);
      }
    }
  }

  _decode(id, buf) {
    const handshakeType = this._handshakeType;
    let handshake, packet;
    try {
      decode(this._expectHandshake);
    } catch (err) {
      d('Failed to decode with%s handshake, retrying...',
        this._expectHandshake ? '' : 'out');
      decode(this._expectHandshake);
    }
    if (!handshake && this._expectHandshake) {
      d('Switching to handshake-free mode.');
      this._expectHandshake = false;
    }
    d('Decoded packet %s', id);
    return {handshake, id, packet};

    function decode(withHandshake) {
      let body;
      if (withHandshake) {
        const obj = handshakeType.decode(buf, 0);
        if (obj.offset < 0) {
          throw new Error('truncated packet handshake');
        }
        handshake = obj.value;
        body = buf.slice(obj.offset);
      } else {
        handshake = null;
        body = buf;
      }
      packet = NettyPacket.fromPayload(body);
    }
  }

  _flush() {
    if (this._buf.length || this._bufs.length) {
      const bufs = this._bufs.slice();
      bufs.unshift(this._buf);
      const err = new Error('trailing data');
      // Attach the data to help debugging (e.g. if the encoded bytes contain a
      // human-readable protocol like HTTP).
      err.trailingData = Buffer.concat(bufs);
      this.emit('error', err);
    }
  }
}

/** Netty-compatible encoding stream. */
class NettyEncoder extends stream.Transform {
  constructor(handshakeType) {
    super({writableObjectMode: true});
    this._handshakeType = handshakeType;
    this.on('finish', () => { this.push(null); });
  }

  _encode(obj) {
    d('Encoding packet %s', obj.id);
    const bufs = [obj.packet.toPayload()];
    if (obj.handshake) {
      bufs.unshift(this._handshakeType.toBuffer(obj.handshake));
    }
    return bufs;
  }

  _transform(obj, encoding, cb) {
    let bufs;
    try {
      bufs = this._encode(obj);
    } catch (err) {
      cb(err);
      return;
    }
    // Header: [ ID, number of frames ]
    const headBuf = Buffer.alloc(8);
    headBuf.writeInt32BE(obj.id, 0);
    headBuf.writeInt32BE(bufs.length, 4);
    this.push(headBuf);
    // Frames, each: [ length, bytes ]
    for (const buf of bufs) {
      this.push(intBuffer(buf.length));
      this.push(buf);
    }
    cb();
  }
}

class NettyPacket {
  constructor(body, headers) {
    this.body = body;
    this.headers = headers || {};

  }

  toPayload() {
    return Buffer.concat([types.mapOfBytes.toBuffer(this.headers), this.body]);
  }

  static ping() {
    return new NettyPacket(Buffer.alloc(1));
  }

  static fromPayload(buf) {
    const pkt = new NettyPacket();
    let obj;
    obj = types.mapOfBytes.decode(buf, 0);
    if (obj.offset < 0) {
      throw new Error('truncated request headers');
    }
    pkt.headers = obj.value;
    pkt.body = buf.slice(obj.offset);
    return pkt;
  }

  static fromSystemError(err) {
    const buf = types.systemError.toBuffer(err);
    const body = Buffer.concat([Buffer.from([1, 0]), buf]);
    return NettyPacket.forPayload(body);
  }
}

function intBuffer(n) {
  const buf = Buffer.alloc(4);
  buf.writeInt32BE(n);
  return buf;
}

function emitIfSwallowed(ee, err) {
  if (!ee.listenerCount('error')) { // Only re-emit if we swallowed it.
    ee.emit('error', err);
  }
}

function isStream(any) {
  return any && typeof any.pipe == 'function';
}

module.exports = {
  NettyClientBridge,
  NettyServerBridge,
};
