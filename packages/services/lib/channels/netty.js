/* jshint esversion: 6, node: true */

'use strict';

const {Service} = require('../service');
const types = require('../types');

const debug = require('debug');
const stream = require('stream');

const {RequestPacket, ResponsePacket, SystemError} = types;
const d = debug('avro:services:channels:netty');

// TODO: Also return a `free` function to allow reusing the input streams?
function netty(readable, writable) {
  if (!writable) {
    writable = readable;
  }
  const cbs = new Map();
  let draining = false;
  let match = 'NONE';
  let clientSvc = null;
  let serverHash = null;
  let serverSvc = null;

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

  const decoder = new NettyDecoder(
    ResponsePacket.fromPayload,
    types.handshakeResponse
  );
  readable
    .on('error', onReadableError)
    .on('end', onReadableEnd)
    .pipe(decoder)
    .on('data', ({handshake, packet: resPkt}) => {
      const id = resPkt.id;
      if (handshake) {
        match = handshake.match;
        d('Response to packet %s has a handshake with match %s.', id, match);
        if (handshake.serverHash || handshake.serverProtocol) {
          serverHash = handshake.serverHash.toString('binary');
          try {
            serverSvc = new Service(JSON.parse(handshake.serverProtocol));
          } catch (err) {
            destroy(err); // Fatal error.
            return;
          }
        } else if (!serverSvc) {
          serverSvc = clientSvc;
        }
      }
      let obj = cbs.get(id);
      if (!obj) {
        d('No callback for packet %s.', id);
        return;
      }
      if (match === 'NONE' && !obj.retried) { // We need to retry this packet.
        d('Retrying packet %s.', id);
        obj.retried = true;
        send(obj.reqPkt);
        return;
      }
      const delay = Date.now() - obj.time;
      d('Received response to packet %s in %sms.', id, delay);
      obj.cb(null, serverSvc, resPkt);
    })
    .on('error', (err) => {
      d('Decoder error: %s', err);
      readable.emit('error', err);
    });

  return channel;

  function send(packet) {
    let handshake = null;
    if (!serverHash) { // No response from server yet.
      handshake = {
        clientHash: Buffer.from(clientSvc.hash, 'binary'),
        serverHash: Buffer.from(clientSvc.hash, 'binary'),
      };
    } else if (match === 'NONE') { // Server doesn't know client protocol.
      handshake = {
        clientHash: Buffer.from(clientSvc.hash, 'binary'),
        clientProtocol: JSON.stringify(clientSvc.protocol),
        serverHash: Buffer.from(serverHash, 'binary'),
      };
    }
    d('Sending packet %s.', packet.id);
    encoder.write({handshake, packet});
  }

  function channel(svc, packet, cb) {
    if (!clientSvc) {
      clientSvc = svc;
    } else if (clientSvc.hash !== svc.hash) {
      destroy(new Error('inconsistent client service'));
      return;
    }
    if (draining) {
      cb(new Error('channel started draining'));
      return;
    }
    if (!readable) {
      cb(new Error('channel was destroyed'));
      return;
    }
    const id = packet.id;
    const cleanup = this.onCancel(() => { cbs.delete(id); });
    cbs.set(id, {
      attempt: 1,
      time: Date.now(),
      cb: (...args) => { cleanup(); cbs.delete(id); cb(...args); },
      reqPkt: match === 'BOTH' ? null : packet, // In case of retry.
    });
    send(packet);
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

class NettyProxy {
  constructor(chan) {
    this._services = new Map();
    this._channel = chan;

    chan(null, {id: -1, messageName: ''}, (err, svc) => {
      if (err) {
        throw err; // Should never happen.
      }
      this._serverService = svc;
      this._services.set(svc.hash, svc);
    });
    d('Proxy ready to serve service %s.', this._serverService.name);
  }

  get services() {
    return this._services;
  }

  proxy(readable, writable) {
    if (!writable) {
      writable = readable;
    }
    const serverSvc = this._serverService;
    let clientSvc = null;

    const decoder = new NettyDecoder(
      RequestPacket.fromPayload,
      types.handshakeRequest
    );

    readable
      .on('error', onReadableError)
      .on('end', onReadableEnd)
      .pipe(decoder)
      .on('data', ({handshake: hreq, packet: reqPkt}) => {
        const id = reqPkt.id;
        if (!hreq && !clientSvc) {
          d('Dropping packet %s, no handshake or client service.', id);
          readable.emit('error', new Error('expected handshake'));
          return;
        }
        let hres = null;
        if (hreq) {
          hres = {match: 'BOTH'};
          const clientHash = hreq.clientHash.toString('binary');
          if (clientSvc && clientSvc.hash !== clientHash) {
            d('Dropping packet %s, incompatible hash: %j', clientHash);
            readable.emit('error', new Error('incompatible hash'));
            return;
          }
          if (hreq.serverHash.toString('binary') !== serverSvc.hash) {
            d('Mismatched server hash in packet %s, sending protocol.', id);
            hres.match = 'CLIENT';
            hres.serverProtocol = JSON.stringify(serverSvc.protocol);
            hres.serverHash = Buffer.from(serverSvc.hash, 'binary');
          }
          clientSvc = this._services.get(clientHash);
          if (!clientSvc) {
            if (!hreq.clientProtocol) {
              d('No service for packet %s, responding with match NONE.', id);
              // A retry will be required.
              const err = new SystemError('ERR_AVRO_UNKNOWN_PROTOCOL');
              hres.match = 'NONE';
              encoder.write({handshake: hres, packet: err.toPacket(id)});
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
            this._services.set(clientHash, clientSvc);
          }
        }
        this._channel(clientSvc, reqPkt, (err, svc, resPkt) => {
          if (err) {
            // The server's channel will only return an error if the client's
            // protocol is incompatible with its own.
            err = new SystemError('ERR_AVRO_INCOMPATIBLE_PROTOCOL', err);
            d('Error while responding to packet %s: %s', id, err);
            encoder.write({handshake: hres, packet: err.toPacket(id)});
            destroy();
            return;
          }
          encoder.write({handshake: hres, packet: resPkt});
        });
      })
      .on('error', (err) => {
        d('Proxy decoder error: %s', err);
        readable.emit('error', err);
      });

    const encoder = new NettyEncoder(types.handshakeResponse);
    encoder
      .on('error', (err) => {
        d('Proxy encoder error: %s', err);
        writable.emit('error', err);
      })
      .pipe(writable)
      .on('finish', onWritableFinish);

    if (readable !== writable) {
      writable.on('error', onWritableError);
    }

    function onReadableEnd() {
      d('Proxy readable end.');
      destroy();
    }

    function onReadableError(err) {
      d('Proxy readable error: %s', err);
      destroy();
      emitIfSwallowed(this, err);
    }

    function onWritableError(err) {
      d('Proxy writable error: %s', err);
      destroy();
      emitIfSwallowed(this, err);
    }

    function onWritableFinish() {
      d('Proxy writable finished.');
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
  constructor(packetDecoder, handshakeType) {
    super({readableObjectMode: true});
    this._connected = false;
    this._packetDecoder = packetDecoder;
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
    const decoder = this._packetDecoder;
    const handshakeType = this._handshakeType;
    let handshake, packet;
    try {
      decode(!this._connected);
    } catch (err) {
      d('Failed to decode with%s handshake, retrying...',
        this._connected ? 'out' : '');
      decode(this._connected);
    }
    if (!handshake && !this._connected) {
      d('Switching to connected mode.');
      this._connected = true;
    }
    d('Decoded packet %s', id);
    return {handshake, packet};

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
      packet = decoder(id, body);
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
    headBuf.writeInt32BE(obj.packet.id, 0);
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

module.exports = {
  NettyProxy,
  netty,
};
