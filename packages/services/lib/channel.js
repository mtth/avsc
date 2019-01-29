/* jshint esversion: 6, node: true */

'use strict';

const {SystemError} = require('./utils');

const debug = require('debug');
const {EventEmitter} = require('events');
const {DateTime, Duration} = require('luxon');

const d = debug('@avro/services:channel');

/**
 * An RPC context thread.
 *
 * Traces are used to control the timing of RPCs, in particular to set
 * deadlines and cancel them early.
 */
class Trace {
  constructor(timeout, parent) {
    if (typeof parent == 'undefined' && Trace.isTrace(timeout)) {
      parent = timeout;
      timeout = undefined;
    }

    this.labels = parent ? Object.create(parent.labels, {}) : {};
    this.deactivatedBy = parent ? parent.deactivatedBy : null;
    this.deadline = deadlineFromTimeout(timeout);

    this._children = [];
    this._fns = new Map();
    this._timer = null;

    if (parent) {
      parent._children.push(this);
      if (parent.deadline) {
        this.deadline = this.deadline ?
           DateTime.min(this.deadline, parent.deadline) :
           parent.deadline;
      }
    }

    if (!this.deactivatedBy && this.deadline) {
      const remainingTimeout = +this.deadline.diffNow();
      if (remainingTimeout <= 0) {
        this._deadlineExceeded();
        return;
      }
      this._timer = setTimeout(() => {
        this._deadlineExceeded();
      }, remainingTimeout);
    }
    Object.seal(this);
  }

  get remainingDuration() {
    return this.deadline ? this.deadline.diffNow() : null;
  }

  get active() {
    return !this.deactivatedBy;
  }

  onceInactive(fn) {
    if (this.deactivatedBy) {
      fn.call(this, this.deactivatedBy);
      return () => false;
    }
    const id = randomId();
    this._fns.set(id, fn);
    return () => this._fns.delete(id);
  }

  _deactivate(err) { // Logic shared by timed and manual cancellations.
    this.deactivatedBy = err;
    for (const fn of this._fns.values()) {
      fn.call(this, err);
    }
    this._fns.clear();
    while (this._children.length) {
      this._children.pop()._deactivate(err);
    }
  }

  expire(cause) {
    if (this.deactivatedBy) {
      return;
    }
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
    this._deactivate(new SystemError('ERR_AVRO_EXPIRED', cause));
  }

  _deadlineExceeded() {
    this._deactivate(new SystemError('ERR_AVRO_DEADLINE_EXCEEDED'));
  }

  static isTrace(any) {
    return !!(any && any._isTrace);
  }

  get _isTrace() {
    return true;
  }
}

/** Parse various timeout formats into a `DateTime` deadline. */
function deadlineFromTimeout(timeout) {
  if (timeout === undefined || timeout === null) {
    return null;
  }
  if (DateTime.isDateTime(timeout)) {
    return timeout;
  }
  if (typeof timeout == 'number') {
    timeout = Duration.fromMillis(timeout);
  }
  if (Duration.isDuration(timeout)) {
    return DateTime.local().plus(timeout);
  }
  throw new Error(`bad timeout: ${timeout}`);
}

/**
 * A serialized message, sent and received via channels.
 *
 * Each packet's ID should be unique.
 */
class Packet {
  constructor(id, svc, body, headers) {
    this.id = id;
    this.service = svc;
    this.body = body;
    this.headers = headers || {};
  }

  static ping(svc, headers) {
    return new Packet(-randomId(), svc, Buffer.from([0, 0]), headers);
  }
}

/** A communication mechanism. */
class Channel extends EventEmitter {
  constructor(handler) {
    super();
    this._handler = handler;
  }

  /** If the trace is inactive, call will _not_ respond. */
  call(trace, preq, cb) {
    if (!trace.active) {
      d('Dropping packet request %s (inactive trace).', preq.id);
      return;
    }
    this.emit('requestPacket', preq, trace);
    this._handler(trace, preq, (err, pres) => {
      if (!trace.active) {
        d('Dropping packet response %s (inactive trace).', preq.id);
        return;
      }
      this.emit('responsePacket', pres, trace);
      cb(err, pres);
    });
  }

  /** Similar to call, if the trace is inactive, ping will _not_ respond. */
  ping(trace, svc, headers, cb) {
    if (!cb && typeof headers == 'function') {
      cb = headers;
      headers = undefined;
    }
    this.call(trace, Packet.ping(svc, headers), (err, pres) => {
      if (err) {
        cb(err);
        return;
      }
      cb(null, pres.service, pres.headers);
    });
  }

  get _isChannel() {
    return true;
  }

  static isChannel(any) {
    return !!(any && any._isChannel);
  }
}

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
  Channel,
  Packet,
  Trace,
  randomId,
};
