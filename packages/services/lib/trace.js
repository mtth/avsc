/* jshint esversion: 6, node: true */

'use strict';

const {SystemError, randomId} = require('./utils');

const backoff = require('backoff');
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
  /**
   * Build a new trace.
   *
   * Options:
   *
   * + free, the trace will not expire with its parent (but will still share
   *   its headers).
   * + headers, initial header values.
   */
  constructor(timeout, parent, opts) {
    this.deadline = deadlineFromTimeout(timeout);
    if (this.deadline === undefined) {
      opts = parent;
      parent = timeout;
      this.deadline = null;
    }
    if (!Trace.isTrace(parent)) {
      opts = parent;
      parent = undefined;
    }
    opts = opts || {};

    this.headers = parent ? Object.create(parent.headers) : {};
    Object.assign(this.headers, opts.headers);
    this.deactivatedBy = parent ? parent.deactivatedBy : null;
    this._children = [];
    this._fns = new Map();
    this._timer = null;

    if (parent) {
      if (!opts.free) {
        parent._children.push(this);
      }
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
      if (!parent || this.deadline < parent.deadline) {
        this._timer = setTimeout(() => {
          this._deadlineExceeded();
        }, remainingTimeout);
      }
    }
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
    const wasActive = this.active;
    if (this.deactivatedBy) {
      return;
    }
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
    this._deactivate(new SystemError('ERR_AVRO_EXPIRED', cause));
    return wasActive;
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
  return undefined;
}

module.exports = {
  Trace,
};
