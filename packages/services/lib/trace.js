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
  constructor(timeout, parent) {
    if (typeof parent == 'undefined' && Trace.isTrace(timeout)) {
      parent = timeout;
      timeout = undefined;
    }

    this.headers = parent ? Object.create(parent.headers, {}) : {};
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
      if (!parent || parent.deadline !== this.deadline) {
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

module.exports = {
  Trace,
};
