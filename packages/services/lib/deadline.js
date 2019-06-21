/* jshint esversion: 6, node: true */

'use strict';

const {SystemError, randomId} = require('./utils');

const debug = require('debug');
const {DateTime, Duration} = require('luxon');

const d = debug('@avro/services:deadline');

/**
 * An RPC context thread.
 *
 * Deadlines are used to control the timing of RPCs, in particular to set
 * deadlines and cancel them early.
 */
class Deadline {

  /** Returns a deadline with no timed expiration. */
  static infinite(parent) {
    return new this(null, parent);
  }

  /** Returns a deadline which expires at the given time. */
  static forDateTime(datetime, parent) {
    return new this(datetime, parent);
  }

  /** Returns a deadline which expires after the given duration. */
  static forDuration(duration, parent) {
    return this.forDateTime(DateTime.local().plus(duration), parent);
  }

  /** Returns a deadline which expires after a given number of milliseconds. */
  static forMillis(millis, parent) {
    return this.forDuration(Duration.fromMillis(millis), parent);
  }

  /**
   * Builds a new deadline.
   *
   * In general, prefer using the more explicit factory methods.
   */
  constructor(expiration, parent) {
    if (expiration !== null && !DateTime.isDateTime(expiration)) {
      throw new Error(`invalid expiration: ${expiration}`);
    }

    this.expiration = expiration;
    this._expiredBy = parent ? parent._expiredBy : undefined;
    this._expiration = null;
    this._children = [];
    this._fns = new Map();
    this._timer = null;

    if (parent) {
      parent._children.push(this);
      if (parent.expiration) {
        this.expiration = this.expiration ?
           DateTime.min(this.expiration, parent.expiration) :
           parent.expiration;
      }
    }

    if (!this.expired && this.expiration) {
      const remainingTimeout = +this.expiration.diffNow();
      if (remainingTimeout <= 0) {
        this.expire(deadlineExceededError());
        return;
      }
      if (!parent || this.expiration < parent.expiration) {
        this._timer = setTimeout(() => {
          this.expire(deadlineExceededError());
        }, remainingTimeout);
      }
    }
  }

  get remainingDuration() {
    return this.expiration ? this.expiration.diffNow() : null;
  }

  get expired() {
    return this._expiredBy !== undefined;
  }

  get expiredBy() {
    return this._expiredBy;
  }

  /**
   * Run a callback when the deadline expires.
   *
   * Note that the callback might never be run if the deadline doesn't have a
   * deadline and is never manually expired. If the deadline has already expired,
   * the callback will be run before `whenExpired` returns.
   *
   * `whenExpired` returns a cleanup function which will unregister the
   * callback. It should be called as soon as possible to avoid memory leaks.
   * This cleanup function returns `true` iff the callback has not already been
   * (fully) run.
   */
  whenExpired(fn) {
    if (this.expired) {
      fn.call(this, this._expiredBy);
      return () => false;
    }
    const id = randomId();
    this._fns.set(id, fn);
    return () => this._fns.delete(id);
  }

  /**
   * Manually expire a deadline.
   *
   * If the deadline has already expired, this method has no effect and will
   * return `false`. Otherwise `expire` will return `true` and the input
   * error--if any--will be forwarded to all expiration callbacks (added via
   * `whenExpired`) and available via `deadline.expiredBy`.
   */
  expire(err) {
    if (this.expired) {
      return false;
    }
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
    this._expiredBy = err || null;
    for (const [id, fn] of this._fns) {
      fn.call(this, err);
      this._fns.delete(id);
    }
    while (this._children.length) {
      this._children.pop().expire(err);
    }
    return true;
  }

  /**
   * Race a callback against the deadline's expiration.
   *
   * Available options:
   *
   * + `expire`, to expire the deadline when the returned function is first
   *   called.
   *
   * If the deadline has already expired, `fn` will be run on the process' next
   * tick.
   */
  wrap(opts, fn) {
    if (!fn && typeof opts == 'function') {
      fn = opts;
      opts = undefined;
    }
    if (this.expired) {
      process.nextTick(fn, this.expiredBy);
      return;
    }
    const self = this;
    const cleanup = this.whenExpired(done);
    return done;

    function done(err, ...args) {
      if (cleanup()) { // First time we are running this callback.
        if (opts && opts.expire) {
          self.expire(err);
        }
        fn.call(this, err, ...args); // jshint ignore:line
      } else if (err) {
        d('Orphaned error (deadline headers: %j).', self.headers, err);
      }
    }
  }

  /**
   * Unreferences the timer for this deadline, if any.
   *
   * This allows the node process to exit even before the timer ticks. For
   * performance reasons, avoid having too many unrefed deadlines.
   */
  unref() {
    if (this._timer) {
      this._timer.unref();
    }
    return this;
  }

  static isDeadline(any) {
    return !!(any && any._isAvroServicesDeadline);
  }

  get _isAvroServicesDeadline() {
    return true;
  }
}

function deadlineExceededError() {
  return new SystemError('ERR_DEADLINE_EXCEEDED');
}

module.exports = {
  Deadline,
};
