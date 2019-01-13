/* jshint esversion: 6, node: true */

'use strict';

const {SystemError} = require('./types');

const {DateTime, Duration} = require('luxon');

class Context {
  constructor(timeout, parent) {
    if (typeof parent == 'undefined' && Context.isContext(timeout)) {
      parent = timeout;
      timeout = undefined;
    }

    this._data = parent ? Object.create(parent._data, {}) : {};
    this._cancelledWith = parent ? parent._cancelledWith : null;
    this._deadline = deadlineFromTimeout(timeout);
    this._children = [];
    this._fns = new Map();
    this._idGenerator = idGenerator();
    this._timer = null;

    if (parent) {
      parent._children.push(this);
      if (parent._deadline) {
        this._deadline = this._deadline ?
           DateTime.min(this._deadline, parent._deadline) :
           parent._deadline;
      }
    }

    if (!this._cancelledWith && this._deadline) {
      const remainingTimeout = +this._deadline.diffNow();
      if (remainingTimeout <= 0) {
        this._expire();
        return;
      }
      this._timer = setTimeout(() => { this._expire(); }, remainingTimeout);
    }
  }

  static isContext(any) {
    return !!(any && any._isAvroContext);
  }

  get _isAvroContext() {
    return true;
  }

  get data() {
    return this._data;
  }

  /** Always in local. */
  get deadline() {
    return this._deadline;
  }

  get remainingDuration() {
    return this._deadline ? this._deadline.diffNow() : null;
  }

  get cancelled() {
    return !!this._cancelledWith;
  }

  get cancelledWith() {
    return this._cancelledWith;
  }

  onCancel(fn) {
    debugger;
    if (this._cancelledWith) {
      fn.call(this, this._cancelledWith);
      return;
    }
    const id = this._idGenerator.next().value;
    this._fns.set(id, fn);
    return () => { this._fns.delete(id); };
  }

  _cancelWith(err) { // Logic shared by timed and manual cancellations.
    debugger;
    this._cancelledWith = err;
    for (const fn of this._fns.values()) {
      fn.call(this, err);
    }
    this._fns.clear();
    while (this._children.length) {
      this._children.pop()._cancelWith(err);
    }
  }

  interrupt(cause) {
    if (this._cancelledWith) {
      return;
    }
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
    return this._cancelWith(new SystemError('ERR_AVRO_INTERRUPTED', cause));
  }

  _expire() {
    this._cancelWith(new SystemError('ERR_AVRO_EXPIRED'));
  }
}

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

/** IDs start from 0 and are guaranteed non-negative. */
function* idGenerator() {
  const mask = -1 >>> 1;
  let id = 0;
  while (true) {
    yield id;
    id = (id + 1) & mask;
  }
}

module.exports = {
  Context,
  idGenerator,
};
