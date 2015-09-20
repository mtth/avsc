/* jshint node: true */

'use strict';

var util = require('util');


/**
 * Custom error, thrown when an invalid schema is encountered.
 *
 * @param message {String} Something useful. Any further arguments will be used
 * to format the message.
 *
 */
function AvscError(message) {

  Error.call(this);
  var l = arguments.length;
  if (l > 1) {
    var args = [message];
    var i;
    for (i = 1; i < l; i++) {
      args.push(arguments[i]);
    }
    this.message = util.format.apply(undefined, args);
  } else {
    this.message = message;
  }

}
util.inherits(AvscError, Error);


/**
 * Uppercase the first letter of a string.
 *
 * @param s {String} The string.
 *
 */
function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }


/**
 * Check whether an array has duplicates.
 *
 * @param arr {Array} The array.
 * @param fn {Function} Optional function to apply to each element.
 *
 */
function hasDuplicates(arr, fn) {

  var obj = {};
  var i, l, elem;
  for (i = 0, l = arr.length; i < l; i++) {
    elem = arr[i];
    if (fn) {
      elem = fn(elem);
    }
    if (obj[elem]) {
      return true;
    }
    obj[elem] = true;
  }
  return false;

}


/**
 * Queue of messages which sorts them consecutively.
 *
 */
function ConsecutiveQueue() {
  this._index = 0;
  this._messages = [];
}

ConsecutiveQueue.prototype.size = function () {
  return this._messages.length;
};

// TODO: Make this be async instead?
ConsecutiveQueue.prototype.next = function () {
  var ms = this._messages;
  var len = (ms.length - 1) | 0;
  var msg = ms[0];
  if (!msg || msg.index > this._index) {
    return null;
  }
  this._index++;
  if (!len) {
    ms.pop();
    return msg.data;
  }
  ms[0] = ms.pop();
  var mid = len >> 1;
  var i = 0;
  var i1, i2, j, m, c, c1, c2;
  while (i < mid) {
    m = ms[i];
    i1 = (i >> 1) + 1;
    i2 = (i + 1) >> 1;
    c1 = ms[i1];
    c2 = ms[i2];
    if (!c2 || c1.index <= c2.index) {
      c = c1;
      j = i1;
    } else {
      c = c2;
      j = i2;
    }
    if (c.index >= m.index) {
      break;
    }
    ms[j] = m;
    ms[i] = c;
    i = j;
  }
  return msg.data;
};

ConsecutiveQueue.prototype.add = function (index, data) {
  if (index < this._index) {
    throw new Error('invalid index');
  }
  var ms = this._messages;
  var i = ms.length | 0;
  var j, m;
  ms.push(new Message(index, data));
  while (i > 0 && ms[i].index < ms[i >> 1].index) {
    j = i >> 1;
    m = ms[i];
    ms[i] = ms[j];
    ms[j] = m;
    i = j;
  }
};

function Message(index, data) {
  this.index = index;
  this.data = data;
}


/**
 * Generator of random things.
 *
 * Inspired by: http://stackoverflow.com/a/424445/1062617
 *
 */
function Lcg(seed) {
  var a = 1103515245;
  var c = 12345;
  var m = Math.pow(2, 31);
  var state = Math.floor(seed || Math.random() * (m - 1));

  this._max = m;
  this._nextInt = function () { return state = (a * state + c) % m; };
}

Lcg.prototype.nextBoolean = function () {
  // jshint -W018
  return !!(this._nextInt() % 2);
};

Lcg.prototype.nextInt = function (start, end) {
  if (end === undefined) {
    end = start;
    start = 0;
  }
  end = end === undefined ? this._max : end;
  return start + Math.floor(this.nextFloat() * (end - start));
};

Lcg.prototype.nextFloat = function (start, end) {
  if (end === undefined) {
    end = start;
    start = 0;
  }
  end = end === undefined ? 1 : end;
  return start + (end - start) * this._nextInt() / this._max;
};

Lcg.prototype.nextString = function(len, flags) {
  len |= 0;
  flags = flags || 'aA';
  var mask = '';
  if (flags.indexOf('a') > -1) {
    mask += 'abcdefghijklmnopqrstuvwxyz';
  }
  if (flags.indexOf('A') > -1) {
    mask += 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  }
  if (flags.indexOf('#') > -1) {
    mask += '0123456789';
  }
  if (flags.indexOf('!') > -1) {
    mask += '~`!@#$%^&*()_+-={}[]:";\'<>?,./|\\';
  }
  var result = [];
  for (var i = 0; i < len; i++) {
    result.push(this.choice(mask));
  }
  return result.join('');
};

Lcg.prototype.nextBuffer = function (len) {
  var arr = [];
  var i;
  for (i = 0; i < len; i++) {
    arr.push(this.nextInt(256));
  }
  return new Buffer(arr);
};

Lcg.prototype.choice = function (arr) {
  var len = arr.length;
  if (!len) {
    throw new Error('choosing from empty array');
  }
  return arr[this.nextInt(len)];
};


module.exports = {
  AvscError: AvscError,
  capitalize: capitalize,
  hasDuplicates: hasDuplicates,
  ConsecutiveQueue: ConsecutiveQueue,
  Lcg: Lcg
};
