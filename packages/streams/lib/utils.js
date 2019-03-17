/* jshint node: true */

'use strict';

/** Various utilities used across this library. */

/**
 * Create a new empty buffer.
 *
 * @param size {Number} The buffer's size.
 */
function newBuffer(size) {
  if (typeof Buffer.alloc == 'function') {
    return Buffer.alloc(size);
  } else {
    return new Buffer(size);
  }
}

/**
 * Create a new buffer with the input contents.
 *
 * @param data {Array|String} The buffer's data.
 * @param enc {String} Encoding, used if data is a string.
 */
function bufferFrom(data, enc) {
  if (typeof Buffer.from == 'function') {
    return Buffer.from(data, enc);
  } else {
    return new Buffer(data, enc);
  }
}

/**
 * Ordered queue which returns items consecutively.
 *
 * This is actually a heap by index, with the added requirements that elements
 * can only be retrieved consecutively.
 */
function OrderedQueue() {
  this._index = 0;
  this._items = [];
}

OrderedQueue.prototype.push = function (item) {
  var items = this._items;
  var i = items.length | 0;
  var j;
  items.push(item);
  while (i > 0 && items[i].index < items[j = ((i - 1) >> 1)].index) {
    item = items[i];
    items[i] = items[j];
    items[j] = item;
    i = j;
  }
};

OrderedQueue.prototype.pop = function () {
  var items = this._items;
  var len = (items.length - 1) | 0;
  var first = items[0];
  if (!first || first.index > this._index) {
    return null;
  }
  this._index++;
  if (!len) {
    items.pop();
    return first;
  }
  items[0] = items.pop();
  var mid = len >> 1;
  var i = 0;
  var i1, i2, j, item, c, c1, c2;
  while (i < mid) {
    item = items[i];
    i1 = (i << 1) + 1;
    i2 = (i + 1) << 1;
    c1 = items[i1];
    c2 = items[i2];
    if (!c2 || c1.index <= c2.index) {
      c = c1;
      j = i1;
    } else {
      c = c2;
      j = i2;
    }
    if (c.index >= item.index) {
      break;
    }
    items[j] = item;
    items[i] = c;
    i = j;
  }
  return first;
};

/**
 * Copy properties from one object to another.
 *
 * @param src {Object} The source object.
 * @param dst {Object} The destination object.
 * @param overwrite {Boolean} Whether to overwrite existing destination
 * properties. Defaults to false.
 */
function copyOwnProperties(src, dst, overwrite) {
  var names = Object.getOwnPropertyNames(src);
  var i, l, name;
  for (i = 0, l = names.length; i < l; i++) {
    name = names[i];
    if (!dst.hasOwnProperty(name) || overwrite) {
      var descriptor = Object.getOwnPropertyDescriptor(src, name);
      Object.defineProperty(dst, name, descriptor);
    }
  }
  return dst;
}

module.exports = {
  bufferFrom: bufferFrom,
  newBuffer: newBuffer,
  OrderedQueue: OrderedQueue,
  copyOwnProperties: copyOwnProperties
};
