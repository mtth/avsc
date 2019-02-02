/* jshint esversion: 6, node: true */

'use strict';

/**
 * Copy properties from one object to another.
 *
 * @param src {Object} The source object.
 * @param dst {Object} The destination object.
 * @param overwrite {Boolean} Whether to overwrite existing destination
 * properties. Defaults to false.
 */
function copyOwnProperties(src, dst, overwrite) {
  if (!src) {
    return dst;
  }
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

/**
 * Returns offset in the string of the end of JSON object (-1 if past the end).
 *
 * To keep the implementation simple, this function isn't a JSON validator. It
 * will gladly return a result for invalid JSON (which is OK since that will be
 * promptly rejected by the JSON parser). What matters is that it is guaranteed
 * to return the correct end when presented with valid JSON.
 *
 * @param str {String} Input string containing serialized JSON..
 * @param pos {Number} Starting position.
 */
function jsonEnd(str, pos) {
  pos = pos | 0;

  // Handle the case of a simple literal separately.
  var c = str.charAt(pos++);
  if (/[\d-]/.test(c)) {
    while (/[eE\d.+-]/.test(str.charAt(pos))) {
      pos++;
    }
    return pos;
  } else if (/true|null/.test(str.slice(pos - 1, pos + 3))) {
    return pos + 3;
  } else if (/false/.test(str.slice(pos - 1, pos + 4))) {
    return pos + 4;
  }

  // String, object, or array.
  var depth = 0;
  var literal = false;
  do {
    switch (c) {
    case '{':
    case '[':
      if (!literal) { depth++; }
      break;
    case '}':
    case ']':
      if (!literal && !--depth) {
        return pos;
      }
      break;
    case '"':
      literal = !literal;
      if (!depth && !literal) {
        return pos;
      }
      break;
    case '\\':
      pos++; // Skip the next character.
    }
  } while ((c = str.charAt(pos++)));

  return -1;
}

module.exports = {
  copyOwnProperties: copyOwnProperties,
  jsonEnd: jsonEnd,
};
