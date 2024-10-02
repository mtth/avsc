let crypto = require('crypto');
let util = typeof process !== "undefined" ? require('util') : { debuglog: console.log };

/**
 * Compute a string's hash.
 *
 * @param str {String} The string to hash.
 * @param algorithm {String} The algorithm used. Defaults to MD5.
 */
function getHash(str, algorithm) {
  algorithm = algorithm || 'md5';
  let hash = crypto.createHash(algorithm);
  hash.end(str);
  let buf = hash.read();
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.length);
}

module.exports = {
  getHash,
  debuglog: util.debuglog
};
