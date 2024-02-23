let crypto = require('crypto');
let util = require('util');

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
  return hash.read();
}

module.exports = {
  getHash,
  debuglog: util.debuglog
};
