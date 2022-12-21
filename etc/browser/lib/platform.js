let md5 = require('./md5');

/**
 * Compute a string's hash.
 *
 * @param str {String} The string to hash.
 * @param algorithm {String} The algorithm used. Defaults to MD5.
 */
function getHash(str, algorithm) {
  algorithm = algorithm || 'md5';
  if (algorithm !== 'md5') {
    throw new Error('only md5 is supported in the browser');
  }
  return md5.md5(str);
}

/**
 * Deprecate a function. Browser stub; doesn't do anything when the deprecated
 * function is called.
 * @param {Function} fn The function to deprecate.
 * @returns That same function
 */
function deprecate (fn) {
  return fn;
}

/**
 * Browser stub for debuglog(). Never does any logging.
 * @returns A function that can be called with log messages. Does nothing with
 * those messages.
 */
function debuglog () {
  return () => {};
}

module.exports = {
  getHash,
  deprecate,
  debuglog
};
