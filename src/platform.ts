import { createHash } from 'crypto';
import { debuglog } from 'util'

/**
 * Compute a string's hash.
 *
 * @param str The string to hash.
 * @param algorithm The algorithm used.
 */
function getHash(str: string, algorithm: string = 'md5') {
  const hash = createHash(algorithm)
  hash.end(str);
  const buf = hash.read();
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.length);
}

export = {
  getHash,
  debuglog,
};
