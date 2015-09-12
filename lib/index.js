'use strict';

function OffsetBuffer(buf, offset) {

  this.buf = buf;
  this.offset = offset | 0;

}

function readLong(obuf) {

  var b = 0;
  var n = 0;
  var k = 0;
  var buf = obuf.buf;
  do {
    b = buf[obuf.offset++];
    n |= (b & 0x7f) << k;
    k += 7
  } while (b & 0x80);
  return (n >> 1) ^ -(n & 1);

};

function writeLong(n, obuf) {

  n = n >= 0 ? n << 1 : (~n << 1) | 1;
  var buf = obuf.buf;
  do {
    buf[obuf.offset] = n & 0x7f;
    n >>= 7;
  } while (n && (buf[obuf.offset++] |= 0x80));
  obuf.offset++;

};

module.exports = {
  OffsetBuffer: OffsetBuffer,
  readLong: readLong,
  writeLong: writeLong
};
