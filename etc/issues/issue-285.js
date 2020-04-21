/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');

const longType = avro.types.LongType.__with({
  fromBuffer: (buf) => buf.readBigInt64LE(),
  toBuffer: (n) => {
    const buf = Buffer.alloc(8);
    buf.writeBigInt64LE(n);
    return buf;
  },
  fromJSON: BigInt,
  toJSON: Number,
  isValid: (n) => typeof n == 'bigint',
  compare: (n1, n2) => { return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1); }
});

const t1 = avro.Type.forSchema('long');
const t2 = avro.Type.forSchema('long', {registry: {'long': longType}});

function toBig(n) {
  const b = t1.toBuffer(n);
  return t2.fromBuffer(b);
}

console.log(toBig(123));
console.log(1n << 33n);
console.log(toBig(Math.pow(2, 33)));

// Unsigned Int 64 value
const value = 2n ** (63n) - 1n;

const encoded = t2.toBuffer(value);
// RangeError [ERR_OUT_OF_RANGE]: The value of "value" is out of range. It must be >= -2147483648 and <= 2147483647. Received 4294967295

const decoded = t2.fromBuffer(encoded);

console.log(value); // valid bigint value 18446744073709551615n
console.log(decoded);

// Signed Int 64 value
const negativeValue = value * -1n ;

const encoded2 = t2.toBuffer(value);
const decoded2 = t2.fromBuffer(encoded2);

const encoded3 = t2.toBuffer(negativeValue);
const decoded3 = t2.fromBuffer(encoded3);

console.log(value); // 9223372036854775807n
console.log(decoded2);

console.log(negativeValue); // -9223372036854775807n
console.log(decoded3);

