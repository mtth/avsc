'use strict';

import * as utils from '../src/utils.js';

test('capitalize', () => {
  expect(utils.capitalize('abc')).toEqual('Abc');
  expect(utils.capitalize('')).toEqual('');
  expect(utils.capitalize('aBc')).toEqual('ABc');
});

test('hasDuplicates', () => {
  expect(utils.hasDuplicates([1, 3, 1])).toBe(true);
  expect(utils.hasDuplicates([])).toBe(false);
  expect(utils.hasDuplicates(['ab', 'cb'])).toBe(false);
  expect(utils.hasDuplicates(['toString'])).toBe(false);
  expect(utils.hasDuplicates(['ab', 'cb'], (s) => s[1])).toBe(true);
});

test('copy own properties', () => {
  class Obj {
    a = 1;
    b = 2;
    static c = 2;
  }
  const obj1 = new Obj();

  const obj2 = {b: 3};
  utils.copyOwnProperties(obj1, obj2);
  expect(obj2).toEqual({a: 1, b: 3});

  const obj3 = {b: 3};
  utils.copyOwnProperties(obj1, obj3, true);
  expect(obj3).toEqual({a: 1, b: 2});
});

describe('Tap', () => {
  const Tap = utils.Tap;

  suite('int & long', () => {
    testWriterReader({
      elems: [0, -1, 109213, -1211, -1312411211, 900719925474090],
      reader() {
        return this.readLong();
      },
      skipper() {
        this.skipLong();
      },
      writer(n) {
        this.writeLong(n);
      },
    });

    test('write', () => {
      const tap = Tap.withCapacity(6);
      tap.writeLong(1440756011948);
      const buf = hexArray('d8ce80bcee53');
      expect(tap.isValid()).toBe(true);
      expect(buf).toEqual(tap.toBuffer());
    });

    test('read', () => {
      const buf = hexArray('d8ce80bcee53');
      expect(Tap.fromBuffer(buf).readLong()).toEqual(1440756011948);
    });
  });

  suite('boolean', () => {
    testWriterReader({
      elems: [true, false],
      reader() {
        return this.readBoolean();
      },
      skipper() {
        this.skipBoolean();
      },
      writer(b) {
        this.writeBoolean(b);
      },
    });
  });

  suite('float', () => {
    testWriterReader({
      elems: [1, 3, 1, -5, 1e9],
      reader() {
        return this.readFloat();
      },
      skipper() {
        this.skipFloat();
      },
      writer(b) {
        this.writeFloat(b);
      },
    });
  });

  suite('double', () => {
    testWriterReader({
      elems: [1, 3, 1, -5, 1e12],
      reader() {
        return this.readDouble();
      },
      skipper() {
        this.skipDouble();
      },
      writer(b) {
        this.writeDouble(b);
      },
    });
  });

  suite('string', () => {
    testWriterReader({
      elems: [
        'ahierw',
        '',
        'alh hewlii! rew',
        'sérialisation',
        'this string should be long enough that a different code path is exercised',
      ],
      reader() {
        return this.readString();
      },
      skipper() {
        this.skipString();
      },
      writer(s) {
        this.writeString(s);
      },
    });
  });

  suite('bytes', () => {
    testWriterReader({
      elems: [
        Uint8Array.from('abc'),
        new Uint8Array(0),
        new Uint8Array([1, 5, 255]),
      ],
      reader() {
        return this.readBytes();
      },
      skipper() {
        this.skipBytes();
      },
      writer(b) {
        this.writeBytes(b);
      },
    });
  });

  suite('fixed', () => {
    testWriterReader({
      elems: [new Uint8Array([1, 5, 255])],
      reader() {
        return this.readFixed(3);
      },
      skipper() {
        this.skipFixed(3);
      },
      writer(b) {
        this.writeFixed(b, 3);
      },
    });
  });

  suite('pack & unpack longs', () => {
    test('unpack single byte', () => {
      const t = Tap.withCapacity(10);
      t.writeLong(5);
      t.pos = 0;
      expect(t.unpackLongBytes()).toEqual(
        new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0])
      );
      t.pos = 0;
      t.writeLong(-5);
      t.pos = 0;
      expect(t.unpackLongBytes()).toEqual(
        new Uint8Array([-5, -1, -1, -1, -1, -1, -1, -1])
      );
      t.pos = 0;
    });

    test('unpack multiple bytes', () => {
      const t = Tap.withCapacity(10);
      let l, unpacked, dv;
      l = 18932;
      t.writeLong(l);
      t.pos = 0;
      unpacked = t.unpackLongBytes();
      dv = new DataView(
        unpacked.buffer,
        unpacked.byteOffset,
        unpacked.byteLength
      );
      expect(dv.getInt32(0, true)).toEqual(l);
      t.pos = 0;
      l = -3210984;
      t.writeLong(l);
      t.pos = 0;
      unpacked = t.unpackLongBytes();
      dv = new DataView(
        unpacked.buffer,
        unpacked.byteOffset,
        unpacked.byteLength
      );
      expect(dv.getInt32(0, true)).toEqual(l);
    });

    test('pack single byte', () => {
      const t = Tap.withCapacity(10);
      const b = Buffer.alloc(8);
      b.fill(0);
      b.writeInt32LE(12);
      t.packLongBytes(b);
      expect(t.pos).toEqual(1);
      t.pos = 0;
      expect(t.readLong()).toEqual(12);
      t.pos = 0;
      b.writeInt32LE(-37);
      b.writeInt32LE(-1, 4);
      t.packLongBytes(b);
      expect(t.pos).toEqual(1);
      t.pos = 0;
      expect(t.readLong()).toEqual(-37);
      t.pos = 0;
      b.writeInt32LE(-1);
      b.writeInt32LE(-1, 4);
      t.packLongBytes(b);
      expect(t.subarray(0, t.pos)).toEqual(new Uint8Array([1]));
      t.pos = 0;
      expect(t.readLong()).toEqual(-1);
    });

    test('roundtrip', () => {
      roundtrip(1231514);
      roundtrip(-123);
      roundtrip(124124);
      roundtrip(109283109271);
      roundtrip(Number.MAX_SAFE_INTEGER);
      roundtrip(Number.MIN_SAFE_INTEGER);
      roundtrip(0);
      roundtrip(-1);

      function roundtrip(n) {
        const t1 = Tap.withCapacity(10);
        const t2 = Tap.withCapacity(10);
        t1.writeLong(n);
        t1.pos = 0;
        t2.packLongBytes(t1.unpackLongBytes());
        expect(t2).toEqual(t1);
      }
    });

    test('roundtrip bytes', () => {
      roundtrip(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]));
      roundtrip(hexArray('9007199254740995'));

      function roundtrip(b1) {
        const t = Tap.withCapacity(10);
        t.packLongBytes(b1);
        t.pos = 0;
        const b2 = t.unpackLongBytes();
        expect(b2).toEqual(b1);
      }
    });
  });

  function testWriterReader(opts) {
    const size = opts.size;
    const elems = opts.elems;
    const writeFn = opts.writer;
    const readFn = opts.reader;
    const skipFn = opts.skipper;
    const name = opts.name || '';

    test('write read ' + name, () => {
      const tap = Tap.withCapacity(size || 1024);
      for (let i = 0, l = elems.length; i < l; i++) {
        tap.arr.fill(0);
        tap.pos = 0;
        const elem = elems[i];
        writeFn.call(tap, elem);
        tap.pos = 0;
        expect(readFn.call(tap)).toEqual(elem);
      }
    });

    test('read over ' + name, () => {
      const tap = Tap.withCapacity(0);
      readFn.call(tap); // Shouldn't throw.
      expect(tap.isValid()).toBe(false);
    });

    test('write over ' + name, () => {
      const tap = Tap.withCapacity(0);
      writeFn.call(tap, elems[0]); // Shouldn't throw.
      expect(tap.isValid()).toBe(false);
    });

    test('skip ' + name, () => {
      const tap = Tap.withCapacity(size || 1024);
      for (let i = 0, l = elems.length; i < l; i++) {
        tap.arr.fill(0);
        tap.pos = 0;
        const elem = elems[i];
        writeFn.call(tap, elem);
        const pos = tap.pos;
        tap.pos = 0;
        skipFn.call(tap, elem);
        expect(tap.pos).toEqual(pos);
      }
    });
  }
});

function hexArray(s: string): Uint8Array {
  return Uint8Array.from(Buffer.from(s, 'hex'));
}
