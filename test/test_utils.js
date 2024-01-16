'use strict';

let utils = require('../lib/utils'),
    assert = require('assert'),
    buffer = require('buffer');

let Buffer = buffer.Buffer;


suite('utils', () => {

  test('capitalize', () => {
    assert.equal(utils.capitalize('abc'), 'Abc');
    assert.equal(utils.capitalize(''), '');
    assert.equal(utils.capitalize('aBc'), 'ABc');
  });

  test('hasDuplicates', () => {
    assert(utils.hasDuplicates([1, 3, 1]));
    assert(!utils.hasDuplicates([]));
    assert(!utils.hasDuplicates(['ab', 'cb']));
    assert(!utils.hasDuplicates(['toString']));
    assert(utils.hasDuplicates(['ab', 'cb'], (s) => { return s[1]; }));
  });

  test('single index of', () => {
    assert.equal(utils.singleIndexOf(null, 1), -1);
    assert.equal(utils.singleIndexOf([2], 2), 0);
    assert.equal(utils.singleIndexOf([3, 3], 3), -2);
    assert.equal(utils.singleIndexOf([2, 4], 4), 1);
  });

  test('abstract function', () => {
    assert.throws(utils.abstractFunction, utils.Error);
  });

  test('copy own properties', () => {
    function Obj() { this.a = 1; this.b = 2; }
    Obj.prototype.c = 2;
    let obj1 = new Obj();

    let obj2 = {b: 3};
    utils.copyOwnProperties(obj1, obj2);
    assert.deepEqual(obj2, {a: 1, b: 3});

    let obj3 = {b: 3};
    utils.copyOwnProperties(obj1, obj3, true);
    assert.deepEqual(obj3, {a: 1, b: 2});
  });

  test('jsonEnd', () => {
    assert.equal(utils.jsonEnd(''), -1);
    assert.equal(utils.jsonEnd('{}a'), 2);
    assert.equal(utils.jsonEnd('{a'), -1);
    assert.equal(utils.jsonEnd('123'), 3);
    assert.equal(utils.jsonEnd('[1,2]'), 5);
    assert.equal(utils.jsonEnd('true'), 4);
    assert.equal(utils.jsonEnd('null'), 4);
    assert.equal(utils.jsonEnd('falseish'), 5);
    assert.equal(utils.jsonEnd('"false"'), 7);
  });

  test('OrderedQueue', () => {

    let seqs = [
      [0],
      [0,1],
      [0,1,2],
      [2,1,0],
      [0,2,1,3],
      [1,3,2,4,0],
      [0,1,2,3]
    ];

    for (let i = 0; i < seqs.length; i++) {
      check(seqs[i]);
    }

    function check(seq) {
      let q = new utils.OrderedQueue();
      assert.strictEqual(q.pop(), null);
      for (let i = 0; i < seq.length; i++) {
        q.push({index: seq[i]});
      }
      for (let i = 0; i < seq.length; i++) {
        let j = q.pop();
        assert.equal(j !== null && j.index, i, seq.join());
      }
    }

  });

  suite('Lcg', () => {

    test('seed', () => {
      let r1 = new utils.Lcg(48);
      let r2 = new utils.Lcg(48);
      assert.equal(r1.nextInt(), r2.nextInt());
    });

    test('integer', () => {
      let r = new utils.Lcg(48);
      let i;
      i = r.nextInt();
      assert(i >= 0 && i === (i | 0));
      i = r.nextInt(1);
      assert.equal(i, 0);
      i = r.nextInt(1, 2);
      assert.equal(i, 1);
    });

    test('float', () => {
      let r = new utils.Lcg(48);
      let f;
      f = r.nextFloat();
      assert(0 <= f && f < 1);
      f = r.nextFloat(0);
      assert.equal(f, 0);
      f = r.nextFloat(1, 1);
      assert.equal(f, 1);
    });

    test('boolean', () => {
      let r = new utils.Lcg(48);
      assert(typeof r.nextBoolean() == 'boolean');
    });

    test('choice', () => {
      let r = new utils.Lcg(48);
      let arr = ['a'];
      assert(r.choice(arr), 'a');
      assert.throws(() => { r.choice([]); });
    });

    test('string', () => {
      let r = new utils.Lcg(48);
      let s;
      s = r.nextString(10, 'aA#!');
      assert.equal(s.length, 10);
      s = r.nextString(5, '#!');
      assert.equal(s.length, 5);
    });

  });

  suite('Tap', () => {

    let BufferPool = utils.BufferPool;

    test('alloc negative length', () => {
      let pool = new BufferPool(16);
      assert.throws(() => { pool.alloc(-1); });
    });

    test('alloc beyond pool size', () => {
      let pool = new BufferPool(4);
      assert.equal(pool.alloc(3).length, 3);
      assert.equal(pool.alloc(2).length, 2);
    });

  });

  suite('Tap', () => {

    let Tap = utils.Tap;

    suite('int & long', () => {

      testWriterReader({
        elems: [0, -1, 109213, -1211, -1312411211, 900719925474090],
        reader: function () { return this.readLong(); },
        skipper: function () { this.skipLong(); },
        writer: function (n) { this.writeLong(n); }
      });

      test('write', () => {

        let tap = Tap.withCapacity(6);
        tap.writeLong(1440756011948);
        let buf = Buffer.from(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
        assert(tap.isValid());
        assert(buf.equals(tap.buf));

      });

      test('read', () => {

        let buf = Buffer.from(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
        assert.equal((Tap.fromBuffer(buf)).readLong(), 1440756011948);

      });

    });

    suite('boolean', () => {

      testWriterReader({
        elems: [true, false],
        reader: function () { return this.readBoolean(); },
        skipper: function () { this.skipBoolean(); },
        writer: function (b) { this.writeBoolean(b); }
      });

    });

    suite('float', () => {

      testWriterReader({
        elems: [1, 3,1, -5, 1e9],
        reader: function () { return this.readFloat(); },
        skipper: function () { this.skipFloat(); },
        writer: function (b) { this.writeFloat(b); }
      });

    });

    suite('double', () => {

      testWriterReader({
        elems: [1, 3,1, -5, 1e12],
        reader: function () { return this.readDouble(); },
        skipper: function () { this.skipDouble(); },
        writer: function (b) { this.writeDouble(b); }
      });

    });

    suite('string', () => {

      testWriterReader({
        elems: ['ahierw', '', 'alh hewlii! rew'],
        reader: function () { return this.readString(); },
        skipper: function () { this.skipString(); },
        writer: function (s) { this.writeString(s); }
      });

    });

    suite('bytes', () => {

      testWriterReader({
        elems: [Buffer.from('abc'), utils.newBuffer(0), Buffer.from([1, 5, 255])],
        reader: function () { return this.readBytes(); },
        skipper: function () { this.skipBytes(); },
        writer: function (b) { this.writeBytes(b); }
      });

    });

    suite('fixed', () => {

      testWriterReader({
        elems: [Buffer.from([1, 5, 255])],
        reader: function () { return this.readFixed(3); },
        skipper: function () { this.skipFixed(3); },
        writer: function (b) { this.writeFixed(b, 3); }
      });

    });

    suite('binary', () => {

      test('write valid', () => {
        let tap = Tap.withCapacity(3);
        let s = '\x01\x02';
        tap.writeBinary(s, 2);
        assert.deepEqual(tap.buf, Buffer.from([1,2,0]));
      });

      test('write invalid', () => {
        let tap = Tap.withCapacity(1);
        let s = '\x01\x02';
        tap.writeBinary(s, 2);
        assert.deepEqual(tap.buf, Buffer.from([0]));
      });

    });

    suite('pack & unpack longs', () => {

      test('unpack single byte', () => {
        let t = Tap.withCapacity(10);
        t.writeLong(5);
        t.pos = 0;
        assert.deepEqual(
          t.unpackLongBytes(),
          Buffer.from([5, 0, 0, 0, 0, 0, 0, 0])
        );
        t.pos = 0;
        t.writeLong(-5);
        t.pos = 0;
        assert.deepEqual(
          t.unpackLongBytes(),
          Buffer.from([-5, -1, -1, -1, -1, -1, -1, -1])
        );
        t.pos = 0;
      });

      test('unpack multiple bytes', () => {
        let t = Tap.withCapacity(10);
        let l;
        l = 18932;
        t.writeLong(l);
        t.pos = 0;
        assert.deepEqual(t.unpackLongBytes().readInt32LE(0), l);
        t.pos = 0;
        l = -3210984;
        t.writeLong(l);
        t.pos = 0;
        assert.deepEqual(t.unpackLongBytes().readInt32LE(0), l);
      });

      test('pack single byte', () => {
        let t = Tap.withCapacity(10);
        let b = utils.newBuffer(8);
        b.fill(0);
        b.writeInt32LE(12);
        t.packLongBytes(b);
        assert.equal(t.pos, 1);
        t.pos = 0;
        assert.deepEqual(t.readLong(), 12);
        t.pos = 0;
        b.writeInt32LE(-37);
        b.writeInt32LE(-1, 4);
        t.packLongBytes(b);
        assert.equal(t.pos, 1);
        t.pos = 0;
        assert.deepEqual(t.readLong(), -37);
        t.pos = 0;
        b.writeInt32LE(-1);
        b.writeInt32LE(-1, 4);
        t.packLongBytes(b);
        assert.deepEqual(t.buf.slice(0, t.pos), Buffer.from([1]));
        t.pos = 0;
        assert.deepEqual(t.readLong(), -1);
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
          let t1 = Tap.withCapacity(10);
          let t2 = Tap.withCapacity(10);
          t1.writeLong(n);
          t1.pos = 0;
          t2.packLongBytes(t1.unpackLongBytes());
          assert.deepEqual(t2, t1);
        }
      });

      test('roundtrip bytes', () => {
        roundtrip(Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]));
        roundtrip(Buffer.from('9007199254740995', 'hex'));

        function roundtrip(b1) {
          let t = Tap.withCapacity(10);
          t.packLongBytes(b1);
          t.pos = 0;
          let b2 = t.unpackLongBytes();
          assert.deepEqual(b2, b1);
        }
      });
    });

    function testWriterReader(opts) {
      let size = opts.size;
      let elems = opts.elems;
      let writeFn = opts.writer;
      let readFn = opts.reader;
      let skipFn = opts.skipper;
      let name = opts.name || '';

      test('write read ' + name, () => {
        let tap = Tap.withCapacity(size || 1024);
        for (let i = 0, l = elems.length; i < l; i++) {
          tap.buf.fill(0);
          tap.pos = 0;
          let elem = elems[i];
          writeFn.call(tap, elem);
          tap.pos = 0;
          assert.deepEqual(readFn.call(tap), elem);
        }
      });

      test('read over ' + name, () => {
        let tap = Tap.withCapacity(0);
        readFn.call(tap); // Shouldn't throw.
        assert(!tap.isValid());
      });

      test('write over ' + name, () => {
        let tap = Tap.withCapacity(0);
        writeFn.call(tap, elems[0]); // Shouldn't throw.
        assert(!tap.isValid());
      });

      test('skip ' + name, () => {
        let tap = Tap.withCapacity(size || 1024);
        for (let i = 0, l = elems.length; i < l; i++) {
          tap.buf.fill(0);
          tap.pos = 0;
          let elem = elems[i];
          writeFn.call(tap, elem);
          let pos = tap.pos;
          tap.pos = 0;
          skipFn.call(tap, elem);
          assert.equal(tap.pos, pos);
        }
      });
    }
  });

});
