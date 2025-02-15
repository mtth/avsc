'use strict';

const utils = require('../lib/utils'),
  assert = require('assert'),
  buffer = require('buffer');

const Buffer = buffer.Buffer;

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
    assert(
      utils.hasDuplicates(['ab', 'cb'], (s) => {
        return s[1];
      })
    );
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
    function Obj() {
      this.a = 1;
      this.b = 2;
    }
    Obj.prototype.c = 2;
    const obj1 = new Obj();

    const obj2 = {b: 3};
    utils.copyOwnProperties(obj1, obj2);
    assert.deepEqual(obj2, {a: 1, b: 3});

    const obj3 = {b: 3};
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
    const seqs = [
      [0],
      [0, 1],
      [0, 1, 2],
      [2, 1, 0],
      [0, 2, 1, 3],
      [1, 3, 2, 4, 0],
      [0, 1, 2, 3],
    ];

    for (let i = 0; i < seqs.length; i++) {
      check(seqs[i]);
    }

    function check(seq) {
      const q = new utils.OrderedQueue();
      assert.strictEqual(q.pop(), null);
      for (let i = 0; i < seq.length; i++) {
        q.push({index: seq[i]});
      }
      for (let i = 0; i < seq.length; i++) {
        const j = q.pop();
        assert.equal(j !== null && j.index, i, seq.join());
      }
    }
  });

  suite('Lcg', () => {
    test('seed', () => {
      const r1 = new utils.Lcg(48);
      const r2 = new utils.Lcg(48);
      assert.equal(r1.nextInt(), r2.nextInt());
    });

    test('integer', () => {
      const r = new utils.Lcg(48);
      let i;
      i = r.nextInt();
      assert(i >= 0 && i === (i | 0));
      i = r.nextInt(1);
      assert.equal(i, 0);
      i = r.nextInt(1, 2);
      assert.equal(i, 1);
    });

    test('float', () => {
      const r = new utils.Lcg(48);
      let f;
      f = r.nextFloat();
      assert(0 <= f && f < 1);
      f = r.nextFloat(0);
      assert.equal(f, 0);
      f = r.nextFloat(1, 1);
      assert.equal(f, 1);
    });

    test('boolean', () => {
      const r = new utils.Lcg(48);
      assert(typeof r.nextBoolean() == 'boolean');
    });

    test('choice', () => {
      const r = new utils.Lcg(48);
      const arr = ['a'];
      assert(r.choice(arr), 'a');
      assert.throws(() => {
        r.choice([]);
      });
    });

    test('string', () => {
      const r = new utils.Lcg(48);
      let s;
      s = r.nextString(10, 'aA#!');
      assert.equal(s.length, 10);
      s = r.nextString(5, '#!');
      assert.equal(s.length, 5);
    });
  });

  suite('Tap', () => {
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
        const buf = Buffer.from([
          '0xd8',
          '0xce',
          '0x80',
          '0xbc',
          '0xee',
          '0x53',
        ]);
        assert(tap.isValid());
        assert(buf.equals(tap.toBuffer()));
      });

      test('read', () => {
        const buf = Buffer.from([
          '0xd8',
          '0xce',
          '0x80',
          '0xbc',
          '0xee',
          '0x53',
        ]);
        assert.equal(Tap.fromBuffer(buf).readLong(), 1440756011948);
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
        elems: [Buffer.from('abc'), Buffer.alloc(0), Buffer.from([1, 5, 255])],
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
        elems: [Buffer.from([1, 5, 255])],
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
        assert.deepEqual(dv.getInt32(0, true), l);
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
        assert.deepEqual(dv.getInt32(0, true), l);
      });

      test('pack single byte', () => {
        const t = Tap.withCapacity(10);
        const b = Buffer.alloc(8);
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
        assert.deepEqual(t.subarray(0, t.pos), Buffer.from([1]));
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
          const t1 = Tap.withCapacity(10);
          const t2 = Tap.withCapacity(10);
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
          const t = Tap.withCapacity(10);
          t.packLongBytes(b1);
          t.pos = 0;
          const b2 = t.unpackLongBytes();
          assert.deepEqual(b2, b1);
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
          assert.deepEqual(readFn.call(tap), elem);
        }
      });

      test('read over ' + name, () => {
        const tap = Tap.withCapacity(0);
        readFn.call(tap); // Shouldn't throw.
        assert(!tap.isValid());
      });

      test('write over ' + name, () => {
        const tap = Tap.withCapacity(0);
        writeFn.call(tap, elems[0]); // Shouldn't throw.
        assert(!tap.isValid());
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
          assert.equal(tap.pos, pos);
        }
      });
    }
  });
});
