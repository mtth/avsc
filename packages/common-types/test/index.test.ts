import {Type} from '@avro/types';

import * as sut from '../src';

describe('bigint type', () => {
  test('roundtrips via buffers', () => {
    const t = Type.forSchema('long', {registry: {long: sut.bigIntLongType}});
    const n = 9007199254740995n;
    const buf = t.binaryEncode(n);
    expect(t.binaryDecode(buf)).toEqual(n);
  });

  test('roundtrips via JSON', () => {
    const opts = {registry: {long: sut.bigIntLongType}};
    const t = Type.forSchema({
      type: 'record',
      name: 'Foo',
      fields: [{name: 'bar', type: 'long'}],
    }, opts);
    const obj = {bar: 1234n};
    const v = t.jsonEncode(obj);
    expect(t.jsonDecode(v)).toEqual(obj);
  });

  test('compares values', () => {
    const t = Type.forSchema('long', {registry: {long: sut.bigIntLongType}});
    const n = 9007199254740995n;
    expect(t.compare(n, n)).toBe(0);
    expect(t.compare(n, 1n)).toBe(1);
    expect(t.compare(0n, 1n)).toBe(-1);
  });
});

describe('decimal type', () => {
  const logicalTypes = {'decimal': sut.DecimalType};

  test('ok with bytes', () => {
    const t = Type.forSchema(
      {type: 'bytes', logicalType: 'decimal', scale: 2, precision: 4},
      {logicalTypes}
    );
    const d = sut.Decimal.forConfig({precision: 4, scale: 2, unscaled: 5});
    const buf = t.binaryEncode(d);
    expect(t.binaryDecode(buf)).toEqual(d);
  });

  test('ok with fixed', () => {
    const t = Type.forSchema(
      {
        type: 'fixed',
        name: 'F5',
        logicalType: 'decimal',
        size: 5,
        scale: 2,
        precision: 4,
      },
      {logicalTypes}
    );
    const d = sut.Decimal.forConfig({precision: 4, scale: 2, unscaled: 10});
    const buf = t.binaryEncode(d);
    expect(t.binaryDecode(buf)).toEqual(d);
  });
});

describe('date type', () => {
  const logicalTypes = {'timestamp-millis': sut.DateType};

  test('ok with standard long', () => {
    const t = Type.forSchema(
      {type: 'long', logicalType: 'timestamp-millis'},
      {logicalTypes}
    );
    const v = t.jsonDecode(1234);
    const buf = t.binaryEncode(v);
    expect(v).toEqual(new Date(1234));
    expect(buf).toEqual(t.binaryEncode(new Date(1234)));
  });

  test('ok with bigint long', () => {
    const t = Type.forSchema(
      {type: 'long', logicalType: 'timestamp-millis'},
      {logicalTypes, registry: {long: sut.bigIntLongType}}
    );
    const m = new Date(12345);
    const v = t.jsonDecode(+m);
    const buf = t.binaryEncode(v);
    expect(v).toEqual(m);
    expect(buf).toEqual(t.binaryEncode(m));
  });

  test('not ok', () => {
    expect(() => {
      Type.forSchema(
        {type: 'string', logicalType: 'timestamp-millis'},
        {assertLogicalTypes: true, logicalTypes}
      );
    }).toThrow(/unsupported/);
  });
});
