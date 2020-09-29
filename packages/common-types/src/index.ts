import {LogicalType, LongType, Schema, Type} from '@avro/types';
import moment, {Moment} from 'moment';
import {VError} from 'verror';

export const bigIntLongType = LongType.__with<bigint>({
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

function checkTypeName(schema: any, names: ReadonlyArray<string>): void {
  const name = schema.type;
  if (!names.includes(name)) {
    throw new VError('unsupported underlying type %s: %j', name, schema)
  }
}

export class MomentType<U = number> extends LogicalType<Moment, U>  {
  protected constructor(schema: Schema, opts: Type.ForSchemaOpts) {
    checkTypeName(schema, ['long']);
    super(schema, opts);
  }

  protected _fromValue(val: U): Moment {
    return moment(this.underlyingType.jsonEncode(val));
  }

  protected _toValue(val: Moment): U {
    return this.underlyingType.jsonDecode(+val);
  }
}
