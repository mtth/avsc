import {LogicalType, LongType, Schema, Type} from '@avro/types';
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

/** Decimal logical type implementation. */
export class DecimalType extends LogicalType<Decimal, Buffer> {
  readonly precision: number;
  readonly scale: number;
  protected constructor(schema: Schema, opts: Type.ForSchemaOpts) {
    checkTypeName(schema, ['bytes', 'fixed']);
    super(schema, opts);

    const {precision, scale} = schema as any;
    if (precision !== (precision | 0) || precision <= 0) {
      throw new VError('invalid precision: %s', precision);
    }
    if (scale !== (scale | 0) || scale < 0 || scale > precision) {
      throw new VError('invalid scale: %s', scale);
    }
    this.precision = precision;
    this.scale = scale;

    const type = this.underlyingType;
    if (Type.isType(type, 'fixed')) {
      const size = (type as any).size;
      if (size > 6) {
        // TODO: Support larger sizes.
        throw new VError('size is too large: %s', size);
      }
      const maxVal = Math.pow(2, 8 * size - 1) - 1;
      const maxPrecision = Math.log(maxVal) / Math.log(10);
      if (precision > (maxPrecision | 0)) {
        throw new VError('fixed size too small to hold required precision');
      }
    }
  }

  private newDecimal(unscaled: number): Decimal {
    const {precision, scale} = this;
    return Decimal.forConfig({precision, scale, unscaled});
  }

  protected _fromValue(buf: Buffer): Decimal {
    return this.newDecimal(buf.readIntBE(0, buf.length));
  }

  protected _toValue(dec: unknown): Buffer {
    if (!(dec instanceof Decimal)) {
      throw new VError('invalid decimal: %j', dec);
    }
    const {precision, scale} = dec;
    if (precision !== this.precision || scale !== this.scale) {
      throw new VError('inconsistent decimal: %j', dec);
    }
    const type = this.underlyingType;
    let buf;
    if (Type.isType(type, 'fixed')) {
      buf = Buffer.alloc((type as any).size);
    } else {
      const val = +dec;
      const size = Math.log(val > 0 ? val : - 2 * val) / (Math.log(2) * 8) | 0;
      buf = Buffer.alloc(size + 1);
    }
    buf.writeIntBE(dec.unscaled, 0, buf.length);
    return buf;
  }

  protected _resolve(writer: Type): ((v: any) => Decimal) | undefined {
    if (
      writer instanceof DecimalType &&
      writer.precision === this.precision &&
      writer.scale === this.scale
    ) {
      return (dec) => dec;
    }
  }

  protected _export(schema: Schema): void {
    Object.assign(schema, {precision: this.precision, scale: this.scale});
  }
}

export interface DecimalConfig {
  readonly precision: number;
  readonly scale: number;
  readonly unscaled: number;
}

export class Decimal {
  private constructor(
    readonly precision: number,
    readonly scale: number,
    readonly unscaled: number
  ) {}

  toNumber(): number {
    return this.unscaled * Math.pow(10, -this.scale);
  }

  static forConfig(cfg: DecimalConfig): Decimal {
    return new Decimal(cfg.precision, cfg.scale, cfg.unscaled);
  }
}

/**
 * A logical type representing millisecond timestamps as JavaScript `Date`s.
 * This type is most commonly used with the `'timestamp-millis'` key.
 */
export class DateType<U = number> extends LogicalType<Date, U>  {
  protected constructor(schema: Schema, opts: Type.ForSchemaOpts) {
    checkTypeName(schema, ['long']);
    super(schema, opts);
  }

  protected _fromValue(val: U): Date {
    return new Date(this.underlyingType.jsonEncode(val));
  }

  protected _toValue(val: unknown): U | undefined {
    if (!(val instanceof Date)) {
      return undefined;
    }
    return this.underlyingType.jsonDecode(+val);
  }

  protected _resolve(writer: Type): ((v: any) => Date) | undefined {
    if (Type.isType(writer, 'long', 'logical:timestamp-millis')) {
      return this._fromValue;
    }
  }
}

export const defaultLogicalTypes = Object.freeze({
  'decimal': DecimalType,
  'timestamp-millis': DateType,
});
