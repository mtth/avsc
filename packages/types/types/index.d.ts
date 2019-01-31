// Note: These typings are incomplete (https://github.com/mtth/avsc/pull/134).
// In particular, they do not contain entries for functions available in the
// browser (functions/methods in etc/browser).

// TODO: Wherever the type is just `any`, it was probably generated
// automatically. Either finish documenting the type signature or document why
// `any` is appropriate.

//"virtual" namespace (no JS, just types) for Avro Schema
declare namespace schema {
  export type AvroSchema = DefinedType | DefinedType[];
  type DefinedType = PrimitiveType | ComplexType | LogicalType | string;
  type PrimitiveType = 'null' | 'boolean' | 'int' | 'long' | 'float' | 'double' | 'bytes' | 'string';
  type ComplexType = NamedType | RecordType | EnumType | MapType | ArrayType | FixedType;
  type LogicalType = ComplexType & LogicalTypeExtension;

  interface NamedType {
    type: PrimitiveType
  }

  interface RecordType {
    type: "record";
    name: string;
    namespace?: string;
    doc?: string;
    aliases?: string[];
    fields: {
      name: string;
      doc?: string;
      type: Schema;
      default?: any;
    }[];
    order?: "ascending" | "descending" | "ignore";
  }

  interface EnumType {
    type: "enum";
    name: string;
    namespace?: string;
    aliases?: string[];
    doc?: string;
    symbols: string[];
  }

  interface ArrayType {
    type: "array";
    items: Schema;
  }

  interface MapType {
    type: "map";
    values: Schema;
  }

  interface FixedType {
    type: "fixed";
    name: string;
    aliases?: string[];
    size: number;
  }

  interface LogicalTypeExtension {
    logicalType: string;
    [param: string]: any;
  }
}

//Types of Options/arguments

type Schema = Type | schema.AvroSchema;

type Callback<V, Err = any> = (err: Err, value: V) => void;

interface ForSchemaOptions {
  assertLogicalTypes: boolean;
  logicalTypes: { [type: string]: new (schema: Schema, opts?: any) => types.LogicalType; };
  namespace: string;
  noAnonymousTypes: boolean;
  registry: { [name: string]: Type };
  typeHook: (schema: Schema, opts: ForSchemaOptions) => Type;
  wrapUnions: boolean | 'auto' | 'always' | 'never';
}

interface TypeOptions extends ForSchemaOptions {
  strictDefaults: boolean;
}

interface ForValueOptions extends TypeOptions {
  emptyArrayType: Type;
  valueHook: (val: any, opts: ForValueOptions) => Type;
}

interface CloneOptions {
  coerceBuffers: boolean;
  fieldHook: (field: types.Field, value: any, type: Type) => any;
  qualifyNames: boolean;
  skipMissingFields: boolean;
  wrapUnions: boolean;
}

interface IsValidOptions {
  noUndeclaredFields: boolean;
  errorHook: (path: string[], val: any, type: Type) => void
}

interface SchemaOptions {
  exportAttrs: boolean;
  noDeref: boolean;
}

declare class Resolver {
  //no public methods
}

// TODO more specific types than `any`
export class Type {
  clone(val: any, opts?: Partial<CloneOptions>): any;
  compare(val1: any, val2: any): number;
  compareBuffers(buf1: Buffer, buf2: Buffer): number;
  createResolver(type: Type): Resolver;
  decode(buf: Buffer, pos?: number, resolver?: Resolver): { value: any, offset: number};
  encode(val: any, buf: Buffer, pos?: number): number;
  equals(type: Type): boolean;
  fingerprint(algorithm?: string): Buffer;
  fromBuffer(buffer: Buffer, resolver?: Resolver, noCheck?: boolean): any;
  fromString(str: string): any;
  isValid(val: any, opts?: Partial<IsValidOptions>): boolean;
  random(): Type;
  schema(opts?: Partial<SchemaOptions>): Schema;
  toBuffer(value: any): Buffer;
  toJSON(): object;
  toString(val?: any): string;
  wrap(val: any): any;
  readonly aliases: string[] | undefined;
  readonly doc: string | undefined;
  readonly name: string | undefined;
  readonly branchName: string | undefined;
  readonly typeName: string;
  static forSchema(schema: Schema, opts?: Partial<ForSchemaOptions>): Type;
  static forTypes(types: Type[], opts?: Partial<TypeOptions>): Type;
  static forValue(value: object, opts?: Partial<ForValueOptions>): Type;
  static isType(arg: any, ...prefix: string[]): boolean;
}

class ArrayType extends Type {
  constructor(schema: Schema, opts: any);
  readonly itemsType: Type;
  random(): ArrayType;
}

class BooleanType extends Type {  // TODO: Document this on the wiki
  constructor();
  random(): BooleanType;
}

class BytesType extends Type {  // TODO: Document this on the wiki
  constructor();
  random(): BytesType;
}

class DoubleType extends Type {  // TODO: Document this on the wiki
  constructor();
  random(): DoubleType;
}

class EnumType extends Type {
  constructor(schema: Schema, opts?: any);
  readonly symbols: string[];
  random(): EnumType;
}

class FixedType extends Type {
  constructor(schema: Schema, opts?: any);
  readonly size: number;
  random(): FixedType;
}

class FloatType extends Type {
  constructor();
  random(): FloatType;
}

class IntType extends Type {
  constructor();
  random(): IntType;
}

class LogicalType extends Type {
  constructor(schema: Schema, opts?: any);
  readonly underlyingType: Type;
  protected _export(schema: Schema): void;
  protected _fromValue(val: any): any;
  protected _resolve(type: Type): any;
  protected _toValue(any: any): any;
  random(): LogicalType;
}

class LongType extends Type {
  constructor();
  random(): LongType;
  static __with(methods: object, noUnpack?: boolean): void;
}

class MapType extends Type {
  constructor(schema: Schema, opts?: any);
  readonly valuesType: any;
  random(): MapType;
}

class NullType extends Type {  // TODO: Document this on the wiki
  constructor();
  random(): NullType;
}

class RecordType extends Type {
  constructor(schema: Schema, opts?: any);
  readonly fields: Field[];
  readonly recordConstructor: any;  // TODO: typeof Record once Record interface/class exists
  field(name: string): Field;
  random(): RecordType;
}

class Field {
  aliases: string[];
  defaultValue(): any;
  name: string;
  order: string;
  type: Type;
}

class StringType extends Type {  // TODO: Document this on the wiki
  constructor();
  random(): StringType;
}

class UnwrappedUnionType extends Type {
  constructor(schema: Schema, opts: any);
  random(): UnwrappedUnionType;
  readonly types: Type[];
}

class WrappedUnionType extends Type {
  constructor(schema: Schema, opts: any);
  random(): WrappedUnionType;
  readonly types: Type[];
}
