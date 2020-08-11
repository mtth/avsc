type PrimTypeName =
  | 'null'
  | 'boolean'
  | 'int' | 'long' | 'float' | 'double'
  | 'string'
  | 'bytes';

interface BaseSchema {
  doc?: string;
  logicalType?: string;
}

interface NamedSchema extends BaseSchema {
  name: string;
  aliases?: string[];
  namespace?: string;
}

interface FieldSchema<E = {}> {
  name: string;
  type: Schema<E>;
  aliases?: string[];
  doc?: string;
  order?: 'ascending' | 'descending' | 'ignore';
  default?: any;
}

type Schema<E = {}> =
  | {type: PrimTypeName} & BaseSchema & E
  | {type: 'array', items: Schema<E>} & BaseSchema & E
  | {type: 'enum', symbols: string[]} & NamedSchema & E
  | {type: 'fixed', size: number} & NamedSchema & E
  | {type: 'map', values: Schema<E>} & BaseSchema & E
  | {type: 'record', fields: FieldSchema<E>[]} & NamedSchema & E
  | Schema<E>[]
  | Type
  | string;

interface TypeJsonEncodeOpts {
  readonly omitDefaultValues?: boolean;
}

interface TypeCheckValidOpts {
  readonly allowUndeclaredFields?: boolean;
}

type ErrorHook = (path: ReadonlyArray<string>, val: any, type: Type) => void;

interface TypeIsValidOpts {
  readonly allowUndeclaredFields?: boolean;
  readonly errorHook?: ErrorHook;
}

interface TypeSchemaOpts {
  readonly exportAttrs?: boolean;
  readonly noDeref?: boolean;
}

type TypeHook = (schema: Schema, opts: TypeForSchemaOpts) => Type | undefined;

interface  LogicalTypeConstructor {
  new(schema: Schema, opts: TypeForSchemaOpts);
}

interface TypeForSchemaOpts {
  readonly assertLogicalTypes?: boolean;
  readonly errorStackTraces?: boolean;
  readonly logicalTypes?: {[name: string]: LogicalTypeConstructor};
  readonly registry?: {[name: string]: Type};
  readonly typeHook?: TypeHook;
  readonly wrapUnions?: 'always' | 'never' | 'auto' | boolean;
}

type ValueHook = (val: any, opts: TypeForValueOpts) => Type | undefined;

interface TypeForValueOpts extends TypeForSchemaOpts {
  readonly valueHook: ValueHook;
  readonly emptyArrayType?: ArrayType;
}

type Resolver<V> = {__type: 'avroTypesResolver'}; // TODO: Find a better way.

export class Type<V = any> {
  readonly name: string | undefined;
  readonly branchName: string | undefined;
  readonly doc: string | undefined;
  binaryDecode(buf: Buffer, resolver?: Resolver<V>, noCheck?: boolean): V;
  binaryEncode(val: V): Buffer;
  binaryDecodeAt(buf: Buffer, pos: number, resolver?: Resolver<V>): {value: V; offset: number};
  binaryEncodeAt(val: V, buf: Buffer, pos: number): number;
  jsonDecode(data: any, resolver?: Resolver<V>, allowUndeclaredFields?: boolean): V;
  jsonEncode(val: V, opts?: TypeJsonEncodeOpts): any;
  clone(val: V): V;
  wrap(val: V): any; // TODO: Per subclass overrides.
  checkValid(val: V, opts?: TypeCheckValidOpts): void;
  isValid(val: V, opts?: TypeIsValidOpts): boolean;
  equals(other: Type): boolean;
  schema(opts?: TypeSchemaOpts): Schema;
  compare(val1: V, val2: V): -1 | 0 | 1;
  binaryCompare(buf1: Buffer, buf2: Buffer): -1 | 0 | 1;
  createResolver(writer: Type): Resolver<V>;
  static isType(val: any, ...prefixes: string[]): boolean;
  static forSchema<T extends Type = Type>(schema: Schema, opts?: TypeForSchemaOpts): T;
  static forTypes(types: [], opts?: TypeForValueOpts): never;
  static forTypes<T extends Type = Type>(types: [T], opts?: TypeForValueOpts): T;
  static forTypes<T extends UnionType = UnionType>(types: ReadonlyArray<Type>, opts?: TypeForValueOpts): T;
  static forValue<T extends Type = Type>(val: any, opts?: TypeForValueOpts): T;
  static __reset(size: number): void;
}

export class NullType extends Type<null> {
  readonly branchName: 'null';
  readonly typeName: 'null';
}

export class BooleanType extends Type<boolean> {
  readonly branchName: 'boolean';
  readonly typeName: 'boolean';
}

export class IntType extends Type<number> {
  readonly branchName: 'int';
  readonly typeName: 'int';
}

export class FloatType extends Type<number> {
  readonly branchName: 'float';
  readonly typeName: 'float';
}

export class StringType extends Type<number> {
  readonly branchName: 'string';
  readonly typeName: 'string';
}

export class DoubleType extends Type<number> {
  readonly branchName: 'double';
  readonly typeName: 'double';
}

export class LongType extends Type<number> {
  readonly branchName: 'long';
  readonly typeName: 'long' | 'abstract:long';
  static __with(): LongType;
}

export class FixedType extends Type<number> {
  readonly name: string;
  readonly branchName: string;
  readonly typeName: 'fixed';
  readonly size: number;
}

export class EnumType extends Type<string> {
  readonly name: string;
  readonly branchName: string;
  readonly typeName: 'enum';
  readonly symbols: ReadonlyArray<string>;
}

export class ArrayType<V = any> extends Type<V[]> {
  readonly branchName: 'array';
  readonly typeName: 'array';
  readonly itemsType: Type<V>;
}

export class MapType<V = any> extends Type<{[key: string]: V}> {
  readonly branchName: 'map';
  readonly typeName: 'map';
  readonly valuesType: Type<V>;
}

interface Field {
  readonly name: string;
  readonly aliases: string[];
  readonly type: Type;
  readonly order: 'ascending' | 'descending' | 'ignore';
  readonly defaultValue: any;
}

interface RecordConstructor<V> {
  new(...args: any[]): V;
  fromBuffer(buf: Buffer): V;
  fromJSON(data: any): V;
  fromObject(obj: any): V;
}

interface GeneratedRecord<V> {
  clone(): V;
  compare(other: V): -1 | 0 | 1;
  isValid(opts?: TypeIsValidOpts): boolean;
  toBuffer(): Buffer;
  toJSON(opts?: TypeJsonEncodeOpts): any;
  toString(): string;
  wrap(): any;
}

export class RecordType<V = any> extends Type<V & GeneratedRecord<V>> {
  readonly name: string;
  readonly branchName: string;
  readonly typeName: 'record' | 'error';
  readonly recordConstructor: RecordConstructor<V & GeneratedRecord<V>>;
  readonly fields: ReadonlyArray<Field>;
  field(name: string): Field | undefined;
}

export class LogicalType<V = any> extends Type<V> {
  readonly branchName: string;
  readonly typeName: string;
  readonly underlyingType: Type;
  protected _toValue(data: any): V;
  protected _fromValue(val: V): any;
  protected _resolve<W = any>(otherType: Type<W>): (otherVal: W) => V;
  protected _export(schema: Schema): void;
}

export class UnionType<V = any> extends Type<V> {
  readonly types: ReadonlyArray<Type>;
}
