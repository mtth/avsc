/**
 * Public interfaces for all schemas and types. These are used to hide
 * implementation details of each type.
 */

export const primitiveTypeNames = [
  'null',
  'boolean',
  'int',
  'long',
  'float',
  'double',
  'string',
  'bytes',
] as const;

/** Supported primitive type names. */
export type PrimitiveTypeName = (typeof primitiveTypeNames)[number];

/** Attributes present in all schemas. */
export interface BaseSchema {
  readonly doc?: string;
  readonly logicalType?: string;
}

/** Attributes present in all named schemas. */
export interface NamedSchema extends BaseSchema {
  readonly name: string;
  readonly aliases?: ReadonlyArray<string>;
  readonly namespace?: string;
}

/** Additional schema properties. */
export interface SchemaExtensions {
  readonly [key: string]: any;
}

interface FieldSchema<E = SchemaExtensions, T = Type> {
  readonly name: string;
  readonly type: Schema<E, T>;
  readonly aliases?: ReadonlyArray<string>;
  readonly doc?: string;
  readonly order?: 'ascending' | 'descending' | 'ignore';
  readonly default?: any;
}

export type Schema<E = SchemaExtensions, T = Type> =
  | ({type: PrimitiveTypeName} & BaseSchema & E)
  | ({type: 'array'; items: Schema<E, T>} & BaseSchema & E)
  | ({type: 'enum'; symbols: ReadonlyArray<string>} & NamedSchema & E)
  | ({type: 'fixed'; size: number} & NamedSchema & E)
  | ({type: 'map'; values: Schema<E, T>} & BaseSchema & E)
  | ({type: 'record'; fields: ReadonlyArray<FieldSchema<E, T>>} & NamedSchema &
      E)
  | ReadonlyArray<Schema<E, T>> // Union
  | string // References
  | T; // Arbitrary other value, used to support already "instantiated" schemas

export type Type<V = any> =
  | NullType
  | BooleanType
  | IntType
  | FloatType
  | DoubleType
  | LongType<V>
  | StringType
  | BytesType
  | FixedType
  | EnumType
  | ArrayType<V>
  | MapType<V>
  | RecordType<V>
  | UnionType<V>
  | LogicalType<V>;

export interface BaseType<V = any, G = {}> {
  /**
   * Type-specific name, present for all types. It matches the `type` field
   * except in the following cases:
   *
   * + Unions (which don't have a type field). It is then equal to
   *   `union:unwrapped` or `union:wrapped` depending on the union.
   * + Logical types, where it is equal to `logical:<logicalType>`.
   * + Abstract longs, where it is `long:abstract`.
   */
  readonly typeName: string;

  /** Name of the branch when this type is nested inside a wrapped union. */
  readonly branchName: string | undefined;

  /** User-defined name, if the type supports it. */
  readonly name: string | undefined;

  /** Name aliases for schema evolution. Undefined for unnamed types. */
  readonly aliases: string[] | undefined;

  /** Optional description. */
  readonly doc: string | undefined;

  binaryDecode(
    buf: Uint8Array,
    resolver?: TypeResolver<V>,
    noCheck?: boolean
  ): V & G;

  binaryDecodeAt(
    buf: Uint8Array,
    pos: number,
    resolver?: TypeResolver<V>
  ): {readonly value: V & G; readonly offset: number};

  binaryEncode(val: V): Uint8Array;

  binaryEncodeAt(val: V, buf: Uint8Array, pos: number): number;

  jsonDecode(
    data: any,
    resolver?: TypeResolver<V>,
    allowUndeclaredFields?: boolean
  ): V & G;

  jsonEncode(val: V, opts?: TypeJsonEncodeOptions): any;

  createResolver<W>(writer: Type<W>): TypeResolver<V, W>;

  checkValid(val: V, opts?: TypeCheckValidOptions): void;

  isValid(val: V, opts?: TypeIsValidOptions): boolean;

  clone(val: V, opts?: TypeCloneOptions): V;

  wrap(val: V): any;

  compare(val1: V, val2: V): -1 | 0 | 1;

  binaryCompare(buf1: Uint8Array, buf2: Uint8Array): -1 | 0 | 1;

  equals(other: Type): boolean;

  schema(opts?: TypeSchemaOptions): Schema;
}

export type TypeResolver<V, W = V> = V & W;

export interface TypeJsonEncodeOptions {
  readonly omitDefaultValues?: boolean;
}

export interface TypeCheckValidOptions {
  readonly allowUndeclaredFields?: boolean;
}

export type ErrorHook = (
  path: ReadonlyArray<string>,
  val: any,
  type: Type
) => void;

export interface TypeIsValidOptions {
  readonly allowUndeclaredFields?: boolean;
  readonly errorHook?: ErrorHook;
}

export interface TypeCloneOptions {}

export interface TypeSchemaOptions {
  readonly exportAttrs?: boolean;
  readonly noDeref?: boolean;
}

export type PrimType<V, N extends string> = BaseType<V> & {
  readonly name: undefined;
  readonly aliases: undefined;
  readonly branchName: N;
  readonly typeName: N;

  wrap(val: V): Record<N, V>;
};

export type NullType = PrimType<null, 'null'>;
export type BooleanType = PrimType<boolean, 'boolean'>;
export type IntType = PrimType<number, 'int'>;
export type FloatType = PrimType<number, 'float'>;
export type DoubleType = PrimType<number, 'double'>;
export type StringType = PrimType<string, 'string'>;
export type BytesType = PrimType<Buffer, 'bytes'>;

export interface LongType<
  V = number,
  N extends 'long' | 'abstract:long' = 'long',
> extends BaseType<V> {
  readonly name: undefined;
  readonly aliases: undefined;
  readonly branchName: 'long';
  readonly typeName: N;
}

export interface FixedType extends BaseType<number> {
  readonly name: string;
  readonly aliases: string[];
  readonly branchName: string;
  readonly typeName: 'fixed';
  readonly size: number;
}

export interface EnumType extends BaseType<string> {
  readonly name: string;
  readonly aliases: string[];
  readonly branchName: string;
  readonly typeName: 'enum';
  readonly symbols: ReadonlyArray<string>;
}

export interface ArrayType<V = any> extends BaseType<V[]> {
  readonly name: undefined;
  readonly aliases: undefined;
  readonly branchName: 'array';
  readonly typeName: 'array';
  readonly itemsType: Type<V>;
}

export interface MapType<V = any> extends BaseType<{[key: string]: V}> {
  readonly name: undefined;
  readonly aliases: undefined;
  readonly branchName: 'map';
  readonly typeName: 'map';
  readonly valuesType: Type<V>;
}

export interface Field {
  readonly name: string;
  readonly aliases: string[];
  readonly type: Type;
  readonly order: 'ascending' | 'descending' | 'ignore';
  readonly defaultValue: any;
}

export interface RecordConstructor<V = {[key: string]: any}> {
  new (...args: any[]): GeneratedRecord<V>;
  fromBinary(buf: Uint8Array): GeneratedRecord<V>;
  fromJSON(obj: any): GeneratedRecord<V>;
}

export type GeneratedRecord<V = {[key: string]: any}> = {
  clone(): V;
  compare(other: V): -1 | 0 | 1;
  checkValid(opts?: TypeCheckValidOptions): void;
  isValid(opts?: TypeIsValidOptions): boolean;
  binaryEncode(): Uint8Array;
  jsonEncode(opts?: TypeJsonEncodeOptions): any;
  wrap(): any;
} & V;

export interface RecordType<V = any> extends BaseType<V, GeneratedRecord<V>> {
  readonly name: string;
  readonly aliases: string[];
  readonly branchName: string;
  readonly typeName: 'record' | 'error';
  readonly recordConstructor: RecordConstructor<V>;
  readonly fields: ReadonlyArray<Field>;

  field(name: string): Field | undefined;
}

export interface Branch<V = any, T = BaseType<V>> {
  readonly type: T;
  unwrap(): V;
}

export interface UnionType<V = any> extends BaseType<V> {
  readonly typeName: 'union:unwrapped' | 'union:wrapped';
  readonly name: undefined;
  readonly branchName: undefined;
  readonly types: ReadonlyArray<Type>;

  branchType<T>(
    branchName: string
  ): (T extends BaseType ? T : Type<T>) | undefined;
}

export interface LogicalType<V = any, U = any, T = Type<U>>
  extends BaseType<V> {
  readonly branchName: string;
  readonly typeName: `logical:${string}`;
  readonly underlyingType: T;
}
