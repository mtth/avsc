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

interface FieldSchema<T = never> {
  readonly name: string;
  readonly type: Schema<T>;
  readonly aliases?: ReadonlyArray<string>;
  readonly doc?: string;
  readonly order?: 'ascending' | 'descending' | 'ignore';
  readonly default?: any;
}

export interface ArraySchema<T = never> extends BaseSchema {
  readonly type: 'array';
  readonly items: Schema<T>;
}

export interface MapSchema<T = never> extends BaseSchema {
  readonly type: 'map';
  readonly values: Schema<T>;
}

export interface EnumSchema extends NamedSchema {
  readonly type: 'enum';
  readonly symbols: ReadonlyArray<string>;
}

export interface FixedSchema extends NamedSchema {
  readonly type: 'fixed';
  readonly size: number;
}

export interface PrimitiveSchema extends BaseSchema {
  readonly type: PrimitiveTypeName;
}

export interface RecordSchema<T = never> extends NamedSchema {
  readonly type: 'record' | 'error';
  readonly fields: ReadonlyArray<FieldSchema<T>>;
}

export type Schema<T = never> =
  | PrimitiveSchema
  | ArraySchema<T>
  | MapSchema<T>
  | EnumSchema
  | FixedSchema
  | RecordSchema<T>
  | ReadonlyArray<Schema<T>> // Union
  | string // References
  | T; // Arbitrary other value, used to support already "instantiated" schemas

export type Type<V = any> =
  | NullType
  | BooleanType
  | IntType
  | FloatType
  | DoubleType
  | LongType
  | StringType
  | BytesType
  | FixedType
  | EnumType
  | ArrayType<V>
  | MapType<V>
  | RecordType<V>
  | UnionType<V>
  | LogicalType<V>;

export interface BaseType<V = any> {
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
  ): V;

  binaryDecodeAt(
    buf: Uint8Array,
    pos: number,
    resolver?: TypeResolver<V>
  ): {readonly value: V; readonly offset: number};

  binaryEncode(val: V): Uint8Array;

  binaryEncodeAt(val: V, buf: Uint8Array, pos: number): number;

  jsonDecode(
    data: unknown,
    resolver?: TypeResolver<V>,
    allowUndeclaredFields?: boolean
  ): V;

  jsonEncode(val: V, opts?: TypeJsonEncodeOptions): unknown;

  createResolver<W>(writer: Type<W>): TypeResolver<V, W>;

  checkValid(val: V, opts?: TypeCheckValidOptions): void;

  isValid(val: V, opts?: TypeIsValidOptions): boolean;

  clone(val: V, opts?: TypeCloneOptions): V;

  wrap(val: V): Branch<V>;

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

export interface TypeCloneOptions {
  readonly fieldHook?: () => void;
  readonly qualifyNames?: boolean;
}

export interface TypeSchemaOptions {
  readonly exportAttrs?: boolean;
  readonly noDeref?: boolean;
}

export type PrimitiveType<V, N extends string> = BaseType<V> & {
  readonly name: undefined;
  readonly aliases: undefined;
  readonly branchName: N;
  readonly typeName: N;

  wrap(val: V): Branch<V, N>;
};

export type NullType = PrimitiveType<null, 'null'>;
export type BooleanType = PrimitiveType<boolean, 'boolean'>;
export type IntType = PrimitiveType<number, 'int'>;
export type FloatType = PrimitiveType<number, 'float'>;
export type DoubleType = PrimitiveType<number, 'double'>;
export type StringType = PrimitiveType<string, 'string'>;
export type BytesType = PrimitiveType<Uint8Array, 'bytes'>;
export type LongType = PrimitiveType<BigInt, 'long'>;

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

export interface RecordConstructor<V> {
  new (...args: any[]): V;
}

export interface RecordType<V = any> extends BaseType<V> {
  readonly name: string;
  readonly aliases: string[];
  readonly branchName: string;
  readonly typeName: 'record' | 'error';
  readonly recordConstructor: RecordConstructor<V>;
  readonly fields: ReadonlyArray<Field>;

  field(name: string): Field | undefined;
}

export type Branch<V = any, N extends string = string> = {
  readonly [K in N]: V;
} & {
  wrappedType(): Type<V>;
  unwrap(): V;
};

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
  readonly typeName: `logical:${string}`;
  readonly branchName: string;
  readonly underlyingType: T;
}
