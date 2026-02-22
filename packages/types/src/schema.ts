/** Supported primitive type names. */
export type PrimTypeName =
  | 'null'
  | 'boolean'
  | 'int' | 'long' | 'float' | 'double'
  | 'string'
  | 'bytes';

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

/** Record field schema. */
export interface FieldSchema<E = {}> {
  readonly name: string;
  readonly type: Schema<E>;
  readonly aliases?: ReadonlyArray<string>;
  readonly doc?: string;
  readonly order?: 'ascending' | 'descending' | 'ignore';
  readonly default?: any;
}

/** Avro schema. */
export type Schema<E = {readonly [key: string]: any}, T = Type> =
  | {type: PrimTypeName} & BaseSchema & E
  | {type: 'array', items: Schema<E, T>} & BaseSchema & E
  | {type: 'enum', symbols: ReadonlyArray<string>} & NamedSchema & E
  | {type: 'fixed', size: number} & NamedSchema & E
  | {type: 'map', values: Schema<E, T>} & BaseSchema & E
  | {type: 'record', fields: ReadonlyArray<FieldSchema<E>>} & NamedSchema & E
  | ReadonlyArray<Schema<E, T>> // Union
  | string // References
  | T; // Extension point


export interface Type<V = any, E = {}> {
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

  binaryDecode(buf: Uint8Array, opts?: BinaryDecodeOptions): V & E;

  binaryDecodeAt(
    buf: Uint8Array,
    pos: number,
    opts?: BinaryDecodeAtOptions,
  ): {readonly value: V & E; readonly offset: number};

  binaryEncode(val: V): Uint8Array;

  binaryEncodeAt(val: V, buf: Uint8Array, pos: number): number;

  jsonDecode(data: any, opts?: JsonDecodeOptions): V & E;

  jsonEncode(val: V, opts?: JsonEncodeOptions): any;

  createResolver<W>(writer: Type<W>): Resolver<V, W>;

  checkValid(val: V, opts?: CheckValidOptions): void;

  isValid(val: V, opts?: IsValidOptions): boolean;

  clone(val: V): V & E;

  wrap(val: V): any;

  compare(val1: V, val2: V): -1 | 0 | 1;

  binaryCompare(buf1: Uint8Array, buf2: Uint8Array): -1 | 0 | 1;

  equals(other: Type): boolean;

  schema(opts?: SchemaOptions): Schema;
}

export type Resolver<V, W = any> = {__type: 'avroTypesResolver'};

interface BinaryDecodeOptions {
  readonly resolver?: Resolver
}
