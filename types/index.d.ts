// Note: this typing file is incomplete (https://github.com/mtth/avsc/pull/134).
// TODO: Wherever the type is just `any`, it was probably generated automatically.
//       Either finish documenting the type signature or document why `any` is appropriate.
// TODO: Wherever the argument names are just `args: any`, it was probably generated from the signature of util.deprecate. Fix argument counts and types.
// NOTE: This does not contain entries for functions available in the browser (functions/methods in etc/browser)

import * as stream from 'stream'; 
import { EventEmitter } from 'events'

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
      type: AvroSchema;
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
    items: AvroSchema;
  }

  interface MapType {
    type: "map";
    values: AvroSchema;
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


type Codec = (buffer: Buffer, callback: Callback<Buffer>) => void;

interface CodecOptions {
  [name: string]: Codec;
}

interface DecoderOptions {
  noDecode: boolean;
  readerSchema: string | object | Type;
  codecs: CodecOptions;
  parseHook: (schema: Schema) => Type
}

interface EncoderOptions {
  blockSize: number;
  codec: string;
  codecs: CodecOptions;
  writeHeader: boolean | 'always' | 'never' | 'auto';
  syncMarker: Buffer;
}

interface ForSchemaOptions {
  assertLogicalTypes: boolean;
  logicalTypes: { [type: string]: types.LogicalType };
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
interface AssembleOptions {
  importHook: (filePath: string, type: 'idl', callback: Callback<object>) => void;
}

interface SchemaOptions {
  exportAttrs: boolean;
  noDeref: boolean;
}

declare class Resolver {
  //no public methods
}

//exported functions

export function assembleProtocol(filePath: string, opts: Partial<AssembleOptions>, callback: Callback<object>): void;
export function assembleProtocol(filePath: string, callback: Callback<object>): void;
export function createFileDecoder(fileName: string, opts?: Partial<DecoderOptions>): streams.BlockDecoder;
export function createFileEncoder(filePath: string, schema: Schema, opts?: Partial<EncoderOptions>): streams.BlockEncoder;
export function createBlobEncoder(schema: Schema, opts?: Partial<EncoderOptions>): stream.Duplex;
export function createBlobDecoder(blob: Blob, opts?: Partial<DecoderOptions>): streams.BlockDecoder;
export function discoverProtocol(transport: Service.Transport, options: any, callback: Callback<any>): void;
export function discoverProtocol(transport: Service.Transport, callback: Callback<any>): void;
export function extractFileHeader(filePath: string, options?: any): void;
export function parse(schemaOrProtocolIdl: string, options?: any): any; // TODO protocol literal or Type
export function readProtocol(protocolIdl: string, options?: Partial<DecoderOptions>): any;
export function readSchema(schemaIdl: string, options?: Partial<DecoderOptions>): Schema;


// TODO more specific types than `any` 
export class Type {
  clone(val: any, opts?: Partial<CloneOptions>): any;
  compare(val1: any, val2: any): number;
  compareBuffers(buf1: Buffer, buf2: Buffer): number;
  createResolver(type: Type): Resolver;
  decode(buf: Buffer, pos?: number, resolver?: Resolver): { value: any, offset: number};
  encode(val: any, buf: Buffer, pos?: number): void;
  equals(type: Type): boolean;
  fingerprint(algorithm?: string): Buffer;
  fromBuffer(buffer: Buffer, resolver?: Resolver, noCheck?: boolean): any;
  fromString(str: string): any;
  inspect(): string;
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

export class Service {
  constructor(name: any, messages: any, types: any, ptcl: any, server: any);
  createClient(options?: Partial<Service.ClientOptions>): Service.Client;
  createServer(options?: Partial<Service.ServerOptions>): Service.Server;
  equals(args: any): any;  // deprecated
  inspect(): string;
  message(name: string): any;
  type(name: string): Type | undefined;

  readonly doc: string | undefined;
  readonly hash: Buffer;
  readonly messages: any[];
  readonly name: string;
  readonly protocol: any;
  readonly types: Type[];

  static compatible(client: Service.Client, server: Service.Server): boolean;
  static forProtocol(protocol: any, options: any): Service;
  static isService(obj: any): boolean;
}

export namespace Service {
  interface ClientChannel extends EventEmitter {
    readonly client: Client;
    readonly destroyed: boolean;
    readonly draining: boolean;
    readonly pending: number;
    readonly timeout: number;
    ping(timeout?: number, cb?: any): void;
    destroy(noWait?: boolean): void;
  }

  interface ServerChannel extends EventEmitter {
    readonly destroyed: boolean;
    readonly draining: boolean;
    readonly pending: number;
    readonly server: Server;
    destroy(noWait?: boolean): void;
  }

  interface ClientOptions {
    buffering: boolean;
    channelPolicy: any;
    strictTypes: boolean;
    timeout: number;
    remoteProtocols: boolean;
  }

  interface ServerOptions {
    objectMode: boolean;
  }

  type TransportFunction = () => void; // TODO

  type Transport = stream.Duplex | TransportFunction;

  interface ChannelCreateOptions {
    objectMode: boolean;
  }

  interface ChannelDestroyOptions {
    noWait: boolean;
  }

  class Server extends EventEmitter {
    constructor(svc: any, opts: any);

    readonly service: Service;
    // on<message>()

    activeChannels(): ServerChannel[];
    createChannel(transport: Transport, options?: Partial<ChannelCreateOptions>): ServerChannel;
    onMessage<T>(name: string, handler: (arg1: any, callback: Callback<T>) => void): this;
    remoteProtocols(): any[];
    use(...args: any[]): this;
  }

  class Client extends EventEmitter {
    constructor(svc: any, opts: any);
    activeChannels(): ClientChannel[];
    createChannel(transport: Transport, options?: Partial<ChannelCreateOptions>): ClientChannel;
    destroyChannels(options?: Partial<ChannelDestroyOptions>): void;
    emitMessage<T>(name: string, req: any, options?: any, callback?: Callback<T>): void // TODO
    remoteProtocols(): any[];
    use(...args: any[]): this;
  }
}

export namespace streams {

  class BlockDecoder extends stream.Duplex {
    constructor(opts?: Partial<DecoderOptions>);
    static defaultCodecs(): CodecOptions;

    //adding all listeners, etc. Can't add a single method override for metadata, all base methods need to be repeated :-(

    addListener(event: string, listener: (...args: any[]) => void): this;
    addListener(event: "close", listener: () => void): this;
    addListener(event: "data", listener: (chunk: Buffer | string) => void): this;
    addListener(event: "end", listener: () => void): this;
    addListener(event: "readable", listener: () => void): this;
    addListener(event: "error", listener: (err: Error) => void): this;
    addListener(event: "metadata", listener: (type: Type, codec: string, header: any) => void): this;

    emit(event: string | symbol, ...args: any[]): boolean;
    emit(event: "close"): boolean;
    emit(event: "data", chunk: Buffer | string): boolean;
    emit(event: "end"): boolean;
    emit(event: "readable"): boolean;
    emit(event: "error", err: Error): boolean;
    emit(event: "metadata", listener: (type: Type, codec: string, header: any) => void): boolean;

    on(event: string, listener: (...args: any[]) => void): this;
    on(event: "close", listener: () => void): this;
    on(event: "data", listener: (chunk: Buffer | string) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "readable", listener: () => void): this;
    on(event: "error", listener: (err: Error) => void): this;
    on(event: "metadata", listener: (type: Type, codec: string, header: any) => void): this;

    once(event: string, listener: (...args: any[]) => void): this;
    once(event: "close", listener: () => void): this;
    once(event: "data", listener: (chunk: Buffer | string) => void): this;
    once(event: "end", listener: () => void): this;
    once(event: "readable", listener: () => void): this;
    once(event: "error", listener: (err: Error) => void): this;
    once(event: "metadata", listener: (type: Type, codec: string, header: any) => void): this;

    prependListener(event: string, listener: (...args: any[]) => void): this;
    prependListener(event: "close", listener: () => void): this;
    prependListener(event: "data", listener: (chunk: Buffer | string) => void): this;
    prependListener(event: "end", listener: () => void): this;
    prependListener(event: "readable", listener: () => void): this;
    prependListener(event: "error", listener: (err: Error) => void): this;
    prependListener(event: "metadata", listener: (type: Type, codec: string, header: any) => void): this;

    prependOnceListener(event: string, listener: (...args: any[]) => void): this;
    prependOnceListener(event: "close", listener: () => void): this;
    prependOnceListener(event: "data", listener: (chunk: Buffer | string) => void): this;
    prependOnceListener(event: "end", listener: () => void): this;
    prependOnceListener(event: "readable", listener: () => void): this;
    prependOnceListener(event: "error", listener: (err: Error) => void): this;
    prependOnceListener(event: "metadata", listener: (type: Type, codec: string, header: any) => void): this;

    removeListener(event: string, listener: (...args: any[]) => void): this;
    removeListener(event: "close", listener: () => void): this;
    removeListener(event: "data", listener: (chunk: Buffer | string) => void): this;
    removeListener(event: "end", listener: () => void): this;
    removeListener(event: "readable", listener: () => void): this;
    removeListener(event: "error", listener: (err: Error) => void): this;
    removeListener(event: "metadata", listener: (type: Type, codec: string, header: any) => void): this;
  }

  class BlockEncoder extends stream.Duplex {
    constructor(schema: Schema, opts?: Partial<EncoderOptions>);
    static defaultCodecs(): CodecOptions;
  }

  class RawDecoder extends stream.Duplex {
    constructor(schema: Schema, opts?: { decode?: boolean });
  }

  class RawEncoder extends stream.Duplex {
    constructor(schema: Schema, opts?: { batchSize?: number });
  }
}

export namespace types {
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
}
