// Note: this typing file is incomplete (https://github.com/mtth/avsc/pull/134).
// TODO: Wherever the type is just `any`, it was probably generated automatically.
//       Either finish documenting the type signature or document why `any` is appropriate.
// TODO: Wherever the argument names are just `args: any`, it was probably generated from the signature of util.deprecate. Fix argument counts and types.

import * as stream from 'stream';
import { EventEmitter } from 'events'

type Schema = string | object;  // TODO object should be further specified

export type Callback<V, Err = any> = (err: Err, value: V) => void;

export type CodecTransformer = (buffer: Buffer, callback: () => void) => Buffer; // TODO

export interface CodecOptions {
  deflate: CodecTransformer;
  snappy: CodecTransformer;
}

export interface Decoder {
  on(type: 'metadata', callback: (type: Type) => void): this;
  on(type: 'data', callback: (value: object) => void): this;
}

export interface Encoder {
  // TODO
}

export interface ReaderOptions {
  // TODO
}

interface AssembleOptions {
  importHook: (filePath: string, type: 'idl', callback: Callback<object>) => void;
}

export function assemble(args: any): any;
export function assembleProtocol(filePath: string, opts: Partial<AssembleOptions>, callback: Callback<object>): void;
export function assembleProtocol(filePath: string, callback: Callback<object>): void;
export function combine(args: any): any;
export function createFileDecoder(fileName: string, codecs?: Partial<CodecOptions>): Decoder;
export function createFileEncoder(filePath: string, schema: any, options?: any): Encoder;
export function discoverProtocol(transport: Service.Transport, options: any, callback: Callback<any>): void;
export function discoverProtocol(transport: Service.Transport, callback: Callback<any>): void;
export function extractFileHeader(filePath: string, options?: any): void;
export function infer(args: any): any;
export function parse(schemaOrProtocolIdl: string, options?: any): Protocol | Type; // TODO
export function readProtocol(protocolIdl: string, options?: Partial<ReaderOptions>): Protocol;
export function readSchema(schemaIdl: string, options?: Partial<ReaderOptions>): Schema;
// TODO streams

// TODO more specific types than `any`
export class Type {
  constructor(schema: any, opts: any);
  clone(val: any, opts?: any): any;
  compare(val1: any, val2: any): number;
  compareBuffers(buf1: any, buf2: any): number;
  createResolver(type: any, opts?: any): any;
  decode(buf: any, pos?: any, resolver?: any): any;
  encode(val: any, buf: any, pos?: any): any;
  equals(type: any): any;
  fingerprint(algorithm?: any): any;
  fromString(str: any): any;
  readonly aliases: any;
  readonly name: string|undefined;
  readonly branchName: string|undefined;
  readonly typeName: string;
  inspect(): string;
  isValid(val: any, opts?: any): any;
  random(): Type;
  schema(opts?: any): any;
  toJSON(): string;
  toString(val?: any): any;
  wrap(val: any): any;
  fromBuffer(buffer: Buffer, resolver: any, noCheck: boolean): Type; // TODO
  toBuffer(value: object): Buffer;
  static forSchema(schema: Schema, opts?: any): Type;
  static forTypes(types: any, opts?: any): Type;
  static forValue(value: object, opts?: any): Type;
  static isType(arg: any): boolean;  // TODO remaining args
}

// TODO: Can this project remove Protocol from types completely, it's a deprecated export?
export class Protocol {
  constructor(name: any, messages: any, types: any, ptcl: any, server: any);
  createClient(opts: any): any;
  createEmitter(args: any): any;
  createListener(args: any): any;
  createServer(opts: any): any;
  emit(args: any): any;
  equals(args: any): any;
  getMessage(args: any): any;
  getMessages(args: any): any;
  getName(args: any): any;
  getSchema(args: any): any;
  getType(args: any): any;
  getTypes(args: any): any;
  inspect(): string;
  message(name: any): any;
  on(args: any): any;
  subprotocol(args: any): any;
  type(name: any): any;
  static compatible(clientSvc: any, serverSvc: any): any;
  static forProtocol(ptcl: any, opts: any): any;
  static isService(any: any): any;
}

export class Service {
  constructor(name: any, messages: any, types: any, ptcl: any, server: any);
  createClient(options?: Partial<Service.ClientOptions>): Service.Client;
  createEmitter(args: any): any;
  createListener(args: any): any;
  createServer(options?: Partial<Service.ServerOptions>): Service.Server;
  emit(args: any): any;
  equals(args: any): any;  // deprecated
  readonly hash: Buffer;
  readonly protocol: any;
  inspect(): string;
  message(name: string): any;
  on(args: any): any;
  subprotocol(args: any): any;
  type(name: string): any;


  static compatible(client: Service.Client, server: Service.Server): boolean;
  static forProtocol(protocol: Protocol, options: any): Service;
  static isService(obj: any): boolean;
}

export namespace Service {
  interface ClientChannel extends EventEmitter {
    readonly client: Client;
    readonly timeout: number;
    readonly destroyed: boolean;
    readonly draining: boolean;
    readonly pending: number;
    ping(timeout: number, cb: any): void;
    destroy(noWait: boolean): void;
  }

  interface ServerChannel extends EventEmitter  {
    readonly server: Server;
    readonly destroyed: boolean;
    readonly draining: boolean;
    readonly pending: number;
    destroy(noWait: boolean): void;
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
    remoteProtocols(): Protocol[];
    use(...args: any[]): this;
  }

  class Client extends EventEmitter {
    constructor(svc: any, opts: any);
    activeChannels(): ClientChannel[];
    createChannel(transport: Transport, options?: Partial<ChannelCreateOptions>): ClientChannel;
    destroyChannels(options?: Partial<ChannelDestroyOptions>): void;
    emitMessage<T>(name: string, req: any, options?: any, callback?: Callback<T>): void // TODO
    remoteProtocols(): Protocol[];
    use(...args: any[]): this;
  }
}

export namespace streams {
  class BlockDecoder {
    constructor(opts: any);
    static defaultCodecs(): any;
  }

  class BlockEncoder {
    constructor(schema: any, opts: any);
    static defaultCodecs(): any;
  }

  class RawDecoder {
    constructor(schema: any, opts: any);
  }

  class RawEncoder {
    constructor(schema: any, opts: any);
  }
}

export namespace types {
  class ArrayType extends Type {
    constructor(schema: any, opts: any);
    readonly itemsType: Type;
    random(): ArrayType;
  }

  class BooleanType extends Type {
    constructor();
    random(): BooleanType;
  }

  class BytesType extends Type {
    constructor();
    random(): BytesType;
  }

  class DoubleType extends Type {
    constructor();
    random(): DoubleType;
  }

  class EnumType extends Type {
    constructor(schema: any, opts: any);
    readonly symbols: string[];
    random(): EnumType;
  }

  class FixedType extends Type {
    constructor(schema: any, opts: any);
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
    constructor(schema: any, opts: any);
    readonly underlyingType: Type;
    random(): LogicalType;
  }

  class LongType extends Type {
    constructor();
    random(): LongType;
  }

  class MapType extends Type {
    constructor(schema: any, opts: any);
    readonly valuesType: any;
    random(): MapType;
  }

  class NullType extends Type {
    constructor();
    random(): NullType;
  }

  class RecordType extends Type {
    constructor(schema: any, opts: any);
    field(name: any): any;
    readonly fields: any[];  // TODO: Field[] once Field interface/class exists
    readonly recordConstructor: any;  // TODO: typeof Record once Record interface/class exists
    random(): RecordType;
  }

  class StringType extends Type {
    constructor();
    random(): StringType;
  }

  class UnwrappedUnionType extends Type {
    constructor(schema: any, opts: any);
    random(): UnwrappedUnionType;
  }

  class WrappedUnionType extends Type {
    constructor(schema: any, opts: any);
    random(): WrappedUnionType;
  }
}
