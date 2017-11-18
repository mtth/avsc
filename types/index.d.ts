// Note: this typing file is incomplete (https://github.com/mtth/avsc/pull/134).

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
  on(type: 'metadata', callback: (type: Type.Type) => void): this;
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

export function assembleProtocol(filePath: string, opts: Partial<AssembleOptions>, callback: Callback<object>): void;
export function assembleProtocol(filePath: string, callback: Callback<object>): void;
export function createFileDecoder(fileName: string, codecs?: Partial<CodecOptions>): Decoder;
export function createFileEncoder(filePath: string, schema: any, options?: any): Encoder;
export function discoverProtocol(transport: Service.Transport, options: any, callback: Callback<any>): void;
export function discoverProtocol(transport: Service.Transport, callback: Callback<any>): void;
export function extractFileHeader(filePath: string, options?: any): void;
export function parse(schemaOrProtocolIdl: string, options?: any): Service.Protocol | Type.Type; // TODO
export function readProtocol(protocolIdl: string, options?: Partial<ReaderOptions>): Service.Protocol;
export function readSchema(schemaIdl: string, options?: Partial<ReaderOptions>): Schema;
// TODO streams
// TODO types

export namespace Type {
  interface Type {
    // TODO clone(val, opts)
    // TODO compare
    // TODO compareBuffers(buf1, buf2)
    // TODO createResolver(type, opts)
    // TODO decode(buf, pos, resolver)
    // TODO encode(val, buf, pos)
    // TODO equals(type)
    // TODO fingerprint(algorithm)
    fromBuffer(buffer: Buffer, resolver: any, noCheck: boolean): Type; // TODO
    // TODO fromString(str)
    // TODO inspect()
    // TODO isValid(val, opts)
    // TODO random()
    // TODO schema(opts)
    toBuffer(value: object): Buffer;
    // TODO toJSON()
    // TODO toString(val)
    // TODO wrap(val)
  }

  function forSchema(schema: Schema): Type;
  function forValue(value: object): Type;
  // TODO function forTypes(types, opts)
  function isType(arg: any): boolean; // TODO
}

export namespace Service {
  interface Protocol {
    [key: string]: any; // TODO object should be further specified
  }

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

  interface Service {
    createClient(options?: Partial<ClientOptions>): Client;
    createServer(options?: Partial<ServerOptions>): Server;
    message(name: string): any;
    type(name: string): any;
  }

  type TransportFunction = () => void; // TODO

  type Transport = stream.Duplex | TransportFunction;

  interface ChannelCreateOptions {
    objectMode: boolean;
  }

  interface ChannelDestroyOptions {
    noWait: boolean;
  }

  interface Server extends EventEmitter {
    readonly service: Service;
    // on<message>()
    activeChannels(): ServerChannel[];
    createChannel(transport: Transport, options?: Partial<ChannelCreateOptions>): ServerChannel;
    onMessage<T>(name: string, handler: (arg1: any, callback: Callback<T>) => void): this;
    remoteProtocols(): Protocol[];
    use(...args: any[]): this;
  }

  interface Client extends EventEmitter {
    activeChannels(): ClientChannel[];
    createChannel(transport: Transport, options?: Partial<ChannelCreateOptions>): ClientChannel;
    destroyChannels(options?: Partial<ChannelDestroyOptions>): void;
    emitMessage<T>(name: string, req: any, options?: any, callback?: Callback<T>): void // TODO
    remoteProtocols(): Protocol[];
    use(...args: any[]): this;
  }

  function compatible(client: Client, server: Server): boolean;
  function forProtocol(protocol: Protocol, options: any): Service;
  function isService(obj: any): boolean;
}
