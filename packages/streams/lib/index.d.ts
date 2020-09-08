import {Schema} from '@avro/types';
import {Duplex} from 'stream';

export type Codec = (buf1: Buffer, cb: (err: Error | null, buf2: Buffer) => void) => void;

export type DefaultCodecs = Record<'null' | 'deflate', Codec>;

export interface BlockInfo {
  readonly valueCount: number;
  readonly rawDataLength: number;
  readonly compressedDataLength: number;
}

export interface BlockDecoderOpts {
  readonly codecs?: {[key: string]: Codec};
  readonly parseHook?: (str: string) => Schema;
  readonly readerSchema?: Schema;
}

export class BlockDecoder extends Duplex {
  constructor(opts?: BlockDecoderOpts);

  static defaultCodecs(): DefaultCodecs;
}

export interface BlockEncoderOpts {
  readonly blockSize?: number;
  readonly codec?: string;
  readonly codecs?: {[key: string]: Codec};
  readonly metadata?: {[key: string]: Buffer};
  readonly check?: boolean | Type.CheckOpts;
  readonly omitHeader?: boolean;
}

export class BlockEncoder extends Duplex {
  constructor(schema: Schema, opts?: BlockEncoderOpts);

  static defaultCodecs(): DefaultCodecs;
}

export interface ExtractFileHeaderOpts {
  readonly decode?: boolean;
  readonly size?: number;
}

export interface BlockStreamHeader {
  readonly magic: Buffer;
  readonly meta: {[key: string]: Buffer | string};
  readonly sync: Buffer;
}

export function extractFileHeader(
  path: string,
  opts?: ExtractFileHeaderOpts
): BlockStreamHeader;

export function createFileDecoder(
  path: string,
  opts?: BlockDecoderOpts
): BlockDecoder;

export function createFileDecoder(
  path: string,
  schema: Schema,
  opts?: BlockEncoderOpts
): BlockEncoder;
