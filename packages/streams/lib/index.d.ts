import {Schema} from '@avro/types';
import {Duplex} from 'stream';

type Codec = (buf1: Buffer, cb: (err: Error | null, buf2: Buffer) => void) => void;

type DefaultCodecs = Record<'null' | 'deflate', Codec>;

interface BlockDecoderOpts {
  readonly codecs?: {[key: string]: Codec};
  readonly parseHook?: (str: string) => Schema;
  readonly readerSchema?: Schema;
}

export class BlockDecoder extends Duplex {
  constructor(opts?: BlockDecoderOpts);

  static defaultCodecs(): DefaultCodecs;
}

interface BlockEncoderOpts {
  readonly blockSize?: number;
  readonly codec?: string;
  readonly codecs?: {[key: string]: Codec};
  readonly metadata?: {[key: string]: Buffer};
  readonly noCheck?: boolean;
  readonly omitHeader?: boolean;
}

export class BlockEncoder extends Duplex {
  new(opts?: BlockEncoderOpts);

  static defaultCodecs(): DefaultCodecs;
}

interface ExtractFileHeaderOpts {
  readonly decode?: boolean;
  readonly size?: number;
}

interface BlockStreamHeader {
  magic: Buffer;
  meta: {[key: string]: Buffer | string};
  sync: Buffer;
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
