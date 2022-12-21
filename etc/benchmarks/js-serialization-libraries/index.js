'use strict';

/**
 * Simple benchmarking entry point.
 *
 */

let avro = require('../../../lib'),
    Benchmark = require('benchmark'),
    commander = require('commander'),
    compactr = require('compactr'),
    flatbuffers = require('flatbuffers'),
    fs = require('fs'),
    msgpack = require('msgpack-lite'),
    Pbf = require('pbf'),
    pbCompile = require('pbf/compile'),
    pbSchema = require('protocol-buffers-schema'),
    protobuf = require('protocol-buffers'),
    protobufjs = require('protobufjs'),
    spack = require('schemapack');


/**
 * Generate statistics for a given schema.
 *
 */
function generateStats(schema, opts) {
  opts = opts || {};

  let type = avro.parse(schema, {wrapUnions: opts.wrapUnions});
  return [DecodeSuite, EncodeSuite].map((Suite) => {
    let stats = [];
    let suite = new Suite(type, opts)
      .on('start', () => { console.error(Suite.key_ + ' ' + type); })
      .on('cycle', (evt) => { console.error('' + evt.target); })
      .run();
    stats.push({
      value: suite.getValue(),
      stats: suite.map((benchmark) => {
        let stats = benchmark.stats;
        return {
          name: benchmark.name,
          mean: stats.mean,
          rme: stats.rme
        };
      })
    });
    return {name: Suite.key_, stats: stats};
  });
}


/**
 * Custom benchmark suite.
 *
 */
class Suite extends Benchmark.Suite {
  constructor (type, opts) {
    super();

    opts = opts || {};
    this._type = type;
    this._compatibleType = avro.parse(type.getSchema(), {
      typeHook: typeHook,
      wrapUnions: opts.wrapUnions
    });
    this._value = opts.value ? type.fromString(opts.value) : type.random();

    Object.keys(opts).forEach(function (name) {
      if (!name.indexOf('_')) {
        return;
      }
      let fn = this['__' + name];
      if (typeof fn == 'function') {
        this.add(name, fn.call(this, opts[name])); // Add benchmark.
      }
    }, this);
  }

  getType (isProtobuf) {
    return isProtobuf ? this._compatibleType : this._type;
  }

  getValue (isProtobuf) {
    if (isProtobuf) {
      let type = this.getType(true); // Read enum values as integers.
      return type.fromBuffer(this.getType().toBuffer(this._value));
    } else {
      return this._value;
    }
  }
}


/**
 * Basic decoding benchmark.
 *
 */
class DecodeSuite extends Suite {
  constructor (type, opts) { super(type, opts); }

  __avsc () {
    let type = this.getType();
    let buf = type.toBuffer(this.getValue());
    return function () {
      let val = type.fromBuffer(buf);
      if (val.$) {
        throw new Error();
      }
    };
  }

  __compactr (args) {
    let schema = compactr.schema(JSON.parse(fs.readFileSync(args)));
    let buf = schema.write(this.getValue()).buffer();
    return function () {
      let obj = schema.read(buf);
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __flatbuffers (args) {
    let root = flatbuffers.compileSchema(fs.readFileSync(args));
    let buf = Buffer.from(root.generate(this.getValue()));
    return function () {
      let obj = root.parse(buf);
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __json () {
    let str = JSON.stringify(this.getValue());
    return function () {
      let obj = JSON.parse(str);
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __jsonString () {
    let type = this.getType();
    let str = type.toString(this.getValue());
    return function () {
      let obj = JSON.parse(str);
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __jsonBinary () {
    let str = JSON.stringify(this.getValue());
    return function () {
      let obj = JSON.parse(str, (key, value) => {
        return (value && value.type === 'Buffer') ? new Buffer(value) : value;
      });
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __msgpackLite () {
    let buf = msgpack.encode(this.getValue());
    return function () {
      let obj = msgpack.decode(buf);
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __pbf (args) {
    let parts = args.split(':');
    let proto = pbSchema.parse(fs.readFileSync(parts[0]));
    let message = pbCompile(proto)[parts[1]];
    let pbf = new Pbf();
    message.write(this.getValue(true), pbf);
    let buf = pbf.finish();
    return function () {
      let obj = message.read(new Pbf(buf));
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __protobufjs (args) {
    let parts = args.split(':');
    let root = protobufjs.parse(fs.readFileSync(parts[0])).root;
    let message = root.lookup(parts[1]);
    let buf = message.encode(this.getValue(true)).finish();
    return function () {
      let obj = message.decode(buf);
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __protocolBuffers (args) {
    let parts = args.split(':');
    let messages = protobuf(fs.readFileSync(parts[0]));
    let message = messages[parts[1]];
    let buf = message.encode(this.getValue(true));
    return function () {
      let obj = message.decode(buf);
      if (obj.$) {
        throw new Error();
      }
    };
  }

  __schemapack (args) {
    let schema = spack.build(JSON.parse(fs.readFileSync(args)));
    let buf = schema.encode(this.getValue(true));
    return function () {
      let obj = schema.decode(buf);
      if (obj.$) {
        throw new Error();
      }
    };
  }
}

DecodeSuite.key_ = 'decode';


/**
 * Basic encoding benchmark.
 *
 */
class EncodeSuite extends Suite {
  constructor (type, opts) { super(type, opts); }

  __avsc () {
    let type = this.getType();
    let val = this.getValue();
    return function () {
      let buf = type.toBuffer(val);
      if (!buf.length) {
        throw new Error();
      }
    };
  }

  __compactr (args) {
    let schema = compactr.schema(JSON.parse(fs.readFileSync(args)));
    let val = this.getValue();
    return function () {
      let buf = schema.write(val).buffer();
      if (!buf.length) {
        throw new Error();
      }
    };
  }

  __flatbuffers (args) {
    let message = flatbuffers.compileSchema(fs.readFileSync(args));
    let val = this.getValue(true);
    return function () {
      let buf = Buffer.from(message.generate(val).buffer);
      if (!buf.length) {
        throw new Error();
      }
    };
  }

  __json () {
    let val = this.getValue();
    return function () {
      let str = JSON.stringify(val);
      if (!str.length) {
        throw new Error();
      }
    };
  }

  __jsonBinary () {
    let val = this.getValue();
    return function () {
      let str = JSON.stringify(val, (key, value) => {
        if (Buffer.isBuffer(value)) {
          return value.toString('binary');
        }
        return value;
      });
      if (!str.length) {
        throw new Error();
      }
    };
  }

  __jsonString () {
    let type = this.getType();
    let obj = JSON.parse(type.toString(this.getValue()));
    return function () {
      let str = JSON.stringify(obj);
      if (!str.length) {
        throw new Error();
      }
    };
  }

  __msgpackLite () {
    let val = this.getValue();
    return function () {
      let buf = msgpack.encode(val);
      if (!buf.length) {
        throw new Error();
      }
    };
  }

  __pbf (args) {
    let parts = args.split(':');
    let proto = pbSchema.parse(fs.readFileSync(parts[0]));
    let message = pbCompile(proto)[parts[1]];
    let val = this.getValue(true);
    return function () {
      let pbf = new Pbf();
      message.write(val, pbf);
      let buf = pbf.finish();
      if (!buf.length) {
        throw new Error();
      }
    };
  }

  __protobufjs (args) {
    let parts = args.split(':');
    let root = protobufjs.parse(fs.readFileSync(parts[0])).root;
    let message = root.lookup(parts[1]);
    let val = this.getValue(true);
    return function () {
      let buf = message.encode(val).finish();
      if (!buf.length) {
        throw new Error();
      }
    };
  }

  __protocolBuffers (args) {
    let parts = args.split(':');
    let messages = protobuf(fs.readFileSync(parts[0]));
    let message = messages[parts[1]];
    let val = this.getValue(true);
    return function () {
      let buf = message.encode(val);
      if (!buf.length) {
        throw new Error();
      }
    };
  }

  __schemapack (args) {
    let schema = spack.build(JSON.parse(fs.readFileSync(args)));
    let val = this.getValue(true);
    return function () {
      let buf = schema.encode(val);
      if (!buf.length) {
        throw new Error();
      }
    };
  }
}

EncodeSuite.key_ = 'encode';

commander
  .usage('[options] <schema>')
  .option('-v, --value <val>', 'Use this value for benchmarking.')
  .option('-w, --wrap-unions', 'Wrap unions.')
  .option('--avsc', 'Benchmark `avsc`.')
  .option('--compactr <path>', 'Benchmark `compactr`.')
  .option('--flatbuffers <path>', 'Benchmark `flatbuffers`.')
  .option('--json', 'Benchmark built-in JSON.')
  .option('--json-binary', 'Benchmark JSON (serializing bytes to strings).')
  .option('--json-string', 'Benchmark JSON (pre-parsing bytes to strings).')
  .option('--msgpack-lite', 'Benchmark `msgpack-lite`.')
  .option('--pbf <path:message>', 'Benchmark `pbf`.')
  .option('--protobufjs <path:message>', 'Benchmark `protobufjs`.')
  .option('--protocol-buffers <path:message>', 'Benchmark `protocol-buffers`.')
  .option('--schemapack <path>', 'Benchmark `schemapack`.')
  .parse(process.argv);

let schema = commander.args[0];
if (!schema) {
  console.error('Missing schema.');
  process.exit(1);
}

let stats = generateStats(schema, commander);
console.log(JSON.stringify(stats));

// Helpers.

/**
 * Typehook to represent enums as integers, required for `protocol-buffers`.
 *
 */
function typeHook(attrs, opts) {
  if (attrs.type === 'enum') {
    return avro.parse('int', opts);
  }
}
