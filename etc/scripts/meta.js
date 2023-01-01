#!/usr/bin/env node

'use strict';

/**
 * A type for types.
 *
 * Useful for many things, here for schema generation and compression.
 *
 * Usages (input is always read from stdin):
 *
 *  ./meta compress # Encode schema as Avro bytes.
 *  ./meta decompress # Decompress schema from above representation.
 *  ./meta random # Generate random schema (might overflow, just retry if so).
 *
 * Warning: randomly generated schemas aren't guaranteed to compile since they
 * might include empty fields and unions or undefined references.
 *
 */

let avro = require('../../lib');


/**
 * Deal with Avro's inconsistent union representation.
 *
 */
class MetaType extends avro.types.LogicalType {
  constructor (attrs, opts) {
    super(attrs, opts);
    this._state = opts.state;
  }

  _fromValue (val) {
    let obj = val.value;
    let attrs = obj[Object.keys(obj)[0]];
    if (attrs.name === '') {
      attrs.name = undefined;
    }
    return attrs;
  }

  _toValue (any) {
    let name = any.getName();
    let obj;
    if (name && this._state.references[name]) {
      obj = {string: name};
    } else if (avro.Type.isType(any, 'union')) {
      obj = {array: any.types};
    } else {
      if (name) {
        this._state.references[name] = true;
      }
      obj = {};
      obj[capitalize(any.typeName) + 'Type'] = any.toJSON();
    }
    return {value: obj};
  }
}


// The global state used to handle references. We must also ensure we have
// enough place to write the full schema in one try, otherwise the global state
// for references will be stale.
let STATE = {references: undefined};
avro.Type.__reset(1048576);

// Finally.
let META_TYPE = avro.parse({
  name: 'MetaType',
  type: 'record',
  logicalType: 'meta',
  fields: [
    {
      name: 'value',
      type: [
        derived('array', [{name: 'items', type: 'MetaType'}]),
        primitive('boolean'),
        primitive('bytes'),
        primitive('double'),
        derived('enum', [
          {name: 'name', type: 'string', 'default': ''},
          {name: 'symbols', type: {type: 'array', items: 'string'}}
        ]),
        derived('fixed', [
          {name: 'name', type: 'string', 'default': ''},
          {name: 'size', type: 'int'}
        ]),
        primitive('float'),
        primitive('int'),
        derived('map', [{name: 'values', type: 'MetaType'}]),
        primitive('long'),
        primitive('null'),
        derived('record', [
          {name: 'name', type: 'string', 'default': ''},
          {
            name: 'fields',
            type: {
              type: 'array',
              items: {
                name: 'Field',
                type: 'record',
                fields: [
                  {name: 'name', type: 'string'},
                  {name: 'type', type: 'MetaType'}
                ]
              }
            }
          }
        ]),
        primitive('string'),
        {type: 'array', items: 'MetaType'}, // Union.
        'string' // Reference.
      ]
    }
  ]
}, {logicalTypes: {meta: MetaType}, state: STATE, wrapUnions: true});

// We override entry points to handle the global state required to handle named
// references correctly. (It might be safe to expose a limited API rather than
// the full one, to enforce that only these two methods are supported.)

META_TYPE.toBuffer = function (val) {
  STATE.references = {};
  return avro.Type.prototype.toBuffer.call(META_TYPE, val);
};

META_TYPE.fromBuffer = function (buf) {
  // We can't do the attrs to type transformation inside the logical type's
  // `_fromValue` method since that method is called in post-order (and we need
  // pre-order to be able to resolve references).
  let attrs = avro.Type.prototype.fromBuffer.call(META_TYPE, buf);
  return avro.Type.forType(attrs, {wrapUnions: true});
};


// Example of things we can do.
switch (process.argv[2]) {
  case 'compress':
    readInput((err, buf) => {
      if (err) {
        throw err;
      }
      let type = avro.Type.forSchema(buf.toString());
      process.stdout.write(META_TYPE.toBuffer(type));
    });
    break;
  case 'decompress':
    readInput((err, buf) => {
      if (err) {
        throw err;
      }
      if (!buf.length) {
        throw new Error('empty input');
      }
      process.stdout.write(META_TYPE.fromBuffer(buf).schema());
    });
    break;
  case 'random':
    console.log(JSON.stringify(META_TYPE.random()));
    break;
  default: {
    console.error(`usage: ${process.argv[1]} (compress|decompress|random)`);
    process.exit(1);
  }
}


// Helpers, mostly to generate repetitive parts of the schema.

function primitive(name) {
  return {
    name: capitalize(name) + 'Type',
    type: 'enum',
    symbols: [name]
  };
}

function derived(name, fields) {
  let typeName = capitalize(name) + 'Type';
  fields.unshift({
    name: 'type',
    type: {type: 'enum', name: typeName + 'Name', symbols: [name]}
  });
  return {name: typeName, type: 'record', fields};
}

function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

function readInput(cb) {
  let bufs = [];
  process.stdin
    .on('error', cb)
    .on('data', (buf) => { bufs.push(buf); })
    .on('end', () => { cb(null, Buffer.concat(bufs)); });
}
