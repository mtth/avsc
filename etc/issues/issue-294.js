/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');

const schema = {
  type: 'record',
  namespace: 'foo',
  name: 'Foo',
  fields: [],
};

function typeHook(schema, opts) {
  let name = schema.name;
  if (!name) {
    return; // Not a named type, use default logic.
  }
  if (!~name.indexOf('.')) {
    const namespace = schema.namespace || opts.namespace;
    if (namespace) {
      name = `${namespace}.${name}`;
    }
  }
  // Return the type registered with the same name, if any.
  return opts.registry[name];
}

const opts = {typeHook};
const tp1 = avro.Type.forSchema(schema, opts);
const tp2 = avro.Type.forSchema(schema, opts);
