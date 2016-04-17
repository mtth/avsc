
// TODO: Subclass EnumType with a class that also keeps track of occurrences of
// each symbols (to make a better decision when combining enums).
// TODO: Use field defaults to represent values that are almost always present?
// TODO: We could extend the subclass defined above to count all union
// branches.
// Coercions record -> map, enum -> string, fixed -> bytes, could be done via
// functions coerce(type, counts) returning true if transformation should
// occur, or simply true to always coerce. Probably inside a `coercions`
// object.

'use strict';
var avro = require('avsc');
/**
 * Small script that can infer an Avro schema from a value.
 *
 * Currently pretty simple and makes a few assumptions on the types used. This
 * should be made a bit more configurable before making it into the library.
 *
 */

var TYPES = (function () {
  var registry = {};
  var names = [
    'int', 'long', 'float', 'double', 'null', 'boolean', 'string'
  ];
  names.forEach(function (name) { registry[name] = createType(name); });
  // Cached array type, used mostly for empty arrays.
  registry.array = createType({type: 'array', items: 'null'});
  return registry;
});

var N = 0;

function getName() { return '_' + N++; }

function createType(attrs) {
  // TODO: Also pass registry?
  return avro.parse(attrs, {unwrapUnions: true});
}

/**
 * Infer schema from value.
 *
 */
function infer(val, opts) {
  opts = opts || {};

  switch (typeof val) {
    case 'string':
      if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(val)) {
        // TODO: Reuse pattern exposed in types.
        return createType({
          name: getName(),
          type: 'enum',
          symbols: [val]
        });
      }
      return TYPES.string;
    case 'boolean':
      return TYPES.boolean;
    case 'number':
      if ((val | 0) === val) {
        return TYPES.int;
      } else if (val % 1 === 0) {
        return TYPES.long;
      } else if (Math.abs(val) < 9007199254740991) {
        return TYPES.float;
      }
      return TYPES.double;
    case 'object':
      if (val === null) {
        return TYPES['null'];
      } else if (Array.isArray(val)) {
        if (!val.length) {
          return TYPES.array;
        }
        return createType({
          type: 'array',
          items: val.slice(1).reduce(
            function (t, v) { return combine(t, infer(v, opts)); },
            infer(val[0], opts)
          )
        });
      } else if (Buffer.isBuffer(val)) {
        return createType({
          name: getName(),
          type: 'fixed',
          size: val.length
        });
      }
      return createType({
        name: getName(), // TODO: Reuse constructor name when not `'Object'`.
        type: 'record',
        fields: Object.keys(val).map(function (n) {
          return {name: n, type: infer(val[n], opts)};
        })
      });
    default:
      // Functions.
      throw new Error('unsupported');
  }
}

function combine(t1, t2) {
  if (t1 === t2 || t1.getFingerprint().equals(t2.getFingerprint())) {
    return t1;
  }

  var n1 = t1.getTypeName();
  var n2 = t2.getTypeName();
  if (n2 === '(union)') {
    return t2.getTypes().reduce(combine, t1);
  }

  var b1 = getTypeBucket(t1);
  var b2 = getTypeBucket(t2);
  if (b1 === '(union)') {
    var buckets = {};
    var ts = t1.getTypes();
    ts.forEach(function (t, i) { buckets[getTypeBucket(t)] = i; });
    var n = buckets[b2];
    if (n !== undefined) {
      // Combine the new type with the branch in the same bucket.
      ts[n] = combine(ts[n], t2);
    } else {
      // Add a new branch to the union.
      ts.push(t2);
    }
    return createType(ts);
  }

  if (b1 === b2) {
    switch (b1) {
      case 'number':
        return combineNumbers(t1, t2);
      case 'string':
        if (n1 === 'string' || n2 === 'string') {
          return t1;
        }
        return combineEnums(t1, t2);
      case 'bytes':
        if (
          n1 === 'fixed' &&
          n2 === 'fixed' &&
          t1.getSize() === t2.getSize()
        ) {
          return t1;
        }
        return TYPES.bytes;
      case 'array':
        return createType({
          type: 'array',
          items: combine(t1.getItemsType(), t2.getItemsType())
        });
      case 'null':
        return t1;
      default:
        // Object, so map or record.
        if (n1 === 'record' && n2 === 'record') {
          return combineRecords(t1, t2);
        }
        return combineObjects(t1, t2);
    }
  } else {
    // Both types are in different buckets and not unions.
    return createType([t1, t2]);
  }
}

function combineNumbers(t1, t2) {
  var typeNames = ['int', 'long', 'float', 'double'];
  var n1 = t1.getTypeName();
  var n2 = t2.getTypeName();
  var index = Math.max(typeNames.indexOf(n1), typeNames.indexOf(n2));
  return TYPES[typeNames[index]];
}

function combineEnums(t1, t2) {
  var s1 = t1.getSymbols();
  var s2 = t2.getSymbols();
  var obj = {};

  var i, l;
  for (i = 0, l = s1.length; i < l; i++) {
    obj[s1[i]] = true;
  }
  for (i = 0, l = s2.length; i < l; i++) {
    obj[s2[i]] = true;
  }

  var symbols = Object.keys(obj);
  // TODO: Configure this threshold.
  if (symbols.length < 4) {
    return createType({name: getName(), type: 'enum', symbols: symbols});
  } else {
    return TYPES.string;
  }
}

function combineRecords(t1, t2) {
  var f1 = t1.getFields();
  var f2 = t2.getFields();
  var o1 = {};
  var o2 = {};

  var i, l, f, t;
  for (i = 0, l = f1.length; i < l; i++) {
    f = f1[i];
    o1[f.getName()] = f.getType();
  }
  for (i = 0, l = f2.length; i < l; i++) {
    f = f2[i];
    t = o1[f.getName()];
    if (t) {
      o2[f.getName()] = combine(t, f.getType());
    } else {
      // TODO: Allow a stricter mode where this would fail. The code below
      // treats missing fields (which should be undefined) as null. The option
      // could be called `noNullUndefined` perhaps.
      o2[f.getName()] = combine(TYPES['null'], f.getType());
    }
  }
  for (i = 0, l = f1.length; i < l; i++) {
    f = f1[i];
    t = o2[f.getName()];
    if (!t) {
      // TODO: Also fail if strict here.
      o2[f.getName()] = combine(TYPES['null'], f.getType());
    }
  }

  var record = createType({
    name: getName(), // TODO: Reuse name if the same.
    type: 'record',
    fields: Object.keys(o2).map(function (n) {
      return {
        name: n,
        type: o2[n],
        'default': isNullable(o2[n]) ? null : undefined
      };
    })
  });

  if (record.getFields().length > 3) {
    // TODO: Allow configuring this.
    return fieldsToMap(record.getFields());
  } else {
    return record;
  }
}

function combineObjects(t1, t2) {
  if (t1.getTypeName() === 'record') {
    t1 = fieldsToMap(t1.getFields());
  }
  if (t2.getTypeName() === 'record') {
    t2 = fieldsToMap(t2.getFields());
  }
  return createType({
    type: 'map',
    vales: combine(t1.getValuesType(), t2.getValuesType())
  });
}

function fieldsToMap(fields) {
  if (!fields.length) {
    throw new Error('empty fields');
  }
  return createType({
    type: 'map',
    values: fields.slice(1).reduce(function (t, f) {
      return combine(t, f.getType());
    }, fields[0].getType())
  });
}

function isNullable(type) {
  // Check whether type is a union with null as a branch.
  if (type.getTypeName() !== '(union)') {
    return false;
  }
  var types = type.getTypes();
  var i, l;
  for (i = 0, l = types.length; i < l; i++) {
    if (types[i].getTypeName() === 'null') {
      return true;
    }
  }
  return false;
}

function getTypeBucket(type) {
  var typeName = type.getTypeName();
  switch (typeName) {
    case 'double':
    case 'float':
    case 'int':
    case 'long':
      return 'number';
    case 'fixed':
      return 'bytes';
    case 'enum':
      return 'string';
    case 'map':
    case 'record':
      return 'object';
    default:
      return typeName; // boolean, bytes, null, string, array, (union).
  }
}

module.exports = {
  infer: infer  
}
