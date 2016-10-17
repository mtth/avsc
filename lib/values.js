/* jshint node: true */

'use strict';

// TODO: Add a few built-in logical types to help inference (e.g. dates)?

/**
 * From values to types.
 *
 * This file also contains the (very related) logic to combine types.
 *
 */

var types = require('./types'),
    utils = require('./utils');


// Convenience imports.
var f = utils.format;
var createType = types.createType;
var getTypeBucket = types.getTypeBucket;

// Placeholder type used when inferring the type of an empty array.
var EMPTY_ARRAY_TYPE = createType({type: 'array', items: 'null'});

/**
 * Infer a type from a value.
 *
 */
function infer(val, opts) {
  opts = opts || {};

  // Optional custom inference hook.
  if (opts.valueHook) {
    var type = opts.valueHook(val, opts);
    if (type !== undefined) {
      if (!types.Type.isType(type)) {
        throw new Error(f('invalid value hook return value: %j', type));
      }
      return type;
    }
  }

  // Default inference logic.
  switch (typeof val) {
    case 'string':
      return createType('string', opts);
    case 'boolean':
      return createType('boolean', opts);
    case 'number':
      if ((val | 0) === val) {
        return createType('int', opts);
      } else if (Math.abs(val) < 9007199254740991) {
        return createType('float', opts);
      }
      return createType('double', opts);
    case 'object':
      if (val === null) {
        return createType('null', opts);
      } else if (Array.isArray(val)) {
        if (!val.length) {
          return EMPTY_ARRAY_TYPE;
        }
        return createType({
          type: 'array',
          items: combine(val.map(function (v) { return infer(v, opts); }))
        }, opts);
      } else if (Buffer.isBuffer(val)) {
        return createType('bytes', opts);
      }
      var fieldNames = Object.keys(val);
      if (fieldNames.some(function (s) { return !types.isValidName(s); })) {
        // We have to fall back to a map.
        return createType({
          type: 'map',
          values: combine(fieldNames.map(function (s) {
            return infer(val[s], opts);
          }), opts)
        }, opts);
      }
      return createType({
        type: 'record',
        fields: fieldNames.map(function (s) {
          return {name: s, type: infer(val[s], opts)};
        })
      }, opts);
    default:
      throw new Error(f('cannot infer type from: %j', val));
  }
}

/**
 * Combine types into one.
 *
 */
function combine(types, opts) {
  if (!types.length) {
    throw new Error('no types to combine');
  }
  if (types.length === 1) {
    return types[0]; // Nothing to do.
  }
  opts = opts || {};

  // Extract any union types, with special care for wrapped unions (see below).
  var expanded = [];
  var numWrappedUnions = 0;
  var isValidWrappedUnion = true;
  types.forEach(function (type) {
    switch (type.getTypeName()) {
      case 'union:unwrapped':
        isValidWrappedUnion = false;
        expanded = expanded.concat(type.getTypes());
        break;
      case 'union:wrapped':
        numWrappedUnions++;
        expanded = expanded.concat(type.getTypes());
        break;
      case 'null':
        expanded.push(type);
        break;
      default:
        isValidWrappedUnion = false;
        expanded.push(type);
    }
  });
  if (numWrappedUnions) {
    if (!isValidWrappedUnion) {
      // It is only valid to combine wrapped unions when no other type is
      // present other than wrapped unions and nulls (otherwise the values of
      // others wouldn't be valid in the resulting union).
      throw new Error('cannot combine wrapped union');
    }
    var branchTypes = {};
    expanded.forEach(function (type) {
      var name = type.getName(true);
      var branchType = branchTypes[name];
      if (!branchType) {
        branchTypes[name] = type;
      } else if (!type.equals(branchType)) {
        throw new Error('inconsistent branch type');
      }
    });
    var wrapUnions = opts.wrapUnions;
    var unionType;
    opts.wrapUnions = true;
    try {
      unionType = createType(Object.keys(branchTypes).map(function (name) {
        return branchTypes[name];
      }), opts);
    } catch (err) {
      opts.wrapUnions = wrapUnions;
      throw err;
    }
    opts.wrapUnions = wrapUnions;
    return unionType;
  }

  // Group types by category, similar to the logic for unwrapped unions.
  var bucketized = {};
  expanded.forEach(function (type) {
    var bucket = getTypeBucket(type);
    var bucketTypes = bucketized[bucket];
    if (!bucketTypes) {
      bucketized[bucket] = bucketTypes = [];
    }
    bucketTypes.push(type);
  });

  // Generate the "augmented" type for each group.
  var buckets = Object.keys(bucketized);
  var augmented = buckets.map(function (bucket) {
    var bucketTypes = bucketized[bucket];
    if (bucketTypes.length === 1) {
      return bucketTypes[0];
    } else {
      switch (bucket) {
        case 'null':
        case 'boolean':
          return bucketTypes[0];
        case 'number':
          return combineNumbers(bucketTypes);
        case 'string':
          return combineStrings(bucketTypes, opts);
        case 'buffer':
          return combineBuffers(bucketTypes, opts);
        case 'array':
          // Remove any sentinel arrays (used when inferring from empty arrays)
          // to avoid making things nullable when they shouldn't be.
          bucketTypes = bucketTypes.filter(function (t) {
            return t !== EMPTY_ARRAY_TYPE;
          });
          if (!bucketTypes.length) {
            // We still don't have a real type, just return the sentinel.
            return EMPTY_ARRAY_TYPE;
          }
          return createType({
            type: 'array',
            items: combine(bucketTypes.map(function (t) {
              return t.getItemsType();
            }))
          }, opts);
        default:
          return combineObjects(bucketTypes, opts);
      }
    }
  });

  if (augmented.length === 1) {
    return augmented[0];
  } else {
    // We return an (unwrapped) union of all augmented types.
    return createType(augmented, opts);
  }
}

/**
 * Combine number types.
 *
 * Note that never have to create a new type here, we are guaranteed to be able
 * to reuse one of the input types as super-type.
 *
 */
function combineNumbers(types) {
  var typeNames = ['int', 'long', 'float', 'double'];
  var superIndex = -1;
  var superType = null;
  var i, l, type, index;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    index = typeNames.indexOf(type.getTypeName());
    if (index > superIndex) {
      superIndex = index;
      superType = type;
    }
  }
  return superType;
}

/**
 * Combine enums and strings.
 *
 * The order of the returned symbols is undefined and the returned enum is
 * anonymous.
 *
 */
function combineStrings(types, opts) {
  var symbols = {};
  var i, l, type, typeSymbols;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    if (type.getTypeName() === 'string') {
      // If at least one of the types is a string, it will be the supertype.
      return type;
    }
    typeSymbols = type.getSymbols();
    var j, m;
    for (j = 0, m = typeSymbols.length; j < m; j++) {
      symbols[typeSymbols[j]] = true;
    }
  }
  return createType({type: 'enum', symbols: Object.keys(symbols)}, opts);
}

/**
 * Combine bytes and fixed.
 *
 * This function is optimized to avoid creating new types when possible: in
 * case of a size mismatch between fixed types, it will continue looking
 * through the array to find an existing bytes type (rather than exit early by
 * creating one eagerly).
 *
 */
function combineBuffers(types, opts) {
  var size = -1;
  var i, l, type;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    if (type.getTypeName() === 'bytes') {
      return type;
    }
    if (size === -1) {
      size = type.getSize();
    } else if (type.getSize() !== size) {
      // Don't create a bytes type right away, we might be able to reuse one
      // later on in the types array. Just mark this for now.
      size = -2;
    }
  }
  return size < 0 ? createType('bytes', opts) : types[0];
}

/**
 * Combine maps and records.
 *
 * Field defaults are kept when possible (i.e. when no coercion to a map
 * happens), with later definitions overriding previous ones.
 *
 */
function combineObjects(types, opts) {
  var allTypes = []; // Field and value types.
  var fieldTypes = {}; // Record field types grouped by field name.
  var fieldDefaults = {};
  var isValidRecord = true;

  // Check whether the final type will be a map or a record.
  var i, l, type, fields;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    if (type.getTypeName() === 'map') {
      isValidRecord = false;
      allTypes.push(type.getValuesType());
    } else {
      fields = type.getFields();
      var j, m, field, fieldDefault, fieldName, fieldType;
      for (j = 0, m = fields.length; j < m; j++) {
        field = fields[j];
        fieldName = field.getName();
        fieldType = field.getType();
        allTypes.push(fieldType);
        if (isValidRecord) {
          if (!fieldTypes[fieldName]) {
            fieldTypes[fieldName] = [];
          }
          fieldTypes[fieldName].push(fieldType);
          fieldDefault = field.getDefault();
          if (fieldDefault !== undefined) {
            // Later defaults will override any previous ones.
            fieldDefaults[fieldName] = fieldDefault;
          }
        }
      }
    }
  }

  if (isValidRecord) {
    // Check that no fields are missing and that we have the approriate
    // defaults for those which are.
    var fieldNames = Object.keys(fieldTypes);
    for (i = 0, l = fieldNames.length; i < l; i++) {
      fieldName = fieldNames[i];
      if (
        fieldTypes[fieldName].length < types.length &&
        fieldDefaults[fieldName] === undefined
      ) {
        // At least one of the records is missing a field with no default.
        if (opts && opts.strictDefaults) {
          isValidRecord = false;
        } else {
          fieldTypes[fieldName].unshift(createType('null', opts));
          fieldDefaults[fieldName] = null;
        }
      }
    }
  }

  var attrs;
  if (isValidRecord) {
    attrs = {
      type: 'record',
      fields: fieldNames.map(function (s) {
        var fieldType = combine(fieldTypes[s], opts);
        var fieldDefault = fieldDefaults[s];
        if (
          fieldDefault !== undefined &&
          ~fieldType.getTypeName().indexOf('union')
        ) {
          // Ensure that the default's corresponding type is first.
          var unionTypes = fieldType.getTypes();
          var i, l;
          for (i = 0, l = unionTypes.length; i < l; i++) {
            if (unionTypes[i].isValid(fieldDefault)) {
              break;
            }
          }
          if (i > 0) {
            var unionType = unionTypes[0];
            unionTypes[0] = unionTypes[i];
            unionTypes[i] = unionType;
            fieldType = createType(unionTypes, opts);
          }
        }
        return {
          name: s,
          type: fieldType,
          'default': fieldDefaults[s]
        };
      })
    };
  } else {
    attrs = {
      type: 'map',
      values: combine(allTypes, opts)
    };
  }
  return createType(attrs, opts);
}


module.exports = {
  combine: combine,
  infer: infer
};
