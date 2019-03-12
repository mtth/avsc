/* jshint node: true */ 
'use strict';

var utils = require('./utils'),
    util = require('util');

var f = util.format;

function fromJSON(any, type, opts) {
  return clone('FROM_JSON', any, type, opts);
}

function fromDefaultJSON(any, type, opts) {
  return clone('FROM_DEFAULT_JSON', any, type, opts);
}

function toJSON(any, type, opts) {
  return clone('TO_JSON', any, type, opts);
}

/** Deep copy. */
function copy(any, type, opts) {
  return clone('COPY', any, type, opts);
}

function clone(mode, any, type, opts) {
  return new Cloner(mode, opts).clone(any, type, []).build(type);
}

/** Value builder. */
function Builder() {
  this.value = undefined;
  this.errors = [];
}

Builder.prototype.isOk = function () { return !this.errors.length; };

Builder.prototype.build = function (type) {
  if (!this.errors.length) {
    return this.value;
  }
  var details = [];
  var i, l;
  for (i = 0, l = this.errors.length; i < l; i++) {
    details.push(f('\t%s', this.errors[i].message));
  }
  var msg = f(
    '%s error(s) when expecting %s:\n%s',
    this.errors.length, typeInfo(type), details.join('\n')
  );
  throw new Error(msg);
};

Builder.prototype.addError = function (desc, val, type, path) {
  var info = typeInfo(type);
  var msg = f('$%s has %s but %s', joinPath(path), info, desc);
  var err = new Error(msg);
  err.value = val;
  err.expectedType = type;
  err.path = path;
  this.errors.push(err);
};

Builder.prototype.copyErrorsFrom = function (builder) {
  if (builder.errors.length) {
    this.errors = this.errors.concat(builder.errors);
  }
};

function typeInfo(type) {
  if (utils.isType(type, 'union')) {
    var names = type.types.map(function (type) { return type.branchName; });
    return f('a type among %s', names.join(', '));
  } else if (utils.isType(type, 'logical')) {
    return f('type %s (%s)', type.typeName, type.branchName);
  } else {
    return f('type %s', type.branchName);
  }
}

function Cloner(mode, opts) {
  opts = opts || {};
  this._mode = mode; // TO_JSON, FROM_JSON, FROM_DEFAULT_JSON, COPY.
  this._allowUndeclaredFields = !!opts.allowUndeclaredFields;
}

Cloner.prototype.clone = function (any, type, path) {
  switch (type.typeName) {
    case 'array':
      return this._onArray(any, type, path);
    case 'map':
      return this._onMap(any, type, path);
    case 'error':
    case 'record':
      return this._onRecord(any, type, path);
    case 'union:unwrapped':
    case 'union:wrapped':
      return this._onUnion(any, type, path);
    default:
      return utils.isType(type, 'logical') ?
        this._onLogical(any, type, path) :
        this._onPrimitive(any, type, path);
  }
};

Cloner.prototype._onLogical = function (any, type, path) {
  var mode = this._mode;
  var builder = new Builder();
  var desc;
  switch (mode) {
    case 'COPY':
    case 'TO_JSON':
      try {
        builder.value = type._toValue(any);
        if (builder.value === undefined) {
          builder.addError('logical type encoding failed', any, type, path);
          return builder;
        }
      } catch (err) {
        desc = f('logical type encoding failed (%s)', err.message);
        builder.addError(desc, any, type, path);
        return builder;
      }
      break;
    default:
      builder.value = any;
  }
  builder = this.clone(builder.value, type.underlyingType, path);
  if (!builder.isOk()) {
    return builder;
  }
  switch (mode) {
    case 'COPY':
    case 'FROM_JSON':
    case 'FROM_DEFAULT_JSON':
      try {
        builder.value = type._fromValue(builder.value);
      } catch (err) {
        desc = f('logical type decoding failed (%s)', err.message);
        builder.addError(desc, any, type, path);
      }
  }
  return builder;
};

Cloner.prototype._onPrimitive = function (any, type, path) {
  var builder = new Builder();
  var isBufferType = utils.isType(type, 'bytes', 'fixed');
  var val = any;
  if (isBufferType) {
    switch (this._mode) {
      case 'FROM_JSON':
      case 'FROM_DEFAULT_JSON':
        if (typeof any != 'string') {
          builder.addError('is not a string', any, type, path);
          return builder;
        }
        val = utils.bufferFrom(any, 'binary');
    }
  }
  if (type.isValid(val)) {
    builder.value = isBufferType ? utils.bufferFrom(val) : val;
  } else {
    builder.addError('has incompatible value', any, type, path);
  }
  if (this._mode === 'TO_JSON' && isBufferType) {
    builder.value = builder.value.toString('binary');
  }
  return builder;
};

Cloner.prototype._onRecord = function (any, type, path) {
  var builder = new Builder();
  var desc;
  if (!any || typeof any != 'object') {
    builder.addError('is not an object', any, type, path);
    return builder;
  }
  var i, l;
  if (!this._allowUndeclaredFields) {
    var extraFields = [];
    var fieldNames = Object.keys(any);
    var fieldName;
    for (i = 0, l = fieldNames.length; i < l; i++) {
      fieldName = fieldNames[i];
      if (!type.field(fieldName)) {
        extraFields.push(fieldName);
      }
    }
    if (extraFields.length) {
      desc = f('has undeclared field(s) (%s)', extraFields.join(', '));
      builder.addError(desc, any, type, path);
    }
  }
  var missingFields = [];
  var args = [undefined];
  var field, fieldAny, fieldVal, fieldPath, fieldValBuilder;
  for (i = 0, l = type.fields.length; i < l; i++) {
    field = type.fields[i];
    fieldAny = any[field.name];
    fieldVal = undefined;
    if (fieldAny === undefined) {
      if (field.defaultValue() === undefined) {
        missingFields.push(field.name);
      }
    } else {
      fieldPath = path.slice();
      fieldPath.push(field.name);
      fieldValBuilder = this.clone(fieldAny, field.type, fieldPath);
      builder.copyErrorsFrom(fieldValBuilder);
      fieldVal = fieldValBuilder.value;
    }
    args.push(fieldVal);
  }
  if (missingFields.length) {
    desc = f(
      'is missing %s required field(s) (%s)',
      missingFields.length,
      missingFields.join()
    );
    builder.addError(desc, any, type, path);
  }
  if (builder.isOk()) {
    if (this._mode === 'TO_JSON') {
      builder.value = {};
      for (i = 0, l = type.fields.length; i < l; i++) {
        builder.value[type.fields[i].name] = args[i + 1];
      }
    } else {
      var Record = type.recordConstructor;
      builder.value = new (Record.bind.apply(Record, args))();
    }
  }
  return builder;
};

Cloner.prototype._onArray = function (any, type, path) {
  var builder = new Builder();
  if (!Array.isArray(any)) {
    builder.addError('is not an array', any, type, path);
    return builder;
  }
  var val = [];
  var i, l, item;
  for (i = 0, l = any.length; i < l; i++) {
    item = any[i];
    var itemPath = path.slice();
    itemPath.push(i);
    var itemBuilder = this.clone(item, type.itemsType, itemPath);
    builder.copyErrorsFrom(itemBuilder);
    if (builder.isOk()) {
      val.push(itemBuilder.value);
    }
  }
  if (builder.isOk()) {
    builder.value = val;
  }
  return builder;
};

Cloner.prototype._onMap = function (any, type, path) {
  var builder = new Builder();
  if (!any || typeof any != 'object') {
    builder.addError('is not an object', any, type, path);
    return builder;
  }
  var val = {};
  var keys = Object.keys(any).sort();
  var i, l;
  for (i = 0, l = keys.length; i < l; i++) {
    var key = keys[i];
    var anyValue = any[key];
    var valuePath = path.slice();
    valuePath.push(key);
    var valueBuilder = this.clone(anyValue, type.valuesType, valuePath);
    builder.copyErrorsFrom(valueBuilder);
    if (builder.isOk()) {
      val[key] = valueBuilder.value;
    }
  }
  if (builder.isOk()) {
    builder.value = val;
  }
  return builder;
};

Cloner.prototype._onUnion = function (any, unionType, path) {
  var mode = this._mode;
  var isWrapped = unionType.typeName === 'union:wrapped';
  var builder = new Builder();
  if (any === null) {
    var i, l;
    for (i = 0, l = unionType.types.length; i < l; i++) {
      if (unionType.types[i].typeName === 'null') {
        builder.value = null;
        return builder;
      }
    }
    builder.addError('is null', any, unionType, path);
    return builder;
  }
  var branchType;
  switch (mode) {
    case 'FROM_DEFAULT_JSON':
      branchType = unionType.types[0];
      if (branchType.typeName === 'null') {
        builder.addError('does not match first (null)', any, unionType, path);
        return builder;
      }
      any = branchType.wrap(any);
      break;
    case 'COPY':
    case 'TO_JSON':
      if (!isWrapped) {
        branchType = unionType.branchType(any);
        if (!branchType) {
          builder.addError('is not a valid branch', any, unionType, path);
          return builder;
        }
        any = branchType.wrap(any);
      }
  }
  if (typeof any != 'object') { // Null will (correctly) be ignored here.
    builder.addError('is not an object', any, unionType, path);
    return builder;
  }
  var keys = Object.keys(any);
  var reason;
  if (keys.length !== 1) {
    reason = f('has %s keys (%s)', keys.length, keys);
    builder.addError(reason, any, unionType, path);
    return builder;
  }
  var key = keys[0];
  branchType = unionType.type(key);
  if (!branchType) {
    reason = f('contains an unknown branch (%s)', key);
    builder.addError(reason, any, unionType, path);
    return builder;
  }
  var branchPath = path.slice();
  branchPath.push(key);
  var branchBuilder = this.clone(any[key], branchType, branchPath);
  builder.copyErrorsFrom(branchBuilder);
  if (branchBuilder.isOk()) {
    if (mode === 'TO_JSON') {
      builder.value = {};
      builder.value[branchType.branchName] = branchBuilder.value;
    } else if (!isWrapped) {
      builder.value = branchBuilder.value;
    } else {
      builder.value = branchType.wrap(branchBuilder.value);
    }
  }
  return builder;
};

function joinPath(parts) {
  var strs = [];
  var i, l, part;
  for (i = 0, l = parts.length; i < l; i++) {
    part = parts[i];
    if (isNaN(part)) {
      strs.push('.' + part);
    } else {
      strs.push('[' + part + ']');
    }
  }
  return strs.join('');
}

module.exports = {
  fromJSON: fromJSON,
  fromDefaultJSON: fromDefaultJSON,
  toJSON: toJSON,
  copy: copy
};
