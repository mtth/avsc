/* jshint node: true */

'use strict';

/* jshint node: true */

// TODO: Add option to prefix nested type declarations with the outer types'
// names.

/** IDL to protocol (services) and schema (types) parsing logic. */

var utils = require('./utils'),
    util = require('util');

var f = util.format;

// Default type references defined by Avro.
var TYPE_REFS = {
  date: {type: 'int', logicalType: 'date'},
  decimal: {type: 'bytes', logicalType: 'decimal'},
  time_ms: {type: 'long', logicalType: 'time-millis'},
  timestamp_ms: {type: 'long', logicalType: 'timestamp-millis'}
};

/** Parse an ILD spec into a protocol. */
function readProtocol(str, opts) {
  var reader = new Reader(str, opts);
  var tk = reader.tk;
  var imports = [];
  var types = [];
  var messages = {};
  var pos;

  // Outer declarations (outside of the protocol block).
  reader.readImports(imports);
  var protocolSchema = {};
  var protocolJavadoc = reader.readJavadoc();
  if (protocolJavadoc !== undefined) {
    protocolSchema.doc = protocolJavadoc;
  }
  reader.readAnnotations(protocolSchema);
  tk.next({val: 'protocol'});
  if (!tk.next({val: '{', silent: true})) {
    // Named protocol.
    protocolSchema.protocol = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
  }

  // Inner declarations.
  while (!tk.next({val: '}', silent: true})) {
    if (!reader.readImports(imports)) {
      var javadoc = reader.readJavadoc();
      var typeSchema = reader.readType();
      var numImports = reader.readImports(imports, true);
      var message = undefined;
      // We mark our position and try to parse a message from here.
      pos = tk.pos;
      if (!numImports && (message = reader.readMessage(typeSchema))) {
        // Note that if any imports were found, we cannot be parsing a message.
        if (javadoc !== undefined && message.schema.doc === undefined) {
          message.schema.doc = javadoc;
        }
        var oneWay = false;
        if (
          message.schema.response === 'void' ||
          message.schema.response.type === 'void'
        ) {
          oneWay = !reader._ackVoidMessages && !message.schema.errors;
          if (message.schema.response === 'void') {
            message.schema.response = 'null';
          } else {
            message.schema.response.type = 'null';
          }
        }
        if (oneWay) {
          message.schema['one-way'] = true;
        }
        if (messages[message.name]) {
          // We have to do this check here otherwise the duplicate will be
          // overwritten (and service instantiation won't be able to catch it).
          throw new Error(f('duplicate message: %s', message.name));
        }
        messages[message.name] = message.schema;
      } else {
        // This was a standalone type definition.
        if (javadoc) {
          if (typeof typeSchema == 'string') {
            typeSchema = {doc: javadoc, type: typeSchema};
          } else if (typeSchema.doc === undefined) {
            typeSchema.doc = javadoc;
          }
        }
        types.push(typeSchema);
        // We backtrack until just before the type's type name and swallow an
        // eventual semi-colon (to make type declarations more consistent).
        tk.pos = pos;
        tk.next({val: ';', silent: true});
      }
      javadoc = undefined;
    }
  }
  tk.next({id: '(eof)'});
  if (types.length) {
    protocolSchema.types = types;
  }
  if (Object.keys(messages).length) {
    protocolSchema.messages = messages;
  }
  return {protocol: protocolSchema, imports: imports};
}

function readSchema(str, opts) {
  var reader = new Reader(str, opts);
  var javadoc = reader.readJavadoc();
  var schema = reader.readType(javadoc === undefined ? {} : {doc: javadoc});
  reader.tk.next({id: '(eof)'}); // Check that we have read everything.
  return schema;
}

function Reader(str, opts) {
  opts = opts || {};

  this.tk = new Tokenizer(str);
  this._ackVoidMessages = !!opts.ackVoidMessages;
  this._implicitTags = !opts.delimitedCollections;
  this._typeRefs = opts.typeRefs || TYPE_REFS;
}

Reader.prototype.readAnnotations = function (schema) {
  var tk = this.tk;
  while (tk.next({val: '@', silent: true})) {
    // Annotations are allowed to have names which aren't valid Avro names,
    // we must advance until we hit the first left parenthesis.
    var parts = [];
    while (!tk.next({val: '(', silent: true})) {
      parts.push(tk.next().val);
    }
    schema[parts.join('')] = tk.next({id: 'json'}).val;
    tk.next({val: ')'});
  }
};

Reader.prototype.readMessage = function (responseSchema) {
  var tk = this.tk;
  var schema = {request: [], response: responseSchema};
  this.readAnnotations(schema);
  var name = tk.next().val;
  if (tk.next().val !== '(') {
    // This isn't a message.
    return;
  }
  if (!tk.next({val: ')', silent: true})) {
    do {
      schema.request.push(this.readField());
    } while (!tk.next({val: ')', silent: true}) && tk.next({val: ','}));
  }
  var token = tk.next();
  switch (token.val) {
    case 'throws':
      // It doesn't seem like the IDL is explicit about which syntax to used
      // for multiple errors. We will assume a comma-separated list.
      schema.errors = [];
      do {
        schema.errors.push(this.readType());
      } while (!tk.next({val: ';', silent: true}) && tk.next({val: ','}));
      break;
    case 'oneway':
      schema['one-way'] = true;
      tk.next({val: ';'});
      break;
    case ';':
      break;
    default:
      throw tk.error('invalid message suffix', token);
  }
  return {name: name, schema: schema};
};

Reader.prototype.readJavadoc = function () {
  var token = this.tk.next({id: 'javadoc', emitJavadoc: true, silent: true});
  if (token) {
    return token.val;
  }
};

Reader.prototype.readField = function () {
  var tk = this.tk;
  var javadoc = this.readJavadoc();
  var schema = {type: this.readType()};
  if (javadoc !== undefined && schema.doc === undefined) {
    schema.doc = javadoc;
  }
  this.readAnnotations(schema);
  schema.name = tk.next({id: 'name'}).val;
  if (tk.next({val: '=', silent: true})) {
    schema['default'] = tk.next({id: 'json'}).val;
  }
  return schema;
};

Reader.prototype.readType = function (schema) {
  schema = schema || {};
  this.readAnnotations(schema);
  schema.type = this.tk.next({id: 'name'}).val;
  switch (schema.type) {
    case 'record':
    case 'error':
      return this.readRecord(schema);
    case 'fixed':
      return this.readFixed(schema);
    case 'enum':
      return this.readEnum(schema);
    case 'map':
      return this.readMap(schema);
    case 'array':
      return this.readArray(schema);
    case 'union':
      if (Object.keys(schema).length > 1) {
        throw new Error('union annotations are not supported');
      }
      return this.readUnion();
    default:
      // Reference.
      var ref = this._typeRefs[schema.type];
      if (ref) {
        delete schema.type; // Always overwrite the type.
        utils.copyOwnProperties(ref, schema);
      }
      return Object.keys(schema).length > 1 ? schema : schema.type;
  }
};

Reader.prototype.readFixed = function (schema) {
  var tk = this.tk;
  if (!tk.next({val: '(', silent: true})) {
    schema.name = tk.next({id: 'name'}).val;
    tk.next({val: '('});
  }
  schema.size = parseInt(tk.next({id: 'number'}).val);
  tk.next({val: ')'});
  return schema;
};

Reader.prototype.readMap = function (schema) {
  var tk = this.tk;
  // Brackets are unwieldy when declaring inline types. We allow for them to be
  // omitted (but we keep the consistency that if the entry bracket is present,
  // the exit one must be as well). Note that this is non-standard.
  var silent = this._implicitTags;
  var implicitTags = tk.next({val: '<', silent: silent}) === undefined;
  schema.values = this.readType();
  tk.next({val: '>', silent: implicitTags});
  return schema;
};

Reader.prototype.readArray = function (schema) {
  var tk = this.tk;
  var silent = this._implicitTags;
  var implicitTags = tk.next({val: '<', silent: silent}) === undefined;
  schema.items = this.readType();
  tk.next({val: '>', silent: implicitTags});
  return schema;
};

Reader.prototype.readEnum = function (schema) {
  var tk = this.tk;
  if (!tk.next({val: '{', silent: true})) {
    schema.name = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
  }
  schema.symbols = [];
  do {
    schema.symbols.push(tk.next().val);
  } while (!tk.next({val: '}', silent: true}) && tk.next({val: ','}));
  return schema;
};

Reader.prototype.readUnion = function () {
  var tk = this.tk;
  var arr = [];
  tk.next({val: '{'});
  do {
    arr.push(this.readType());
  } while (!tk.next({val: '}', silent: true}) && tk.next({val: ','}));
  return arr;
};

Reader.prototype.readRecord = function (schema) {
  var tk = this.tk;
  if (!tk.next({val: '{', silent: true})) {
    schema.name = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
  }
  schema.fields = [];
  while (!tk.next({val: '}', silent: true})) {
    schema.fields.push(this.readField());
    tk.next({val: ';'});
  }
  return schema;
};

Reader.prototype.readImports = function (imports, maybeMessage) {
  var tk = this.tk;
  var numImports = 0;
  var pos = tk.pos;
  while (tk.next({val: 'import', silent: true})) {
    if (!numImports && maybeMessage && tk.next({val: '(', silent: true})) {
      // This will happen if a message is named import.
      tk.pos = pos;
      return;
    }
    var kind = tk.next({id: 'name'}).val;
    var fname = JSON.parse(tk.next({id: 'string'}).val);
    tk.next({val: ';'});
    imports.push({kind: kind, name: fname});
    numImports++;
  }
  return numImports;
};

// Helpers.

/**
 * Simple class to split an input string into tokens.
 *
 * There are different types of tokens, characterized by their `id`:
 *
 * + `number` numbers.
 * + `name` references.
 * + `string` double-quoted.
 * + `operator`, anything else, always single character.
 * + `javadoc`, only emitted when `next` is called with `emitJavadoc` set.
 * + `json`, only emitted when `next` is called with `'json'` as `id` (the
 *   tokenizer doesn't have enough context to predict these).
 */
function Tokenizer(str) {
  this._str = str;
  this.pos = 0;
}

Tokenizer.prototype.next = function (opts) {
  var token = {pos: this.pos, id: undefined, val: undefined};
  var javadoc = this._skip(opts && opts.emitJavadoc);
  if (javadoc) {
    token.id = 'javadoc';
    token.val = javadoc;
  } else {
    var pos = this.pos;
    var str = this._str;
    var c = str.charAt(pos);
    if (!c) {
      token.id = '(eof)';
    } else {
      if (opts && opts.id === 'json') {
        token.id = 'json';
        this.pos = this._endOfJson();
      } else if (c === '"') {
        token.id = 'string';
        this.pos = this._endOfString();
      } else if (/[0-9]/.test(c)) {
        token.id = 'number';
        this.pos = this._endOf(/[0-9]/);
      } else if (/[`A-Za-z_.]/.test(c)) {
        token.id = 'name';
        this.pos = this._endOf(/[`A-Za-z0-9_.]/);
      } else {
        token.id = 'operator';
        this.pos = pos + 1;
      }
      token.val = str.slice(pos, this.pos);
      if (token.id === 'json') {
        // Let's be nice and give a more helpful error message when this occurs
        // (JSON parsing errors wouldn't let us find the location otherwise).
        try {
          token.val = JSON.parse(token.val);
        } catch (err) {
          throw this.error('invalid JSON', token);
        }
      } else if (token.id === 'name') {
        // Unescape names (our parser doesn't need them).
        token.val = token.val.replace(/`/g, '');
      }
    }
  }

  var err;
  if (opts && opts.id && opts.id !== token.id) {
    err = this.error(f('expected ID %s', opts.id), token);
  } else if (opts && opts.val && opts.val !== token.val) {
    err = this.error(f('expected value %s', opts.val), token);
  }
  if (!err) {
    return token;
  } else if (opts && opts.silent) {
    this.pos = token.pos; // Backtrack to start of token.
    return undefined;
  } else {
    throw err;
  }
};

Tokenizer.prototype.error = function (reason, context) {
  // Context must be either a token or a position.
  var isToken = typeof context != 'number';
  var pos = isToken ? context.pos : context;
  var str = this._str;
  var lineNum = 1;
  var lineStart = 0;
  var i;
  for (i = 0; i < pos; i++) {
    if (str.charAt(i) === '\n') {
      lineNum++;
      lineStart = i;
    }
  }
  var msg = isToken ? f('invalid token %j: %s', context, reason) : reason;
  var err = new Error(msg);
  err.token = isToken ? context : undefined;
  err.lineNum = lineNum;
  err.colNum = pos - lineStart;
  return err;
};

/** Skip whitespace and comments. */
Tokenizer.prototype._skip = function (emitJavadoc) {
  var str = this._str;
  var isJavadoc = false;
  var pos, c;

  while ((c = str.charAt(this.pos)) && /\s/.test(c)) {
    this.pos++;
  }
  pos = this.pos;
  if (c === '/') {
    switch (str.charAt(this.pos + 1)) {
    case '/':
      this.pos += 2;
      while ((c = str.charAt(this.pos)) && c !== '\n') {
        this.pos++;
      }
      return this._skip(emitJavadoc);
    case '*':
      this.pos += 2;
      if (str.charAt(this.pos) === '*') {
        isJavadoc = true;
      }
      while ((c = str.charAt(this.pos++))) {
        if (c === '*' && str.charAt(this.pos) === '/') {
          this.pos++;
          if (isJavadoc && emitJavadoc) {
            return extractJavadoc(str.slice(pos + 3, this.pos - 2));
          }
          return this._skip(emitJavadoc);
        }
      }
      throw this.error('unterminated comment', pos);
    }
  }
};

/** Generic end of method. */
Tokenizer.prototype._endOf = function (pat) {
  var pos = this.pos;
  var str = this._str;
  while (pat.test(str.charAt(pos))) {
    pos++;
  }
  return pos;
};

/** Find end of a string. */
Tokenizer.prototype._endOfString = function () {
  var pos = this.pos + 1; // Skip first double quote.
  var str = this._str;
  var c;
  while ((c = str.charAt(pos))) {
    if (c === '"') {
      // The spec doesn't explicitly say so, but IDLs likely only
      // allow double quotes for strings (C- and Java-style).
      return pos + 1;
    }
    if (c === '\\') {
      pos += 2;
    } else {
      pos++;
    }
  }
  throw this.error('unterminated string', pos - 1);
};

/** Find end of JSON object, throwing an error if the end is reached first. */
Tokenizer.prototype._endOfJson = function () {
  var pos = utils.jsonEnd(this._str, this.pos);
  if (pos < 0) {
    throw this.error('invalid JSON', pos);
  }
  return pos;
};

/**
 * Extract Javadoc contents from the comment.
 *
 * The parsing done is very simple and simply removes the line prefixes and
 * leading / trailing empty lines. It's better to be conservative with
 * formatting rather than risk losing information.
 */
function extractJavadoc(str) {
  var lines = str
    .replace(/^[ \t]+|[ \t]+$/g, '') // Trim whitespace.
    .split('\n').map(function (line, i) {
      return i ? line.replace(/^\s*\*\s?/, '') : line;
    });
  while (!lines[0]) {
    lines.shift();
  }
  while (!lines[lines.length - 1]) {
    lines.pop();
  }
  return lines.join('\n');
}


module.exports = {
  Tokenizer: Tokenizer,
  readProtocol: readProtocol,
  readSchema: readSchema,
};
