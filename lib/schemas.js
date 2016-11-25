/* jshint node: true */

'use strict';

/**
 * IDL to schema parsing logic.
 *
 */

var files = require('./files'),
    utils = require('./utils'),
    path = require('path'),
    util = require('util');


var f = util.format;

/**
 * Assemble an IDL file into a decoded schema.
 *
 */
function assembleProtocolSchema(fpath, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  opts = opts || {};
  if (!opts.importHook) {
    opts.importHook = files.createImportHook();
  }

  // Types found in imports. We store them separately to be able to insert them
  // in the correct order in the final attributes.
  var importedTypes = [];
  var schema, imports;
  opts.importHook(fpath, 'idl', function (err, str) {
    if (err) {
      cb(err);
      return;
    }
    if (!str) {
      // Skipped import (likely already imported).
      cb(null, {});
      return;
    }
    try {
      var protocol = _readProtocol(str, opts);
    } catch (err) {
      err.path = fpath; // To help debug which file caused the error.
      cb(err);
      return;
    }
    schema = protocol.schema;
    imports = protocol.imports;
    fetchImports();
  });

  function fetchImports() {
    var info = imports.shift();
    if (!info) {
      // We are done with this file. We prepend all imported types to this
      // file's and we can return the final result.
      if (importedTypes.length) {
        schema.types = schema.types ?
          importedTypes.concat(schema.types) :
          importedTypes;
      }
      cb(null, schema);
    } else {
      var importPath = path.join(path.dirname(fpath), info.name);
      if (info.kind === 'idl') {
        assembleProtocolSchema(importPath, opts, mergeImportedSchema);
      } else {
        // We are importing a protocol or schema file.
        opts.importHook(importPath, info.kind, function (err, str) {
          if (err) {
            cb(err);
            return;
          }
          switch (info.kind) {
            case 'protocol':
            case 'schema':
              if (str === undefined) {
                // Flag used to signal an already imported file by the default
                // import hooks. Implementors who wish to disallow duplicate
                // imports should provide a custom hook which throws an error
                // when a duplicate import is detected.
                mergeImportedSchema(null, {});
                return;
              }
              try {
                var obj = JSON.parse(str);
              } catch (err) {
                err.path = importPath;
                cb(err);
                return;
              }
              var schema = info.kind === 'schema' ? {types: [obj]} : obj;
              mergeImportedSchema(null, schema);
              break;
            default:
              cb(new Error(f('invalid import kind: %s', info.kind)));
          }
        });
      }
    }
  }

  function mergeImportedSchema(err, importedSchema) {
    if (err) {
      cb(err);
      return;
    }
    // Merge  first the types (where we don't need to check for duplicates
    // since `parse` will take care of it), then the messages (where we need
    // to, as duplicates will overwrite each other).
    (importedSchema.types || []).forEach(function (typeSchema) {
      // Ensure the imported protocol's namespace is inherited correctly (it
      // might be different from the current one).
      if (typeSchema.namespace === undefined) {
        var namespace = importedSchema.namespace;
        if (!namespace) {
          var match = /^(.*)\.[^.]+$/.exec(importedSchema.protocol);
          if (match) {
            namespace = match[1];
          }
        }
        typeSchema.namespace = namespace || '';
      }
      importedTypes.push(typeSchema);
    });
    try {
      Object.keys(importedSchema.messages || {}).forEach(function (name) {
        if (!schema.messages) {
          schema.messages = {};
        }
        if (schema.messages[name]) {
          throw new Error(f('duplicate message: %s', name));
        }
        schema.messages[name] = importedSchema.messages[name];
      });
    } catch (err) {
      cb(err);
      return;
    }
    fetchImports(); // Continue importing any remaining imports.
  }
}

// Parsing functions.

/**
 * Convenience function to parse multiple inputs into attributes.
 *
 * It should cover most basic use-cases but has a few limitations:
 *
 * + It doesn't allow passing options to the parsing step.
 * + The protocol/type inference logic can be deceived.
 *
 * The parsing logic is as follows:
 *
 * + If `str` contains a `/` and is a path to an existing file, it will first
 *   be read as JSON, then as an IDL protocol if JSON parsing failed. If either
 *   succeeds, the result is returned, otherwise the next steps are run using
 *   the file's content instead of the input path.
 * + If `str` is a valid JSON string, it is parsed then returned.
 * + If `str` is a valid IDL protocol definition, it is parsed and returned if
 *   no imports are present (and an error is thrown if there are any imports).
 * + If `str` is a valid IDL type definition, it is parsed and returned.
 * + If neither of the above cases apply, `str` is returned.
 *
 */
function parseSchema(str) {
  var schema;
  if (typeof str == 'string' && ~str.indexOf('/') && files.existsSync(str)) {
    // Try interpreting `str` as path to a file contain a JSON schema or an IDL
    // protocol. Note that we add the second check to skip primitive references
    // (e.g. `"int"`, the most common use-case for `parse`).
    var contents = files.readFileSync(str, {encoding: 'utf8'});
    try {
      return JSON.parse(contents);
    } catch (err) {
      var opts = {importHook: files.createSyncImportHook()};
      assembleProtocolSchema(str, opts, function (err, protocolSchema) {
        schema = err ? contents : protocolSchema;
      });
    }
  } else {
    schema = str;
  }
  if (typeof schema != 'string' || schema === 'null') {
    // This last predicate is to allow `parseSchema('null')` to work similarly
    // to `parseSchema('int')` and other primitives (null needs to be handled
    // separately since it is also a valid JSON identifier).
    return schema;
  }
  try {
    return JSON.parse(schema);
  } catch (err) {
    try {
      return parseProtocolSchema(schema);
    } catch (err) {
      try {
        return parseTypeSchema(schema);
      } catch (err) {
        return schema;
      }
    }
  }
}

function parseProtocolSchema(str, opts) {
  var protocol = _readProtocol(str, opts);
  if (protocol.imports.length) {
    // Imports can only be resolved when the IDL file is provided via its
    // path, we fail rather than silently ignore imports.
    throw new Error('unresolvable import');
  }
  return protocol.schema;
}

function parseTypeSchema(str) {
  var tk = new Tokenizer(str);
  var javadoc = _readJavadoc(tk);
  var schema = _readType(tk, javadoc === undefined ? {} : {doc: javadoc});
  tk.next({id: '(eof)'});
  return schema;
}

function _readProtocol(str, opts) {
  var tk = new Tokenizer(str);
  var imports = [];
  var types = [];
  var messages = {};
  var pos;

  // Outer declarations (outside of the protocol block).
  _readImports(tk, imports);
  var protocolSchema = {};
  var protocolJavadoc = _readJavadoc(tk);
  if (protocolJavadoc !== undefined) {
    protocolSchema.doc = protocolJavadoc;
  }
  _readAnnotations(tk, protocolSchema);
  tk.next({val: 'protocol'});
  if (!tk.next({val: '{', silent: true})) {
    // Named protocol.
    protocolSchema.protocol = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
  }

  // Inner declarations.
  while (!tk.next({val: '}', silent: true})) {
    if (!_readImports(tk, imports)) {
      var javadoc = _readJavadoc(tk);
      var typeSchema = _readType(tk);
      var numImports = _readImports(tk, imports, true);
      var message = undefined;
      // We mark our position and try to parse a message from here.
      pos = tk.pos;
      if (!numImports && (message = _readMessage(tk, typeSchema))) {
        // Note that if any imports were found, we cannot be parsing a message.
        if (javadoc !== undefined && message.schema.doc === undefined) {
          message.schema.doc = javadoc;
        }
        var oneWay = false;
        if (
          message.schema.response === 'void' ||
          message.schema.response.type === 'void'
        ) {
          oneWay = !(opts && opts.ackVoidMessages) && !message.schema.errors;
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
          // overwritten (and `parse` won't be able to catch it).
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
  return {schema: protocolSchema, imports: imports};
}

function _readAnnotations(tk, schema) {
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
}

function _readMessage(tk, responseSchema) {
  var schema = {request: [], response: responseSchema};
  _readAnnotations(tk, schema);
  var name = tk.next().val;
  if (tk.next().val !== '(') {
    // This isn't a message.
    return;
  }
  if (!tk.next({val: ')', silent: true})) {
    do {
      schema.request.push(_readField(tk));
    } while (!tk.next({val: ')', silent: true}) && tk.next({val: ','}));
  }
  var token = tk.next();
  switch (token.val) {
    case 'throws':
      // It doesn't seem like the IDL is explicit about which syntax to used
      // for multiple errors. We will assume a comma-separated list.
      schema.errors = [];
      do {
        schema.errors.push(_readType(tk));
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
}

function _readJavadoc(tk) {
  var token = tk.next({id: 'javadoc', emitJavadoc: true, silent: true});
  if (token) {
    return token.val;
  }
}

function _readField(tk) {
  var javadoc = _readJavadoc(tk);
  var schema = {type: _readType(tk)};
  if (javadoc !== undefined && schema.doc === undefined) {
    schema.doc = javadoc;
  }
  _readAnnotations(tk, schema);
  schema.name = tk.next({id: 'name'}).val;
  if (tk.next({val: '=', silent: true})) {
    schema['default'] = tk.next({id: 'json'}).val;
  }
  return schema;
}

function _readType(tk, schema) {
  schema = schema || {};
  _readAnnotations(tk, schema);
  schema.type = tk.next({id: 'name'}).val;
  switch (schema.type) {
    case 'record':
    case 'error':
      return _readRecord(tk, schema);
    case 'fixed':
      return _readFixed(tk, schema);
    case 'enum':
      return _readEnum(tk, schema);
    case 'map':
      return _readMap(tk, schema);
    case 'array':
      return _readArray(tk, schema);
    case 'union':
      if (Object.keys(schema).length > 1) {
        throw new Error('union annotations are not supported');
      }
      return _readUnion(tk);
    default:
      // Reference.
      return Object.keys(schema).length > 1 ? schema : schema.type;
  }
}

function _readFixed(tk, schema) {
  if (!tk.next({val: '(', silent: true})) {
    schema.name = tk.next({id: 'name'}).val;
    tk.next({val: '('});
  }
  schema.size = parseInt(tk.next({id: 'number'}).val);
  tk.next({val: ')'});
  return schema;
}

function _readMap(tk, schema) {
  // Brackets are unwieldy when declaring inline types. We allow for them to be
  // omitted (but we keep the consistency that if the entry bracket is present,
  // the exit one must be as well). Note that this is non-standard.
  var implicitTags = tk.next({val: '<', silent: true}) === undefined;
  schema.values = _readType(tk);
  tk.next({val: '>', silent: implicitTags});
  return schema;
}

function _readArray(tk, schema) {
  var implicitTags = tk.next({val: '<', silent: true}) === undefined;
  schema.items = _readType(tk);
  tk.next({val: '>', silent: implicitTags});
  return schema;
}

function _readEnum(tk, schema) {
  if (!tk.next({val: '{', silent: true})) {
    schema.name = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
  }
  schema.symbols = [];
  do {
    schema.symbols.push(tk.next().val);
  } while (!tk.next({val: '}', silent: true}) && tk.next({val: ','}));
  return schema;
}

function _readUnion(tk) {
  var arr = [];
  tk.next({val: '{'});
  do {
    arr.push(_readType(tk));
  } while (!tk.next({val: '}', silent: true}) && tk.next({val: ','}));
  return arr;
}

function _readRecord(tk, schema) {
  if (!tk.next({val: '{', silent: true})) {
    schema.name = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
  }
  schema.fields = [];
  while (!tk.next({val: '}', silent: true})) {
    schema.fields.push(_readField(tk));
    tk.next({val: ';'});
  }
  return schema;
}

function _readImports(tk, imports, maybeMessage) {
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
}

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
 *
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
      // The specification doesn't explicitly say so, but IDLs likely only
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
 *
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
  assembleProtocolSchema: assembleProtocolSchema,
  parseProtocolSchema: parseProtocolSchema,
  parseSchema: parseSchema,
  parseTypeSchema: parseTypeSchema
};
