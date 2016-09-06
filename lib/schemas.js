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
function assemble(fpath, opts, cb) {
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
  var attrs, imports;
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
      var protocol = parse(str, opts);
    } catch (err) {
      err.path = fpath; // To help debug which file caused the error.
      cb(err);
      return;
    }
    attrs = protocol.attrs;
    imports = protocol.imports;
    fetchImports();
  });

  function fetchImports() {
    var info = imports.shift();
    if (!info) {
      // We are done with this file. We prepend all imported types to this
      // file's and we can return the final result. We also perform a JSON
      // serialization rountrip to remove non-numerical attributes from unions
      // and transform Javadocs into strings.
      if (importedTypes.length) {
        attrs.types = attrs.types ?
          importedTypes.concat(attrs.types) :
          importedTypes;
      }
      cb(null, JSON.parse(JSON.stringify(attrs)));
    } else {
      var importPath = path.join(path.dirname(fpath), info.name);
      if (info.kind === 'idl') {
        assemble(importPath, opts, mergeImportedAttrs);
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
              try {
                var obj = JSON.parse(str);
              } catch (err) {
                err.path = importPath;
                cb(err);
                return;
              }
              var attrs = info.kind === 'schema' ? {types: [obj]} : obj;
              mergeImportedAttrs(null, attrs);
              break;
            default:
              cb(new Error(f('invalid import kind: %s', info.kind)));
          }
        });
      }
    }
  }

  function mergeImportedAttrs(err, importedAttrs) {
    if (err) {
      cb(err);
      return;
    }
    // Merge  first the types (where we don't need to check for duplicates
    // since `parse` will take care of it), then the messages (where we need
    // to, as duplicates will overwrite each other).
    (importedAttrs.types || []).forEach(function (typeAttrs) {
      // Ensure the imported protocol's namespace is inherited correctly (it
      // might be different from the current one).
      if (typeAttrs.namespace === undefined) {
        var namespace = importedAttrs.namespace;
        if (!namespace) {
          var match = /^(.*)\.[^.]+$/.exec(importedAttrs.protocol);
          if (match) {
            namespace = match[1];
          }
        }
        typeAttrs.namespace = namespace || '';
      }
      importedTypes.push(typeAttrs);
    });
    try {
      Object.keys(importedAttrs.messages || {}).forEach(function (name) {
        if (!attrs.messages) {
          attrs.messages = {};
        }
        if (attrs.messages[name]) {
          throw new Error(f('duplicate message: %s', name));
        }
        attrs.messages[name] = importedAttrs.messages[name];
      });
    } catch (err) {
      cb(err);
      return;
    }
    fetchImports(); // Continue importing any remaining imports.
  }
}

/**
 * Parse an IDL into attributes.
 *
 * Not to be confused with `avro.parse` which parses attributes into types.
 *
 */
function parse(str, opts) {
  var parser = new Parser(str, opts);
  return {attrs: parser._readProtocol(), imports: parser._imports};
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
 * + `json`, special, must be asked for (the tokenizer doesn't have enough
 *   context to predict these).
 *
 * This tokenizer also handles Javadoc extraction, via the `addJavadoc` method.
 *
 */
function Tokenizer(str) {
  this._str = str;
  this._pos = 0;
  this._queue = new BoundedQueue(3); // Bounded queue of last emitted tokens.
  this._token = undefined; // Current token.
  this._doc = undefined; // Javadoc.
}

Tokenizer.prototype.get = function (opts) {
  if (opts && opts.id && opts.id !== this._token.id) {
    throw this.error(f('expected %s but got %s', opts.id, this._token.val));
  } else if (opts && opts.val && opts.val !== this._token.val) {
    throw this.error(f('expected %s but got %s', opts.val, this._token.val));
  } else {
    return this._token;
  }
};

Tokenizer.prototype.next = function (opts) {
  this._skip();
  this._queue.push(this._pos);
  var pos = this._pos;
  var str = this._str;
  var c = str.charAt(pos);
  var id;

  if (!c) {
    if (opts && opts.id === '(eof)') {
      return {id: '(eof)'};
    } else {
      throw this.error('unexpected end of input');
    }
  }

  if (opts && opts.id === 'json') {
    id = 'json';
    this._pos = this._endOfJson();
  } else if (c === '"') {
    id = 'string';
    this._pos = this._endOfString();
  } else if (/[0-9]/.test(c)) {
    id = 'number';
    this._pos = this._endOf(/[0-9]/);
  } else if (/[`A-Za-z_.]/.test(c)) {
    id = 'name';
    this._pos = this._endOf(/[`A-Za-z0-9_.]/);
  } else {
    id = 'operator';
    this._pos = pos + 1;
  }

  this._token = {id: id, val: str.slice(pos, this._pos)};
  if (id === 'json') {
    // Let's be nice and give a more helpful error message when this occurs
    // (JSON parsing errors wouldn't let us find the location otherwise).
    try {
      this._token.val = JSON.parse(this._token.val);
    } catch (err) {
      throw this.error('invalid JSON');
    }
  } else if (id === 'name') {
    // Unescape names (our parser doesn't need them).
    this._token.val = this._token.val.replace(/`/g, '');
  }
  return this.get(opts);
};

Tokenizer.prototype.prev = function (opts) {
  var pos = this._queue.pop();
  if (pos === undefined) {
    throw new Error('cannot backtrack more');
  }
  this._pos = pos;
  return this.get(opts);
};

Tokenizer.prototype.error = function (msg) {
  var pos = this._queue.peek() || 1; // Use after whitespace position.
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
  var err = new Error(msg);
  err.lineNum = lineNum;
  err.colNum = pos - lineStart;
  return err;
};

Tokenizer.prototype.addJavadoc = function (attrs) {
  if (this._doc === undefined || attrs.doc !== undefined) {
    return;
  }
  attrs.doc = this._doc;
  this._doc = undefined;
};

/** Skip whitespace and comments. */
Tokenizer.prototype._skip = function () {
  var str = this._str;
  var pos, c; // `pos` used for javadocs.

  while ((c = str.charAt(this._pos)) && /\s/.test(c)) {
    this._pos++;
  }
  if (c === '/') {
    switch (str.charAt(this._pos + 1)) {
    case '/':
      this._pos += 2;
      while ((c = str.charAt(this._pos)) && c !== '\n') {
        this._pos++;
      }
      return this._skip();
    case '*':
      this._pos += 2;
      if (str.charAt(this._pos) === '*') {
        pos = this._pos + 1;
      }
      while ((c = str.charAt(this._pos++))) {
        if (c === '*' && str.charAt(this._pos) === '/') {
          this._pos++;
          if (pos !== undefined) {
            this._doc = new Javadoc(str.slice(pos, this._pos - 2));
          }
          return this._skip();
        }
      }
      throw this.error('unterminated comment');
    }
  }
};

/** Generic end of method. */
Tokenizer.prototype._endOf = function (pat) {
  var pos = this._pos;
  var str = this._str;
  while (pat.test(str.charAt(pos))) {
    pos++;
  }
  return pos;
};

/**
 * Find end of a string.
 *
 * The specification doesn't explicitly say so, but IDLs likely only allow
 * double quotes for strings (C- and Java-style).
 *
 */
Tokenizer.prototype._endOfString = function () {
  var pos = this._pos + 1; // Skip first double quote.
  var str = this._str;
  var c;
  while ((c = str.charAt(pos))) {
    if (c === '"') {
      return pos + 1;
    }
    if (c === '\\') {
      pos += 2;
    } else {
      pos++;
    }
  }
  throw this.error('unterminated string');
};

/**
 * Returns end of JSON object, throwing an error if the end is reached first.
 *
 */
Tokenizer.prototype._endOfJson = function () {
  var pos = utils.jsonEnd(this._str, this._pos);
  if (pos < 0) {
    throw new Error('invalid JSON at ' + this._pos);
  }
  return pos;
};

/**
 * Parser from tokens to attributes.
 *
 */
function Parser(str, opts) {
  this._oneWayVoid = !!(opts && opts.oneWayVoid);
  this._reassignJavadoc = !!(opts && opts.reassignJavadoc);
  this._imports = [];
  this._tk = new Tokenizer(str);
  this._tk.next(); // Prime tokenizer.
}

Parser.prototype._readProtocol = function () {
  var tk = this._tk;
  var attrs = {};
  var types = [];
  var messages = {};
  var hasMessage = false;
  while (tk.get().val === 'import') {
    this._readImport();
  }
  while (tk.get().val === '@') {
    this._readAnnotation(attrs);
  }
  tk.addJavadoc(attrs);
  tk.get({val: 'protocol'});
  attrs.protocol = tk.next({id: 'name'}).val;
  tk.next({val: '{'});
  tk.next();
  while (tk.get().val !== '}') {
    if (tk.get().val === 'import') {
      this._readImport();
    } else {
      var typeAttrs = this._readType();
      if (typeAttrs.name) {
        // This was a named type declaration. Not very clean to rely on this,
        // but since the IDL spec doesn't consistently delimit type
        // declaration (e.g. fixed end with `;` but other bracketed types
        // don't) we aren't able to tell whether this is the start of a
        // message otherwise.
        types.push(typeAttrs);
      } else {
        hasMessage = true;
        var oneWay = false;
        if (typeAttrs === 'void' || typeAttrs.type === 'void') {
          if (this._oneWayVoid) {
            oneWay = true;
          }
          if (typeAttrs === 'void') {
            typeAttrs = 'null';
          } else {
            typeAttrs.type = 'null';
          }
        }
        var message = this._readMessage(typeAttrs);
        if (oneWay) {
          message.attrs['one-way'] = true;
        }
        if (messages[message.name]) {
          // We have to do this check here otherwise the duplicate will be
          // overwritten (and `parse` won't be able to catch it).
          throw new Error(f('duplicate message: %s', message.name));
        }
        messages[message.name] = message.attrs;
      }
    }
  }
  tk.next({id: '(eof)'});
  if (types.length) {
    attrs.types = types;
  }
  if (hasMessage) {
    attrs.messages = messages;
  }
  return attrs;
};

Parser.prototype._readImport = function () {
  var tk = this._tk;
  tk.get({val: 'import'});
  var kind = tk.next({id: 'name'}).val;
  var fname = JSON.parse(tk.next({id: 'string'}).val);
  this._imports.push({kind: kind, name: fname});
  tk.next({val: ';'});
  tk.next();
};

Parser.prototype._readAnnotation = function (attrs) {
  var tk = this._tk;
  tk.get({val: '@'});
  // Annotations are allowed to have names which aren't valid Avro names,
  // we must advance until we hit the first left parenthesis.
  var parts = [];
  while (tk.next().val !== '(') {
    parts.push(tk.get().val);
  }
  attrs[parts.join('')] = tk.next({id: 'json'}).val;
  tk.next({val: ')'});
  tk.next();
};

Parser.prototype._readMessage = function (responseAttrs) {
  var tk = this._tk;
  var messageAttrs;
  if (this._reassignJavadoc) {
    messageAttrs = {};
    messageAttrs.response = reassignJavadoc(responseAttrs, messageAttrs);
  } else {
    messageAttrs = {response: responseAttrs};
  }
  while (tk.get().val === '@') {
    this._readAnnotation(messageAttrs);
  }

  var name = tk.get({id: 'name'}).val;
  messageAttrs.request = [];
  tk.next({val: '('});
  if (tk.next().val !== ')') {
    tk.prev();
    do {
      tk.next(); // Skip `(` or `,`.
      messageAttrs.request.push(this._readField());
    } while (tk.get().val !== ')');
  }
  if (tk.next().val === 'throws') {
    // It doesn't seem like the IDL allows multiple error types, even though
    // the spec always prescribes a union (or they don't indicate which
    // syntax to use). To be safe, we'll only allow one custom error type.
    tk.next();
    messageAttrs.errors = [this._readType()];
  } else if (tk.get().val === 'oneway') {
    tk.next();
    messageAttrs['one-way'] = true;
  }
  tk.get({val: ';'});
  tk.next();
  return {name: name, attrs: messageAttrs};
};

Parser.prototype._readField = function () {
  var tk = this._tk;
  var attrs = {type: this._readType()};
  if (this._reassignJavadoc) {
    attrs.type = reassignJavadoc(attrs.type, attrs);
  }
  while (tk.get().val === '@') {
    this._readAnnotation(attrs);
  }
  tk.addJavadoc(attrs);
  attrs.name = tk.get({id: 'name'}).val;
  if (tk.next().val === '=') {
    attrs['default'] = tk.next({id: 'json'}).val;
    tk.next();
  }
  return attrs;
};

Parser.prototype._readType = function () {
  var tk = this._tk;
  var attrs = {};
  while (tk.get().val === '@') {
    this._readAnnotation(attrs);
  }
  tk.addJavadoc(attrs);

  switch (tk.get().val) {
    case 'record':
    case 'error':
      return this._readRecord(attrs);
    case 'fixed':
      return this._readFixed(attrs);
    case 'enum':
      return this._readEnum(attrs);
    case 'map':
      return this._readMap(attrs);
    case 'array':
      return this._readArray(attrs);
    case 'union':
      return this._readUnion(attrs);
    default:
      var type = tk.get().val;
      tk.next();
      if (Object.keys(attrs).length) {
        attrs.type = type;
        return attrs;
      } else {
        return type;
      }
  }
};

Parser.prototype._readFixed = function (attrs) {
  var tk = this._tk;
  attrs.type = tk.get({val: 'fixed'}).val;
  attrs.name = tk.next({id: 'name'}).val;
  tk.next({val: '('});
  attrs.size = parseInt(tk.next({id: 'number'}).val);
  tk.next({val: ')'});
  if (tk.next().val === ';') {
    tk.next();
  }
  return attrs;
};

Parser.prototype._readMap = function (attrs) {
  var tk = this._tk;
  attrs.type = tk.get({val: 'map'}).val;
  tk.next({val: '<'});
  tk.next();
  attrs.values = this._readType();
  tk.get({val: '>'});
  tk.next();
  return attrs;
};

Parser.prototype._readArray = function (attrs) {
  var tk = this._tk;
  attrs.type = tk.get({val: 'array'}).val;
  tk.next({val: '<'});
  tk.next();
  attrs.items = this._readType();
  tk.get({val: '>'});
  tk.next();
  return attrs;
};

Parser.prototype._readEnum = function (attrs) {
  var tk = this._tk;
  attrs.type = tk.get({val: 'enum'}).val;
  attrs.name = tk.next({id: 'name'}).val;
  tk.next({val: '{'});
  attrs.symbols = [];
  do {
    attrs.symbols.push(tk.next().val);
  } while (tk.next().val !== '}');
  tk.next();
  return attrs;
};

Parser.prototype._readUnion = function (attrs) {
  var tk = this._tk;
  var arr = [];
  tk.get({val: 'union'});
  tk.next({val: '{'});
  do {
    tk.next();
    arr.push(this._readType());
  } while (tk.get().val !== '}');
  tk.next();
  Object.keys(attrs).forEach(function (name) {
    // We can do this since `JSON.stringify` will ignore non-numeric keys on
    // array objects. This lets us be consistent with field and message
    // attribute transfer (e.g. for `doc` and `order`).
    arr[name] = attrs[name];
  });
  return arr;
};

Parser.prototype._readRecord = function (attrs) {
  var tk = this._tk;
  attrs.type = tk.get({id: 'name'}).val;
  attrs.name = tk.next({id: 'name'}).val;
  attrs.fields = [];
  tk.next({val: '{'});
  while (tk.next().val !== '}') {
    attrs.fields.push(this._readField());
    tk.get({val: ';'});
  }
  tk.next();
  return attrs;
};

/**
 * Simple bounded queue.
 *
 * Not the fastest, but will definitely do.
 *
 */
function BoundedQueue(length) {
  this._length = length | 0;
  this._data = [];
}

BoundedQueue.prototype.push = function (val) {
  this._data.push(val);
  if (this._data.length > this._length) {
    this._data.shift();
  }
};

BoundedQueue.prototype.peek = function () {
  return this._data[this._data.length - 1];
};

BoundedQueue.prototype.pop = function () { return this._data.pop(); };

/**
 * Javadoc wrapper class.
 *
 * This is used to be able to distinguish between normal `doc` annotations and
 * Javadoc comments, to correctly support the `reassignJavadoc` option.
 *
 * The parsing done is very simple and simply removes the line prefixes and
 * leading / trailing empty lines. It's better to be conservative with
 * formatting rather than risk losing information.
 *
 */
function Javadoc(str) {
  str = str.replace(/^[ \t]+|[ \t]+$/g, ''); // Trim whitespace.
  var lines = str.split('\n').map(function (line, i) {
    return i ? line.replace(/^\s*\*\s?/, '') : line;
  });
  while (!lines[0]) {
    lines.shift();
  }
  while (!lines[lines.length - 1]) {
    lines.pop();
  }
  this._str = lines.join('\n');
}

Javadoc.prototype.toJSON = function () { return this._str; };

/**
 * Transfer a key from an object to another and return the new source.
 *
 * If the source becomes an object with a single type attribute set, its `type`
 * attribute is returned instead.
 *
 */
function reassignJavadoc(from, to) {
  if (!(from.doc instanceof Javadoc)) {
    // Nothing to transfer.
    return from;
  }
  to.doc = from.doc;
  delete from.doc;
  return Object.keys(from).length === 1 ? from.type : from;
}


module.exports = {
  BoundedQueue: BoundedQueue,
  Tokenizer: Tokenizer,
  assemble: assemble,
  parse: parse
};
