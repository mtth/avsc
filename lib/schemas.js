/* jshint node: true */

'use strict';

/**
 * Schema parsing and loading utilities.
 *
 */

var fs = require('fs'),
    path = require('path'),
    util = require('util');


var f = util.format;


/**
 * Try to load a schema.
 *
 * This method will attempt to load schemas from a file if the schema passed is
 * a string which isn't valid JSON and contains at least one slash.
 *
 */
function load(schema) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      if (~schema.indexOf('/')) {
        // This can't be a valid name, so we interpret is as a filepath. This
        // makes is always feasible to read a file, independent of its name
        // (i.e. even if its name is valid JSON), by prefixing it with `./`.
        obj = JSON.parse(fs.readFileSync(schema));
      }
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return obj;
}

/**
 * Assemble an IDL file into a decoded schema.
 *
 * attrs: name, namespace, types, messages, (and any other annotations?)
 *
 */
function assemble(fpath, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }

  opts = opts || {};
  opts.imports = opts.imports || {};

  var dpath = path.dirname(fpath);
  var attrs = opts.imports[fpath];
  if (attrs) {
    // Already imported, just skip. We don't do any circularity checks because
    // any such errors will be caught at parsing time.
    cb(null, attrs);
    return;
  }
  attrs = opts.imports[fpath] = {types: [], messages: {}};

  var tokenizer, token;
  fs.readFile(fpath, {encoding: 'utf8'}, function (err, str) {
    if (err) {
      cb(err);
      return;
    }
    tokenizer = new Tokenizer(str);
    assemblePreamble(null, null, cb);
  });

  function assemblePreamble(err, importedAttrs) {
    // Merge imported attributes. We do this first to avoid duplicating error
    // checking (we guard it by a branch instead).
    if (importedAttrs) {
      attrs.types = attrs.types.concat(importedAttrs.types);
      Object.keys(importedAttrs.messages).forEach(function (name) {
        if (attrs.messages[name]) {
          err = new Error(f('duplicate message: %s', name));
          return;
        }
        attrs.messages[name] = importedAttrs.messages[name];
      });
    }

    if (err) {
      cb(err);
      return;
    }

    token = tokenizer.next();
    if (token.val === 'import') {
      assembleImport();
      return;
    }

    try {
      while (token.val === '@') {
        assembleAnnotation(attrs);
        token = tokenizer.next();
      }
      if (token.val !== 'protocol') {
        throw tokenizer.error('expected protocol definition');
      }
      assembleProtocol();
      if (tokenizer.next()) {
        throw tokenizer.error('trailing input');
      }
      cb(null, attrs);
    } catch (err) {
      cb(new Error(f('%s when parsing %s', err.message, fpath)));
    }
  }

  function assembleImport() {
    try {
      var importKind = tokenizer.next({id: 'name'}).val;
      var fpath = path.join(dpath, JSON.parse(tokenizer.next({id: 'string'})));
      tokenizer.next({val: ';'});
    } catch (err) {
      cb(err);
      return;
    }

    if (token.val === 'idl') {
      assemble(fpath, opts, assemblePreamble);
    } else {
      fs.readFile(fpath, function (err, str) {
        if (err) {
          cb(err);
          return;
        }

        try {
          var attrs = JSON.parse(str);
        } catch (err) {
          cb(err);
          return;
        }

        switch (importKind) {
        case 'protocol':
          assemblePreamble(null, attrs);
          break;
        case 'types':
          assemblePreamble(null, {types: [attrs]});
          break;
        default:
          assemblePreamble(tokenizer.error('invalid import'));
        }
      });
    }
  }

  function assembleAnnotation() {
    // Annotations are allowed to have names which aren't valid Avro names, we
    // must advance until we hit the first left parenthesis.
    var parts = [];
    var token;
    while ((token = tokenizer.next())) {
      if (token.val === '(') {
        // Done parsing the annotation's name.
        attrs[parts.join('')] = JSON.parse(tokenizer.next({id: 'json'}).val);
        tokenizer.next({val: ')'});
        return;
      }
      parts.push(token.val);
    }
    throw tokenizer.error('unexpected end of annotation');
  }

  function assembleProtocol() {
    attrs.name = tokenizer.next({id: 'name'});
    tokenizer.next({val: '{'});
    var typeAttrs, token;
    while ((token = tokenizer.next())) {
      if (token.val === '}') {
        return;
      }

      // Parse a type or message definition (and potentially any preceding
      // annotations).
      typeAttrs = {};
      while (token && token.val === '@') {
        assembleAnnotation(typeAttrs);
        token = tokenizer.next();
      }

      // TODO: backtick escaping.

      switch (token.val) {
      case 'record':
      case 'error':
      case 'fixed':
      case 'enum':
      case 'union':
      case 'map':
      case 'array':
        // assembleType(typeAttrs, token.val);
        break;
      default:
        // name = assembleMessage(subattrs, token.val);
        // attrs.messages[name] = subattrs;
      }
    }
    throw tokenizer.error('unexpected end of protocol');
  }

  // function assembleMessage(attrs, ) {}

  // function assembleType(attrs, type) {
  //   attrs.type = name;
  // } // Also errors.

  // function assembleFixed(attrs) {}

  // function assembleEnum(attrs) {}
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
 */
function Tokenizer(str) {
  this._str = str;
  this._pos = 0;
  this._queue = new BoundedQueue(3); // Bounded queue of last emitted tokens.
}

Tokenizer.prototype.next = function (opts) {
  var pos = this._pos;
  var str = this._str;
  var c, id;

  if (opts && opts.noSkip) {
    c = str.charAt(this._pos);
  } else {
    while ((c = str.charAt(this._pos)) && /\s/.test(c)) {
      // Skip whitespace.
      this._pos++;
    }
  }

  if (c === '/') {
    // Check if we must skip a comment.
    switch (str.charAt(this._pos + 1)) {
    case '/':
      this._pos += 2;
      while ((c = str.charAt(this._pos)) && c !== '\n') {
        this._pos++;
      }
      break;
    case '*':
      this._pos += 2;
      while ((c = str.charAt(this._pos++))) {
        if (c === '*' && str.charAt(this._pos) === '/') {
          this._pos++;
          break;
        }
      }
      throw this.error('unterminated comment');
    }
  }

  // Save previous position, in case the consumer backtracks. We save both the
  // position before whitespace gets stripped and after to ensure correct
  // behavior even when the options will change on the next call (but still
  // produce good error messages).
  this._queue.push([pos, this._pos]);
  pos = this._pos;

  if (!c) {
    // EOF.
    if (opts && (opts.id || opts.val)) {
      throw this.error('unexpected end of input');
    }
    return null;
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
  } else if (/[A-Za-z_]/.test(c)) {
    id = 'name';
    this._pos = this._endOf(/[A-Za-z0-9_]/);
  } else {
    id = 'operator';
    this._pos = pos + 1;
  }

  var token = {id: id, val: str.slice(pos, this._pos)};
  if (opts && opts.id && opts.id !== token.id) {
    throw this.error(f('expected %s but got %s', opts.id, id));
  } else if (opts && opts.val && opts.val !== token.val) {
    throw this.error(f('expected %s but got %s', opts.val, token.val));
  } else {
    return token;
  }
};

Tokenizer.prototype.prev = function () {
  var pos = this._queue.pop()[0]; // Before any whitespace.
  if (pos === undefined) {
    throw new Error('cannot backtrack more');
  }
  this._pos = pos;
};

Tokenizer.prototype.error = function (msg) {
  var pos = this._queue.peek()[1]; // Use after whitespace position.
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
  return new Error(f('%s at %d:%d', msg, lineNum, pos - lineStart));
};

/**
 * Generic end of method.
 *
 */
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
  this.throwError('unterminated string at ' + this._pos);
};

/**
 * Returns end of JSON object, -1 if the end of the string is reached first.
 *
 * To keep the implementation simple, this function isn't a JSON validator. It
 * will gladly return a result for invalid JSON (which is OK since that will be
 * promptly rejected by the JSON parser). What matters is that it is guaranteed
 * to return the correct end when presented with valid JSON.
 *
 * As a side note, it is unfortunate that this is required. Mixing JSON and
 * custom IDL syntax is not at all elegant (some tokens now have multiple
 * meanings, depending on whether they are part of a default definition or
 * not).
 *
 */
Tokenizer.prototype._endOfJson = function () {
  var pos = this._pos;
  var str = this._str;

  // Handle the case of a single number separately (JSON numbers always start
  // either with `-` or a digit).
  var c = str.charAt(pos++);
  if (/[\d-]/.test(c)) {
    while (/[eE\d.+-]/.test(str.charAt(pos))) {
      pos++;
    }
    return pos;
  }

  // String, object, or array.
  var depth = 0;
  var literal = false;
  do {
    switch (c) {
    case '{':
    case '[':
      if (!literal) { depth++; }
      break;
    case '}':
    case ']':
      if (!literal && !--depth) {
        return pos;
      }
      break;
    case '"':
      literal = !literal;
      if (!depth && !literal) {
        return pos;
      }
      break;
    case '\\':
      pos++; // Skip the next character.
    }
  } while ((c = str.charAt(pos++)));

  throw new Error('invalid JSON at ' + this._pos);
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

BoundedQueue.prototype.pop = function () { return this._data.pop(); };

BoundedQueue.prototype.peek = function () {
  return this._data[this._data.length - 1];
};


module.exports = {
  BoundedQueue: BoundedQueue,
  Tokenizer: Tokenizer,
  assemble: assemble,
  load: load
};
