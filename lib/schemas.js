/* jshint node: true */

'use strict';

/**
 * Schema parsing and loading utilities.
 *
 */

var fs = require('fs');


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
 * Assemble a schema from one or more IDL files.
 *
 */
function assemble(fpath, cb) {
  fs.readFile(fpath, function (err, data) {
    if (err) {
      cb(err);
      return;
    }

    data.split(/([ \t\r]+|[^A-Za-z0-9_])/);
    // var n = 0;


  });
}


// Helpers.


// ids: json, literal, name, operator
function Tokenizer(str) {
  this._str = str;
  this._pos = 0;
}

Tokenizer.prototype.advance = function (id, noSkip) {
  var pos = this._pos;
  var str = this._str;
  var c;

  if (!noSkip) {
    while ((c = str.charAt(pos)) && /\s/.test(c)) {
      // Skip whitespace.
      pos++;
    }
    this._pos = pos;
  }

  if (!c) {
    // EOF.
    return null;
  }

  if (c === '/') {
    // Check if we must skip a comment.
    switch (str.charAt(pos + 1)) {
    case '/':
      pos += 2;
      while ((c = str.charAt(pos)) && c !== '\n') {
        pos++;
      }
      return this.advance(id, noSkip);
    case '*':
      while ((c = str.charAt(pos++))) {
        if (c === '*' && str.charAt(pos) === '/') {
          return this.advance(id, noSkip);
        }
      }
      throw new Error('unterminated comment at ' + this._pos);
    }
  }

  if (id === 'json') {
    this._pos = endOfJson(str, pos);
  } else if (c === '"') {
    // Specification doesn't explicitly say, but IDLs likely only allow double
    // quotes for strings (C- and Java-style).
    id = 'string';
    this._pos = endOfString(str, pos);
  } else if (/[A-Za-z_]/.test(c)) {
    id = 'literal';
    this._pos = endOfLiteral(str, pos);
  } else {
    id = 'operator';
    this._pos = pos + 1;
  }
  return {id: id, val: str.slice(pos, this._pos)};
};

function endOfString(str, start) {
  var pos = (start | 0) + 1;
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
  throw new Error('unterminated string at ' + start);
}

function endOfLiteral(str, start) {
  var pos = (start | 0) + 1; // Skip first character.
  while (/[A-Za-z0-9_]/.test(str.charAt(pos))) {
    pos++;
  }
  return pos;
}

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
function endOfJson(str, start) {
  var pos = start | 0;

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

  throw new Error('invalid JSON at ' + start);
}


module.exports = {
  Tokenizer: Tokenizer,
  assemble: assemble,
  endOfJson: endOfJson,
  load: load
};
