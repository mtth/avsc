/**
 * Avro record.
 *
 * Values are represented as instances of a programmatically generated
 * constructor (similar to a "specific record"), available via the
 * `getRecordConstructor` method. This "specific record class" gives
 * significant speedups over using generics objects.
 *
 * Note that vanilla objects are still accepted as valid as long as their
 * fields match (this makes it much more convenient to do simple things like
 * update nested records).
 *
 * This type is also used for errors (similar, except for the extra `Error`
 * constructor call) and for messages (see comment below).
 */
class RecordType extends Type {
  constructor(schema, opts) {
    opts = Object.assign({}, opts);

    if (schema.namespace !== undefined) {
      opts.namespace = schema.namespace;
    } else if (schema.name) {
      // Fully qualified names' namespaces are used when no explicit namespace
      // attribute was specified.
      const ns = utils.impliedNamespace(schema.name);
      if (ns !== undefined) {
        opts.namespace = ns;
      }
    }
    super(schema, opts);

    if (!Array.isArray(schema.fields)) {
      throw new Error(`non-array record fields: ${j(schema.fields)}`);
    }
    if (
      utils.hasDuplicates(schema.fields, (f) => {
        return f.name;
      })
    ) {
      throw new Error(`duplicate field name:${j(schema.fields)}`);
    }
    this._fieldsByName = {};
    this.fields = Object.freeze(
      schema.fields.map(function (f) {
        const field = new Field(f, opts);
        this._fieldsByName[field.name] = field;
        return field;
      }, this)
    );
    this._branchConstructor = this._createBranchConstructor();
    this._isError = schema.type === 'error';
    this.recordConstructor = this._createConstructor(
      opts.errorStackTraces,
      opts.omitRecordMethods
    );
    this._read = this._createReader();
    this._skip = this._createSkipper();
    this._write = this._createWriter();
    this._check = this._createChecker();

    Object.freeze(this);
  }

  _getConstructorName() {
    return this.name
      ? utils.capitalize(utils.unqualify(this.name))
      : this._isError
        ? 'Error$'
        : 'Record$';
  }

  _createConstructor(errorStack, plainRecords) {
    const outerArgs = [];
    const innerArgs = [];
    const ds = []; // Defaults.
    let innerBody = '';
    let stackField;
    for (let i = 0, l = this.fields.length; i < l; i++) {
      const field = this.fields[i];
      const defaultValue = field.defaultValue;
      const hasDefault = defaultValue() !== undefined;
      const name = field.name;
      if (
        errorStack &&
        this._isError &&
        name === 'stack' &&
        Type.isType(field.type, 'string') &&
        !hasDefault
      ) {
        // We keep track of whether we've encountered a valid stack field (in
        // particular, without a default) to populate a stack trace below.
        stackField = field;
      }
      innerArgs.push('v' + i);
      innerBody += '  ';
      if (!hasDefault) {
        innerBody += 'this.' + name + ' = v' + i + ';\n';
      } else {
        innerBody += 'if (v' + i + ' === undefined) { ';
        innerBody += 'this.' + name + ' = d' + ds.length + '(); ';
        innerBody += '} else { this.' + name + ' = v' + i + '; }\n';
        outerArgs.push('d' + ds.length);
        ds.push(defaultValue);
      }
    }
    if (stackField) {
      // We should populate a stack trace.
      innerBody += '  if (this.stack === undefined) { ';
      /* istanbul ignore else */
      if (typeof Error.captureStackTrace == 'function') {
        // v8 runtimes, the easy case.
        innerBody += 'Error.captureStackTrace(this, this.constructor);';
      } else {
        // A few other runtimes (e.g. SpiderMonkey), might not work everywhere.
        innerBody += 'this.stack = Error().stack;';
      }
      innerBody += ' }\n';
    }
    let outerBody = 'return function ' + this._getConstructorName() + '(';
    outerBody += innerArgs.join() + ') {\n' + innerBody + '};';

    const Record = new Function(outerArgs.join(), outerBody).apply(
      undefined,
      ds
    );
    if (plainRecords) {
      return Record;
    }

    const self = this;
    Record.getType = function () {
      return self;
    };
    Record.type = self;
    if (this._isError) {
      Record.prototype = Object.create(Error.prototype, {
        constructor: {
          value: Record,
          enumerable: false,
          writable: true,
          configurable: true,
        },
      });
      Record.prototype.name = this._getConstructorName();
    }
    Record.prototype.clone = function (o) {
      return self.clone(this, o);
    };
    Record.prototype.compare = function (v) {
      return self.compare(this, v);
    };
    Record.prototype.isValid = function (o) {
      return self.isValid(this, o);
    };
    Record.prototype.toBuffer = function () {
      return self.toBuffer(this);
    };
    Record.prototype.toString = function () {
      return self.toString(this);
    };
    Record.prototype.wrap = function () {
      return self.wrap(this);
    };
    Record.prototype.wrapped = Record.prototype.wrap; // Deprecated.
    return Record;
  }

  _createChecker() {
    const names = [];
    const values = [];
    const name = this._getConstructorName();
    let body = 'return function check' + name + '(v, f, h, p) {\n';
    body += '  if (\n';
    body += '    v === null ||\n';
    body += "    typeof v != 'object' ||\n";
    body += '    (f && !this._checkFields(v))\n';
    body += '  ) {\n';
    body += '    if (h) { h(v, this); }\n';
    body += '    return false;\n';
    body += '  }\n';
    if (!this.fields.length) {
      // Special case, empty record. We handle this directly.
      body += '  return true;\n';
    } else {
      let field;
      for (let i = 0, l = this.fields.length; i < l; i++) {
        field = this.fields[i];
        names.push('t' + i);
        values.push(field.type);
        if (field.defaultValue() !== undefined) {
          body += '  var v' + i + ' = v.' + field.name + ';\n';
        }
      }
      body += '  if (h) {\n';
      body += '    var b = 1;\n';
      body += '    var j = p.length;\n';
      body += "    p.push('');\n";
      for (let i = 0, l = this.fields.length; i < l; i++) {
        field = this.fields[i];
        body += "    p[j] = '" + field.name + "';\n";
        body += '    b &= ';
        if (field.defaultValue() === undefined) {
          body += 't' + i + '._check(v.' + field.name + ', f, h, p);\n';
        } else {
          body += 'v' + i + ' === undefined || ';
          body += 't' + i + '._check(v' + i + ', f, h, p);\n';
        }
      }
      body += '    p.pop();\n';
      body += '    return !!b;\n';
      body += '  } else {\n    return (\n      ';
      body += this.fields
        .map((field, i) => {
          return field.defaultValue() === undefined
            ? 't' + i + '._check(v.' + field.name + ', f)'
            : '(v' + i + ' === undefined || t' + i + '._check(v' + i + ', f))';
        })
        .join(' &&\n      ');
      body += '\n    );\n  }\n';
    }
    body += '};';

    return new Function(names.join(), body).apply(undefined, values);
  }

  _createReader() {
    const names = [];
    const values = [this.recordConstructor];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      names.push('t' + i);
      values.push(this.fields[i].type);
    }
    const name = this._getConstructorName();
    let body = 'return function read' + name + '(t) {\n';
    body += '  return new ' + name + '(\n    ';
    body += names
      .map((s) => {
        return s + '._read(t)';
      })
      .join(',\n    ');
    body += '\n  );\n};';
    names.unshift(name);
    // We can do this since the JS spec guarantees that function arguments are
    // evaluated from left to right.

    return new Function(names.join(), body).apply(undefined, values);
  }

  _createSkipper() {
    const args = [];
    let body = 'return function skip' + this._getConstructorName() + '(t) {\n';
    const values = [];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      args.push('t' + i);
      values.push(this.fields[i].type);
      body += '  t' + i + '._skip(t);\n';
    }
    body += '}';

    return new Function(args.join(), body).apply(undefined, values);
  }

  _createWriter() {
    // We still do default handling here, in case a normal JS object is passed.
    const args = [];
    const name = this._getConstructorName();
    let body = 'return function write' + name + '(t, v) {\n';
    const values = [];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      const field = this.fields[i];
      args.push('t' + i);
      values.push(field.type);
      body += '  ';
      if (field.defaultValue() === undefined) {
        body += 't' + i + '._write(t, v.' + field.name + ');\n';
      } else {
        const value = field.type.toBuffer(field.defaultValue());
        args.push('d' + i);
        values.push(value);
        body += 'var v' + i + ' = v.' + field.name + ';\n';
        body += 'if (v' + i + ' === undefined) {\n';
        body += '    t.writeFixed(d' + i + ', ' + value.length + ');\n';
        body += '  } else {\n    t' + i + '._write(t, v' + i + ');\n  }\n';
      }
    }
    body += '}';

    return new Function(args.join(), body).apply(undefined, values);
  }

  _update(resolver, type, opts) {
    if (!hasCompatibleName(this, type, !opts.ignoreNamespaces)) {
      throw new Error(`no alias found for ${type.name}`);
    }

    const rFields = this.fields;
    const wFields = type.fields;
    const wFieldsMap = utils.toMap(wFields, (f) => {
      return f.name;
    });

    const innerArgs = []; // Arguments for reader constructor.
    const resolvers = {}; // Resolvers keyed by writer field name.
    for (let i = 0; i < rFields.length; i++) {
      const field = rFields[i];
      const names = getAliases(field);
      const matches = [];
      for (let j = 0; j < names.length; j++) {
        const name = names[j];
        if (wFieldsMap[name]) {
          matches.push(name);
        }
      }
      if (matches.length > 1) {
        throw new Error(
          `ambiguous aliasing for ${type.name}.${field.name} (${matches.join(', ')})`
        );
      }
      if (!matches.length) {
        if (field.defaultValue() === undefined) {
          throw new Error(
            `no matching field for default-less ${type.name}.${field.name}`
          );
        }
        innerArgs.push('undefined');
      } else {
        const name = matches[0];
        const fieldResolver = {
          resolver: field.type.createResolver(wFieldsMap[name].type, opts),
          name: '_' + field.name, // Reader field name.
        };
        if (!resolvers[name]) {
          resolvers[name] = [fieldResolver];
        } else {
          resolvers[name].push(fieldResolver);
        }
        innerArgs.push(fieldResolver.name);
      }
    }

    // See if we can add a bypass for unused fields at the end of the record.
    let lazyIndex = -1;
    let i = wFields.length;
    while (i && resolvers[wFields[--i].name] === undefined) {
      lazyIndex = i;
    }

    const uname = this._getConstructorName();
    const args = [uname];
    const values = [this.recordConstructor];
    let body = '  return function read' + uname + '(t, b) {\n';
    for (let i = 0; i < wFields.length; i++) {
      if (i === lazyIndex) {
        body += '  if (!b) {\n';
      }
      const field = type.fields[i];
      const name = field.name;
      if (resolvers[name] === undefined) {
        body += ~lazyIndex && i >= lazyIndex ? '    ' : '  ';
        args.push('r' + i);
        values.push(field.type);
        body += 'r' + i + '._skip(t);\n';
      } else {
        let j = resolvers[name].length;
        while (j--) {
          body += ~lazyIndex && i >= lazyIndex ? '    ' : '  ';
          args.push('r' + i + 'f' + j);
          const fieldResolver = resolvers[name][j];
          values.push(fieldResolver.resolver);
          body += 'var ' + fieldResolver.name + ' = ';
          body += 'r' + i + 'f' + j + '._' + (j ? 'peek' : 'read') + '(t);\n';
        }
      }
    }
    if (~lazyIndex) {
      body += '  }\n';
    }
    body += '  return new ' + uname + '(' + innerArgs.join() + ');\n};';

    resolver._read = new Function(args.join(), body).apply(undefined, values);
  }

  _match(tap1, tap2) {
    const fields = this.fields;
    for (let i = 0, l = fields.length; i < l; i++) {
      const field = fields[i];
      let order = field._order;
      const type = field.type;
      if (order) {
        order *= type._match(tap1, tap2);
        if (order) {
          return order;
        }
      } else {
        type._skip(tap1);
        type._skip(tap2);
      }
    }
    return 0;
  }

  _checkFields(obj) {
    const keys = Object.keys(obj);
    for (let i = 0, l = keys.length; i < l; i++) {
      if (!this._fieldsByName[keys[i]]) {
        return false;
      }
    }
    return true;
  }

  _copy(val, opts) {
    const hook = opts && opts.fieldHook;
    const values = [undefined];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      const field = this.fields[i];
      let value = val[field.name];
      if (
        value === undefined &&
        Object.prototype.hasOwnProperty.call(field, 'defaultValue')
      ) {
        value = field.defaultValue();
      }
      if ((opts && !opts.skip) || value !== undefined) {
        value = field.type._copy(value, opts);
      }
      if (hook) {
        value = hook(field, value, this);
      }
      values.push(value);
    }
    const Record = this.recordConstructor;
    return new (Record.bind.apply(Record, values))();
  }

  _deref(schema, derefed, opts) {
    schema.fields = this.fields.map((field) => {
      const fieldType = field.type;
      const fieldSchema = {
        name: field.name,
        type: fieldType._attrs(derefed, opts),
      };
      if (opts.exportAttrs) {
        const val = field.defaultValue();
        if (val !== undefined) {
          // We must both unwrap all unions and coerce buffers to strings.
          fieldSchema['default'] = fieldType._copy(val, {coerce: 3, wrap: 3});
        }
        const fieldOrder = field.order;
        if (fieldOrder !== 'ascending') {
          fieldSchema.order = fieldOrder;
        }
        const fieldAliases = field.aliases;
        if (fieldAliases.length) {
          fieldSchema.aliases = fieldAliases;
        }
        const fieldDoc = field.doc;
        if (fieldDoc !== undefined) {
          fieldSchema.doc = fieldDoc;
        }
      }
      return fieldSchema;
    });
  }

  compare(val1, val2) {
    const fields = this.fields;
    for (let i = 0, l = fields.length; i < l; i++) {
      const field = fields[i];
      const name = field.name;
      let order = field._order;
      const type = field.type;
      if (order) {
        order *= type.compare(val1[name], val2[name]);
        if (order) {
          return order;
        }
      }
    }
    return 0;
  }

  field(name) {
    return this._fieldsByName[name];
  }

  getField(name) {
    return this._fieldsByName[name];
  }

  getFields() {
    return this.fields;
  }

  getRecordConstructor() {
    return this.recordConstructor;
  }

  get typeName() {
    return this._isError ? 'error' : 'record';
  }
}

/** A record field. */
class Field {
  constructor(schema, opts) {
    const name = schema.name;
    if (typeof name != 'string' || !utils.isValidName(name)) {
      throw new Error(`invalid field name: ${name}`);
    }

    this.name = name;
    this.type = Type.forSchema(schema.type, opts);
    this.aliases = schema.aliases || [];
    this.doc = schema.doc !== undefined ? '' + schema.doc : undefined;

    this._order = (function (order) {
      switch (order) {
        case 'ascending':
          return 1;
        case 'descending':
          return -1;
        case 'ignore':
          return 0;
        default:
          throw new Error(`invalid order: ${j(order)}`);
      }
    })(schema.order === undefined ? 'ascending' : schema.order);

    const value = schema['default'];
    if (value !== undefined) {
      // We need to convert defaults back to a valid format (unions are
      // disallowed in default definitions, only the first type of each union is
      // allowed instead).
      // http://apache-avro.679487.n3.nabble.com/field-union-default-in-Java-td1175327.html
      const type = this.type;
      let val;
      try {
        val = type._copy(value, {coerce: 2, wrap: 2});
      } catch (err) {
        let msg = `incompatible field default ${j(value)} (${err.message})`;
        if (Type.isType(type, 'union')) {
          const t = j(type.types[0]);
          msg += `, union defaults must match the first branch's type (${t})`;
        }
        throw new Error(msg);
      }
      // The clone call above will throw an error if the default is invalid.
      if (isPrimitive(type.typeName) && type.typeName !== 'bytes') {
        // These are immutable.
        this.defaultValue = function () {
          return val;
        };
      } else {
        this.defaultValue = function () {
          return type._copy(val);
        };
      }
    }

    Object.freeze(this);
  }

  defaultValue() {} // Undefined default.

  getDefault() {}

  getAliases() {
    return this.aliases;
  }

  getName() {
    return this.name;
  }

  getOrder() {
    return this.order;
  }

  getType() {
    return this.type;
  }

  get order() {
    return ['descending', 'ignore', 'ascending'][this._order + 1];
  }
}
