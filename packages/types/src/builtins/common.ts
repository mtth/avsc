import {BaseType, Type} from '../interfaces.js';
import {Tap} from '../binary.js';
import {printJSON as j} from '../utils.js';

// Encoding tap (shared for performance).
const TAP = Tap.withCapacity(1024);

// Currently active logical type, used for name redirection.
let activeLogicalType: Type | null = null;

// Underlying types of logical types currently being instantiated. This is used
// to be able to reference names (i.e. for branches) during instantiation.
const activeUnderlyingTypes: [Type, Type][] = [];

/**
 * "Abstract" base Avro type.
 *
 * This class' constructor will register any named types to support recursive
 * schemas. All type values are represented in memory similarly to their JSON
 * representation, except for:
 *
 * + `bytes` and `fixed` which are represented as `Uint8Array`s.
 * + `union`s which will be "unwrapped" unless the `wrapUnions` option is set.
 *
 *  See individual subclasses for details.
 */
export abstract class RealType<V = any> implements BaseType<V> {
  readonly typeName: string;
  readonly name: string | undefined;
  readonly aliases: string[] | undefined;
  readonly doc: string | undefined;
  protected constructor(schema: NamedSchema, opts?: TypeOptions) {
    let type;
    if (activeLogicalType) {
      type = activeLogicalType;
      activeUnderlyingTypes.push([activeLogicalType, this]);
      activeLogicalType = null;
    } else {
      type = this;
    }
    this.name = undefined;
    this.aliases = undefined;
    this.doc = schema && schema.doc ? '' + schema.doc : undefined;

    if (schema) {
      // This is a complex (i.e. non-primitive) type.
      let name = schema.name;
      const namespace =
        schema.namespace === undefined
          ? opts && opts.namespace
          : schema.namespace;
      if (name !== undefined) {
        // This isn't an anonymous type.
        name = maybeQualify(name, namespace);
        if (isPrimitive(name)) {
          // Avro doesn't allow redefining primitive names.
          throw new Error(`cannot rename primitive type: ${j(name)}`);
        }
        const registry = opts && opts.registry;
        if (registry) {
          if (registry[name] !== undefined) {
            throw new Error(`duplicate type name: ${name}`);
          }
          registry[name] = type;
        }
      } else if (opts && opts.noAnonymousTypes) {
        throw new Error(`missing name property in schema: ${j(schema)}`);
      }
      this.name = name;
      this.aliases = schema.aliases
        ? schema.aliases.map((s: string) => {
            return maybeQualify(s, namespace);
          })
        : [];
    }
  }

  static __reset(size: number): void {
    TAP.reinitialize(size);
  }

  get branchName(): string {
    const type = Type.isType(this, 'logical') ? this.underlyingType : this;
    if (type.name) {
      return type.name;
    }
    if (Type.isType(type, 'abstract')) {
      return type._concreteTypeName;
    }
    return Type.isType(type, 'union') ? undefined : type.typeName;
  }

  clone(val: V, opts?: CloneOptions): any {
    if (opts) {
      opts = {
        coerce: !!opts.coerceBuffers | 0, // Coerce JSON to Buffer.
        fieldHook: opts.fieldHook,
        qualifyNames: !!opts.qualifyNames,
        skip: !!opts.skipMissingFields,
        wrap: !!opts.wrapUnions | 0, // Wrap first match into union.
      };
      return this._copy(val, opts);
    }
    // If no modifications are required, we can get by with a serialization
    // roundtrip (generally much faster than a standard deep copy).
    return this.fromBuffer(this.toBuffer(val));
  }

  compareBuffers(buf1: Uint8Array, buf2: Uint8Array): number {
    return this._match(Tap.fromBuffer(buf1), Tap.fromBuffer(buf2));
  }

  createResolver(type: Type, opts?: CreateResolverOptions): TypeResolver {
    if (!Type.isType(type)) {
      // More explicit error message than the "incompatible type" thrown
      // otherwise (especially because of the overridden `toJSON` method).
      throw new Error(`not a type: ${j(type)}`);
    }

    if (
      !Type.isType(this, 'union', 'logical') &&
      Type.isType(type, 'logical')
    ) {
      // Trying to read a logical type as a built-in: unwrap the logical type.
      // Note that we exclude unions to support resolving into unions containing
      // logical types.
      return this.createResolver(type.underlyingType, opts);
    }

    opts = Object.assign({}, opts);
    opts.registry = opts.registry || {};

    let resolver, key;
    if (
      Type.isType(this, 'record', 'error') &&
      Type.isType(type, 'record', 'error')
    ) {
      // We allow conversions between records and errors.
      key = this.name + ':' + type.name; // ':' is illegal in Avro type names.
      resolver = opts.registry[key];
      if (resolver) {
        return resolver;
      }
    }

    resolver = new TypeResolver(this);
    if (key) {
      // Register resolver early for recursive schemas.
      opts.registry[key] = resolver;
    }

    if (Type.isType(type, 'union')) {
      const resolvers = type.types.map(function (t) {
        return this.createResolver(t, opts);
      }, this);
      resolver._read = function (tap) {
        const index = tap.readLong();
        const resolver = resolvers[index];
        if (resolver === undefined) {
          throw new Error(`invalid union index: ${index}`);
        }
        return resolvers[index]._read(tap);
      };
    } else {
      this._update(resolver, type, opts);
    }

    if (!resolver._read) {
      throw new Error(`cannot read ${type} as ${this}`);
    }
    return Object.freeze(resolver);
  }

  decode(buf: Uint8Array, pos?: number, resolver?: TypeResolver): V {
    const tap = Tap.fromBuffer(buf, pos);
    const val = readValue(this, tap, resolver);
    if (!tap.isValid()) {
      return {value: undefined, offset: -1};
    }
    return {value: val, offset: tap.pos};
  }

  encode(val: V, buf: Uint8Array, pos?: number): number {
    const tap = Tap.fromBuffer(buf, pos);
    this._write(tap, val);
    if (!tap.isValid()) {
      // Don't throw as there is no way to predict this. We also return the
      // number of missing bytes to ease resizing.
      return buf.length - tap.pos;
    }
    return tap.pos;
  }

  equals(type: Type, opts?: TypeEqualsOptions): boolean {
    const canon = // Canonical equality.
      Type.isType(type) && this._getCachedHash() === type._getCachedHash();
    if (!canon || !(opts && opts.strict)) {
      return canon;
    }
    return (
      JSON.stringify(this.schema({exportAttrs: true})) ===
      JSON.stringify(type.schema({exportAttrs: true}))
    );
  }

  fromBuffer(buf: Uint8Array, resolver?: TypeResolver, noCheck?: boolean): V {
    const tap = Tap.fromBuffer(buf, 0);
    const val = readValue(this, tap, resolver, noCheck);
    if (!tap.isValid()) {
      throw new Error('truncated buffer');
    }
    if (!noCheck && tap.pos < buf.length) {
      throw new Error('trailing data');
    }
    return val;
  }

  fromString(str: string): any {
    return this._copy(JSON.parse(str), {coerce: 2});
  }

  inspect(): string {
    const typeName = this.typeName;
    const className = getClassName(typeName);
    if (isPrimitive(typeName)) {
      // The class name is sufficient to identify the type.
      return `<${className}>`;
    }
    // We add a little metadata for convenience.
    const obj = this.schema({exportAttrs: true, noDeref: true});
    if (typeof obj == 'object' && !Type.isType(this, 'logical')) {
      obj.type = undefined; // Would be redundant with constructor name.
    }
    return `<${className} ${j(obj)}>`;
  }

  isValid(val: V, opts?: IsValidOptions): boolean {
    // We only have a single flag for now, so no need to complicate things.
    const flags = (opts && opts.noUndeclaredFields) | 0;
    const errorHook = opts && opts.errorHook;
    let hook, path;
    if (errorHook) {
      path = [];
      hook = function (any, type) {
        errorHook.call(this, path.slice(), any, type, val);
      };
    }
    return this._check(val, flags, hook, path);
  }

  schema(opts?: SchemaOptions): Schema {
    // Copy the options to avoid mutating the original options object when we
    // add the registry of dereferenced types.
    return this._attrs(
      {},
      {
        exportAttrs: !!(opts && opts.exportAttrs),
        noDeref: !!(opts && opts.noDeref),
      }
    );
  }

  toBuffer(val: V): Uint8Array {
    TAP.pos = 0;
    this._write(TAP, val);
    if (TAP.isValid()) {
      return TAP.toBuffer();
    }
    const buf = new Uint8Array(TAP.pos);
    this._write(Tap.fromBuffer(buf), val);
    return buf;
  }

  toJSON(): unknown {
    // Convenience to allow using `JSON.stringify(type)` to get a type's schema.
    return this.schema({exportAttrs: true});
  }

  toString(val?: any): string {
    if (val === undefined) {
      // Consistent behavior with standard `toString` expectations.
      return JSON.stringify(this.schema({noDeref: true}));
    }
    return JSON.stringify(this._copy(val, {coerce: 3}));
  }

  wrap(val: any): any {
    const Branch = this._branchConstructor;
    return Branch === null ? null : new Branch(val);
  }

  _attrs(derefed, opts) {
    // This function handles a lot of the common logic for schema generation
    // across types, for example keeping track of which types have already been
    // de-referenced (i.e. derefed).
    const name = this.name;
    if (name !== undefined) {
      if (opts.noDeref || derefed[name]) {
        return name;
      }
      derefed[name] = true;
    }
    let schema = {};
    // The order in which we add fields to the `schema` object matters here.
    // Since JS objects are unordered, this implementation (unfortunately)
    // relies on engines returning properties in the same order that they are
    // inserted in. This is not in the JS spec, but can be "somewhat" safely
    // assumed (see http://stackoverflow.com/q/5525795/1062617).
    if (this.name !== undefined) {
      schema.name = name;
    }
    schema.type = this.typeName;
    const derefedSchema = this._deref(schema, derefed, opts);
    if (derefedSchema !== undefined) {
      // We allow the original schema to be overridden (this will happen for
      // primitive types and logical types).
      schema = derefedSchema;
    }
    if (opts.exportAttrs) {
      if (this.aliases && this.aliases.length) {
        schema.aliases = this.aliases;
      }
      if (this.doc !== undefined) {
        schema.doc = this.doc;
      }
    }
    return schema;
  }

  _createBranchConstructor() {
    const name = this.branchName;
    if (name === 'null') {
      return null;
    }
    const attr = ~name.indexOf('.') ? "this['" + name + "']" : 'this.' + name;
    const body = 'return function Branch$(val) { ' + attr + ' = val; };';

    const Branch = new Function(body)();
    Branch.type = this;

    Branch.prototype.unwrap = new Function('return ' + attr + ';');
    Branch.prototype.unwrapped = Branch.prototype.unwrap; // Deprecated.
    return Branch;
  }

  _peek(tap: Tap): any {
    const pos = tap.pos;
    const val = this._read(tap);
    tap.pos = pos;
    return val;
  }

  abstract compare(obj1: unknown, obj2: unknown): boolean;
  abstract _check();
  _copy(): unknown;
  _deref() {
    utils.abstractFunction();
  }
  _match() {
    utils.abstractFunction();
  }
  _read() {
    utils.abstractFunction();
  }
  _skip() {
    utils.abstractFunction();
  }
  _update() {
    utils.abstractFunction();
  }
  _write() {
    utils.abstractFunction();
  }
}

/** Derived type abstract class. */
class LogicalType extends RealType {
  private _logicalTypeName: string;
  constructor(schema: Schema, opts?: TypeOptions) {
    super(schema, opts);
    this._logicalTypeName = schema.logicalType;
    activeLogicalType = this;
    try {
      this._underlyingType = Type.forSchema(schema, opts);
    } finally {
      activeLogicalType = null;
      // Remove the underlying type now that we're done instantiating. Note that
      // in some (rare) cases, it might not have been inserted; for example, if
      // this constructor was manually called with an already instantiated type.
      const l = activeUnderlyingTypes.length;
      if (l && activeUnderlyingTypes[l - 1][0] === this) {
        activeUnderlyingTypes.pop();
      }
    }
    // We create a separate branch constructor for logical types to keep them
    // monomorphic.
    if (Type.isType(this.underlyingType, 'union')) {
      this._branchConstructor = this.underlyingType._branchConstructor;
    } else {
      this._branchConstructor = this.underlyingType._createBranchConstructor();
    }
    // We don't freeze derived types to allow arbitrary properties. Implementors
    // can still do so in the subclass' constructor at their convenience.
  }

  get typeName() {
    return 'logical:' + this._logicalTypeName;
  }

  get underlyingType() {
    if (this._underlyingType) {
      return this._underlyingType;
    }
    // If the field wasn't present, it means the logical type isn't complete
    // yet: we're waiting on its underlying type to be fully instantiated. In
    // this case, it will be present in the `activeUnderlyingTypes` array.
    for (let i = 0, l = activeUnderlyingTypes.length; i < l; i++) {
      const arr = activeUnderlyingTypes[i];
      if (arr[0] === this) {
        return arr[1];
      }
    }
    return undefined;
  }

  getUnderlyingType() {
    return this.underlyingType;
  }

  _read(tap: Tap): any {
    return this._fromValue(this.underlyingType._read(tap));
  }

  _write(tap, any) {
    this.underlyingType._write(tap, this._toValue(any));
  }

  _check(any, flags, hook, path) {
    let val;
    try {
      val = this._toValue(any);
    } catch (err) {
      // Handled below.
    }
    if (val === undefined) {
      if (hook) {
        hook(any, this);
      }
      return false;
    }
    return this.underlyingType._check(val, flags, hook, path);
  }

  _copy(any, opts) {
    const type = this.underlyingType;
    switch (opts && opts.coerce) {
      case 3: // To string.
        return type._copy(this._toValue(any), opts);
      case 2: // From string.
        return this._fromValue(type._copy(any, opts));
      default: // Normal copy.
        return this._fromValue(type._copy(this._toValue(any), opts));
    }
  }

  _update(resolver, type, opts) {
    const _fromValue = this._resolve(type, opts);
    if (_fromValue) {
      resolver._read = function (tap) {
        return _fromValue(type._read(tap));
      };
    }
  }

  compare(obj1: unknown, obj2: unknown): boolean {
    const val1 = this._toValue(obj1);
    const val2 = this._toValue(obj2);
    return this.underlyingType.compare(val1, val2);
  }

  _deref(schema, derefed, opts) {
    const type = this.underlyingType;
    const isVisited = type.name !== undefined && derefed[type.name];
    schema = type._attrs(derefed, opts);
    if (!isVisited && opts.exportAttrs) {
      if (typeof schema == 'string') {
        schema = {type: schema};
      }
      schema.logicalType = this._logicalTypeName;
      this._export(schema);
    }
    return schema;
  }

  _skip(tap) {
    this.underlyingType._skip(tap);
  }

  // Unlike the other methods below, `_export` has a reasonable default which we
  // can provide (not exporting anything).
  _export(/* schema */) {}

  // Methods to be implemented.
  _fromValue() {
    utils.abstractFunction();
  }
  _toValue() {
    utils.abstractFunction();
  }
  _resolve() {
    utils.abstractFunction();
  }
}

/** TypeResolver to read a writer's schema as a new schema. */
class TypeResolver {
  constructor(readerType) {
    // Add all fields here so that all resolvers share the same hidden class.
    this._readerType = readerType;
    this._read = null;
    this.itemsType = null;
    this.size = 0;
    this.symbols = null;
    this.valuesType = null;
  }

  inspect() {
    return '<TypeResolver>';
  }
}

TypeResolver.prototype._peek = Type.prototype._peek;

/** Read a value from a tap. */
function readValue(type, tap, resolver, lazy) {
  if (resolver) {
    if (resolver._readerType !== type) {
      throw new Error('invalid resolver');
    }
    return resolver._read(tap, lazy);
  }
  return type._read(tap);
}

/**
 * Get all aliases for a type (including its name). The input is typically a
 * type or a field. Its aliases property must exist and be an array.
 */
function getAliases(obj: Type | Field): string[] {
  const names = {};
  if (obj.name) {
    names[obj.name] = true;
  }
  const aliases = obj.aliases;
  for (let i = 0, l = aliases.length; i < l; i++) {
    names[aliases[i]] = true;
  }
  return Object.keys(names);
}

/** Checks if a type can be read as another based on name resolution rules. */
function hasCompatibleName(reader, writer, strict) {
  if (!writer.name) {
    return true;
  }
  const name = strict ? writer.name : utils.unqualify(writer.name);
  const aliases = getAliases(reader);
  for (let i = 0, l = aliases.length; i < l; i++) {
    let alias = aliases[i];
    if (!strict) {
      alias = utils.unqualify(alias);
    }
    if (alias === name) {
      return true;
    }
  }
  return false;
}

/**
 * Throw a somewhat helpful error on invalid object.
 *
 * This method is mostly used from `_write` to signal an invalid object for a
 * given type. Note that this provides less information than calling `isValid`
 * with a hook since the path is not propagated (for efficiency reasons).
 */
function throwInvalidError(val: unknown, type: Type): never {
  throw new Error(`invalid ${j(type.schema())}: ${j(val)}`);
}

function maybeQualify(name: string, ns?: string): string {
  const unqualified = utils.unqualify(name);
  // Primitives are always in the global namespace.
  return isPrimitive(unqualified) ? unqualified : utils.qualify(name, ns);
}

/**
 * Return a type's class name from its Avro type name. We can't simply use
 * `constructor.name` since it isn't supported in all browsers.
 */
function getClassName(typeName: string): string {
  if (typeName === 'error') {
    typeName = 'record';
  } else {
    const match = /^([^:]+):(.*)$/.exec(typeName);
    if (match) {
      if (match[1] === 'union') {
        typeName = match[2] + 'Union';
      } else {
        // Logical type.
        typeName = match[1];
      }
    }
  }
  return utils.capitalize(typeName) + 'Type';
}

export function anonymousName(): string {
  return 'Anonymous'; // TODO: May unique.
}

export function isType<N extends string>(arg: unknown, ...prefixes: N[]): arg is Type & {readonly typeName: `${N}${string}`} {
  if (!arg || !(arg instanceof RealType)) {
    // Not fool-proof, but most likely good enough.
    return false;
  }
  return prefixes.some((p) => arg.typeName.startsWith(p));
}
