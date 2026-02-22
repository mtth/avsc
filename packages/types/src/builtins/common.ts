import {
  BaseType,
  BaseSchema,
  Branch,
  ErrorHook,
  LogicalType,
  NamedSchema,
  Schema,
  Type,
  TypeCloneOptions,
  TypeIsValidOptions,
  TypeSchemaOptions,
  PrimitiveTypeName,
  primitiveTypeNames,
} from '../interfaces.js';
import {Tap} from '../binary.js';
import {
  assert,
  capitalize,
  printJSON as j,
  qualify,
  unqualify,
} from '../utils.js';

// Encoding tap (shared for performance).
const TAP = Tap.withCapacity(1024);

export function resizeDefaultBuffer(size: number): void {
  TAP.reinitialize(size);
}

// Currently active logical type, used for name redirection.
let activeLogicalType: Type | null = null;

// Underlying types of logical types currently being instantiated. This is used
// to be able to reference names (i.e. for branches) during instantiation.
const activeUnderlyingTypes: [Type, RealType][] = [];

export type TypeRegistry = Map<string, Type>;

export function isType<N extends string>(
  arg: unknown,
  ...prefixes: N[]
): arg is Type & {readonly typeName: `${N}${string}`} {
  if (!arg || !(arg instanceof RealType)) {
    // Not fool-proof, but most likely good enough.
    return false;
  }
  return prefixes.some((p) => arg.typeName.startsWith(p));
}

export interface TypeContext {
  readonly factory: (s: Schema, ctx: TypeContext) => Type;
  readonly registry: TypeRegistry;
  readonly namespace?: string;
  readonly depth: number; // TODO: Use.
}

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
  abstract readonly typeName: string;
  readonly name: string | undefined;
  readonly aliases: string[] | undefined;
  readonly doc: string | undefined;
  protected branchConstructor: BranchConstructor | undefined;
  protected constructor(schema: BaseSchema | NamedSchema, ctx: TypeContext) {
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
    this.doc = schema.doc ? '' + schema.doc : undefined;

    if ('name' in schema) {
      // This is a complex (i.e. non-primitive) type.
      let name = schema.name;
      const namespace =
        schema.namespace === undefined
          ? ctx.namespace
          : schema.namespace;
      if (name !== undefined) {
        // This isn't an anonymous type.
        name = maybeQualify(name, namespace);
        if (isPrimitive(name)) {
          // Avro doesn't allow redefining primitive names.
          throw new Error(`cannot rename primitive type: ${j(name)}`);
        }
        const registry = ctx.registry;
        if (registry) {
          if (registry.has(name)) {
            throw new Error(`duplicate type name: ${name}`);
          }
          registry.set(name, type as any);
        }
      }
      this.name = name;
      this.aliases = schema.aliases
        ? schema.aliases.map((s: string) => maybeQualify(s, namespace))
        : [];
    }
  }

  get branchName(): string | undefined  {
    let t: Type = this as any;
    if (isType(t, 'logical')) {
      t = t.underlyingType;
    }
    if (t.name) {
      return t.name;
    }
    return isType(t, 'union') ? undefined : t.typeName;
  }

  clone(val: V, opts?: TypeCloneOptions): any {
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
    return this.binaryDecode(this.binaryEncode(val));
  }

  binaryCompare(buf1: Uint8Array, buf2: Uint8Array): -1 | 0 | 1 {
    return this._match(Tap.fromBuffer(buf1), Tap.fromBuffer(buf2));
  }

  createResolver(type: Type, opts?: CreateResolverOptions): TypeResolver {
    if (!isType(type)) {
      // More explicit error message than the "incompatible type" thrown
      // otherwise (especially because of the overridden `toJSON` method).
      throw new Error(`not a type: ${j(type)}`);
    }

    if (!isType(this, 'union', 'logical') && isType(type, 'logical')) {
      // Trying to read a logical type as a built-in: unwrap the logical type.
      // Note that we exclude unions to support resolving into unions containing
      // logical types.
      return this.createResolver(type.underlyingType, opts);
    }

    opts = Object.assign({}, opts);
    opts.registry = opts.registry || {};

    let resolver, key;
    if (isType(this, 'record', 'error') && isType(type, 'record', 'error')) {
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

    if (isType(type, 'union')) {
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

  binaryDecodeAt(buf: Uint8Array, pos?: number, resolver?: TypeResolver): V {
    const tap = Tap.fromBuffer(buf, pos);
    const val = readValue(this, tap, resolver);
    if (!tap.isValid()) {
      return {value: undefined, offset: -1};
    }
    return {value: val, offset: tap.pos};
  }

  binaryEncodeAt(val: V, buf: Uint8Array, pos?: number): number {
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
      isType(type) && this._getCachedHash() === type._getCachedHash();
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
    if (typeof obj == 'object' && !isType(this, 'logical')) {
      obj.type = undefined; // Would be redundant with constructor name.
    }
    return `<${className} ${j(obj)}>`;
  }

  isValid(arg: unknown, opts?: TypeIsValidOptions): boolean {
    // We only have a single flag for now, so no need to complicate things.
    const flags = (opts && opts.allowUndeclaredFields) | 0;
    const errorHook = opts && opts.errorHook;
    let hook, path;
    if (errorHook) {
      path = [];
      hook = function (any, type) {
        errorHook.call(this, path.slice(), any, type, val);
      };
    }
    return this._check(arg, flags, hook, path);
  }

  schema<E = SchemaExtensions>(opts?: TypeSchemaOptions): Schema<E, never> {
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

  binaryEncode(val: V): Uint8Array {
    TAP.pos = 0;
    this._write(TAP, val);
    if (TAP.isValid()) {
      return TAP.toBuffer();
    }
    const buf = new Uint8Array(TAP.pos);
    this._write(Tap.fromBuffer(buf), val);
    return buf;
  }

  jsonEncode(val?: V): unknown {
    return this._copy(val, {coerce: 3});
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

  _peek(tap: Tap): any {
    const pos = tap.pos;
    const val = this._read(tap);
    tap.pos = pos;
    return val;
  }

  protected abstract compare(obj1: unknown, obj2: unknown): -1 | 0 | 1;

  protected abstract _check(
    arg: unknown,
    flags: any,
    hook: ErrorHook,
    path: string[]
  ): boolean;

  protected abstract _copy(): unknown;

  protected abstract _deref(): any;

  protected abstract _match(tap1: Tap, tap2: Tap): -1 | 0 | 1;

  abstract _read(tap: Tap): V;

  protected abstract _skip(tap: Tap): void;

  protected abstract _update(resolver: TypeResolver): void;

  protected abstract _write(tap: Tap, val: V): void;
}

/** Derived type abstract class. */
export abstract class RealLogicalType extends RealType implements LogicalType {
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
    if (isType(this.underlyingType, 'union')) {
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

  override _read(tap: Tap): any {
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
  protected abstract _fromValue(val: any): any;
  protected abstract _toValue(arg: any): any;
  protected abstract _resolve();
}

/** TypeResolver to read a writer's schema as a new schema. */
class TypeResolver {
  _read: ((tap: Tap, lazy: boolean) => any) | undefined;
  symbols: ReadonlyArray<string> | undefined;
  itemsType: Type | undefined;
  valuesType: Type | undefined;
  size: number = 0;
  constructor(readonly readerType: RealType) {}

  inspect() {
    // TODO: Use symbol.
    return '<TypeResolver>';
  }

  _peek(tap: Tap): any {
    const pos = tap.pos;
    const val = this._read(tap);
    tap.pos = pos;
    return val;
  }
}

/** Read a value from a tap. */
function readValue(
  type: RealType,
  tap: Tap,
  resolver: TypeResolver,
  lazy: boolean
): any {
  if (resolver) {
    if (resolver.readerType !== type) {
      throw new Error('invalid resolver');
    }
    assert(resolver._read, 'uninitialized resolver');
    return resolver._read(tap, lazy);
  }
  return type._read(tap);
}

/**
 * Get all aliases for a type (including its name). The input is typically a
 * type or a field. Its aliases property must exist and be an array.
 */
function getAliases(obj: {
  readonly name?: string;
  readonly aliases?: ReadonlyArray<string>;
}): string[] {
  const names = new Set<string>();
  if (obj.name) {
    names.add(obj.name);
  }
  for (const alias of obj.aliases ?? []) {
    names.add(alias);
  }
  return [...names];
}

/** Checks if a type can be read as another based on name resolution rules. */
function hasCompatibleName(
  reader: RealType,
  writer: RealType,
  strict: boolean
) {
  if (!writer.name) {
    return true;
  }
  const name = strict ? writer.name : unqualify(writer.name);
  for (let alias of getAliases(reader)) {
    if (!strict) {
      alias = unqualify(alias);
    }
    if (alias === name) {
      return true;
    }
  }
  return false;
}

/**
 * Check whether a type's name is a primitive. Sample inputs: `'string'`,
 * `'array'`.
 */
function isPrimitive(typeName: string): typeName is PrimitiveTypeName {
  return primitiveTypeNames.includes(typeName as any);
}

/**
 * Throw a somewhat helpful error on invalid object.
 *
 * This method is mostly used from `_write` to signal an invalid object for a
 * given type. Note that this provides less information than calling `isValid`
 * with a hook since the path is not propagated (for efficiency reasons).
 */
export function invalidValueError(val: unknown, type: Type): Error {
  return new Error(`invalid ${j(type.schema())}: ${j(val)}`);
}

function maybeQualify(name: string, ns?: string): string {
  const unqualified = unqualify(name);
  // Primitives are always in the global namespace.
  return isPrimitive(unqualified) ? unqualified : qualify(name, ns);
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
        typeName = match[1]!;
      }
    }
  }
  return capitalize(typeName) + 'Type';
}

export function anonymousName(): string {
  return 'Anonymous'; // TODO: May unique.
}

type BranchConstructor = (v: unknown) => Branch;

export function createBranchConstructor(t: RealType): BranchConstructor | null {
  const name = t.branchName;
  assert(name, 'missing name');
  if (name === 'null') {
    return null;
  }
  const attr = name.includes('.') ? "this['" + name + "']" : 'this.' + name;
  const body = 'return function Branch$(val) { ' + attr + ' = val; };';

  const Branch = new Function(body)();
  Branch.prototype.wrappedType = t;
  Branch.prototype.unwrap = new Function('return ' + attr + ';');
  return Branch;
}
