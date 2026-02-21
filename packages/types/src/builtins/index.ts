import {ArrayType, LogicalType,Schema, Type} from '../interfaces.js';
import {assert, printJSON as j} from '../utils.js';
import {isBufferLike} from '../binary.js';
import {RealType, anonymousName, isType} from './common.js';
import {RealUnwrappedUnionType, RealWrappedUnionType} from './unions.js';

export {isType} from './common.js';

function todo(..._args: any): Error {
  return new Error('todo');
}

export function parseType<V = Type>(
  schema: Schema,
  opts?: ParseTypeOptions
): V extends Type ? V : Type<V> {
  opts = {...opts, registry: opts?.registry ?? {}};

  const UnionType = (function (wrapUnions: any) {
    if (wrapUnions === true) {
      wrapUnions = 'always';
    } else if (wrapUnions === false) {
      wrapUnions = 'never';
    } else if (wrapUnions === undefined) {
      wrapUnions = 'auto';
    } else if (typeof wrapUnions == 'string') {
      wrapUnions = wrapUnions.toLowerCase();
    } else if (typeof wrapUnions == 'function') {
      wrapUnions = 'auto';
    }
    switch (wrapUnions) {
      case 'always':
        return RealWrappedUnionType;
      case 'never':
        return RealUnwrappedUnionType;
      case 'auto':
        return undefined; // Determined dynamically later on.
      default:
        throw new Error(`invalid wrap unions option: ${j(wrapUnions)}`);
    }
  })(opts.wrapUnions);

  if (schema === null) {
    // Let's be helpful for this common error.
    throw new Error('invalid type: null (did you mean "null"?)');
  }

  if (isType(schema)) {
    return schema as any;
  }

  let type;
  if (opts.typeHook && (type = opts.typeHook(schema, opts))) {
    if (!isType(type)) {
      throw new Error(`invalid typehook return value: ${j(type)}`);
    }
    return type as any;
  }

  if (typeof schema == 'string') {
    // Type reference.
    schema = maybeQualify(schema, opts.namespace);
    type = opts.registry[schema];
    if (type) {
      // Type was already defined, return it.
      return type;
    }
    if (isPrimitive(schema)) {
      // Reference to a primitive type. These are also defined names by
      // default so we create the appropriate type and it to the registry for
      // future reference.
      type = Type.forSchema({type: schema}, opts);
      opts.registry[schema] = type;
      return type as any;
    }
    throw new Error(`undefined type name: ${schema}`);
  }

  if (schema.logicalType && opts.logicalTypes && !activeLogicalType) {
    const DerivedType = opts.logicalTypes[schema.logicalType];
    // TODO: check to ensure DerivedType was derived from LogicalType via ES6
    // subclassing; otherwise it will not work properly
    if (DerivedType) {
      const namespace = opts.namespace;
      const registry = {};
      Object.keys(opts.registry).forEach((key: string) => {
        registry[key] = opts.registry[key];
      });
      try {
        return new DerivedType(schema, opts);
      } catch (err) {
        if (opts.assertLogicalTypes) {
          // The spec mandates that we fall through to the underlying type if
          // the logical type is invalid. We provide this option to ease
          // debugging.
          throw err;
        }
        activeLogicalType = null;
        opts.namespace = namespace;
        opts.registry = registry;
      }
    }
  }

  if (Array.isArray(schema)) {
    // Union.
    // We temporarily clear the logical type since we instantiate the branch's
    // types before the underlying union's type (necessary to decide whether
    // the union is ambiguous or not).
    const logicalType = activeLogicalType;
    activeLogicalType = null;
    const types = schema.map((obj) => {
      return Type.forSchema(obj, opts);
    });
    let projectionFn;
    if (!UnionType) {
      if (typeof opts.wrapUnions === 'function') {
        // we have a projection function
        projectionFn = opts.wrapUnions(types);
        UnionType =
          typeof projectionFn !== 'undefined'
            ? UnwrappedUnionType
            : WrappedUnionType;
      } else {
        UnionType = isAmbiguous(types)
          ? WrappedUnionType
          : UnwrappedUnionType;
      }
    }
    activeLogicalType = logicalType;
    type = new UnionType(types, opts, projectionFn);
  } else {
    // New type definition.
    type = (function (typeName) {
      const Type = constructors[typeName];
      if (Type === undefined) {
        throw new Error(`unknown type: ${j(typeName)}`);
      }
      return new Type(schema, opts);
    })(schema.type);
  }
  return type;
}

export interface ParseTypeOptions {
  readonly assertLogicalTypes?: boolean;
  readonly errorStackTraces?: boolean;
  readonly logicalTypes?: {
    readonly [name: string]: new (schema: Schema, opts?: ParseTypeOptions) => LogicalType;
  };
  readonly omitRecordMethods?: boolean;
  readonly recordSizeProperty?: string | symbol;
  readonly registry?: {[name: string]: Type};
  readonly typeHook?: TypeHook;
  readonly wrapUnions?: 'auto' | 'always' | 'never' | boolean| ((types: ReadonlyArray<Type>) => boolean);
}

export type TypeHook = (
  schema: Schema,
  opts: ParseTypeOptions
) => Type | undefined;

export function combineTypes(types: [], opts?: CombineTypesOptions): never;
export function combineTypes<T extends Type>(
  types: [T],
  opts?: CombineTypesOptions
): T;
export function combineTypes<T>(
  types: ReadonlyArray<Type>,
  opts?: CombineTypesOptions
): T extends Type ? T : Type<T>;
export function combineTypes<T>(
  types: ReadonlyArray<Type>,
  opts?: CombineTypesOptions
): T extends Type ? T : Type<T> {
  if (!types.length) {
    throw new Error('no types to combine');
  }
  if (types.length === 1) {
    return types[0] as any; // Nothing to do.
  }
  opts = Object.assign({}, opts);

  // Extract any union types, with special care for wrapped unions (see
  // below).
  let expanded = [];
  let numWrappedUnions = 0;
  let isValidWrappedUnion = true;
  types.forEach((type) => {
    switch (type.typeName) {
      case 'union:unwrapped':
        isValidWrappedUnion = false;
        expanded = expanded.concat(type.types);
        break;
      case 'union:wrapped':
        numWrappedUnions++;
        expanded = expanded.concat(type.types);
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
    const branchTypes = {};
    expanded.forEach((type) => {
      const name = type.branchName;
      const branchType = branchTypes[name];
      if (!branchType) {
        branchTypes[name] = type;
      } else if (!type.equals(branchType)) {
        throw new Error('inconsistent branch type');
      }
    });
    const wrapUnions = opts.wrapUnions;
    let unionType;
    opts.wrapUnions = true;
    try {
      unionType = Type.forSchema(
        Object.keys(branchTypes).map((name) => {
          return branchTypes[name];
        }),
        opts
      );
    } catch (err) {
      throw err;
    } finally {
      opts.wrapUnions = wrapUnions;
    }
    return unionType;
  }

  // Group types by category, similar to the logic for unwrapped unions.
  const bucketized = {};
  expanded.forEach((type) => {
    const bucket = getTypeBucket(type);
    let bucketTypes = bucketized[bucket];
    if (!bucketTypes) {
      bucketized[bucket] = bucketTypes = [];
    }
    bucketTypes.push(type);
  });

  // Generate the "augmented" type for each group.
  const buckets = Object.keys(bucketized);
  const augmented = buckets.map((bucket) => {
    let bucketTypes = bucketized[bucket];
    if (bucketTypes.length === 1) {
      return bucketTypes[0];
    }
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
        // Remove any sentinel arrays (used when inferring from empty
        // arrays) to avoid making things nullable when they shouldn't be.
        bucketTypes = bucketTypes.filter((t) => {
          return t !== opts.emptyArrayType;
        });
        if (!bucketTypes.length) {
          // We still don't have a real type, just return the sentinel.
          return opts.emptyArrayType;
        }
        return Type.forSchema(
          {
            type: 'array',
            items: Type.forTypes(
              bucketTypes.map((t) => {
                return t.itemsType;
              }),
              opts
            ),
          },
          opts
        );
      default:
        return combineObjects(bucketTypes, opts);
    }
  });

  if (augmented.length === 1) {
    return augmented[0];
  }
  // We return an (unwrapped) union of all augmented types.
  return parseType(augmented, opts);
}

export interface CombineTypesOptions extends ParseTypeOptions {
  readonly strictDefaults?: boolean;
}

export function inferType<V>(
  val: V,
  opts?: InferTypeOptions
): V extends Type ? V : Type<V> {
  opts = Object.assign({}, opts);

  // Sentinel used when inferring the types of empty arrays.
  opts.emptyArrayType =
    opts.emptyArrayType ||
    Type.forSchema({
      type: 'array',
      items: 'null',
    });

  // Optional custom inference hook.
  if (opts.valueHook) {
    const type = opts.valueHook(val, opts);
    if (type !== undefined) {
      if (!isType(type)) {
        throw new Error(`invalid value hook return value: ${j(type)}`);
      }
      return type;
    }
  }

  // Default inference logic.
  switch (typeof val) {
    case 'string':
      return parseType('string', opts);
    case 'boolean':
      return parseType('boolean', opts);
    case 'number':
      if ((val | 0) === val) {
        return parseType('int', opts);
      } else if (Math.abs(val) < 9007199254740991) {
        return parseType('float', opts);
      }
      return parseType('double', opts);
    case 'object': {
      if (val === null) {
        return parseType('null', opts);
      } else if (Array.isArray(val)) {
        if (!val.length) {
          return opts.emptyArrayType;
        }
        return parseType(
          {
            type: 'array',
            items: Type.forTypes(
              val.map((v) => {
                return Type.forValue(v, opts);
              }),
              opts
            ),
          },
          opts
        );
      } else if (isBufferLike(val)) {
        return parseType('bytes', opts);
      }
      const fieldNames = Object.keys(val);
      if (
        fieldNames.some((s) => {
          return !utils.isValidName(s);
        })
      ) {
        // We have to fall back to a map.
        return parseType(
          {
            type: 'map',
            values: Type.forTypes(
              fieldNames.map((s) => {
                return Type.forValue(val[s], opts);
              }),
              opts
            ),
          },
          opts
        );
      }
      return parseType(
        {
          type: 'record',
          name: anonymousName(),
          fields: fieldNames.map((s) => {
            return {name: s, type: parseType(val[s], opts)};
          }),
        },
        opts
      );
    }
    default:
      throw new Error(`cannot infer type from: ${j(val)}`);
  }
}

export interface InferTypeOptions extends CombineTypesOptions {
  readonly emptyArrayType?: ArrayType;
  readonly valueHook?: ValueHook;
}

export type ValueHook = (val: any, opts: InferTypeOptions) => Type | undefined;

/**
 * Combine number types. Note that we never have to create a new type here, we
 * are guaranteed to be able to reuse one of the input types as super-type.
 */
function combineNumbers(types: ReadonlyArray<Type>): Type {
  assert(types.length > 0, 'empty types');
  const typeNames = ['int', 'long', 'float', 'double'];
  let superIndex = -1;
  let superType = null;
  for (const t of types) {
    const index = typeNames.indexOf(t.typeName);
    if (index > superIndex) {
      superIndex = index;
      superType = t;
    }
  }
  assert(superType != null, 'no supertype');
  return superType;
}

/** Combine enums and strings. */
function combineStrings(
  types: ReadonlyArray<Type>,
  opts?: CombineTypesOptions
): Type {
  assert(types.length > 0, 'empty types');
  const symbols = new Set<string>();
  for (const t of types) {
    if (t.typeName === 'string') {
      // If at least one of the types is a string, it will be the supertype.
      return t;
    }
    assert(t.typeName === 'enum', 'not an enum');
    for (const s of t.symbols) {
      symbols.add(s);
    }
  }
  return parseType(
    {
      type: 'enum',
      name: anonymousName(),
      symbols: [...symbols],
    },
    opts
  );
}

/**
 * Combine bytes and fixed. This function is optimized to avoid creating new
 * types when possible: in case of a size mismatch between fixed types, it will
 * continue looking through the array to find an existing bytes type (rather
 * than exit early by creating one eagerly).
 */
function combineBuffers(
  types: ReadonlyArray<Type>,
  opts?: CombineTypesOptions
): Type {
  assert(types.length > 0, 'empty types');
  let size = -1;
  for (const t of types) {
    if (t.typeName === 'bytes') {
      return t;
    }
    assert(t.typeName === 'fixed', 'unexpected type');
    if (size === -1) {
      size = t.size;
    } else if (t.size !== size) {
      // Don't create a bytes type right away, we might be able to reuse one
      // later on in the types array. Just mark this for now.
      size = -2;
    }
  }
  return size < 0 ? parseType('bytes', opts) : types[0];
}

/**
 * Combine maps and records. Field defaults are kept when possible (i.e. when no
 * coercion to a map happens), with later definitions overriding previous ones.
 */
function combineObjects(
  types: ReadonlyArray<Type>,
  opts?: CombineTypesOptions
): Type {
  const allTypes = []; // Field and value types.
  const fieldTypes = new Map<string, any[]>(); // Field types grouped by name.
  const fieldDefaults = new Map<string, any>();
  let isValidRecord = true;

  // Check whether the final type will be a map or a record.
  for (const t of types) {
    if (t.typeName === 'map') {
      isValidRecord = false;
      allTypes.push(t.valuesType);
    } else {
      assert(t.typeName === 'record', 'unexpected type');
      for (const f of t.fields) {
        const fieldName = f.name;
        const fieldType = f.type;
        allTypes.push(fieldType);
        if (isValidRecord) {
          let ts = fieldTypes.get(fieldName);
          if (!ts) {
            ts = [];
            fieldTypes.set(fieldName, ts);
          }
          ts.push(fieldType);
          const fieldDefault = f.defaultValue();
          if (fieldDefault !== undefined) {
            // Later defaults will override any previous ones.
            fieldDefaults.set(fieldName, fieldDefault);
          }
        }
      }
    }
  }

  if (isValidRecord) {
    // Check that no fields are missing and that we have the approriate
    // defaults for those which are.
    for (const [name, ts] of fieldTypes) {
      if (ts.length < types.length && fieldDefaults.get(name) === undefined) {
        // At least one of the records is missing a field with no default.
        if (opts && opts.strictDefaults) {
          isValidRecord = false;
        } else {
          ts.unshift(parseType('null', opts));
          fieldDefaults.set(name, null);
        }
      }
    }
  }

  let schema;
  if (isValidRecord) {
    schema = {
      type: 'record',
      fields: [...fieldTypes.entries()].map(([n, ts]) => {
        let fieldType = combineTypes(ts, opts);
        const fieldDefault = fieldDefaults.get(n);
        if (
          fieldDefault !== undefined &&
          fieldType.typeName === 'union:unwrapped'
        ) {
          // Ensure that the default's corresponding type is first.
          const unionTypes = fieldType.types.slice();
          let i = 0,
            l = unionTypes.length;
          for (; i < l; i++) {
            if (unionTypes[i].isValid(fieldDefault)) {
              break;
            }
          }
          if (i > 0) {
            const unionType = unionTypes[0];
            unionTypes[0] = unionTypes[i];
            unionTypes[i] = unionType;
            fieldType = parseType(unionTypes, opts);
          }
        }
        return {
          name: s,
          type: fieldType,
          default: fieldDefaults.get(s),
        };
      }),
    };
  } else {
    schema = {type: 'map', values: combineTypes(allTypes, opts)};
  }
  return parseType(schema, opts);
}
