/** Various utilities used across this library. */

// Valid (field, type, and symbol) name regex.
const NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Uppercase the first letter of a string. */
export function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/** Compare two numbers. */
export function compare(n1: number, n2: number): number {
  return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
}

/** Check whether an array has duplicates. */
export function hasDuplicates<V, K>(
  arr: ReadonlyArray<V>,
  fn?: (val: V) => K
): boolean {
  const keys = new Set<unknown>(fn ? arr.map(fn) : arr);
  return keys.size !== arr.length;
}

/**
 * Copy properties from one object to another. The optional `overwrite` input
 * determines whether to overwrite existing destination properties. Defaults to
 * false.
 */
export function copyOwnProperties<O>(
  src: object,
  dst: O,
  overwrite?: boolean
): O {
  const names = Object.getOwnPropertyNames(src);
  for (let i = 0, l = names.length; i < l; i++) {
    const name = names[i]!;
    if (!Object.prototype.hasOwnProperty.call(dst, name) || overwrite) {
      const descriptor = Object.getOwnPropertyDescriptor(src, name)!;
      Object.defineProperty(dst, name, descriptor);
    }
  }
  return dst;
}

/** Check whether a string is a valid Avro identifier. */
export function isValidName(str: string): boolean {
  return NAME_PATTERN.test(str);
}

/**
 * Verify and return fully qualified name. The input name is a full or short
 * name. It can be prefixed with a dot to force global namespace. The namespace
 * is optional.
 */
export function qualify(name: string, namespace?: string): string {
  if (~name.indexOf('.')) {
    name = name.replace(/^\./, ''); // Allow absolute referencing.
  } else if (namespace) {
    name = namespace + '.' + name;
  }
  name.split('.').forEach((part) => {
    if (!isValidName(part)) {
      throw new Error(`invalid name: ${printJSON(name)}`);
    }
  });
  return name;
}

/** Remove namespace from a (full or short) name. */
export function unqualify(name: string): string {
  const parts = name.split('.');
  return parts[parts.length - 1]!;
}

/**
 * Return the namespace implied by a (full or short) name. If short, the
 * returned namespace will be undefined.
 */
export function impliedNamespace(name: string): string | undefined {
  const match = /^(.*)\.[^.]+$/.exec(name);
  return match ? match[1] : undefined;
}

/** Prints an object as a string; mostly used for printing objects in errors. */
export function printJSON(obj: any): string {
  const seen = new Set();
  try {
    return JSON.stringify(obj, (_key, value) => {
      if (seen.has(value)) {
        return '[Circular]';
      }
      if (typeof value === 'object' && value !== null) {
        seen.add(value);
      }

      if (typeof BigInt !== 'undefined' && value instanceof BigInt) {
        return `[BigInt ${value.toString()}n]`;
      }
      return value;
    });
  } catch (err) {
    return '[object]';
  }
}

/** Assert that a condition holds or throw an error. */
export function assert(pred: unknown, msg: string): asserts pred {
  if (pred) {
    return;
  }
  throw new Error('Assertion failed: ' + msg);
}
