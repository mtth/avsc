/* jshint esversion: 6, node: true */

'use strict';

var files = require('./files'),
    parsing = require('./parsing'),
    utils = require('./utils'),
    path = require('path'),
    util = require('util');

var f = util.format;

/** Assemble an IDL file into a decoded protocol. */
function assembleProtocol(fpath, opts, cb) {
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
  var protocol, imports;
  opts.importHook(fpath, 'idl', function (err, str) {
    if (err) {
      cb(err);
      return;
    }
    if (str === undefined) {
      // Skipped import (likely already imported).
      cb(null, {});
      return;
    }
    try {
      var obj = parsing.readProtocol(str, opts);
    } catch (err) {
      err.path = fpath; // To help debug which file caused the error.
      cb(err);
      return;
    }
    protocol = obj.protocol;
    imports = obj.imports;
    fetchImports();
  });

  function fetchImports() {
    var info = imports.shift();
    if (!info) {
      // We are done with this file. We prepend all imported types to this
      // file's and we can return the final result.
      if (importedTypes.length) {
        protocol.types = protocol.types ?
          importedTypes.concat(protocol.types) :
          importedTypes;
      }
      cb(null, protocol);
    } else {
      var importPath = path.join(path.dirname(fpath), info.name);
      if (info.kind === 'idl') {
        assembleProtocol(importPath, opts, mergeImportedSchema);
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
    // since instantiating the service will take care of it), then the messages
    // (where we need to, as duplicates will overwrite each other).
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
        if (!protocol.messages) {
          protocol.messages = {};
        }
        if (protocol.messages[name]) {
          throw new Error(f('duplicate message: %s', name));
        }
        protocol.messages[name] = importedSchema.messages[name];
      });
    } catch (err) {
      cb(err);
      return;
    }
    fetchImports(); // Continue importing any remaining imports.
  }
}

/** Assemble an IDL file into a decoded protocol synchronously. */
function assembleProtocolSync(fpath, opts) {
  opts = utils.copyOwnProperties(opts, {});
  if (opts.importHook) {
    opts.importHook = toAsync(opts.importHook);
  } else {
    opts.importHook = files.createSyncImportHook();
  }
  var assembled;
  assembleProtocol(fpath, opts, function (err, ptcl) {
    if (err) {
      throw err;
    }
    assembled = ptcl;
  });
  return assembled;
}

/** Transform a synchronous signature hook into an asynchronous one. */
function toAsync(hook) {
  return function (fpath, kind, cb) {
    try {
      var str = hook(fpath, kind);
    } catch (err) {
      cb(err);
      return;
    }
    cb(null, str);
  };
}

module.exports = {
  assembleProtocol: assembleProtocol,
  assembleProtocolSync: assembleProtocolSync,
};
