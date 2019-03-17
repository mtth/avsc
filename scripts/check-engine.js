/* jshint node: true */

'use strict';

/**
 * Script to determine whether a package can be run on the current node version.
 *
 * Usage: node check-engine.js ./path/to/package/
 *
 * The command will exit with code 0 if the package is compatible and 1
 * otherwise.
 *
 * Note: This script is not executable since it must be run via the node binary
 * installed by nvm on Travis VMs.
 */

var checks = require('npm-install-checks');
var path = require('path');

var dpath = process.argv[2];
if (!dpath) {
  throw new Error('missing package path');
}

var target = require(path.resolve(path.join(dpath, 'package.json')));
checks.checkEngine(target, '', process.version, false, true, function (err) {
  process.exit(err ? 1 : 0);
});
