/* jshint node: true */

'use strict';

/**
 * Filesystem specifics.
 *
 * This module contains functions only used by node.js. It is shimmed by
 * another module when `avsc` is required from `browserify`.
 */

var fs = require('fs'),
    path = require('path');


module.exports = {
  // Proxy a few methods to better shim them for browserify.
  existsSync: fs.existsSync,
  readFileSync: fs.readFileSync
};
