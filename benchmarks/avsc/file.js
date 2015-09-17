/* jshint node: true */

'use strict';

var file = require('../../lib/file'),
    fs = require('fs');

// Buffer.prototype.toJSON = function () { return this.toString('binary'); };

var stream = fs.createReadStream('dat/user-100000.avro');
var reader = new file.Decoder(stream);
