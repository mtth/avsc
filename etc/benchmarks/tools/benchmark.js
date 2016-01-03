/* jshint node: true */

'use strict';

var Benchmark = require('benchmark'),
    avsc = require('../../../lib'),
    fs = require('fs'),
    path = require('path'),
    msgpack = require('msgpack-lite');


function getSchemaSuite(path) {
  var type = avsc.parse(path);
  var val = type.random();
  var avroBuf = val.$toBuffer();
  var str = JSON.stringify(val);
  var msgpackBuf = msgpack.encode(val);

  return new Benchmark.Suite(path)
    .add('type.toBuffer', function () { type.toBuffer(val); })
    .add('type.fromBuffer', function () { type.fromBuffer(avroBuf); })
    .add('msgpack.encode', function () { msgpack.encode(val); })
    .add('msgpack.decode', function () { msgpack.decode(msgpackBuf); })
    .add('JSON.stringify', function () { JSON.stringify(val); })
    .add('JSON.parse', function () { JSON.parse(str); });
}

var dpath = './schemas';
fs.readdirSync(dpath).forEach(function (fname) {
  var fpath = path.join(dpath, fname);
  getSchemaSuite(fpath)
    .on('start', function () { console.log(fpath); })
    .on('complete', function (evt) { console.log('' + evt.target); })
    .run();
});
