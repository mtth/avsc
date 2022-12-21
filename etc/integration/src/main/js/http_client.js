'use strict';

let avro = require('../../../../../lib'),
    assert = require('assert'),
    http = require('http');


let protocol = avro.parse('./src/main/avro/math.avpr');

let ee = protocol.createEmitter(function (cb) {
  return http.request({
    port: 8888,
    headers: {'content-type': 'avro/binary'},
    method: 'POST'
  }).on('response', function (res) { cb(res); });
});

protocol.emit('add', {pair: {left: 2, right: 5}}, ee, function (err, res) {
  assert.strictEqual(err, null);
  assert.equal(res, 7);
});
