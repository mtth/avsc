'use strict';

let avro = require('../../../../../lib'),
    net = require('net');


let protocol = avro.parse('./src/main/avro/math.avpr')
  .on('add', function (req, ee, cb) {
    let res = req.pair.left + req.pair.right;
    console.log(req);
    console.log(res);
    cb(null, res);
  });

net.createServer(function (con) { protocol.createListener(con); })
  .listen(65111, function () { console.log('listening'); });
