/* jshint esversion: 6, node: true */

'use strict';

var avro = require('../../../lib/');
var express = require('express');
var bodyParser = require('body-parser');
var path = require('path');

var app = express();

var fpath = path.join(__dirname, 'Log.avdl');
avro.assembleProtocolSchema(fpath, function (err, schema) {
  var ptcl = avro.Protocol.forSchema(schema);
  var server = ptcl.createServer()
    .onCreateEntries(function (entries, cb) {
      cb(null, entries.length);
    });

  app.post('/avro', function (req, res) {
    server.createListener(function (cb) {
      cb(null, res);
      return req;
    });
  });

  app.use('/json', bodyParser.json());
  app.post('/json', function (req, res) {
    var entries = req.body.entries;
    res.status(200).send('' + entries.length);
  });

  app.listen(8000);
});
