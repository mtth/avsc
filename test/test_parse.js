/* jshint node: true, mocha: true */

'use strict';

var parse = require('../lib/parse'),
    assert = require('assert'),
    fs = require('fs');

suite('parse', function () {

  test('register', function () {

    var schema = loadSchema('dat/weather.avsc');
    assert('test.Weather' in parse.registerRecords(schema));

  });

});

function loadSchema(path) {

  return JSON.parse(fs.readFileSync(path));

}
