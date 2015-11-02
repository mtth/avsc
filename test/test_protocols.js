/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
    schemas = require('../lib/schemas'),
    assert = require('assert');


suite('protocols', function () {

  suite('Protocol', function () {

    test('valid', function () {
      var p = new protocols.Protocol({
        namespace: 'foo',
        protocol: 'HelloWorld',
        types: [
          {
            name: 'Greeting',
            type: 'record',
            fields: [{name: 'message', type: 'string'}]
          },
          {
            name: 'Curse',
            type: 'error',
            fields: [{name: 'message', type: 'string'}]
          }
        ],
        messages: {
          hello: {
            request: [{name: 'greeting', type: 'Greeting'}],
            response: 'Greeting',
            errors: ['Curse']
          },
          hi: {
          request: [{name: 'hey', type: 'string'}],
          response: 'null',
          'one-way': true
          }
        }
      });
      assert.equal(p.getName(), 'foo.HelloWorld');
      assert(p.getTypes()['foo.Greeting'] instanceof schemas.types.RecordType);
    });

    test('missing messages', function () {
      assert.doesNotThrow(function () {
        new protocols.Protocol({namespace: 'com.acme', protocol: 'Hello'});
      });
    });

    test('missing name', function () {
      assert.throws(function () {
        new protocols.Protocol({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', function () {
      assert.throws(function () {
        new protocols.Protocol({
          namespace: 'com.acme',
          protocol: 'HelloWorld',
          messages: {
            hello: {
              request: [{name: 'greeting', type: 'Greeting'}],
              response: 'Greeting'
            }
          }
        });
      });
    });

  });

  suite('Message', function () {

    test('missing response', function () {
      assert.throws(function () {
        new protocols.Message({
          request: [{name: 'greeting', type: 'string'}]
        });
      });
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        new protocols.Message({
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      });
      // Non-empty errors.
      assert.throws(function () {
        new protocols.Message({
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      });
    });
  });

});
