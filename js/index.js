/* jshint browser: true, browserify: true */

(function () {
  'use strict';

  var avsc = require('avsc'),
      buffer = require('buffer'),
      $ = require('jquery');


  require('jquery-ui');
  window.avsc = avsc;

  $( function() {
    var parsedSchema;
    var encodedErrorElement = $('#encoded-error');
    var decodedErrorElement = $('#decoded-error');
    /* Validate schema after each new character. */
    $('#schema').keyup(function(event) {
        validateSchema();
    });

    function validateSchema() {
      parsedSchema = null;
      var elem = $('#schema');
      var error_elem = $('#schema-error');
      try {
        var schema = readInput('#schema');
        parsedSchema = avsc.parse(schema);
        error_elem.removeClass('error');
        error_elem.addClass('valid');
        error_elem.text('Valid Schema!');
      } catch (err) {
        showError(error_elem, err);
      }
    }
 
    $('#encode').click(function() {
      if (parsedSchema) {
        try {
          var input = readInput('#input');
          var output = parsedSchema.encode(input);
          $('#output').val(bufferToStr(output));
          clearErrors();
        }catch(err) {
          showError(decodedErrorElement, err);
        }
      } else {
        showError(decodedErrorElement, 'No valid schema found!');
      }
     
    });

     $('#decode').click(function() {
      if (parsedSchema) {
        var input = readBuffer('#output');
        try {
          var decoded = parsedSchema.decode(input);
          decoded = JSON.stringify(decoded, null, 2);
          $('#input').val(decoded);
          clearErrors();
        }catch(err) {
          showError(encodedErrorElement,err);
        }
      } else {
        showError(encodedErrorElement, 'No valid schema found!');
      }
    });

    $('#random').click(function() {
      if (parsedSchema) {
        try{
          var random = parsedSchema.random();
          var randomStr = JSON.stringify(random, null, 2);
          $('#input').val(randomStr);
        } catch(err) {
          showError($('#schema-error'),err);
        }
      }
    });

    /* Clear any error messages shown in input/output boxes. */
    function clearErrors() {
      
      decodedErrorElement.text('');
      decodedErrorElement.addClass('hidden');
      encodedErrorElement.text('');
      encodedErrorElement.addClass('hidden');
    }
    /* Show `err` in the `element`. */
    function showError(element, err) {
      if (err != null) {
        element.removeClass('hidden');
        element.addClass('error');
        element.text(err);
      }
    }
  });

  function readInput(elementId) {
    var rawSchema = $.trim($(elementId).val());
    /* Handle primitive types that don't need to be json. */
    if (!rawSchema.startsWith('{')) {
      return rawSchema;
    }
    var parsedSchema = JSON.parse(rawSchema);
    return parsedSchema;
  }

  function readBuffer(elementId) {
    var rawInput = $.trim($(elementId).val());
    var jsonInput = JSON.parse(rawInput);
    var buf = new Buffer(jsonInput);
    return buf; 
  }

  function bufferToStr(buffer) {
    var str = '[';
    var nonEmpty = false;
    for (var b of buffer) {
      if(nonEmpty) {
        str = str + ', ';
      }
      str = str + b;
      nonEmpty = true;
    }
    return str + ']'
  }
})();
