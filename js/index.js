/* jshint browser: true, browserify: true */

(function () {
  'use strict';

  var avsc = require('avsc'),
      buffer = require('buffer'),
      $ = require('jquery');


  require('jquery-ui');
  window.avsc = avsc;

  $( function() {
    resize();
    var parsedSchema;
    var encodedErrorElement = $('#encoded-error');
    var decodedErrorElement = $('#decoded-error');
    window.onresize = function(event) {
      resize();
    }
    /* Validate schema after each new character. */
    $('#schema').on('paste keyup', function(e) {
      setTimeout(function(){
        validateSchema();
      }, 0);
    });

    $('#input').on('paste keyup', function(event) {
      setTimeout(function() {
        encode();
      }, 0);
    }); 

    $('#output').on('paste keyup', function(event) {
      setTimeout(function() {
        decode();
      }, 0);
    });

   $('#encode').click(function() {
      encode();     
    });

    $('#decode').click(function() {
      decode();
    });

    $('#random').click(function() {
      generateRandom();
    });

   function validateSchema() {
      parsedSchema = null;
      var elem = $('#schema');
      var error_elem = $('#schema-error');
      try {
        var schema = readInput('#schema');
        parsedSchema = avsc.parse(schema);
        clearError(error_elem, 'Valid Schema!');
        generateRandom();
      } catch (err) {
        showError(error_elem, err);
      }
    }

    function clearError(errorElement, msg) {
      errorElement.removeClass('error');
      errorElement.removeClass('hidden');
      errorElement.addClass('valid');
      errorElement.text(msg);
    }
 
    function generateRandom() {
      if (parsedSchema) {
        try{
          var random = parsedSchema.random();
          var randomStr = JSON.stringify(random, null, 2);
          $('#input').val(randomStr);
          encode(); /* Update encoded string too. */
          clearErrors();
          clearError(decodedErrorElement, 'Valid input!');
        } catch(err) {
          showError($('#schema-error'),err);
        }
      }

    }
    function encode() {
      if (parsedSchema) {
        try {
          var input = readInput('#input');
          var output = parsedSchema.encode(input);
          $('#output').val(bufferToStr(output));
          clearErrors();
          clearError(decodedErrorElement, 'Valid Input!');
        }catch(err) {
          showError(decodedErrorElement, err);
        }
      } else {
        showError(decodedErrorElement, 'No valid schema found!');
      }
    }

    function decode() {
      if (parsedSchema) {
        var input = readBuffer('#output');
        try {
          var decoded = parsedSchema.decode(input);
          decoded = JSON.stringify(decoded, null, 2);
          $('#input').val(decoded);
          clearErrors();
          clearError(encodedErrorElement, 'Valid encoded record!');
        }catch(err) {
          showError(encodedErrorElement,err);
        }
      } else {
        showError(encodedErrorElement, 'No valid schema found!');
      }
    }

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
    var hexArray = rawInput.split('\n');
    var i;
    var size = hexArray.length;
    var buffer = [];
    for (i =0; i < size; i++){
      buffer.push(new Buffer(hexArray[i].substr(2), 'hex'));
    }
    return Buffer.concat(buffer);
  }

  function bufferToStr(buffer) {
    var size = buffer.length;
    var hexPrefix = '0x';
    var outStr = '';
    var i;
    for (i = 0; i < size; i++) {
      outStr += hexPrefix + 
                buffer.toString('hex', i , i+1) +
                '\n';
    }
    return outStr;
  }
  /* Adjust textbox heights according to current window size */
  function resize() {
    var vph = $(window).height();
    $('.textbox').css({'height': vph - 100});
  }

})();
