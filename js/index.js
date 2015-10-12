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
    var inputElement = $('#input');
    var outputElement = $('#output');
    window.onresize = function(event) {
      resize();
    }
    /* Validate schema after each new character. */
    $('#schema').on('paste keyup', function(e) {
      setTimeout(function(){
        validateSchema();
      }, 0);
    }).on('input propertychange paste', function(e) {
        setTimeout(function() {
          generateRandom();
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

    $('#random').click(function () {   
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
          var randomStr = parsedSchema.toString(random);
          inputElement.val(randomStr);
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
          var input = readInput(inputElement);
          var output = parsedSchema.toBuffer(input);
          outputElement.val(bufferToStr(output));
          clearErrors();
          clearError(decodedErrorElement, 'Valid input!');
        }catch(err) {
          clearErrors();
          showError(decodedErrorElement, err);
          clearText(outputElement);
        }
      } else {
        showError(decodedErrorElement, 'No valid schema found!');
      }
    }

    function decode() {
      if (parsedSchema) {
        try {
          var input = readBuffer(outputElement);
          var decoded = parsedSchema.decode(input);
          //todo: probably do sth here
          $(inputElement).val(decoded);
          clearErrors();
          clearError(encodedErrorElement, 'Valid encoded record!');
        }catch(err) {
          clearErrors();
          showError(encodedErrorElement,err);
          clearText(inputElement);
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

    function clearText(element) {
      element.val('');
    }
 
    function readInput(elementId) {
      var rawInput = $.trim($(elementId).val());
      /* Handle primitive types that don't need to be json. */
      if (!rawInput.startsWith('{')) {
        return rawInput;
      }
      if (!!parsedSchema) {
        return parsedSchema.fromString(rawInput);
      } else {
        /* When parsing the schema itself */
        return JSON.parse(rawInput);
      }
    }

    function readBuffer(elementId) {
      var rawInput = $.trim($(elementId).val());
      var hexArray = rawInput.split('\n');
      var i;
      var size = hexArray.length;
      var buffer = [];
      for (i =0; i < size; i++){
        buffer.push(new Buffer(hexArray[i], 'hex'));
      }
      return Buffer.concat(buffer);
    }

    function bufferToStr(buffer) {
      var size = buffer.length;
      var outStr = '';
      var i;
      for (i = 0; i < size; i++) {
        outStr +=  buffer.toString('hex', i , i+1) +
                  '\n';
      }
      return outStr;
    }
    /* Adjust textbox heights according to current window size */
    function resize() {
      $('#table').removeClass('hidden');
      var vph = $(window).height();
      $('.textbox').css({'height': vph - 200});
    }
 });
})();
