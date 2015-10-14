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
    var encodedValidElement = $('#output-valid');
    var decodedValidElement = $('#input-valid');
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
    }).on('input paste', function(e) {
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
      var valid_elem = $('#schema-valid');
      var error_elem = $('#schema-error');
      try {
        var schema = readInput('#schema');
        parsedSchema = avsc.parse(schema);
        toggleError(error_elem, valid_elem, null);
      } catch (err) {
        toggleError(error_elem, valid_elem, err);
        clearValidIcons();
      }
    }
    /* If msg is null, make the valid_element visible, otherwise 
    show `msg` in errorElement. */
    function toggleError(errorElement, valid_element, msg) {
      if(!!msg) {
        errorElement.removeClass('hidden');
        errorElement.text(msg);
        valid_element.addClass('hidden');
      } else {
        errorElement.addClass('hidden');
        errorElement.text("");
        valid_element.show('slow');
      }
    }
 
    function generateRandom() {
      if (parsedSchema) {
        try{
          var random = parsedSchema.random();
          var randomStr = parsedSchema.toString(random);
          inputElement.val(randomStr);
          encode(); /* Update encoded string too. */
          clearErrors();
          toggleError(decodedErrorElement, decodedValidElement, null);
        } catch(err) {
          toggleError($('#schema-error'), $('#schema-valid'), err);
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
          toggleError(decodedErrorElement, decodedValidElement, null);
          toggleError(encodedErrorElement, encodedValidElement, null);
        }catch(err) {
          clearErrors();
          clearValidIcons();
          toggleError(decodedErrorElement, decodedValidElement, err);
          clearText(outputElement);
        }
      } else {
        toggleError(decodedErrorElement, decodedValidElement, 'No valid schema found!');
      }
    }

    function decode() {
      if (parsedSchema) {
        try {
          var input = readBuffer(outputElement);
          var decoded = parsedSchema.fromBuffer(input);
          //todo: probably do sth here
          $(inputElement).val(parsedSchema.toString(decoded));
          clearErrors();
          toggleError(encodedErrorElement, encodedValidElement,null);
        }catch(err) {
          clearErrors();
          clearValidIcons();
          toggleError(encodedErrorElement,encodedValidElement, err);
          clearText(inputElement);
        }
      } else {
        toggleError(encodedErrorElement, encodedValidElement, 'No valid schema found!');
      }
    }

    /* Clear any error messages shown in input/output boxes. */
    function clearErrors() {
      
      decodedErrorElement.text('');
      decodedErrorElement.addClass('hidden');
      encodedErrorElement.text('');
      encodedErrorElement.addClass('hidden');
    }

    function clearValidIcons() {
      decodedValidElement.hide("slow");
      encodedValidElement.hide("slow");
    }
    
    function clearText(element) {
      element.val('');
    }
 
    function readInput(elementId) {
      var rawInput = $.trim($(elementId).val());
      if (!!parsedSchema) {
        return parsedSchema.fromString(rawInput);
      } else {
        /* When parsing the schema itself */
        return JSON.parse(rawInput);
      }
    }

    function readBuffer(elementId) {
      var rawInput = $.trim($(elementId).val());
      var hexArray = rawInput.split(', ');
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
      var commaNeeded = false;
      for (i = 0; i < size; i++) {
        if(commaNeeded) {
          outStr += ', ';
        }
        commaNeeded = true;
        outStr +=  buffer.toString('hex', i , i+1);
      }
      return outStr;
    }
    /* Adjust textbox heights according to current window size */
    function resize() {
      $('#table').removeClass('hidden');
      var vph = $(window).height();
      $('.textbox').css({'height': 0.8 *vph});
    }
 });
})();
