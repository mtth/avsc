/* jshint browser: true, browserify: true */
(function () {
  'use strict';
  global.jQuery = require("jquery")
  require('jquery-ui');
  var avsc = require('avsc'),
      buffer = require('buffer'),
      $ = require('jquery');
  require('./lib/jquery-lettering.min.js'); 
  require('jquery-highlight');
  window.avsc = avsc;
  $( function() {
    resize();
    var encodedErrorElement = $('#encoded-error');
    var decodedErrorElement = $('#decoded-error');
    var encodedValidElement = $('#output-valid');
    var decodedValidElement = $('#input-valid');
    var inputElement = $('#input');
    var outputElement = $('#output');
 
    window.onresize = function(event) {
      resize();
    }
    /* When pasting something into an editable div, it 
     * pastes all the html styles with it too, which need to be cleaned up.
     *copied from: http://stackoverflow.com/questions/2176861/javascript-get-clipboard-data-on-paste-event-cross-browser */
    $('[contenteditable]').on('paste',function(e) {
      e.preventDefault();
      var text = (e.originalEvent || e).clipboardData.getData('text/plain');
      window.document.execCommand('insertText', false, text);
      if(e.target.id === 'schema') {
        validateSchema();
        generateRandom();
      }
    });

    /* Validate schema after each new character. */
    $('#schema').on('keyup', function(e) {
      setTimeout(function(){
         validateSchema();
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

  function wrapWordsInSpan(element) {
    element.html(element.text().replace(/\b(\w+)\b/g, "<span>$1</span>"));
  }

   function validateSchema() {
      window.schema = null;
      var elem = $('#schema');
      var valid_elem = $('#schema-valid');
      var error_elem = $('#schema-error');
      try {
        var rawSchema = readSchemaFromInput();
        window.schema = avsc.parse(rawSchema);
        toggleError(error_elem, valid_elem, null);
      } catch (err) {
        toggleError(error_elem, valid_elem, err);
        clearValidIcons();
      }
    }
    function generateRandom() {
      if (window.schema) {
        try{
          var random = window.schema.random();
          var randomStr = window.schema.toString(random);
          var randomJson = JSON.parse(randomStr);
          inputElement.text(JSON.stringify(randomJson, null, 2));
          wrapWordsInSpan(inputElement);
          encode(); /* Update encoded string too. */
        } catch(err) {
          toggleError($('#schema-error'), $('#schema-valid'), err);
        }
      }

    }
    function encode() {
      if (window.schema) {
        try {
          var input = readInput();
          var output = window.schema.toBuffer(input);
          outputElement.text(bufferToStr(output));
          clearErrors();
          wrapWordsInSpan(outputElement);
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
      if (window.schema) {
        try {
          var input = readBuffer(outputElement);
          var decoded = window.schema.fromBuffer(input);
          var decodedStr = window.schema.toString(decoded);
          var decodedJson = JSON.parse(decodedStr);
          $(inputElement).text(JSON.stringify(decodedJson, null, 2));
          clearErrors();
          toggleError(decodedErrorElement, decodedValidElement, null);
          toggleError(encodedErrorElement, encodedValidElement, null);
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
      element.text('');
    }
    /* If the schema is pasted with proper json formats, simply json.parse wouldn't work.*/
    function readSchemaFromInput() {
      var trimmedInput = $.trim($('#schema').text()).replace(/\s/g, "");
      return JSON.parse(trimmedInput);
    }

    function readInput() {
      var rawInput = $.trim($(inputElement).text());
      if(!!window.schema) {
        return window.schema.fromString(rawInput);
      } else {
        return JSON.parse(rawInput);
      }
    }
    /*Used for decoding.
    *Read the text represented as space-seperated hex numbers in elementId
    *and construct a Buffer object*/
    function readBuffer(elementId) {
      var rawInput = $.trim(outputElement.text());
      var hexArray = rawInput.split(/[\s,]+/);
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
      for (i = 1; i <= size; i++) {
        outStr +=  buffer.toString('hex', i-1 , i) + ' ';
        if (i % 8 == 0 ) {
          outStr += '\n';
        } 
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
