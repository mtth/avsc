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
    /* Change the color if this is the active textarea*/
    $('.textbox').click( function() { 
      $(this).addClass('editing');
    }).blur(function() {
      $(this).removeClass('editing');
     });

    /* Validate schema after each new character. */
    $('#schema').keyup(function(event) {
        validateSchema();
    });
    function validateSchema() {
      var elem = $('#schema');
      var error_elem = $('#schema-error');
      try {
        var schema = readInput('#schema');
        var p = avsc.parse(schema);
        error_elem.removeClass('error');
        error_elem.addClass('valid');
        error_elem.text('Valid Schema!');
      } catch (err) {
        showError(error_elem, err);
      }
    }
 
    $('#encode').click(function() {
      var schema = readInput('#schema');
      var input = readInput('#input');
      var p = avsc.parse(schema);
      try {
        var output = p.encode(input);
        $('#output').val(bufferToStr(output));
      }catch(err) {
        showError($('#decoded-error'),err);
      } 
    });

     $('#decode').click(function() {
      var schema = readInput('#schema');
      var input = readBuffer('#output');
      try {
        var p = avsc.parse(schema);
        var decoded = p.decode(input);
        
        decoded = JSON.stringify(decoded, null, 2);
        $('#input').val(decoded);
      }catch(err) {
        showError($('#encoded-error'),err);
      } 
    });

    $('#random').click(function() {
      try{
        var schema = readInput('#schema');
        var p = avsc.parse(schema);
        var random = p.random();
        var randomStr = JSON.stringify(random, null, 2);
        $('#randomInput').val(randomStr);
      } catch(err) {
        showError($('#schema-error'),err);
      }
    });


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

  function resize() {
    var vph = $(window).height();
    $('.container').css({'height': vph + 'px'});
  }
  window.onresize = function(event) {
    resize();
  }
 
})();
