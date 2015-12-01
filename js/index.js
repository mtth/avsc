/* jshint browser: true, browserify: true */
var cache = {},
    guidCounter = 1,
    dataKey = 'data'  + (new Date).getTime();
 
(function () {
  'use strict';
  global.jQuery = require("jquery")
  require('jquery-ui');
  var avsc = require('avsc'),
      buffer = require('buffer'),
      $ = require('jquery');
  window.avsc = avsc;
  $( function() {

    var outputErrorElement = $('#encoded-error'),
        inputErrorElement = $('#decoded-error'),
        encodedValidElement = $('#output-valid'),
        decodedValidElement = $('#input-valid'),
        schemaErrorElement = $('#schema-error'),
        schemaValidElement = $('#schema-valid'),
        schemaElement = document.getElementById('schema'),
        inputElement = $('#input'),
        outputElement = $('#output'),
        width = 100,
        body = document.getElementsByTagName('body')[0],
        arrayKeyPattern = /-(\d+)-/g,
        reservedKeysPattern = /-[a-z]+-/g,
        typingTimer,
        eventObj = Event,
        doneTypingInterval = 500; // wait for some time before processing user input.
    
    resize();
    window.onresize = function(event) {
      resize();
    }
    window.reverseIndexMap = [];  
       
    /* When pasting something into an editable div, it 
     * pastes all the html styles with it too, which need to be cleaned up.
     * copied from: 
     * http://stackoverflow.com/questions/2176861/javascript-get-clipboard-data-on-paste-event-cross-browser */

    $('[contenteditable]').on('paste',function(e) {
      e.preventDefault();
      clearTimeout(typingTimer);

      var text = (e.originalEvent || e).clipboardData.getData('text/plain');
      window.document.execCommand('insertText', false, text);
      if(e.target.id === 'schema') {
        if(updateContent(schemaElement)) {
          eventObj.trigger('schema-changed');
          generateRandom();
        };
      }
    });

    $('#schema').on('keyup', function() {
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function () {
        if(updateContent(schemaElement)) {
          eventObj.trigger('schema-changed');
        }
      }, doneTypingInterval);
    }).on('keydown', function() {
      clearTimeout(typingTimer);
    });

    $('#input').on('paste keyup', function(event) {

      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        if(updateContent(inputElement)) {
          eventObj.trigger('input-changed');
        };
      }, doneTypingInterval);
    }).on('keydown', function() {
      clearTimeout(typingTimer);
    }).on('mouseover', 'span', function(event) {
      if (window.instrumented) {
         /* It's important to clear it when the mouse moves from one span to another with the same parent,
           * to clear out the parent being highlighted. */
        clearHighlights();

        /*Will also automatically highlight all nested children.*/
        highlight($(this)); 

        /*So that the parent won't be highlighted (because we are using mouseover and not mouseenter)*/
        event.stopPropagation(); 

        var path = getPathFromParents($(this));
        var position = findPositionOf(path);
        highlightOutput(position.start, position.end); 
      } else 
        console.log("No instrumented type found");
    }).on('mouseleave', 'span', function(event) {
      clearHighlights();
    });

    $('#output').on('paste keyup', function(event) {
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        if(updateContent(outputElement)) {
          eventObj.trigger('output-changed');
        };
      }, doneTypingInterval);
    }).on('mouseover', 'div', function(event) {
      if (window.reverseIndexMap) {
        clearHighlights();
        event.stopPropagation();
        
        var path = getPathFromClasses($(this));
        //TODO: why should these two be differnet? 
        var outputSelector = '.' + path.join('.');
        var inputSelector = '.' + path.join(' .');
        var inputCandidates = $(inputElement).find(inputSelector);
        
        // TODO: well... here comes the hack... 
        if(inputCandidates.length == 0) {
          path.pop();
          inputSelector = '.' + path.join(' .');
          inputCandidates = $(inputElement).find(inputSelector);
        }

        highlight($(outputElement).children(outputSelector));
        highlight(inputCandidates);
      }
    }).on('mouseleave', 'div', function (event) { 
      clearHighlights(); 
    }).on('keydown', function(event) {
      clearTimeout(typingTimer);
    });

    $('#random').click(function () {   
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        generateRandom();
      }, doneTypingInterval);
    });

    eventObj.on('schema-changed', function() {
      runPreservingCursorPosition( 'schema', validateSchema);
    });

    eventObj.on('input-changed', function() {
      runPreservingCursorPosition( 'input' , function () {
        var rawInput = $.trim($(inputElement).text());        
        try {
          var inputJson = JSON.parse(rawInput);
          var inputRecord = window.schema._copy(inputJson, {coerce: 2});
          var invalidPaths = [];
          var isValid = window.schema.isValid(inputRecord, {errorHook: function (p) {
            invalidPaths.push(p.join());
          }});
          if(!isValid) {
            eventObj.trigger('invalid-input', invalidPaths);
          } else {
            eventObj.trigger('valid-input');
          }
          // Wrap key values in <span>.
          setInputText(rawInput);
        } catch (err) {
          eventObj.trigger('invalid-input', err);
        }
      });
      encode();
    });

    eventObj.on('output-changed', function() {
      decode();
    });

    eventObj.on('valid-schema', function() {
      hideError(schemaErrorElement, schemaValidElement);
    });

    eventObj.on('invalid-schema', function (message) {
      showError(schemaErrorElement, message);
    });

    eventObj.on('valid-input', function () { 
      hideError(inputErrorElement, decodedValidElement);
    });

    eventObj.on('invalid-input', function(message) {
      showError(inputErrorElement, message);
    });

    eventObj.on('valid-output', function () { 
      hideError(outputErrorElement, encodedValidElement);
    });

    eventObj.on('invalid-output', function(message) {
      showError(outputErrorElement, message);
    });

    
    /**
    * Will save cursor position inside element `elemId` before running callback function f,
    * and restores it after f is finished. 
    */ 
    function runPreservingCursorPosition(elementId, f) {
     //Get current position.
      var range = window.getSelection().getRangeAt(0);
      var el = document.getElementById(elementId);
      var position = getCharacterOffsetWithin(range, el);
      f();
      setCharacterOffsetWithin(range, el, position);
    } 
    /*
    * When the input text changes, the whole text is replaced with new <span> elements,
    * and the previous cursor position will be lost. 
    *
    * This function will go through all the child elements of `node` and sets the
    * caret to the `position`th character.
    */ 

    function setCharacterOffsetWithin(range, node, position) {
      var treeWalker = document.createTreeWalker(
          node,
          NodeFilter.SHOW_TEXT
      );
      var charCount = 0, foundRange = false;
      while (treeWalker.nextNode() && !foundRange) {
          if (charCount + treeWalker.currentNode.length < position)
            charCount += treeWalker.currentNode.length;
          else {
            var newRange = document.createRange();
            newRange.setStart(treeWalker.currentNode, position - charCount);
            newRange.setEnd(treeWalker.currentNode, position - charCount);
            newRange.collapse(true);

            var sel = window.getSelection();
            sel.removeAllRanges();
            sel.addRange(newRange);
            foundRange = true;
          }
      }
    }
    
    /**
    * From: http://jsfiddle.net/timdown/2YcaX/
    * http://stackoverflow.com/questions/4767848/get-caret-cursor-position-in-contenteditable-area-containing-html-content
    */
    function getCharacterOffsetWithin(range, node) {
      var treeWalker = document.createTreeWalker(
          node,
          NodeFilter.SHOW_TEXT,
          function(node) {
              var nodeRange = document.createRange();
              nodeRange.selectNode(node);
              return nodeRange.compareBoundaryPoints(Range.END_TO_END, range) < 1 ?
                  NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
          },
          false
      );
      var charCount = 0;
      while (treeWalker.nextNode()) {
          charCount += treeWalker.currentNode.length;
      }
      if (range.startContainer.nodeType == Node.TEXT_NODE) { 
          charCount += range.startOffset;
      }
      return charCount;
    }

    /**
    * Goes through all parents of an element, and concatenates
    * their classes to generate the path for the given key.  
    */
    function getPathFromParents(element) {
      var selfClass = $.trim(element.attr('class').replace(reservedKeysPattern, ''));
      var parents = element.parents('span').map(function () {
        var parentClass = $(this).attr('class').replace(reservedKeysPattern, '');
        return $.trim(parentClass);
      }).get();
      parents.reverse(); /* parents() will go through parents starting from the inner most,
                            so it needs to be reversed to get the correct path. */

      if (selfClass != '' ) 
        parents.push(selfClass); /* The innermost class is not part of the parents. Adding it here. */
      return parents;
    }

    function getPathFromClasses(element) {
      return $.trim(element.attr('class').replace(reservedKeysPattern, '')).split(' ');
    }

    /**
    * find the start and end index of an entry in its encoded representation
    * using the instrumented type already loaded in window.instrumented.
    *
    */
    function findPositionOf(path) {
      var current = window.instrumented;
      path.forEach(function(entry) {
        var arrayKey = arrayKeyPattern.exec(entry);
        var nextKey = arrayKey ? arrayKey[1] : entry;
        if (nextKey in current.value) {
          current = current.value[nextKey];
        } else {
          $.each(current.value, function(k,v) {
            current = v;
            return false;
          });
        }
      });
      return current;
    }

    /*
    * Find all spans that have the same class, and highlights them,
    * so if a key is selected, its value will be also highlighted, and vice versa.  
    */
    function highlightAllMatching(classesString) {
      var rawClasses = classesString[0] == ' ' ? classesString : ' ' + classesString;
      rawClasses = rawClasses.replace(/ /g, ' .');
      $(rawClasses).each( function(i) {
        highlight($(this));
      });
    }

    /**
    * Highlight the entries between `start` and `end` in the output (encoded) text.
    */  
    function highlightOutput(start, end) {
      outputElement.children('div').each(function( index ) {
        if (index >= start && index < end) {
          highlight($(this));
        }
      });
    }

    /* Add -highlight- to the element class */
    function highlight(element) {
      element.addClass('-highlight-');
    }

    function addClassToOutputWithRange(cls, start, end) {
      outputElement.children('span').each(function( index ) {
        if (index >= start && index < end) {
          $(this).addClass(cls);
        }
      });
      for (var i = start; i < end; i++) {
        if (reverseIndexMap[i]) {
          reverseIndexMap[i].push(cls);
        } else {
          reverseIndexMap[i] = [cls];
        }
      }
    }
    

    /**
    * Remove `highlight` from all spans. 
    */
    function clearHighlights() {
      $('span').removeClass('-highlight-');
      $('div').removeClass('-highlight-');
    }

    /**
    * set the input box's text to inputStr, 
    * where all key, values are wrapped in <span> elements
    * with the 'path' set as the span class. 
    */
    function setInputText(inputStr) {
      var input = JSON.parse(inputStr);
      var stringified = stringify(input, 1); 
      inputElement.html(stringified);
    }

    /**
    * Set the output box's text to outputStr where each byte is wrapped in <span>
    * elements and each line contains 8 bytes.
    */ 

    function setOutputText(outputStr) { 
      var res = '';
      var str = outputStr.replace(/\s+/g, '');
      var i, len;
      for (i =0, len = str.length; i < len; i += 2){
        res += createDiv(window.reverseIndexMap[i/2], str[i] + str[i + 1] + '&nbsp;');
      }
      outputElement.html(res);
    }

    /**
    * Similar to JSON.stringify, but will wrap each key and value 
    * with <span> tags. 
    * Does a DFS over the obj, to propagate the parent keys to each 
    * child element to be set in the span's class attribute.
    * @param obj The object to stringify
    * @param depth Current indention level.
    */
    function stringify(obj, depth) {

      var res = '';
      if ( obj == null ) {
        return createDiv('-null-', 'null');
      }
      if (typeof obj === 'number') {
        return  createDiv('-number-', obj + '');
      }
      if (typeof obj === 'boolean') {
        return createDiv('-boolean-', obj + '');
      }
      if (typeof obj === 'string') {
        // Calling json.stringify here to handle the fixed types.
        // I have no idea why just printing them doesn't work.
        return createDiv('-string-', JSON.stringify(obj));
      }
      var comma = false;
      if (obj instanceof Array) {
        res += '[<br/>';
        $.each(obj, function(index, value) {
          if (comma) res += ',<br/>';
          // Use '-' as a special character, which can not exist in schema keys 
          // but is a valid character for css class.
          res += createSpan(index , indent(depth) + stringify(value, depth + 1));
          comma = true;
        });
        res += '<br/>' + indent(depth - 1) + ']';
        return res;
      } 
      res += '{<br/>';
      comma = false;
      $.each(obj, function(key, value) {
        if (comma) res += ',<br/>';
        res += createSpan(key, indent(depth) + '"' + key + '": ' + stringify(value, depth + 1));
        comma = true;
      });
      res += '<br/>' + indent(depth - 1) + '}';
      return res;
    }

    function createSpan(cl, str) {
      return '<span class="' + cl + '">' + str + '</span>'; 
    }

    function createDiv(cl, str) {
      return '<div class="-inline- ' + cl + '">' + str + '</div>';
    }
    function indent(depth) { 
      var res = '';
      for (var i = 0 ; i < 2 * depth; i++) res += ' ';
      return res;
    }

    function validateSchema() {
      window.schema = null;
      try {
        var rawSchema = readSchemaFromInput();
        $(schemaElement).text(JSON.stringify(rawSchema, null, 2));
        window.schema = avsc.parse(rawSchema);
        eventObj.trigger('valid-schema');
      } catch (err) {
        eventObj.trigger('invalid-schema', err);
      }
    }
    function generateRandom() {
      if (window.schema) {
        var random = window.schema.random();
        var randomStr = window.schema.toString(random);
        setInputText(randomStr);
        eventObj.trigger('input-changed');
      }
    }

    /**
    * Read the input as text from inputElement.
    * Instrument it and update window.instrumented.
    * Encode it and set the outputElement's text to the encoded data
    */   
    function encode() {
      if (window.schema) {
        try {
          var input = readInput();
          window.instrumented = instrumentObject(window.schema, input);
          window.reverseIndexMap = computeReverseIndex(window.instrumented);
          var output = window.schema.toBuffer(input);
          setOutputText(output.toString('hex'));
          eventObj.trigger('valid-output');
        }catch(err) {
          eventObj.trigger('invalid-input', err);
        }
      }
    }

    function decode() {
      if (window.schema) {
        try {
          var input = readBuffer(outputElement);
          var decoded = window.schema.fromBuffer(input);
          var decodedStr = window.schema.toString(decoded);
          eventObj.trigger('valid-output');
          setInputText(decodedStr);
        }catch(err) {
          eventObj.trigger('invalid-output', err);
        }
      }
    }

    function showError(errorElem, msg) {
      errorElem.text(msg);
      errorElem.removeClass('-hidden-');
    };

    function hideError(errorElem, validElem) {
      errorElem.text("");
      errorElem.addClass('-hidden-');
      validElem.show('slow').delay(500).hide('slow');
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
    
    /* Adjust textbox heights according to current window size */
    function resize() {
      $('#table').removeClass('-hidden-');
      var vph = $(window).height();
      $('.-textbox-').css({'height': 0.8 *vph});
      width = outputElement.width();
    }

    /**
     * Will update the old value of the element to the new one, 
     * and returns true if the content changed. 
    */
    function  updateContent(element) {
      var newText = $.trim($(element).text()).replace(/\s+/g, '');
      if (!element.data) {
        element.data = {};
      }
      if (!element.data['oldValue'] || 
          element.data['oldValue'] != newText) {
        element.data['oldValue'] =  newText;
        return true;
      }
      return false;
    }

    function instrument(schema) {
      if (schema instanceof avsc.types.Type) {
        schema = schema.getSchema();
      }
      var refs = [];
      return avsc.parse(schema, {typeHook: hook});

      function hook(schema, opts) {
        if (~refs.indexOf(schema)) {
          return;
        }
        refs.push(schema);

        if (schema.type === 'record') {
          schema.fields.forEach(function (f) { f['default'] = undefined; });
        }

        var name = schema.name;
        if (name) {
          schema.name = 'r' + Math.random().toString(36).substr(2, 6);
        }
        var wrappedSchema = {
          name: name || 'r' + Math.random().toString(36).substr(2, 6),
          namespace: schema.namespace,
          type: 'record',
          fields: [{name: 'value', type: schema}]
        };
        refs.push(wrappedSchema);

        var type = avsc.parse(wrappedSchema, opts);
        var read = type._read;
        type._read = function (tap) {
          var pos = tap.pos;
          var obj = read.call(type, tap);
          obj.start = pos;
          obj.end = tap.pos;
          return obj;
        };
        return type;
      }
    }

    /**
     * Convenience method to instrument a single object.
     * 
     * @param type {Type} The type to be instrumented.
     * @param obj {Object} A valid instance of `type`.
     * 
     * Returns an representation of `obj` with start and end markers.
     * 
     */
    function instrumentObject(type, obj) {
      return instrument(type).fromBuffer(type.toBuffer(obj));
    }

    /**
     * Creates an array of size buffer.length, 
     * where each index will contain a string representing
     * the path in the input record corresponding to this byte.
     */
    function computeReverseIndex(obj) {
      if (!obj) {
        return;
      }
      // initialize an array with all empty elements;
      var size = obj.end;
      var res = Array.apply(null, Array(size))
                     .map(String.prototype.valueOf,"");
      assignLabels('', obj, res);
      return res;
    }

    function assignLabels(key, node, res) {
      if (node.hasOwnProperty('start') && node.hasOwnProperty('end')) {
        appendLabel(node.start, node.end, key, res);
      }
      var valueNode = node.value;
      if (valueNode) {
        for (var child in valueNode) {
          if (valueNode.hasOwnProperty(child)) {
            assignLabels(child, valueNode[child], res);
          }
        }
      }
    }

    function appendLabel(start, end, label, arr) {
      for (var i = start; i < end; i++)
        arr[i] += ' ' + label;
    }

 });
})();

/* Get the associated data of `elem` from the global cache.
 * (Will create a new entry in cache the first time called for an `elem`.)
*/

var Event = {
  on: function(event, callback) {
    this.hasOwnProperty('events') || (this.events = {});
    this.events.hasOwnProperty(event) || (this.events[event] = []);
    this.events[event].push(callback);
  },
  trigger: function(event) {
    var tail = Array.prototype.slice.call(arguments, 1),
        callbacks = this.events[event];
    for (var i = 0, l = callbacks.length; i < l ; i++) {
      callbacks[i].apply(this, tail); // To pass parameters to calback not as an array, but as individual function arguments.  
    }
  }
};

