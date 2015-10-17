/* jshint node: true */

'use strict';
var avsc = require('avsc'),
    buffer = require('buffer');
  
/*
* Gets the full path to a field name, 
* and returns two schemas ending before and after the field.
* The full path is given as an array of field names. 
*/
function getSchemasUntil(path, type) {
  if (! path instanceof Array) {
    console.log("Invalid path: " + path);
    return null;
  }
}
