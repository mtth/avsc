/* jshint browser: true, browserify: true */


/* Get the associated data of `elem` from the global cache.
 * (Will create a new entry in cache the first time called for an `elem`.)
*/

var Event = {
  on: function(event, callback) {
    this.hasOwnProperty('events') || (this.events = {});
    this.events.hasOwnProperty(event) || (this.events[event] = []);
    this.events[event].push(callback);
    return this;
  },
  trigger: function(event) {
    var tail = Array.prototype.slice.call(arguments, 1),
        callbacks = this.events[event];
    for (var i = 0, l = callbacks.length; i < l ; i++) {
      callbacks[i].apply(this, tail); // To pass parameters to calback not as an array, but as individual function arguments.  
    }
  }
};

function arraysEqual(a1, a2) {
  if(a1.length !== a2.length) { return false; }
  for (var i = 0; i < a1.length; i++ ) {
    if (a1[i] !== a2[i]) { return false; }
  }
  return true;
}


module.exports = {
  eventObj: Event,
  arraysEqual: arraysEqual
}

