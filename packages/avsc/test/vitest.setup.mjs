import {test as vitestTest, it as vitestIt} from 'vitest';

function wrapTest(original) {
  return (name, fn, timeout) => {
    if (typeof fn === 'function' && fn.length > 0) {
      return original(
        name,
        () =>
          new Promise((resolve, reject) => {
            let finished = false;
            function done(err) {
              if (finished) {
                return;
              }
              finished = true;
              if (err) {
                reject(err);
                return;
              }
              resolve();
            }
            try {
              fn(done);
            } catch (err) {
              reject(err);
            }
          }),
        timeout
      );
    }
    return original(name, fn, timeout);
  };
}

globalThis.test = wrapTest(vitestTest);
globalThis.it = wrapTest(vitestIt);
