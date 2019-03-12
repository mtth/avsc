const rollup = require('rollup');
const resolve = require('rollup-plugin-node-resolve');
const cjs = require('rollup-plugin-commonjs');
const terser = require('rollup-plugin-terser').terser;
const builtins = require('rollup-plugin-node-builtins');

async function build() {
  const avsc = await rollup.rollup({
    input: './etc/browser/avsc-types.js',
    plugins: [
      builtins(),
      //nodeglobals(),
      resolve({browser: true /*,preferBuiltins: false*/ }),
      cjs(),
      terser(),
    ]
  })
    
  await avsc.write({
    name: "leader",
    format: 'iife',
    file: './avsc.js',
    sourcemap: true
  });

}
build().catch(err => {
  console.error(err)
})
