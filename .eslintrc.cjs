module.exports = {
  parserOptions: {
    ecmaVersion: 6,
    impliedStrict: true
  },
  env: {
    commonjs: true,
    'shared-node-browser': true
  },

  overrides: [
    {
      files: ['*'],
      excludedFiles: ['etc/issues/**'],

      extends: [
        'eslint:recommended'
      ],
      rules: {
        indent: ['error', 2, {
          SwitchCase: 1,
          VariableDeclarator: 2
        }],
        semi: ['error', 'always'],
        quotes: ['error', 'single'],
        'no-empty': ['off'],
        'no-constant-condition': ['error', {checkLoops: false}],
        'no-eval': ['error'],
        'no-new-func': ['error'],
        'no-loop-func': ['error'],
        'max-len': ['warn', 80, {
          ignoreUrls: true,
          ignoreStrings: true
        }],

        'no-var': ['error'],
        'prefer-arrow-callback': ['error'],
        'arrow-parens': ['error', 'always']
      },
    },
    {
      files: [
        '.eslintrc.cjs',
        'lib/**',
        'etc/benchmarks/**',
        'etc/integration/**',
        'etc/issues/**',
        'etc/schemas/**',
        'etc/scripts/**',
      ],
      env: {
        node: true
      }
    },
    {
      files: ['test/**'],
      env: {
        mocha: true,
        node: true
      }
    },
    {
      files: ['etc/browser/**'],
      env: {
        browser: true
      }
    },
    {
      files: ['etc/issues/**'],
      parserOptions: {
        ecmaVersion: 11
      },
      env: {
        es2020: true
      },
      extends: []
    }
  ]
};
