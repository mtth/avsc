module.exports = {
  parserOptions: {
    ecmaVersion: 2021,
    impliedStrict: true
  },
  env: {
    commonjs: true,
    es6: true,
    'shared-node-browser': true
  },

  overrides: [
    {
      files: ['*'],
      excludedFiles: ['dist/**'],

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
        'arrow-parens': ['error', 'always'],
        'no-unused-vars': ['error', {
          ignoreRestSiblings: true,
        }]
      },
    },
    {
      files: [
        '.eslintrc.cjs',
        'lib/**',
        'etc/benchmarks/**',
        'etc/integration/**',
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
    }
  ]
};
