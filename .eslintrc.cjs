const fs = require('fs');
const path = require('path');

const prettierOptions = JSON.parse(fs.readFileSync(path.resolve(__dirname, '.prettierrc'), 'utf8'));

module.exports = {
  env: {
    browser: true,
    es6: true,
    mocha: true,
  },
  root: true,
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended', 'prettier'],
  plugins: ['@typescript-eslint', 'prettier'],
  globals: {},
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2022,
    sourceType: 'module',
  },
  rules: {
    'max-len': ['error', { code: 160 }],
    'prettier/prettier': ['error', prettierOptions],
    'import/prefer-default-export': 'off',
    'import/extensions': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
  },
};
