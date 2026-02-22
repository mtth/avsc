import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: ['test/**/*.js'],
    exclude: ['test/dat/**'],
    setupFiles: ['./test/vitest.setup.mjs'],
    coverage: {
      provider: 'v8',
      reportsDirectory: '../../out/coverage/avsc',
    },
  },
});
