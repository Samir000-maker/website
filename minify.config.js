/**
 * minify.config.js
 * 
 * Standalone Webpack and Terser configuration template for compiling, minifying, 
 * and bundling frontend scripts.
 * 
 * Note: This configuration compiles the original scripts from the root directory
 * into a single output folder (e.g. dist/) keeping their relative filenames intact,
 * completely disabling source maps for production.
 * 
 * To use this configuration:
 * 1. Install devDependencies:
 *    npm install --save-dev webpack webpack-cli terser-webpack-plugin javascript-obfuscator
 * 2. Run the build:
 *    npx webpack --config minify.config.js
 */

import path from 'path';
import { fileURLToPath } from 'url';
import TerserPlugin from 'terser-webpack-plugin';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default {
  mode: 'production',
  // Disable source maps entirely
  devtool: false,
  entry: {
    'index': './index.js',
    'app': './app.js',
    'chat': './chat.js',
    'call': './call.js',
    'mood': './mood.js',
    'social-club': './social-club.js',
    'pwa-install': './pwa-install.js',
    'fluid-bg': './fluid-bg.js',
    'sw': './sw.js',
    'browser-debug': './browser-debug.js'
  },
  output: {
    filename: '[name].js',
    path: path.resolve(__dirname, 'dist'),
    clean: true,
    // Keep global variables accessible on the window object
    libraryTarget: 'window'
  },
  optimization: {
    minimize: true,
    minimizer: [
      new TerserPlugin({
        terserOptions: {
          compress: {
            drop_console: false, // keep console logs for troubleshooting
            drop_debugger: true, // remove debugger statements
          },
          mangle: {
            keep_fnames: true, // preserve function names to prevent scope bugs
          },
          format: {
            comments: false, // strip out comments (including source maps comments)
          }
        },
        extractComments: false,
      }),
    ],
  }
};
