import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Sensitive files that should never be accessed by client requests
const SENSITIVE_FILES = [
  'server.js',
  'database.js',
  'config.js',
  'firebase-auth.js',
  'cloudflare-storage.js',
  'profile-cache.js',
  'matchmaking.js',
  'ecosystem.config.cjs',
  'ecosystem.config.js',
  'package.json',
  'package-lock.json',
  '.env',
  '.git',
  '.gitignore'
];

// Core frontend script files that require protection and dynamically served unminified
const PROTECTED_SCRIPTS = [
  'chat.js',
  'call.js',
  'mood.js',
  'index.js',
  'social-club.js',
  'app.js',
  'pwa-install.js',
  'fluid-bg.js',
  'sw.js',
  'browser-debug.js'
];

/**
 * Validates whether the request is a direct access request (like curl, address-bar typing)
 */
function isDirectAccess(req) {
  const secFetchDest = req.headers['sec-fetch-dest'];
  const secFetchMode = req.headers['sec-fetch-mode'];
  const secFetchSite = req.headers['sec-fetch-site'];
  const referer = req.headers['referer'];
  const host = req.headers['host'];

  // 1. Check if modern browser sec-fetch headers indicate direct navigation or non-script destination
  if (secFetchMode === 'navigate' || secFetchDest === 'document' || secFetchDest === 'iframe') {
    return true;
  }

  // 2. If it is cross-site loading (unless it is localhost development)
  if (secFetchSite && secFetchSite !== 'same-origin' && secFetchSite !== 'none') {
    const isLocalhost = host && (host.includes('localhost') || host.includes('127.0.0.1'));
    if (!isLocalhost) {
      return true;
    }
  }

  // 3. Referer check
  if (referer) {
    try {
      const refUrl = new URL(referer);
      const isLocalhost = host && (host.includes('localhost') || host.includes('127.0.0.1'));
      if (refUrl.host !== host && !isLocalhost) {
        return true;
      }
    } catch (e) {
      return true; // Invalid referer URL format
    }
  } else {
    // If there is no referer and no sec-fetch headers, it's likely a direct command-line or bot load
    if (!secFetchDest && !secFetchMode) {
      return true;
    }
  }

  return false;
}

export default function securityMiddleware(req, res, next) {
  const reqPath = req.path;
  const decodedPath = decodeURIComponent(reqPath);
  const baseName = path.basename(decodedPath);

  // 1. Block access to source maps (.js.map)
  if (reqPath.endsWith('.js.map') || reqPath.includes('.js.map')) {
    console.warn(`[Security] Blocked attempt to download source map: ${reqPath}`);
    res.setHeader('SourceMap', '');
    res.setHeader('X-SourceMap', '');
    return res.status(404).send('Not Found');
  }

  // 2. Block access to sensitive server-side files
  const isSensitive = SENSITIVE_FILES.some(file => {
    const lowerName = baseName.toLowerCase();
    const lowerFile = file.toLowerCase();
    return lowerName === lowerFile || decodedPath.toLowerCase().includes(`/${lowerFile}`);
  });

  if (isSensitive) {
    console.warn(`[Security] Blocked unauthorized access to sensitive server file: ${reqPath} from IP ${req.ip}`);
    return res.status(403).send('Forbidden: Access is denied.');
  }

  // 3. Custom secure API route for loading scripts explicitly (e.g. GET /api/js/chat.js)
  const apiJsMatch = reqPath.match(/^\/api\/js\/(.+)$/);
  if (apiJsMatch) {
    const filename = apiJsMatch[1];
    if (PROTECTED_SCRIPTS.includes(filename)) {
      if (isDirectAccess(req)) {
        console.warn(`[Security] Blocked direct API access to script: ${filename}`);
        return res.status(403).send('Forbidden: Direct script access is prohibited.');
      }

      const filePath = path.join(__dirname, filename);
      if (fs.existsSync(filePath)) {
        try {
          let content = fs.readFileSync(filePath, 'utf8');
          // Dynamically strip any leftover source map comments to prevent DevTools from querying them
          content = content.replace(/\/\/#\s*sourceMappingURL=.*/g, '');

          // Apply strict security and anti-scraping headers
          res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
          res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
          res.setHeader('Pragma', 'no-cache');
          res.setHeader('Expires', '0');
          res.setHeader('X-Content-Type-Options', 'nosniff');
          res.setHeader('SourceMap', '');
          res.setHeader('X-SourceMap', '');

          return res.send(content);
        } catch (err) {
          console.error(`[Security] Error reading file ${filename}:`, err);
          return res.status(500).send('Internal Server Error');
        }
      } else {
        return res.status(404).send('Not Found');
      }
    }
  }

  // 4. Intercept direct frontend script requests (e.g. /chat.js)
  if (PROTECTED_SCRIPTS.includes(baseName) && reqPath.endsWith('.js')) {
    if (isDirectAccess(req)) {
      console.warn(`[Security] Blocked direct script load: ${reqPath}`);
      return res.status(403).send('Forbidden: Direct script access is prohibited.');
    }

    const filePath = path.join(__dirname, baseName);
    if (fs.existsSync(filePath)) {
      try {
        let content = fs.readFileSync(filePath, 'utf8');
        content = content.replace(/\/\/#\s*sourceMappingURL=.*/g, '');

        res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.setHeader('SourceMap', '');
        res.setHeader('X-SourceMap', '');

        return res.send(content);
      } catch (err) {
        console.error(`[Security] Error reading file ${baseName}:`, err);
        return res.status(500).send('Internal Server Error');
      }
    }
  }

  // 5. Apply direct access validation and headers to dynamic config endpoints (without serving the file content directly)
  if (reqPath === '/env-config.js' || reqPath === '/enc-config.js') {
    if (isDirectAccess(req)) {
      console.warn(`[Security] Blocked direct access to dynamic config: ${reqPath}`);
      return res.status(403).send('Forbidden: Direct access to configuration is prohibited.');
    }

    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('SourceMap', '');
    res.setHeader('X-SourceMap', '');
    
    return next();
  }

  // Pass-through for other assets (HTML, images, stylesheets)
  next();
}
