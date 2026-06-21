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

// Core frontend script files that require protection
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
  // If the request has our custom secure fetch header, it is valid
  if (req.headers['x-secure-fetch'] === 'true') {
    return false;
  }

  const secFetchDest = req.headers['sec-fetch-dest'];
  const secFetchMode = req.headers['sec-fetch-mode'];
  const secFetchSite = req.headers['sec-fetch-site'];
  const referer = req.headers['referer'];
  const host = req.headers['host'];

  // Allow service worker requests or standard script destination
  if (secFetchDest === 'script' || secFetchDest === 'serviceworker') {
    return false;
  }

  // Block direct browser navigation or document/iframe requests
  if (secFetchMode === 'navigate' || secFetchDest === 'document' || secFetchDest === 'iframe') {
    return true;
  }

  // Block cross-site loads
  if (secFetchSite && secFetchSite !== 'same-origin' && secFetchSite !== 'none') {
    const isLocalhost = host && (host.includes('localhost') || host.includes('127.0.0.1'));
    if (!isLocalhost) {
      return true;
    }
  }

  // Referer validation
  if (referer) {
    try {
      const refUrl = new URL(referer);
      const isLocalhost = host && (host.includes('localhost') || host.includes('127.0.0.1'));
      if (refUrl.host !== host && !isLocalhost) {
        return true;
      }
    } catch (e) {
      return true;
    }
  } else {
    // Block command-line/bot scraping
    if (!secFetchDest && !secFetchMode) {
      return true;
    }
  }

  return false;
}

/**
 * Parses HTML and rewrites script tags to fetch and evaluate JavaScript dynamically.
 * This prevents scripts from appearing in the Chrome DevTools 'Sources' file tree.
 */
function rewriteHtmlScripts(html) {
  let scriptCounter = 0;

  return html.replace(/<script\s+([^>]*src=["']([^"']+)["'][^>]*)>\s*<\/script>/gi, (match, attrs, src) => {
    // 1. Skip tailwindcss to prevent FOUC / styling configuration issues
    if (src.includes('tailwindcss') || src.includes('tailwind.config')) {
      return match;
    }

    // Parse filename and check if it is a protected script
    const cleanSrc = src.split('?')[0];
    const baseName = path.basename(cleanSrc);
    const isSecure = PROTECTED_SCRIPTS.includes(baseName) || baseName === 'env-config.js' || baseName === 'enc-config.js';

    const type = isSecure ? 'secure' : 'external';
    
    // Map secure scripts to our API endpoint
    let url = src;
    if (isSecure) {
      url = cleanSrc.startsWith('/api/js/') ? cleanSrc : `/api/js/${baseName}`;
    }

    scriptCounter++;

    // Generate secure dynamic queue loader script
    return `
<script id="sec-loader-${scriptCounter}">
  (function() {
    window._secureScriptQueue = window._secureScriptQueue || [];
    const prev = window._secureScriptQueue.length > 0 
      ? window._secureScriptQueue[window._secureScriptQueue.length - 1].promise 
      : Promise.resolve();
    
    let resolveFn;
    const promise = new Promise((resolve) => { resolveFn = resolve; });
    window._secureScriptQueue.push({ promise });

    prev.then(async () => {
      try {
        if ("${type}" === "secure") {
          const res = await fetch("${url}", { headers: { "X-Secure-Fetch": "true" } });
          if (!res.ok) throw new Error("Load failed");
          const code = await res.text();
          (0, eval)(code);
        } else {
          await new Promise((resSec, rejSec) => {
            const script = document.createElement("script");
            script.src = "${url}";
            script.onload = resSec;
            script.onerror = rejSec;
            document.head.appendChild(script);
          });
        }
      } catch (err) {
        console.error("Failed to load script: ${url}", err);
      } finally {
        resolveFn();
      }
    });
  })();
</script>
`;
  });
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

  // 3. Dynamic HTML rewriting to inject secure loaders
  const acceptHeader = req.headers['accept'] || '';
  const isHtmlRequest = reqPath.endsWith('.html') || reqPath === '/' || (!path.extname(reqPath) && acceptHeader.includes('text/html'));

  if (isHtmlRequest) {
    let filePath = '';
    if (reqPath === '/') {
      filePath = path.join(__dirname, 'index.html');
    } else if (reqPath.endsWith('.html')) {
      filePath = path.join(__dirname, reqPath);
    } else {
      filePath = path.join(__dirname, `${reqPath}.html`);
    }

    if (fs.existsSync(filePath)) {
      try {
        let html = fs.readFileSync(filePath, 'utf8');
        html = rewriteHtmlScripts(html);

        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        return res.send(html);
      } catch (err) {
        console.error(`[Security] Error processing HTML file ${filePath}:`, err);
      }
    }
  }

  // 4. Handle dynamic configuration endpoint rewrite
  if (reqPath === '/api/js/env-config.js' || reqPath === '/api/js/enc-config.js') {
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

    // Internally rewrite the request URL so server.js handles the configuration output
    req.url = '/env-config.js';
    return next();
  }

  // 5. Custom secure API route for loading scripts explicitly (e.g. GET /api/js/chat.js)
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
          console.error(`[Security] Error reading file ${filename}:`, err);
          return res.status(500).send('Internal Server Error');
        }
      } else {
        return res.status(404).send('Not Found');
      }
    }
  }

  // 6. Block direct requests to raw javascript files in root folder (redirect to 403)
  if (PROTECTED_SCRIPTS.includes(baseName) && reqPath.endsWith('.js')) {
    if (isDirectAccess(req)) {
      console.warn(`[Security] Blocked direct script load: ${reqPath}`);
      return res.status(403).send('Forbidden: Direct script access is prohibited.');
    }

    // Allow normal script load (e.g. if loaded directly in old legacy manner, serve it)
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

  // Pass-through for other assets
  next();
}
