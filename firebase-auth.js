import admin from 'firebase-admin';
import fs from 'fs';
import path from 'path';
import config from './config.js';

let firebaseInitialized = false;

/**
 * Initialize Firebase Admin SDK
 * Must be called before ANY token verification
 */
export function initializeFirebase() {
  if (firebaseInitialized) {
    return admin;
  }

  try {
    const isProduction = (process.env.NODE_ENV || '').toLowerCase() === 'production';

    // ===============================
    // PRODUCTION (service account)
    // ===============================
    if (config.FIREBASE_SERVICE_ACCOUNT_PATH) {
      const serviceAccountPath = path.resolve(
        config.FIREBASE_SERVICE_ACCOUNT_PATH
      );

      if (!fs.existsSync(serviceAccountPath)) {
        throw new Error(
          `Firebase service account file not found at ${serviceAccountPath}`
        );
      }

      const serviceAccount = JSON.parse(
        fs.readFileSync(serviceAccountPath, 'utf8')
      );

      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: config.FIREBASE_PROJECT_ID
      });

      console.log('✅ Firebase Admin initialized with service account');
    }

    // ===============================
    // DEVELOPMENT FALLBACK (NO AUTH)
    // ===============================
    else {
      if (isProduction) {
        throw new Error(
          'Missing FIREBASE_SERVICE_ACCOUNT_PATH in production. Refusing to initialize Firebase Admin without credentials.'
        );
      }

      admin.initializeApp({
        projectId: config.FIREBASE_PROJECT_ID
      });

      console.warn(
        '⚠️ Firebase initialized WITHOUT credentials.\n' +
        '⚠️ Token verification is DISABLED.\n' +
        '⚠️ DO NOT USE THIS IN PRODUCTION.'
      );
    }

    firebaseInitialized = true;
    return admin;
  } catch (error) {
    console.error('❌ Firebase initialization failed:', error);
    throw error;
  }
}

/**
 * Ensure Firebase is initialized before use
 */
function ensureFirebaseInitialized() {
  if (!firebaseInitialized) {
    initializeFirebase();
  }
}

/**
 * Verify Firebase ID token
 */
export async function verifyToken(idToken) {
  ensureFirebaseInitialized();

  if (!idToken) {
    throw new Error('Missing Firebase ID token');
  }

  try {
    return await admin.auth().verifyIdToken(idToken);
  } catch (error) {
    // 🔒 Normalize Firebase errors
    if (error.code === 'auth/id-token-expired') {
      throw new Error('TOKEN_EXPIRED');
    }

    throw new Error('TOKEN_INVALID');
  }
}

/**
 * Required authentication middleware
 */
export async function authenticateFirebase(req, res, next) {
  try {
    const authHeader = req.headers.authorization;

    console.log('🔎 Token received');

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Missing Authorization header'
      });
    }

    const idToken = authHeader.substring(7);
    const decodedToken = await verifyToken(idToken);

    console.log('✅ Firebase user verified:', decodedToken?.uid);

    req.firebaseUser = decodedToken;
    next();
  } catch (error) {
    console.error('❌ Token verification failed');
    try {
      console.error('   Firebase projectId(config):', config.FIREBASE_PROJECT_ID || '(empty)');
      console.error('   Service account configured:', !!config.FIREBASE_SERVICE_ACCOUNT_PATH);
    } catch { }

    try {
      const authHeader = req.headers.authorization;
      const idToken = authHeader?.startsWith('Bearer ') ? authHeader.substring(7) : null;
      if (idToken) {
        const parts = idToken.split('.');
        if (parts.length === 3) {
          const payloadRaw = Buffer.from(parts[1], 'base64').toString('utf8');
          const payload = JSON.parse(payloadRaw);
          console.error('   Token details:', {
            aud: payload?.aud || null,
            iss: payload?.iss || null,
            sub: payload?.sub || null,
            user_id: payload?.user_id || null,
            email: payload?.email || null,
            exp: payload?.exp || null,
            iat: payload?.iat || null
          });
        }
      }
    } catch { }

    try {
      console.error('   Firebase verify error:', {
        code: error?.code || null,
        message: error?.message || null
      });
    } catch { }

    const message =
      error.message === 'TOKEN_EXPIRED'
        ? 'Authentication token expired'
        : 'Invalid authentication token';

    return res.status(401).json({
      error: 'Unauthorized',
      message
    });
  }
}

/**
 * Optional authentication middleware
 * (never blocks request)
 */
export async function optionalFirebaseAuth(req, res, next) {
  try {
    const authHeader = req.headers.authorization;

    if (authHeader?.startsWith('Bearer ')) {
      const idToken = authHeader.substring(7);
      req.firebaseUser = await verifyToken(idToken);
    }
  } catch {
    // Intentionally ignored
  }

  next();
}
