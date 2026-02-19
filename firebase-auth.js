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
