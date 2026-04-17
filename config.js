export const config = {
  // Server configuration
  PORT: process.env.PORT || 10000,

  allow_multiple_tabs: false,

  // ======================
  // SAFE PUBLIC CONFIG
  // ======================

  // Firebase Web Config (SAFE to expose)
  FIREBASE_WEB_CONFIG: {
    apiKey: "AIzaSyA0aEEUk7uPGk2vK69HjhW7Ug0GCWOksLU",
    authDomain: "projectt3-8c55e.firebaseapp.com",
    projectId: "projectt3-8c55e",
    storageBucket: "projectt3-8c55e.firebasestorage.app",
    messagingSenderId: "64611387728",
    appId: "1:64611387728:web:2e53a18151ab3de1b60455",
    measurementId: "G-0Z91D0Q6EE"
  },

  R2_PUBLIC_URL: "https://pub-b86353e4f63d45f8bf7e94b3143a1d8b.r2.dev",

  // ======================
  // SERVER ENV ONLY
  // ======================

  CLOUDFLARE_TURN_TOKEN_ID: process.env.CLOUDFLARE_TURN_TOKEN_ID || '',
  CLOUDFLARE_TURN_API_TOKEN: process.env.CLOUDFLARE_TURN_API_TOKEN || '',

  CLOUDFLARE_ENDPOINT: process.env.CLOUDFLARE_ENDPOINT || '',
  BUCKET_NAME: process.env.BUCKET_NAME || '',
  ACCESS_KEY: process.env.ACCESS_KEY || '',
  SECRET_KEY: process.env.SECRET_KEY || '',

  MONGO_URI: process.env.MONGO_URI || '',
  DB_NAME: process.env.DB_NAME || 'db',

  FIREBASE_PROJECT_ID: process.env.FIREBASE_PROJECT_ID || '',
  FIREBASE_SERVICE_ACCOUNT_PATH: process.env.FIREBASE_SERVICE_ACCOUNT_PATH || '',

  // ======================
  // APP CONFIG
  // ======================

  ROOM_DURATION_MINUTES: 30,
  MAX_USERS_PER_ROOM: 6,
  GLOBAL_SOCIAL_ROOM_SIZE: parseInt(process.env.GLOBAL_SOCIAL_ROOM_SIZE || '10', 10),
  NOTES_PAGE_SIZE: 25,
  PROFILE_CACHE_TTL_SECONDS: 86400,

  MATCHMAKING_TIMEOUT: 30000,
  MIN_USERS_FOR_ROOM: 2,

  MAX_FILE_SIZE: 5 * 1024 * 1024,
  MAX_CHAT_ATTACHMENT_BYTES: 10 * 1024 * 1024,
  MAX_NOTE_LENGTH: 500,

  MAX_MATCHMAKING_REQUESTS_PER_MINUTE: 10,

  MOODS: [
    { id: 'happy', name: 'Happy', emoji: '😊' },
    { id: 'sad', name: 'Sad', emoji: '😢' },
    { id: 'angry', name: 'Angry', emoji: '😠' },
    { id: 'lonely', name: 'Lonely', emoji: '😔' },
    { id: 'calm', name: 'Calm', emoji: '😌' },
    { id: 'excited', name: 'Excited', emoji: '🤩' },
    { id: 'tired', name: 'Tired', emoji: '😴' },
    { id: 'stressed', name: 'Stressed', emoji: '😣' },
    { id: 'confused', name: 'Confused', emoji: '😕' }
  ]
};

export default config;
