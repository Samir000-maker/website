// Universal configuration variables
export const config = {
  // Server configuration
  PORT: process.env.PORT || 3000,

  CLOUDFLARE_TURN_TOKEN_ID: process.env.CLOUDFLARE_TURN_TOKEN_ID || '',
  CLOUDFLARE_TURN_API_TOKEN: process.env.CLOUDFLARE_TURN_API_TOKEN || '',

  // Cloudflare R2 configuration
  CLOUDFLARE_ENDPOINT: process.env.CLOUDFLARE_ENDPOINT || '',
  BUCKET_NAME: process.env.BUCKET_NAME || '',
  ACCESS_KEY: process.env.ACCESS_KEY || '',
  SECRET_KEY: process.env.SECRET_KEY || '',

  R2_PUBLIC_URL: process.env.R2_PUBLIC_URL || '',
  // MongoDB configuration
  MONGO_URI: process.env.MONGO_URI || '',
  DB_NAME: process.env.DB_NAME || 'db',

  // App-level variables
  ROOM_DURATION_MINUTES: 30,
  MAX_USERS_PER_ROOM: 4, // Changed from 4 to 2 for easier testing
  GLOBAL_SOCIAL_ROOM_SIZE: parseInt(process.env.GLOBAL_SOCIAL_ROOM_SIZE || '2', 10),
  NOTES_PAGE_SIZE: 25,
  PROFILE_CACHE_TTL_SECONDS: 86400, // 24 hours

  // Matchmaking configuration
  MATCHMAKING_TIMEOUT: 30000, // 5 seconds - global configurable search timeout
  MIN_USERS_FOR_ROOM: 2, // Minimum users required to create a room

  // File upload limits
  MAX_FILE_SIZE: 5 * 1024 * 1024, // 5MB (profile pictures)
  MAX_CHAT_ATTACHMENT_BYTES: 10 * 1024 * 1024, // 10MB for chat attachments
  // NOTE: If using nginx reverse proxy, add: client_max_body_size 10M;
  // For Cloudflare: attachment uploads may need larger Limits in dashboard
  MAX_NOTE_LENGTH: 500,

  // Rate limiting
  MAX_MATCHMAKING_REQUESTS_PER_MINUTE: 10,

  // Firebase (you'll need to add your Firebase service account key)
  FIREBASE_PROJECT_ID: process.env.FIREBASE_PROJECT_ID || 'projectt3-8c55e',

  FIREBASE_SERVICE_ACCOUNT_PATH: process.env.FIREBASE_SERVICE_ACCOUNT_PATH || '',

  // Available moods
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
