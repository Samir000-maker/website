// Universal configuration variables
export const config = {
  // Server configuration
  PORT: process.env.PORT || 3000,
  
  // Cloudflare R2 configuration
  CLOUDFLARE_ENDPOINT: "https://d90a9dc9787962f745733993f3f1766d.r2.cloudflarestorage.com",
  BUCKET_NAME: "my-app-posts",
  ACCESS_KEY: "5d5420127538adcf5c50d41735752ffd",
  SECRET_KEY: "6cbc2488a34434653f26399d1644260b50731e7316c97ac4ba7364d66373ff1b",
  
  R2_PUBLIC_URL: "https://pub-b86353e4f63d45f8bf7e94b3143a1d8b.r2.dev",


  // MongoDB configuration
  MONGO_URI: 'mongodb+srv://samir_:fitara@cluster0.cmatn6k.mongodb.net/appdb?retryWrites=true&w=majority',
  DB_NAME: 'db',
  
  // App-level variables
  ROOM_DURATION_MINUTES: 10,
  MAX_USERS_PER_ROOM: 4, // Changed from 4 to 2 for easier testing
  NOTES_PAGE_SIZE: 25,
  PROFILE_CACHE_TTL_SECONDS: 86400, // 24 hours
  
  // File upload limits
  MAX_FILE_SIZE: 5 * 1024 * 1024, // 5MB
  MAX_NOTE_LENGTH: 500,
  
  // Rate limiting
  MAX_MATCHMAKING_REQUESTS_PER_MINUTE: 10,
  
  // Firebase (you'll need to add your Firebase service account key)
  FIREBASE_PROJECT_ID: process.env.FIREBASE_PROJECT_ID || 'projectt3-8c55e',
  
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
