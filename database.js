import { MongoClient } from 'mongodb';
import config from './config.js';

let db = null;
let client = null;

/**
 * Connect to MongoDB
 */
export async function connectDB() {
  if (db) {
    return db;
  }

  try {
    client = new MongoClient(config.MONGO_URI, {
      maxPoolSize: 50,
      minPoolSize: 10,
      maxIdleTimeMS: 30000,
    });

    await client.connect();
    db = client.db(config.DB_NAME);

    console.log('✅ Connected to MongoDB');

    // Create necessary indexes
    await createIndexes();

    return db;
  } catch (err) {
    console.error('❌ MongoDB connection error:', err);
    process.exit(1);
  }
}

/**
 * Create indexes to avoid full collection scans
 */
async function createIndexes() {
  if (!db) return;

  try {
    await db.collection('users').createIndex({ username: 1 }, { unique: true });
    await db.collection('users').createIndex({ email: 1 });
    await db.collection('users').createIndex({ createdAt: -1 });

    await db.collection('notes').createIndex({ createdAt: -1 });
    await db.collection('notes').createIndex({ userId: 1, createdAt: -1 });
    await db.collection('notes').createIndex({ mood: 1, createdAt: -1 });

    console.log('✅ Database indexes created');
  } catch (err) {
    console.error('❌ Error creating indexes:', err);
  }
}

/**
 * Get DB instance
 */
export function getDB() {
  if (!db) {
    throw new Error('Database not initialized. Call connectDB() first.');
  }
  return db;
}

/**
 * Close DB connection
 */
export async function closeDB() {
  if (client) {
    await client.close();
    console.log('✅ MongoDB connection closed');
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  await closeDB();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await closeDB();
  process.exit(0);
});
