import { Level } from 'level';
import config from './config.js';
import { getDB } from './database.js';
import { ObjectId } from 'mongodb';

// Persistent LevelDB cache
const cacheDB = new Level('./profile-cache', {
  valueEncoding: 'json',
});

/**
 * Get single user profile (cache-first)
 */
export async function getUserProfile(userId) {
  const cacheKey = `user:${userId}`;

  try {
    const cached = await cacheDB.get(cacheKey);
    if (cached?.timestamp) {
      const age = Date.now() - cached.timestamp;
      if (age < config.PROFILE_CACHE_TTL_SECONDS * 1000) {
        return cached.data;
      }
    }
  } catch {
    // cache miss
  }

  try {
    const db = getDB();
    const user = await db.collection('users').findOne(
      { _id: new ObjectId(userId) },
      { projection: { username: 1, pfpUrl: 1, email: 1 } }
    );

    if (user) {
      await cacheDB.put(cacheKey, {
        data: user,
        timestamp: Date.now(),
      });
    }

    return user;
  } catch (err) {
    console.error('❌ getUserProfile error:', err);
    return null;
  }
}

/**
 * Get multiple user profiles efficiently
 */
export async function getUserProfiles(userIds) {
  const profiles = [];
  const missingIds = [];

  for (const id of userIds) {
    const cacheKey = `user:${id}`;
    try {
      const cached = await cacheDB.get(cacheKey);
      if (cached?.timestamp) {
        const age = Date.now() - cached.timestamp;
        if (age < config.PROFILE_CACHE_TTL_SECONDS * 1000) {
          profiles.push(cached.data);
          continue;
        }
      }
    } catch {
      // cache miss
    }
    missingIds.push(id);
  }

  if (missingIds.length === 0) {
    return profiles;
  }

  try {
    const db = getDB();
    const objectIds = missingIds.map(id => new ObjectId(id));

    const users = await db
      .collection('users')
      .find(
        { _id: { $in: objectIds } },
        { projection: { username: 1, pfpUrl: 1, email: 1 } }
      )
      .toArray();

    for (const user of users) {
      const key = `user:${user._id.toString()}`;
      await cacheDB.put(key, {
        data: user,
        timestamp: Date.now(),
      });
      profiles.push(user);
    }

    return profiles;
  } catch (err) {
    console.error('❌ getUserProfiles error:', err);
    return profiles;
  }
}

/**
 * Update cache entry
 */
export async function updateUserProfileCache(userId, profile) {
  await cacheDB.put(`user:${userId}`, {
    data: profile,
    timestamp: Date.now(),
  });
}

/**
 * Invalidate cache entry
 */
export async function invalidateUserProfileCache(userId) {
  try {
    await cacheDB.del(`user:${userId}`);
  } catch {
    // ignore
  }
}

/**
 * Clear entire cache
 */
export async function clearCache() {
  await cacheDB.clear();
}
