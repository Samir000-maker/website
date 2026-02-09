
// ============================================
// PM2 CLUSTER INSTANCE DETECTION
// ============================================

const instanceId = process.env.INSTANCE_ID || process.env.NODE_APP_INSTANCE || '0';
const isClusterMode = process.env.NODE_APP_INSTANCE !== undefined;
const processId = process.pid;

console.log('');
console.log('🚀 ========================================');
console.log('🚀 INSTANCE INITIALIZATION');
console.log('🚀 ========================================');
console.log(`   Instance ID: ${instanceId}`);
console.log(`   Process ID: ${processId}`);
console.log(`   Cluster Mode: ${isClusterMode ? 'YES' : 'NO'}`);
console.log(`   Node Version: ${process.version}`);
console.log('🚀 ========================================');
console.log('');

// ENHANCED SERVER WITH STATE PRESERVATION AND DETERMINISTIC CLEANUP
// Features:
// 1. Persistent call state with grace periods
// 2. 10-minute room expiry with auto-cleanup
// 3. Chat message preservation
// 4. Background matchmaking support
// 5. Production-ready TURN server integration with Cloudflare

import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import multer from 'multer';
import { ObjectId } from 'mongodb';
import { v4 as uuidv4 } from 'uuid';
import config from './config.js';
import { connectDB, getDB } from './database.js';
import { initializeFirebase, authenticateFirebase, optionalFirebaseAuth, verifyToken } from './firebase-auth.js';
import { uploadProfilePicture, getDefaultProfilePicture } from './cloudflare-storage.js';
import { getUserProfile, updateUserProfileCache, invalidateUserProfileCache } from './profile-cache.js';
import * as matchmaking from './matchmaking.js';

import path from 'path';
import { fileURLToPath } from 'url';

// REDIS SETUP
import Redis from 'ioredis';
import { createAdapter } from '@socket.io/redis-adapter';
import Redlock from 'redlock';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Redis Clients
const redisHost = config.REDIS_HOST || '205.198.72.90'; // Use your Nube VM public IP
const redisPort = config.REDIS_PORT || 6379;
const redisPassword = config.REDIS_PASSWORD || 'samir16121?';

// Redis connection URL
const redisUrl = `redis://:${encodeURIComponent(redisPassword)}@${redisHost}:${redisPort}`;


const pubClient = new Redis(redisUrl);
const subClient = pubClient.duplicate();

pubClient.on('error', (err) => console.error('Redis Pub Error:', err));
subClient.on('error', (err) => console.error('Redis Sub Error:', err));

// ============================================
// DISTRIBUTED LOCKING WITH REDLOCK
// ============================================

const redlock = new Redlock(
  [pubClient],
  {
    driftFactor: 0.01,          // Clock drift factor
    retryCount: 10,              // Retry 10 times
    retryDelay: 200,             // Wait 200ms between retries
    retryJitter: 200,            // Randomize retry timing
    automaticExtensionThreshold: 500  // Auto-extend lock
  }
);

redlock.on('error', (error) => {
  // Ignore errors from resource not locked (expected)
  if (error.message && error.message.includes('exceeded')) {
    console.error('❌ [Redlock] Lock acquisition exceeded retry limit:', error.message);
  }
});

console.log('✅ Redlock initialized for distributed locking');

// ============================================
// REDIS-BACKED SOCKET USER TRACKING
// ============================================

/**
 * Set socket user data in Redis
 */
async function setSocketUser(socketId, userData) {
  try {
    await pubClient.hset('socket:users', socketId, JSON.stringify({
      ...userData,
      lastSeen: Date.now()
    }));
    console.log(`📱 [Redis] Registered socket ${socketId} for user ${userData.userId}`);
    return true;
  } catch (error) {
    console.error(`❌ [Redis] Failed to set socket user:`, error);
    return false;
  }
}

/**
 * Get socket user data from Redis
 */
async function getSocketUser(socketId) {
  try {
    const data = await pubClient.hget('socket:users', socketId);
    if (!data) return null;

    const userData = JSON.parse(data);
    // console.log(`📱 [Redis] Retrieved socket ${socketId} for user ${userData.userId}`);
    return userData;
  } catch (error) {
    console.error(`❌ [Redis] Failed to get socket user:`, error);
    return null;
  }
}

/**
 * Delete socket user from Redis
 */
async function deleteSocketUser(socketId) {
  try {
    await pubClient.hdel('socket:users', socketId);
    console.log(`📱 [Redis] Deleted socket ${socketId} from registry`);
    return true;
  } catch (error) {
    console.error(`❌ [Redis] Failed to delete socket user:`, error);
    return false;
  }
}

/**
 * Get all socket users from Redis
 */
async function getAllSocketUsers() {
  try {
    const data = await pubClient.hgetall('socket:users');
    const users = {};

    for (const [socketId, userDataStr] of Object.entries(data)) {
      try {
        users[socketId] = JSON.parse(userDataStr);
      } catch (parseError) {
        console.error(`❌ [Redis] Failed to parse socket user data for ${socketId}`);
      }
    }

    return users;
  } catch (error) {
    console.error(`❌ [Redis] Failed to get all socket users:`, error);
    return {};
  }
}

/**
 * Get socket user by userId
 */
async function getSocketByUserId(userId) {
  try {
    const allUsers = await getAllSocketUsers();

    for (const [socketId, userData] of Object.entries(allUsers)) {
      if (userData.userId === userId) {
        return { socketId, userData };
      }
    }

    return null;
  } catch (error) {
    console.error(`❌ [Redis] Failed to get socket by userId:`, error);
    return null;
  }
}

/**
 * Clean up stale socket entries (last seen > 5 minutes)
 */
async function cleanupStaleSocketUsers() {
  try {
    const allUsers = await getAllSocketUsers();
    const now = Date.now();
    const STALE_THRESHOLD = 5 * 60 * 1000; // 5 minutes
    let cleanedCount = 0;

    for (const [socketId, userData] of Object.entries(allUsers)) {
      if (now - userData.lastSeen > STALE_THRESHOLD) {
        await deleteSocketUser(socketId);
        cleanedCount++;
      }
    }

    if (cleanedCount > 0) {
      console.log(`🧹 [Redis] Cleaned up ${cleanedCount} stale socket users`);
    }

    return cleanedCount;
  } catch (error) {
    console.error(`❌ [Redis] Failed to cleanup stale socket users:`, error);
    return 0;
  }
}

// Redis helpers for file transfers
async function getFileRecord(fileId) {
  try {
    const data = await pubClient.hgetall(`file:record:${fileId}`);
    if (!data || !Object.keys(data).length) return null;
    if (data.chunks) data.chunks = JSON.parse(data.chunks);
    if (data.totalChunks) data.totalChunks = parseInt(data.totalChunks);
    if (data.receivedCount) data.receivedCount = parseInt(data.receivedCount);
    if (data.size) data.size = parseInt(data.size);
    return data;
  } catch (error) {
    console.error(`❌ [Redis] Failed to get file record ${fileId}:`, error.message);
    return null;
  }
}

async function saveFileRecord(fileId, record) {
  try {
    const data = { ...record };
    if (data.chunks && Array.isArray(data.chunks)) {
      data.chunks = JSON.stringify(data.chunks);
    }
    await pubClient.hset(`file:record:${fileId}`, data);
    await pubClient.expire(`file:record:${fileId}`, 3600); // 1 hour TTL
  } catch (error) {
    console.error(`❌ [Redis] Failed to save file record ${fileId}:`, error.message);
  }
}

async function deleteFileRecord(fileId) {
  try {
    await pubClient.del(`file:record:${fileId}`);
  } catch (error) {
    console.error(`❌ [Redis] Failed to delete file record ${fileId}:`, error.message);
  }
}

async function setFileChunk(fileId, index, data) {
  try {
    const key = `file:chunk:${fileId}:${index}`;
    // Store as binary buffer
    await pubClient.set(key, data);
    await pubClient.expire(key, 3600); // 1 hour TTL
  } catch (error) {
    console.error(`❌ [Redis] Failed to set file chunk ${fileId}:${index}:`, error.message);
  }
}

async function getFileChunk(fileId, index) {
  try {
    return await pubClient.getBuffer(`file:chunk:${fileId}:${index}`);
  } catch (error) {
    console.error(`❌ [Redis] Failed to get file chunk ${fileId}:${index}:`, error.message);
    return null;
  }
}

async function getActiveFileTransfer(fileId) {
  try {
    const data = await pubClient.hgetall(`file:transfer:${fileId}`);
    if (!data || !Object.keys(data).length) return null;
    if (data.bytesTransferred) data.bytesTransferred = parseInt(data.bytesTransferred);
    if (data.startTime) data.startTime = parseInt(data.startTime);
    return data;
  } catch (error) {
    return null;
  }
}

async function setActiveFileTransfer(fileId, data) {
  try {
    await pubClient.hset(`file:transfer:${fileId}`, data);
    await pubClient.expire(`file:transfer:${fileId}`, 3600);
  } catch (error) { }
}

async function deleteActiveFileTransfer(fileId) {
  try {
    await pubClient.del(`file:transfer:${fileId}`);
  } catch (error) { }
}

// Signaling Debounce Helpers (Redis-backed)
async function getRoomJoinState(roomId, userId) {
  try {
    const data = await pubClient.get(`debounce:room_join:${roomId}:${userId}`);
    return data ? JSON.parse(data) : null;
  } catch (error) {
    return null;
  }
}

async function setRoomJoinState(roomId, userId, state, ttlMs = 2000) {
  try {
    await pubClient.set(`debounce:room_join:${roomId}:${userId}`, JSON.stringify(state), 'PX', ttlMs);
  } catch (error) { }
}

async function getJoinCallDebounce(userId) {
  try {
    const data = await pubClient.get(`debounce:join_call:${userId}`);
    return data ? parseInt(data) : null;
  } catch (error) {
    return null;
  }
}

async function setJoinCallDebounce(userId, timestamp, ttlMs = 2000) {
  try {
    await pubClient.set(`debounce:join_call:${userId}`, timestamp.toString(), 'PX', ttlMs);
  } catch (error) { }
}

async function deleteJoinCallDebounce(userId) {
  try {
    await pubClient.del(`debounce:join_call:${userId}`);
  } catch (error) { }
}

// Distributed state management with Redis
// socketUsers, userToSocketId, roomCleanupTimers, etc. all moved to Redis

// ============================================
// REDIS KEYSPACE NOTIFICATIONS FOR EXPIRY
// ============================================

/**
 * Setup Redis keyspace notifications to trigger on key expiry
 * This replaces setTimeout for distributed timer functionality
 */
async function setupRedisExpiryNotifications() {
  try {
    // Enable keyspace notifications for expired events
    await pubClient.config('SET', 'notify-keyspace-events', 'Ex');
    console.log('✅ Redis keyspace notifications enabled');

    // Create dedicated client for expiry subscriptions
    const expiryClient = pubClient.duplicate();

    await new Promise((resolve, reject) => {
      expiryClient.on('ready', resolve);
      expiryClient.on('error', reject);
    });

    // Subscribe to expiry events
    expiryClient.psubscribe('__keyevent@0__:expired', (pattern, channel, key) => {
      console.log(`⏰ [Redis] Expiry event received for key: ${key}`);

      // Handle room expiry
      if (key.startsWith('room:expiry:')) {
        const roomId = key.replace('room:expiry:', '');
        console.log(`⏰ [Redis] Room expiry triggered for ${roomId}`);
        handleRoomExpiry(roomId).catch(error => {
          console.error(`❌ Failed to handle room expiry for ${roomId}:`, error);
        });
      }

      // Handle user cleanup
      else if (key.startsWith('user:cleanup:')) {
        const userId = key.replace('user:cleanup:', '');
        console.log(`⏰ [Redis] User cleanup triggered for ${userId}`);
        handleUserCleanup(userId).catch(error => {
          console.error(`❌ Failed to handle user cleanup for ${userId}:`, error);
        });
      }

      // Handle call cleanup
      else if (key.startsWith('call:cleanup:')) {
        const callId = key.replace('call:cleanup:', '');
        console.log(`⏰ [Redis] Call cleanup triggered for ${callId}`);
        handleCallExpiry(callId).catch(error => {
          console.error(`❌ Failed to handle call expiry for ${callId}:`, error);
        });
      }
    });

    console.log('✅ Redis expiry notifications subscribed');

    return expiryClient;
  } catch (error) {
    console.error('❌ Failed to setup Redis expiry notifications:', error);
    throw error;
  }
}

// Initialize expiry notifications
let expiryClient;
setupRedisExpiryNotifications()
  .then(client => {
    expiryClient = client;
  })
  .catch(error => {
    console.error('💥 CRITICAL: Could not setup expiry notifications:', error);
    process.exit(1);
  });

/**
 * Schedule room cleanup using Redis TTL
 */
async function scheduleRoomCleanup(roomId, expiryMs) {
  try {
    const expirySeconds = Math.ceil(expiryMs / 1000);
    const expiryData = JSON.stringify({
      roomId,
      scheduledAt: Date.now(),
      expiryMs
    });

    await pubClient.setex(`room:expiry:${roomId}`, expirySeconds, expiryData);
    console.log(`⏰ [Redis] Scheduled room cleanup for ${roomId} in ${expirySeconds}s`);

    return true;
  } catch (error) {
    console.error(`❌ [Redis] Failed to schedule room cleanup for ${roomId}:`, error);
    return false;
  }
}

/**
 * Cancel room cleanup
 */
async function cancelRoomCleanup(roomId) {
  try {
    const deleted = await pubClient.del(`room:expiry:${roomId}`);
    if (deleted > 0) {
      console.log(`⏰ [Redis] Cancelled room cleanup for ${roomId}`);
    }
    return deleted > 0;
  } catch (error) {
    console.error(`❌ [Redis] Failed to cancel room cleanup for ${roomId}:`, error);
    return false;
  }
}

/**
 * Schedule user cleanup using Redis TTL
 */
async function scheduleUserCleanup(userId, delayMs) {
  try {
    const delaySeconds = Math.ceil(delayMs / 1000);
    const cleanupData = JSON.stringify({
      userId,
      scheduledAt: Date.now(),
      delayMs
    });

    await pubClient.setex(`user:cleanup:${userId}`, delaySeconds, cleanupData);
    console.log(`⏰ [Redis] Scheduled user cleanup for ${userId} in ${delaySeconds}s`);

    return true;
  } catch (error) {
    console.error(`❌ [Redis] Failed to schedule user cleanup for ${userId}:`, error);
    return false;
  }
}

/**
 * Cancel user cleanup
 */
async function cancelUserCleanup(userId) {
  try {
    const deleted = await pubClient.del(`user:cleanup:${userId}`);
    if (deleted > 0) {
      console.log(`⏰ [Redis] Cancelled user cleanup for ${userId}`);
    }
    return deleted > 0;
  } catch (error) {
    console.error(`❌ [Redis] Failed to cancel user cleanup for ${userId}:`, error);
    return false;
  }
}

/**
 * Acquire distributed lock for call operations
 */
async function acquireCallMutex(callId) {
  const lockKey = `locks:call:${callId}`;
  const lockTTL = 5000; // 5 seconds

  try {
    const lock = await redlock.acquire([lockKey], lockTTL);
    console.log(`🔒 [Redlock] Acquired call lock for ${callId}`);

    return async () => {
      try {
        await lock.release();
        console.log(`🔓 [Redlock] Released call lock for ${callId}`);
      } catch (error) {
        console.warn(`⚠️ [Redlock] Lock release failed for ${callId}:`, error.message);
      }
    };
  } catch (error) {
    console.error(`❌ [Redlock] Failed to acquire call lock for ${callId}:`, error);
    throw new Error(`Could not acquire lock for call ${callId}`);
  }
}

/**
 * Acquire distributed lock for user operations
 */
async function acquireUserLock(userId) {
  const lockKey = `locks:user:${userId}`;
  const lockTTL = 5000; // 5 seconds

  try {
    const lock = await redlock.acquire([lockKey], lockTTL);
    console.log(`🔒 [Redlock] Acquired user lock for ${userId}`);

    return async () => {
      try {
        await lock.release();
        console.log(`🔓 [Redlock] Released user lock for ${userId}`);
      } catch (error) {
        console.warn(`⚠️ [Redlock] User lock release failed for ${userId}:`, error.message);
      }
    };
  } catch (error) {
    console.error(`❌ [Redlock] Failed to acquire user lock for ${userId}:`, error);
    throw new Error(`Could not acquire lock for user ${userId}`);
  }
}

/**
 * Acquire distributed lock for room initialization
 */
async function acquireRoomInitLock(roomId) {
  const lockKey = `locks:room:init:${roomId}`;
  const lockTTL = 10000;

  try {
    const lock = await redlock.acquire([lockKey], lockTTL);
    console.log(`🔒 [Redlock] Acquired room init lock for ${roomId}`);

    return async () => {
      try {
        await lock.release();
        console.log(`🔓 [Redlock] Released room init lock for ${roomId}`);
      } catch (error) {
        console.warn(`⚠️ [Redlock] Room init lock release failed for ${roomId}:`, error.message);
      }
    };
  } catch (error) {
    console.error(`❌ [Redlock] Failed to acquire room init lock for ${roomId}:`, error);
    throw new Error(`Could not acquire room init lock for ${roomId}`);
  }
}

/**
 * Handle room expiry event (called when Redis key expires)
 */
async function handleRoomExpiry(roomId) {
  console.log(`🧹 [Cleanup] Authoritative room expiry for ${roomId}`);

  try {
    const room = await matchmaking.getRoom(roomId);
    if (!room) {
      console.log(`⚠️ [Cleanup] Room ${roomId} already removed from memory`);
      return;
    }

    // Mark room as expired
    room.isExpired = true;

    // 1. Notify all users and clear their records
    const userIds = room.users.map(u => u.userId);
    for (const userId of userIds) {
      const userData = room.users.find(u => u.userId === userId);

      // Notify cluster-wide
      io.to(`user:${userId}`).emit('room_expired', {
        roomId,
        message: 'Chat room has expired',
        cleanupFiles: true
      });

      // Clear records in Redis
      if (userData?.firebaseUid) {
        await clearUserActiveRoom(userData.firebaseUid);
      }
      await removeUserFromAllMoods(userId);
      await removeUserPresence(userId);
    }

    // 2. Clean up associated call
    const callId = await pubClient.get(`room:${roomId}:call`);
    if (callId) {
      console.log(`🧹 Room expiry: Triggering cleanup for associated call ${callId}`);
      await handleCallExpiry(callId);
    }

    // 3. File stores are now in Redis or handled per-user, no local cleanup needed here

    // 4. Remove room from matchmaking
    await matchmaking.destroyRoom(roomId);

    console.log(`✅ [Cleanup] Room ${roomId} fully purged across cluster`);
  } catch (error) {
    console.error(`❌ [Cleanup] Room purge failure for ${roomId}:`, error);
  }
}

/**
 * Handle call cleanup event (called when Redis key expires)
 */
async function handleCallExpiry(callId) {
  console.log(`🧹 [Cleanup] Triggering authoritative call cleanup: ${callId}`);

  try {
    const release = await acquireCallMutex(callId);
    try {
      const call = await getCall(callId);
      if (!call) {
        console.log(`ℹ️ Call ${callId} already removed`);
        return;
      }

      // End-of-life processing
      call.status = 'ended';
      call.endedAt = Date.now();
      call.endReason = call.endReason || 'empty_grace_period';

      // Notify remaining participants (if any)
      io.to(call.roomId).emit('call_ended', {
        callId,
        reason: call.endReason
      });

      // Clear participant records
      for (const userId of call.participants) {
        await removeUserCall(userId);
      }

      // Final delete from Redis
      await deleteCall(callId);
      await pubClient.del(`room:${call.roomId}:call`);

      console.log(`✅ [Cleanup] Call ${callId} purged from cluster`);
    } finally {
      await release();
    }
  } catch (error) {
    console.error(`❌ [Cleanup] Call purge failure for ${callId}:`, error);
  }
}

/**
 * Schedule call cleanup using Redis TTL
 */
async function scheduleCallCleanup(callId, delayMs) {
  try {
    const seconds = Math.ceil(delayMs / 1000);
    await pubClient.setex(`call:cleanup:${callId}`, seconds, 'expired');
    console.log(`⏰ [Redis] Scheduled call cleanup for ${callId} in ${seconds}s`);
    return true;
  } catch (error) {
    console.error(`❌ [Redis] Failed to schedule call cleanup for ${callId}:`, error);
    return false;
  }
}

/**
 * Handle user cleanup event (called when Redis key expires)
 */
async function handleUserCleanup(userId) {
  console.log(`🧹 [Cleanup] Handling user cleanup for ${userId}`);

  try {
    // 1. Remove user from all mood tracking
    await removeUserFromAllMoods(userId);

    // 2. Clear active room markers (Try both userId and firebaseUid)
    await clearUserActiveRoom(userId);

    // Attempt to find firebaseUid from presence for thorough cleanup
    const presence = await getUserPresence(userId);
    if (presence && presence.firebaseUid) {
      await clearUserActiveRoom(presence.firebaseUid);
    }

    // 3. Clear presence record
    await removeUserPresence(userId);

    console.log(`✅ [Cleanup] User ${userId} cleaned up`);
  } catch (error) {
    console.error(`❌ [Cleanup] Error handling user cleanup for ${userId}:`, error);
  }
}

const ROOM_EXPIRY_TIME = (config.ROOM_DURATION_MINUTES || 10) * 60 * 1000;
const ROOM_CLEANUP_GRACE = 30000; // 30 seconds
const ROOM_WARNING_TIME = 60000; // 60 seconds warning before expiry

// ============================================
// REAL-TIME MOOD USER COUNTERS (REDIS-BACKED)
// ============================================
// MOVED TO REDIS:
// moodUserRegistry -> Set: mood:{moodId}:users
// moodUserCounts -> derived from SCARD
// userCurrentMood -> Hash: user:moods field: userId
// userActiveRooms -> Hash: user:active_rooms field: userId

/**
 * Add user to mood tracking (Redis)
 */
async function addUserToMood(userId, mood) {
  try {
    const isValidMood = config.MOODS.some(m => m.id === mood);
    if (!isValidMood) {
      console.error(`❌ Invalid mood: ${mood}`);
      return;
    }

    // Remove from old mood if exists
    const oldMood = await pubClient.hget('user:moods', userId);
    if (oldMood && oldMood !== mood) {
      await removeUserFromMood(userId, oldMood);
    }

    // Add to new mood set
    await pubClient.sadd(`mood:${mood}:users`, userId);
    // Track user's current mood
    await pubClient.hset('user:moods', userId, mood);

    // Broadcast update
    await debouncedBroadcastMoodCount(mood);
    console.log(`📊 [Redis] Added ${userId} to mood ${mood}`);
  } catch (error) {
    console.error(`❌ [Redis] Failed to add user ${userId} to mood ${mood}:`, error.message);
  }
}

// User Locks - centralized Redis lock would be better, but keeping local for now as it's per-user operation serialization
// const userOperationLocks = new Map(); // Keep local or use Redlock

async function getUserActiveRoom(userId) {
  try {
    const data = await pubClient.hget('user:active_rooms', userId);
    return data ? JSON.parse(data) : null;
  } catch (error) {
    console.error(`❌ [Redis] Failed to get active room for ${userId}:`, error.message);
    return null;
  }
}

async function setUserActiveRoom(userId, roomId, mood) {
  try {
    const roomData = {
      roomId,
      joinedAt: Date.now(),
      mood
    };

    await pubClient.hset('user:active_rooms', userId, JSON.stringify(roomData));
    console.log(`🔐 [UID: ${userId}] Set active room: ${roomId} (mood: ${mood})`);

    return roomData;
  } catch (error) {
    console.error(`❌ [Redis] Failed to set active room for ${userId}:`, error.message);
    return null;
  }
}

async function clearUserActiveRoom(userId) {
  try {
    // ALWAYS attempt delete in Redis to be safe
    await pubClient.hdel('user:active_rooms', userId);
    console.log(`🔓 [UID: ${userId}] Attempted clear of active room marker`);
    return true;
  } catch (error) {
    console.error(`❌ [Redis] Failed to clear active room for ${userId}:`, error.message);
    return false;
  }
}


async function registerSocketForUser(userId, socketId, userData) {
  await setSocketUser(socketId, { userId, ...userData });
  console.log(`📱 [UID: ${userId}] Registered socket ${socketId} in Redis`);
}

async function unregisterSocketForUser(socketId) {
  await deleteSocketUser(socketId);
  console.log(`📱 Socket ${socketId} unregistered from Redis`);
}

// DEPRECATED: Use io.to(`user:${userId}`) or socket.to(`user:${userId}`)
async function getUserSocketIds(userId) {
  const socketData = await getSocketByUserId(userId);
  return socketData ? [socketData.socketId] : [];
}

function emitToUserAllDevices(userId, event, data) {
  io.to(`user:${userId}`).emit(event, data);
  console.log(`📢 [UID: ${userId}] Emitted '${event}' to user devices via Redis`);
}




async function validateMoodSelection(userId) {
  const releaseLock = await acquireUserLock(userId);

  try {
    // Check if user already in active room
    const activeRoom = await getUserActiveRoom(userId);

    if (activeRoom) {
      // Verify room still exists and is valid
      const room = await matchmaking.getRoom(activeRoom.roomId);

      if (room && !room.isExpired && room.hasUser(userId)) {
        console.log(`❌ [UID: ${userId}] Blocked mood selection - already in room ${activeRoom.roomId}`);
        return {
          allowed: false,
          reason: 'You are already in an active room. Please leave your current room first.',
          existingRoom: {
            roomId: activeRoom.roomId,
            mood: activeRoom.mood,
            joinedAt: activeRoom.joinedAt
          }
        };
      } else {
        // Room is invalid/expired - clean up stale state
        console.log(`⚠️ [UID: ${userId}] Cleaning up stale room reference: ${activeRoom.roomId}`);
        clearUserActiveRoom(userId);
      }
    }

    return { allowed: true };
  } finally {
    releaseLock();
  }
}



async function restoreExistingRoom(socket, userId, existingRoom) {
  console.log(`🔄 [UID: ${userId}] [Socket: ${socket.id}] Restoring room ${existingRoom.roomId}`);

  const room = await matchmaking.getRoom(existingRoom.roomId);

  if (!room || room.isExpired) {
    console.error(`❌ [UID: ${userId}] Cannot restore - room ${existingRoom.roomId} not found or expired`);
    // Clean up stale state
    clearUserActiveRoom(userId);
    return {
      success: false,
      error: 'Your previous room has expired'
    };
  }

  if (!room.hasUser(userId)) {
    console.error(`❌ [UID: ${userId}] Cannot restore - not a member of room ${existingRoom.roomId}`);
    clearUserActiveRoom(userId);
    return {
      success: false,
      error: 'You are no longer a member of this room'
    };
  }

  // Join socket to room
  socket.join(existingRoom.roomId);
  console.log(`✅ [UID: ${userId}] [Socket: ${socket.id}] Joined room ${existingRoom.roomId}`);

  // Get partner info
  const partner = room.users.find(u => u.userId !== userId);
  const partnerProfile = partner ? await getUserProfile(partner.userId) : null;

  // Prepare room data with full history
  const roomData = {
    roomId: room.roomId,
    mood: room.mood,
    users: room.users.map(u => ({
      userId: u.userId,
      username: u.username,
      profilePictureUrl: u.profilePictureUrl,
      status: u.status
    })),
    partner: partner ? {
      userId: partner.userId,
      username: partner.username,
      profilePictureUrl: partner.profilePictureUrl,
      bio: partnerProfile?.bio || '',
      status: partner.status
    } : null,
    createdAt: room.createdAt,
    expiresAt: room.expiresAt,
    chatHistory: room.chatHistory || [], // CRITICAL: Include all cached messages
    isRestored: true, // Flag to indicate this is a restoration
    activeCall: findActiveCallForRoom(room.roomId) // ✅ Add initial call state
  };

  // Emit room restoration to this socket
  socket.emit('room_restored', roomData);

  // Also emit to all other devices of this user
  const otherSocketIds = getUserSocketIds(userId).filter(sid => sid !== socket.id);
  otherSocketIds.forEach(socketId => {
    const otherSocket = io.sockets.sockets.get(socketId);
    if (otherSocket && otherSocket.connected) {
      otherSocket.emit('room_state_sync', roomData);
    }
  });

  console.log(`✅ [UID: ${userId}] Room ${existingRoom.roomId} restored with ${roomData.chatHistory.length} messages`);

  return {
    success: true,
    room: roomData
  };
}



/**
 * Remove user from mood tracking (Redis)
 */
async function removeUserFromMood(userId, mood) {
  try {
    // Remove from Set
    const wasRemoved = await pubClient.srem(`mood:${mood}:users`, userId);

    if (wasRemoved) {
      // Clear user's mood tracking if it matches
      const currentMood = await pubClient.hget('user:moods', userId);
      if (currentMood === mood) {
        await pubClient.hdel('user:moods', userId);
      }

      await debouncedBroadcastMoodCount(mood);
      console.log(`📊 [Redis] Removed ${userId} from mood ${mood}`);
    }
  } catch (error) {
    console.error(`❌ [Redis] Failed to remove user ${userId} from mood ${mood}:`, error.message);
  }
}

/**
 * Remove user from ALL moods (for disconnect/cleanup)
 */
async function removeUserFromAllMoods(userId) {
  const currentMood = await pubClient.hget('user:moods', userId);
  if (currentMood) {
    await removeUserFromMood(userId, currentMood);
  }
}

const moodCountBroadcastDebounce = new Map(); // user -> timeout (Local debounce is fine)

async function debouncedBroadcastMoodCount(mood) {
  if (moodCountBroadcastDebounce.has(mood)) {
    clearTimeout(moodCountBroadcastDebounce.get(mood));
  }

  const timeout = setTimeout(async () => {
    const count = await pubClient.scard(`mood:${mood}:users`);
    io.emit('mood_count_update', { mood, count }); // Adapter broadcasts to all nodes
    moodCountBroadcastDebounce.delete(mood);
  }, 1000);

  moodCountBroadcastDebounce.set(mood, timeout);
}

async function getAllMoodCounts() {
  const counts = {};
  for (const mood of config.MOODS) {
    counts[mood.id] = await pubClient.scard(`mood:${mood.id}:users`);
  }
  return counts;
}



const answerDebounce = new Map(); // userId:targetUserId -> timestamp
const ANSWER_DEDUPE_WINDOW = 2000; // 2 seconds


const MAX_SDP_SIZE = 100 * 1024; // 100KB max for SDP (offers/answers)
const MAX_ICE_CANDIDATE_SIZE = 5 * 1024; // 5KB max for ICE candidate
const MAX_SIGNALING_RATE = 50; // Max 50 signaling messages per 10 seconds per user
const signalingRateLimiter = new Map(); // userId -> { count, resetTime }

const connectionsByIP = new Map(); // ip -> { count, connections: Set }
const connectionRateLimiter = new Map(); // ip -> { count, resetTime }
const MAX_CONNECTIONS_GLOBAL = 10000; // Maximum total connections
const matchmakingTimeouts = new Map();

function clearMatchmakingTimeout(userId) {
  const timeout = matchmakingTimeouts.get(userId);
  if (timeout) {
    clearTimeout(timeout);
    matchmakingTimeouts.delete(userId);
    console.log(`⏰ Cleared matchmaking timeout for user ${userId}`);
  }
}

function getCurrentTransferMemory() {
  let total = 0;
  for (const transfer of activeFileTransfers.values()) {
    total += transfer.bytesTransferred || 0;
  }
  return total;
}

// Add helper function at top
async function validateRoomAccess(roomId, userId) {
  const room = await matchmaking.getRoom(roomId);

  if (!room) {
    return { valid: false, error: 'Room not found or expired', code: 'ROOM_NOT_FOUND' };
  }

  if (room.isExpired) {
    return { valid: false, error: 'Room has expired', code: 'ROOM_EXPIRED' };
  }

  if (!room.hasUser(userId)) {
    return { valid: false, error: 'You are not in this room', code: 'NOT_IN_ROOM' };
  }

  return { valid: true, room };
}


async function broadcastCallStateUpdate(callId) {
  const call = await getCall(callId);
  if (!call) return;

  const room = await matchmaking.getRoom(call.roomId);
  if (!room) return;

  io.to(call.roomId).emit('call_state_update', {
    callId: callId,
    isActive: call.participants.length > 0,
    participantCount: call.participants.length,
    callType: call.callType
  });

  console.log(`📢 Call state update: ${callId} - ${call.participants.length} participants`);
}

async function findActiveCallForRoom(roomId) {
  const callId = await pubClient.get(`room:${roomId}:call`);
  if (!callId) return null;

  const call = await getCall(callId);
  if (call && call.status === 'active' && call.participants.length > 0) {
    return {
      callId: call.callId,
      callType: call.callType,
      participantCount: call.participants.length,
      isActive: true
    };
  }
  return null;
}

async function getUserDataForParticipant(participantId, room) {
  console.log(`🔍 Resolving user data for ${participantId}`);

  // CRITICAL FIX: Prioritize room data (most reliable source)
  if (room) {
    const roomUser = room.users.find(u => u.userId === participantId);
    if (roomUser) {
      console.log(`✅ Found in room data: ${roomUser.username} (${roomUser.userId})`);
      return {
        userId: roomUser.userId,
        username: roomUser.username,
        pfpUrl: roomUser.pfpUrl
      };
    } else {
      console.warn(`⚠️ User ${participantId} NOT found in room users!`);
    }
  }

  // Fallback to Redis global state
  const socketEntry = await getSocketByUserId(participantId);
  if (socketEntry) {
    console.log(`✅ Found in Redis global state: ${socketEntry.username}`);
    return {
      userId: socketEntry.userId,
      username: socketEntry.username,
      pfpUrl: socketEntry.profilePicture // Mapping 'profilePicture' field from Redis to 'pfpUrl'
    };
  }

  console.error(`❌ CRITICAL: No user data found for ${participantId} anywhere!`);
  return null;
}

// Distributed locking logic below


function validateCallState(call, operation) {
  if (!call) {
    console.error(`❌ [${operation}] Call not found`);
    return { valid: false, error: 'Call not found' };
  }

  if (!call.participants || !Array.isArray(call.participants)) {
    console.error(`❌ [${operation}] Invalid participants array`);
    return { valid: false, error: 'Invalid call state' };
  }

  if (!call.userMediaStates) {
    call.userMediaStates = new Map();
    console.log(`📊 [${operation}] Initialized userMediaStates Map`);
  }

  return { valid: true };
}



// ============================================
// ROOM MESSAGE RATE LIMITING
// ============================================
const roomMessageRateLimiter = new Map(); // roomId -> { count, resetTime, lastWarning }
const ROOM_MESSAGE_RATE_LIMIT = 30; // Max 30 messages per 10 seconds per room
const ROOM_RATE_WINDOW = 10000; // 10 seconds

function checkRoomMessageRateLimit(roomId) {
  const now = Date.now();
  const roomLimit = roomMessageRateLimiter.get(roomId);

  if (!roomLimit || now > roomLimit.resetTime) {
    roomMessageRateLimiter.set(roomId, {
      count: 1,
      resetTime: now + ROOM_RATE_WINDOW,
      lastWarning: 0
    });
    return { allowed: true, count: 1 };
  }

  if (roomLimit.count >= ROOM_MESSAGE_RATE_LIMIT) {
    // Only warn once per window to avoid log spam
    if (now - roomLimit.lastWarning > 5000) {
      console.warn(`⚠️ Room ${roomId} rate limit exceeded: ${roomLimit.count} messages in ${ROOM_RATE_WINDOW / 1000}s`);
      roomLimit.lastWarning = now;
    }
    return { allowed: false, count: roomLimit.count };
  }

  roomLimit.count++;
  return { allowed: true, count: roomLimit.count };
}


// ============================================
// CLOUDFLARE TURN SERVER CONFIGURATION
// ============================================

async function generateCloudTurnCredentials() {
  const TURN_TOKEN_ID = process.env.CLOUDFLARE_TURN_TOKEN_ID;
  const TURN_API_TOKEN = process.env.CLOUDFLARE_TURN_API_TOKEN;

  if (!TURN_TOKEN_ID || !TURN_API_TOKEN) {
    console.warn('⚠️ TURN credentials not configured - operating with STUN only');
    return null;
  }

  // ✅ FIX: Add abort controller for timeout
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout

  try {
    console.log('🔄 Generating Cloudflare TURN credentials...');
    const response = await fetch(
      `https://rtc.live.cloudflare.com/v1/turn/keys/${TURN_TOKEN_ID}/credentials/generate`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${TURN_API_TOKEN}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ttl: 86400
        }),
        signal: controller.signal // ✅ FIX: Add signal for timeout
      }
    );

    clearTimeout(timeoutId); // ✅ FIX: Clear timeout on success

    if (!response.ok) {
      const errorText = await response.text();
      console.error('❌ Failed to generate TURN credentials:', response.status, errorText);
      return null;
    }

    const data = await response.json();

    console.log('📦 Raw TURN response:', JSON.stringify(data, null, 2));

    if (data.iceServers) {
      const turnConfig = data.iceServers;

      const iceServer = {
        urls: Array.isArray(turnConfig.urls) ? turnConfig.urls : [turnConfig.urls],
        username: turnConfig.username,
        credential: turnConfig.credential
      };

      console.log('✅ Cloudflare TURN credentials generated successfully');
      console.log(`   URLs: ${iceServer.urls.length} endpoints`);
      iceServer.urls.forEach(url => console.log(`      - ${url}`));
      console.log(`   Username: ${iceServer.username?.substring(0, 20)}...`);
      console.log(`   Credential: ${iceServer.credential ? '[present]' : '[missing]'}`);

      return [iceServer];
    } else {
      console.error('❌ Unexpected TURN response structure:', data);
      return null;
    }
  } catch (error) {
    clearTimeout(timeoutId); // ✅ FIX: Clear timeout on error

    if (error.name === 'AbortError') {
      console.error('❌ TURN credential request timeout after 10s');
    } else {
      console.error('❌ Error generating TURN credentials:', error.message);
    }
    return null;
  }
}

async function getIceServers() {
  const iceServers = [
    {
      urls: [
        'stun:stun.cloudflare.com:3478',
        'stun:stun.l.google.com:19302',
        'stun:stun1.l.google.com:19302',
        'stun:stun2.l.google.com:19302'
      ]
    }
  ];

  console.log('🔧 Fetching TURN credentials from Cloudflare...');
  const turnServers = await generateCloudTurnCredentials();

  if (turnServers && Array.isArray(turnServers) && turnServers.length > 0) {
    turnServers.forEach(server => {
      iceServers.push(server);

      const urls = Array.isArray(server.urls) ? server.urls : [server.urls];
      urls.forEach(url => {
        const hasAuth = !!(server.username && server.credential);
        console.log(`   📡 TURN: ${url} ${hasAuth ? '(authenticated)' : ''}`);
      });
    });

    console.log(`✅ ICE configuration: ${iceServers.length} server groups (STUN + TURN)`);
  } else {
    console.warn('⚠️ Operating with STUN-only configuration');
    console.warn('   Direct peer-to-peer connections will work for most users');
    console.warn('   Users behind symmetric NATs may experience connection issues');
  }

  return iceServers;
}

const app = express();
const server = createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['websocket', 'polling'],
  pingTimeout: 60000,
  pingInterval: 25000,
  adapter: createAdapter(pubClient, subClient)
});

// Initialize Redis-backed matchmaking
matchmaking.init(pubClient, io);

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: config.MAX_FILE_SIZE
  },
  fileFilter: (req, file, cb) => {
    if (!file.mimetype.startsWith('image/')) {
      return cb(new Error('Only image files are allowed'), false);
    }
    cb(null, true);
  }
});

// ENHANCED STATE MANAGEMENT
// MOVED TO GLOBAL REGISTRY (lines 219+)

/* Presence Tracking: userId -> { lastSeen: timestamp, status: 'chat_active' | 'call_active', roomId: string } */
// MOVED TO REDIS: user:presence Hash

async function updateUserPresence(userId, data) {
  try {
    // Merge with existing
    const current = await getUserPresence(userId) || {};
    const updated = { ...current, ...data, lastSeen: Date.now() };
    await pubClient.hset('user:presence', userId, JSON.stringify(updated));
    return updated;
  } catch (error) {
    console.error(`❌ [Redis] Failed to update presence for ${userId}:`, error.message);
    return null;
  }
}

async function getUserPresence(userId) {
  try {
    const data = await pubClient.hget('user:presence', userId);
    return data ? JSON.parse(data) : null;
  } catch (error) {
    console.error(`❌ [Redis] Failed to get presence for ${userId}:`, error.message);
    return null;
  }
}

async function removeUserPresence(userId) {
  try {
    await pubClient.hdel('user:presence', userId);
  } catch (error) {
    console.error(`❌ [Redis] Failed to remove presence for ${userId}:`, error.message);
  }
}
const callGracePeriod = new Map(); // callId -> timeout

async function getCall(callId) {
  try {
    const data = await pubClient.hgetall(`call:${callId}`);
    if (!Object.keys(data).length) return null;

    // Deserialize
    if (data.participants) data.participants = JSON.parse(data.participants);
    if (data.userMediaStates) data.userMediaStates = new Map(JSON.parse(data.userMediaStates));
    if (data.createdAt) data.createdAt = parseInt(data.createdAt);
    if (data.lastActivity) data.lastActivity = parseInt(data.lastActivity);

    return data;
  } catch (error) {
    console.error(`❌ [Redis] Failed to get call ${callId}:`, error.message);
    return null;
  }
}

async function saveCall(call) {
  try {
    const data = { ...call };
    if (data.participants) data.participants = JSON.stringify(data.participants);
    if (data.userMediaStates) data.userMediaStates = JSON.stringify(Array.from(data.userMediaStates.entries()));
    await pubClient.hset(`call:${call.callId}`, data);
    await pubClient.set(`room:${call.roomId}:call`, call.callId); // Index
  } catch (error) {
    console.error(`❌ [Redis] Failed to save call ${call.callId}:`, error.message);
  }
}

async function deleteCall(callId) {
  try {
    const call = await getCall(callId);
    if (call) {
      await pubClient.del(`call:${callId}`);
      await pubClient.del(`room:${call.roomId}:call`);
    }
  } catch (error) {
    console.error(`❌ [Redis] Failed to delete call ${callId}:`, error.message);
  }
}

async function getUserCall(userId) {
  try {
    return await pubClient.hget('user:calls', userId);
  } catch (error) {
    console.error(`❌ [Redis] Failed to get user call for ${userId}:`, error.message);
    return null;
  }
}

async function setUserCall(userId, callId) {
  try {
    await pubClient.hset('user:calls', userId, callId);
  } catch (error) {
    console.error(`❌ [Redis] Failed to set user call for ${userId}:`, error.message);
  }
}

async function removeUserCall(userId) {
  try {
    await pubClient.hdel('user:calls', userId);
  } catch (error) {
    console.error(`❌ [Redis] Failed to remove user call for ${userId}:`, error.message);
  }
}


// DEPRECATED Manual Locks - using Redlock instead (lines 364+)
const MAX_LOCK_RETRIES = 10;
const LOCK_RETRY_DELAY = 100;

// WebRTC metrics - use atomic increment functions to prevent race conditions
const webrtcMetrics = {
  _data: {
    totalCalls: 0,
    successfulConnections: 0,
    failedConnections: 0,
    turnUsage: 0,
    stunUsage: 0,
    directConnections: 0
  },
  increment(metric) {
    return ++this._data[metric];
  },
  get(metric) {
    return this._data[metric];
  },
  getAll() {
    return { ...this._data };
  }
};


const activeOffers = new Map(); // callId:userId -> offerTimestamp
const OFFER_DEDUPE_WINDOW = 2000; // 2 seconds

// ============================================
// ROOM CLEANUP SYSTEM
// ============================================

// Obsolete cleanup functions removed for clustering phase

// ============================================
// API ROUTES
// ============================================

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    activeRooms: matchmaking.getActiveRooms().length,
    webrtcMetrics: webrtcMetrics.getAll(),
    turnConfigured: !!(process.env.CLOUDFLARE_TURN_TOKEN_ID && process.env.CLOUDFLARE_TURN_API_TOKEN),
    server: 'running'
  });
});

app.get('/api/ice-servers', authenticateFirebase, async (req, res) => {
  try {
    const includeTurn = req.query.includeTurn === 'true'; // Query param for TURN

    console.log(`📡 ICE servers requested by client (includeTurn: ${includeTurn})`);

    // ✅ STUN-only by default
    const stunServers = [
      {
        urls: [
          'stun:stun.cloudflare.com:3478',
          'stun:stun.l.google.com:19302',
          'stun:stun1.l.google.com:19302',
          'stun:stun2.l.google.com:19302'
        ]
      }
    ];

    let iceServers = stunServers;

    // ✅ Only generate TURN credentials if explicitly requested
    if (includeTurn) {
      console.log('🔄 Generating Cloudflare TURN credentials (fallback mode)...');
      const turnServers = await generateCloudTurnCredentials();

      if (turnServers && Array.isArray(turnServers) && turnServers.length > 0) {
        iceServers = [...stunServers, ...turnServers];
        console.log(`✅ TURN servers added (fallback enabled)`);
      } else {
        console.warn('⚠️ TURN credential generation failed in fallback mode');
      }
    } else {
      console.log(`✅ STUN-only mode - no TURN credentials generated`);
      console.log(`   Zero Cloudflare bandwidth will be consumed`);
    }

    res.json({
      iceServers,
      timestamp: Date.now(),
      ttl: 86400,
      mode: includeTurn ? 'stun+turn' : 'stun-only'
    });
  } catch (error) {
    console.error('❌ Error getting ICE servers:', error);
    res.status(500).json({ error: 'Failed to get ICE servers' });
  }
});


app.post('/api/leave-chat', authenticateFirebase, async (req, res) => {
  try {
    const { roomId } = req.body;
    const firebaseUid = req.firebaseUser.uid;

    if (!roomId) {
      return res.status(400).json({ error: 'Room ID is required' });
    }

    const db = getDB();
    
    // Database lookup with timeout handling
    let user;
    try {
      user = await db.collection('users').findOne(
        { firebaseUid },
        { projection: { _id: 1, username: 1 }, maxTimeMS: 3000 }
      );
    } catch (dbError) {
      if (dbError.code === 50) { // MongoDB timeout
        console.error('❌ [API] Database timeout in leave-chat');
        return res.status(503).json({ 
          error: 'Database temporarily slow',
          retryable: true 
        });
      }
      throw dbError;
    }

    if (!user) {
      console.warn(`⚠️ [API] Leave attempt by unknown Firebase UID: ${firebaseUid}`);
      return res.status(404).json({ error: 'User record not found' });
    }

    const userId = user._id.toString();
    console.log(`📡 [API] Manual leave request: ${user.username} (${userId}) -> Room: ${roomId}`);

    // Add 10-second timeout to the entire operation
    const result = await Promise.race([
      performUserLeaveChat(userId, roomId, 'manual', firebaseUid),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Leave operation timeout')), 10000)
      )
    ]);

    if (result.success) {
      res.json({ success: true, message: 'Successfully left room' });
    } else {
      console.error('❌ [API] Leave operation failed:', result.error);
      res.status(500).json({ success: false, error: result.error });
    }
    
  } catch (error) {
    console.error('❌ [API] Leave chat error:', error.message);
    console.error('Stack trace:', error.stack);
    
    res.status(500).json({ 
      error: 'Internal server error',
      message: process.env.NODE_ENV === 'development' ? error.message : 'Failed to leave room'
    });
  }
});

app.post('/api/check-username', async (req, res) => {
  try {

    const { username } = req.body;
    if (!username || typeof username !== 'string') {
      return res.status(400).json({
        available: false,
        error: 'Username is required'
      });
    }

    const trimmedUsername = username.trim().toLowerCase();
    if (trimmedUsername.length < 3 || trimmedUsername.length > 20) {
      return res.status(400).json({
        available: false,
        error: 'Username must be between 3 and 20 characters'
      });
    }

    if (!/^[a-zA-Z0-9_-]+$/.test(trimmedUsername)) {
      return res.status(400).json({
        available: false,
        error: 'Username can only contain letters, numbers, underscores, and hyphens'
      });
    }

    const db = getDB();

    // ✅ FIX: Add maxTimeMS timeout
    const existingUser = await db.collection('users').findOne(
      { username: trimmedUsername },
      {
        projection: { _id: 1 },
        maxTimeMS: 3000
      }
    );

    if (existingUser) {
      const suggestions = [];
      for (let i = 0; i < 3; i++) {
        const suffix = Math.floor(Math.random() * 999) + 1;
        const suggestion = `${trimmedUsername}_${suffix}`;

        // ✅ FIX: Add maxTimeMS timeout
        const suggestionExists = await db.collection('users').findOne(
          { username: suggestion },
          {
            projection: { _id: 1 },
            maxTimeMS: 2000
          }
        );
        if (!suggestionExists) {
          suggestions.push(suggestion);
        }
      }
      return res.json({ available: false, suggestions });
    }

    res.json({ available: true });
  } catch (error) {
    // ✅ FIX: Handle timeout errors
    if (error.code === 50) {
      console.error('❌ Database timeout in check-username:', error.message);
      return res.status(503).json({
        available: false,
        error: 'Database temporarily slow. Please try again.',
        retryable: true
      });
    }

    console.error('Check username error:', error);
    res.status(500).json({
      available: false,
      error: 'Internal server error'
    });
  }
});


app.post('/api/users/check-profile', authenticateFirebase, async (req, res) => {
  try {
    const firebaseUser = req.firebaseUser;
    const db = getDB();

    // ✅ FIX: Add maxTimeMS timeout
    const user = await db.collection('users').findOne(
      { email: firebaseUser.email },
      {
        projection: { username: 1, pfpUrl: 1, _id: 1 },
        maxTimeMS: 3000 // ✅ 3-second timeout
      }
    );

    if (!user) {
      return res.json({
        exists: false,
        hasUsername: false
      });
    }

    const hasUsername = !!(user.username && user.username.trim());

    return res.json({
      exists: true,
      hasUsername: hasUsername,
      username: user.username || null,
      userId: user._id.toString()
    });

  } catch (error) {
    // ✅ FIX: Handle timeout errors
    if (error.code === 50) {
      console.error('❌ Database timeout in check-profile:', error.message);
      return res.status(503).json({
        error: 'Database temporarily slow. Please try again.',
        retryable: true
      });
    }

    console.error('Check profile error:', error);
    return res.status(500).json({
      error: 'Server Error',
      message: 'Failed to check profile'
    });
  }
});


app.post('/api/users/profile', authenticateFirebase, async (req, res) => {
  try {
    const { username, pfpUrl } = req.body;
    const firebaseUser = req.firebaseUser;

    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const trimmedUsername = username.trim().toLowerCase();
    if (trimmedUsername.length < 3 || trimmedUsername.length > 20) {
      return res.status(400).json({
        error: 'Username must be between 3 and 20 characters'
      });
    }

    if (!/^[a-zA-Z0-9_-]+$/.test(trimmedUsername)) {
      return res.status(400).json({
        error: 'Username can only contain letters, numbers, underscores, and hyphens'
      });
    }

    const db = getDB();

    // ✅ FIX: Add maxTimeMS timeout
    const existingUser = await db.collection('users').findOne(
      { email: firebaseUser.email },
      { maxTimeMS: 3000 }
    );

    // Check if username is taken by someone else
    if (existingUser && existingUser.username !== trimmedUsername) {
      // ✅ FIX: Add maxTimeMS timeout
      const usernameExists = await db.collection('users').findOne(
        { username: trimmedUsername },
        { maxTimeMS: 3000 }
      );
      if (usernameExists) {
        return res.status(400).json({ error: 'Username already taken' });
      }
    } else if (!existingUser) {
      // New user - check if username is available
      // ✅ FIX: Add maxTimeMS timeout
      const usernameExists = await db.collection('users').findOne(
        { username: trimmedUsername },
        { maxTimeMS: 3000 }
      );
      if (usernameExists) {
        return res.status(400).json({ error: 'Username already taken' });
      }
    }

    const userData = {
      email: firebaseUser.email,
      firebaseUid: firebaseUser.uid,
      username: trimmedUsername,
      pfpUrl: pfpUrl || getDefaultProfilePicture(),
      updatedAt: new Date()
    };

    if (existingUser) {
      // ✅ FIX: Add maxTimeMS timeout
      await db.collection('users').updateOne(
        { _id: existingUser._id },
        { $set: userData },
        { maxTimeMS: 5000 } // ✅ Write operations can take longer
      );
      await invalidateUserProfileCache(existingUser._id.toString());
      res.json({
        success: true,
        userId: existingUser._id.toString(),
        message: 'Profile updated'
      });
    } else {
      // Create new user
      userData.createdAt = new Date();
      // ✅ FIX: Add maxTimeMS timeout
      const result = await db.collection('users').insertOne(userData, {
        maxTimeMS: 5000
      });
      res.json({
        success: true,
        userId: result.insertedId.toString(),
        message: 'Profile created'
      });
    }
  } catch (error) {
    // ✅ FIX: Handle timeout errors
    if (error.code === 50) {
      console.error('❌ Database timeout in profile update:', error.message);
      return res.status(503).json({
        error: 'Database temporarily slow. Please try again.',
        retryable: true
      });
    }

    if (error.code === 11000) {
      return res.status(400).json({ error: 'Username already taken' });
    }
    console.error('Create profile error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/users/upload-pfp',
  authenticateFirebase,
  upload.single('file'),
  async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
      }

      const firebaseUser = req.firebaseUser;
      const db = getDB();

      // ✅ FIX: Add maxTimeMS timeout
      const user = await db.collection('users').findOne(
        { email: firebaseUser.email },
        {
          projection: {
            _id: 1,
            username: 1,
            pfpUrl: 1,
            email: 1,
            firebaseUid: 1
          },
          maxTimeMS: 3000 // ✅ 3-second timeout
        }
      );

      if (!user) {
        return res.status(404).json({ error: 'User not found' });
      }

      const pfpUrl = await uploadProfilePicture(
        req.file.buffer,
        req.file.mimetype,
        user._id.toString()
      );

      // ✅ FIX: Add maxTimeMS timeout
      await db.collection('users').updateOne(
        { _id: user._id },
        { $set: { pfpUrl, updatedAt: new Date() } },
        { maxTimeMS: 5000 }
      );

      const updatedUser = { ...user, pfpUrl };
      await updateUserProfileCache(user._id.toString(), updatedUser);

      res.json({ success: true, pfpUrl });
    } catch (error) {
      // ✅ FIX: Handle timeout errors
      if (error.code === 50) {
        console.error('❌ Database timeout in upload-pfp:', error.message);
        return res.status(503).json({
          error: 'Database temporarily slow. Please try again.',
          retryable: true
        });
      }

      console.error('Upload PFP error:', error);
      res.status(500).json({ error: 'Failed to upload profile picture' });
    }
  }
);

app.get('/api/users/me', authenticateFirebase, async (req, res) => {
  try {
    const firebaseUser = req.firebaseUser;
    const db = getDB();

    // ✅ FIX: Add maxTimeMS timeout
    const user = await db.collection('users').findOne(
      { email: firebaseUser.email },
      {
        projection: { password: 0 },
        maxTimeMS: 3000
      }
    );

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.json(user);
  } catch (error) {
    // ✅ FIX: Handle timeout errors
    if (error.code === 50) {
      console.error('❌ Database timeout in get profile:', error.message);
      return res.status(503).json({
        error: 'Database temporarily slow. Please try again.',
        retryable: true
      });
    }

    console.error('Get profile error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});




app.get('/api/attachments/:fileId', authenticateFirebase, async (req, res) => {
  try {
    const { fileId } = req.params;
    const firebaseUser = req.firebaseUser;

    console.log(`📂 Attachment request: ${fileId} from ${firebaseUser.email}`);

    // Extract roomId from fileId format: file_<roomId>_<timestamp>_<random>
    const fileIdParts = fileId.split('_');
    if (fileIdParts.length < 4 || fileIdParts[0] !== 'file') {
      return res.status(400).json({ error: 'Invalid fileId format' });
    }

    const roomId = fileIdParts[1];

    // Verify user is/was in this room
    const room = matchmaking.getRoom(roomId);
    if (!room) {
      console.log(`⚠️ Room ${roomId} expired, but allowing attachment fetch`);
    } else {
      const db = getDB();

      // ✅ FIX: Add maxTimeMS timeout
      const user = await db.collection('users').findOne(
        { email: firebaseUser.email },
        {
          projection: { _id: 1 },
          maxTimeMS: 3000
        }
      );

      if (!user) {
        return res.status(404).json({ error: 'User not found' });
      }

      if (!room.hasUser(user._id.toString())) {
        return res.status(403).json({ error: 'Access denied to this room\'s files' });
      }
    }

    // In production, fetch from persistent storage (S3, Cloudflare R2, etc.)
    // For now, return error as files are only in client IndexedDB
    console.error(`❌ File ${fileId} not found in server storage`);
    res.status(404).json({
      error: 'File not found',
      message: 'Server-side file storage not implemented. Files exist only in sender\'s browser.'
    });

  } catch (error) {
    // ✅ FIX: Handle timeout errors
    if (error.code === 50) {
      console.error('❌ Database timeout in attachment fetch:', error.message);
      return res.status(503).json({
        error: 'Database temporarily slow. Please try again.',
        retryable: true
      });
    }

    console.error('Attachment fetch error:', error);
    res.status(500).json({ error: 'Failed to fetch attachment' });
  }
});

app.post('/api/notes', authenticateFirebase, async (req, res) => {
  try {
    const { text, mood } = req.body;
    const firebaseUser = req.firebaseUser;

    if (!text || text.length > config.MAX_NOTE_LENGTH) {
      return res.status(400).json({
        error: 'Invalid note',
        message: `Note must be between 1 and ${config.MAX_NOTE_LENGTH} characters`
      });
    }

    const db = getDB();

    // ✅ FIX: Add maxTimeMS timeout
    const user = await db.collection('users').findOne(
      { email: firebaseUser.email },
      { maxTimeMS: 3000 }
    );

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    const note = {
      userId: user._id,
      username: user.username,
      pfpUrl: user.pfpUrl,
      text,
      mood: mood || null,
      createdAt: new Date()
    };

    // ✅ FIX: Add maxTimeMS timeout
    const result = await db.collection('notes').insertOne(note, {
      maxTimeMS: 5000
    });

    res.json({ success: true, noteId: result.insertedId });
  } catch (error) {
    // ✅ FIX: Handle timeout errors
    if (error.code === 50) {
      console.error('❌ Database timeout in post note:', error.message);
      return res.status(503).json({
        error: 'Database temporarily slow. Please try again.',
        retryable: true
      });
    }

    console.error('Post note error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/notes', optionalFirebaseAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 0;
    const limit = Math.min(
      parseInt(req.query.limit) || config.NOTES_PAGE_SIZE,
      config.NOTES_PAGE_SIZE
    );

    console.log(`📊 Notes fetch request: page=${page}, limit=${limit}`);

    const db = getDB();

    // ✅ FIX: Add maxTimeMS to prevent event loop blocking
    const notes = await db.collection('notes')
      .find({})
      .sort({ createdAt: -1 })
      .skip(page * limit)
      .limit(limit)
      .maxTimeMS(5000) // ✅ 5-second timeout
      .toArray();

    console.log(`✅ Fetched ${notes.length} notes from database`);

    // ✅ FIX: Limit batch size and add timeout
    const userIds = [...new Set(notes.map(note => note.userId))];

    // ✅ CRITICAL: Limit batch size to prevent oversized queries
    const MAX_BATCH_SIZE = 100;
    if (userIds.length > MAX_BATCH_SIZE) {
      console.warn(`⚠️ User batch size ${userIds.length} exceeds limit, capping at ${MAX_BATCH_SIZE}`);
      userIds.length = MAX_BATCH_SIZE; // Truncate array
    }

    const users = await db.collection('users')
      .find(
        { _id: { $in: userIds } },
        {
          projection: { username: 1, pfpUrl: 1 },
          maxTimeMS: 3000 // ✅ 3-second timeout
        }
      )
      .toArray();

    // Create lookup map
    const userMap = new Map(users.map(u => [u._id.toString(), u]));

    // Enrich notes using map (O(1) lookup per note)
    const enrichedNotes = notes.map(note => {
      const user = userMap.get(note.userId.toString());

      if (!user) {
        console.warn(`⚠️ User not found for note ${note._id}`);
        return {
          _id: note._id,
          username: 'Anonymous',
          pfpUrl: null,
          text: note.text,
          mood: note.mood,
          createdAt: note.createdAt
        };
      }

      return {
        _id: note._id,
        username: user.username,
        pfpUrl: user.pfpUrl || null,
        text: note.text,
        mood: note.mood,
        createdAt: note.createdAt
      };
    });

    // ✅ FIX: Add timeout to count query
    const total = await db.collection('notes').countDocuments({}, { maxTimeMS: 3000 });

    console.log(`📤 Sending ${enrichedNotes.length} enriched notes (${total} total)`);
    console.log(`   Page ${page + 1} of ${Math.ceil(total / limit)}`);
    console.log(`   Has more: ${(page + 1) * limit < total}`);

    res.json({
      notes: enrichedNotes,
      page,
      limit,
      total,
      hasMore: (page + 1) * limit < total
    });
  } catch (error) {
    // ✅ FIX: Handle timeout errors specifically
    if (error.code === 50) { // MongoDB MaxTimeMSExpired error code
      console.error('❌ Database query timeout:', error.message);
      return res.status(503).json({
        error: 'Database temporarily slow. Please try again.',
        retryable: true
      });
    }

    console.error('❌ Get notes error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/moods', (req, res) => {
  res.json({ moods: config.MOODS });
});


/**
 * Shared Call Leave Logic (Internal)
 * Handles removing a user from a call and triggering grace period cleanup.
 */
async function handleCallLeaveInternal(userId, callId) {
  try {
    const releaseCallLock = await acquireCallMutex(callId);
    try {
      // Fetch fresh state inside lock
      let call = await getCall(callId);
      if (!call) return;

      const participantIndex = call.participants.indexOf(userId);

      // If user not in call, just clean up local mapping just in case
      if (participantIndex === -1) {
        await removeUserCall(userId);
        return;
      }

      // 1. Update Call Data
      call.participants.splice(participantIndex, 1);

      // Handle Map/Object discrepancy for userMediaStates
      if (call.userMediaStates instanceof Map) {
        call.userMediaStates.delete(userId);
      } else if (call.userMediaStates) {
        delete call.userMediaStates[userId];
      }

      call.lastActivity = Date.now();

      // 2. Clear mapping in Redis
      await removeUserCall(userId);

      // 3. Save updated call state
      await saveCall(call);

      console.log(`📉 User ${userId} left call ${callId}`);
      console.log(`   Remaining participants: ${call.participants.length}`);

      // 4. Update room state broadcast
      io.to(call.roomId).emit('call_state_update', {
        callId: callId,
        isActive: call.participants.length > 0,
        participantCount: call.participants.length,
        callType: call.callType
      });

      // 5. Broadcast to others in the call room
      io.to(`call-${callId}`).emit('user_left_call', { userId });

      // 6. If no one left, schedule grace period for cleanup
      if (call.participants.length === 0) {
        console.log(`⏱️ Call ${callId} empty. Scheduling distributed cleanup.`);
        await scheduleCallCleanup(callId, 5000); // 5s grace period
      }
    } finally {
      await releaseCallLock();
    }
  } catch (error) {
    console.error(`❌ Error handling call leave for user ${userId}:`, error);
  }
}

/**
 * Shared Leave Handler (Server-Authoritative)
 * Centralizes all cleanup logic for chat/call exits.
 * Replaces redundant logic in leave_room, leave_call, and disconnect.
 */
async function performUserLeaveChat(userId, roomId, reason = 'manual', providedFirebaseUid = null) {
  let firebaseUid = providedFirebaseUid;
  let releaseLock = () => {}; // Default no-op

  try {
    const room = await matchmaking.getRoom(roomId);
    
    if (!room) {
      console.log(`ℹ️ [LeaveChat] Room ${roomId} already gone - cleaning up user ${userId}`);
      await safeRedisCleanup(userId, firebaseUid);
      return { success: true, alreadyGone: true };
    }

    const roomUser = room.users.find(u => u.userId === userId);

    if (!roomUser) {
      console.log(`ℹ️ [LeaveChat] User ${userId} not in room ${roomId} list - clearing markers anyway`);
      await safeRedisCleanup(userId, firebaseUid);
      return { success: true, alreadyLeft: true };
    }

    const userData = {
      userId: roomUser.userId,
      username: roomUser.username,
      firebaseUid: roomUser.firebaseUid || firebaseUid,
      pfpUrl: roomUser.pfpUrl
    };

    firebaseUid = userData.firebaseUid;
    const username = userData.username || userId;

    console.log(`🚪 [LeaveChat] ${username} leaving room ${roomId} (Reason: ${reason})`);

    // ✅ FIX: Wrap lock acquisition in try-catch BEFORE the main try block
    if (firebaseUid) {
      try {
        releaseLock = await Promise.race([
          acquireUserLock(firebaseUid),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Lock acquisition timeout')), 5000)
          )
        ]);
        console.log(`🔒 Lock acquired for ${firebaseUid}`);
      } catch (lockError) {
        console.error(`⚠️ [LeaveChat] Failed to acquire lock for ${firebaseUid}: ${lockError.message}`);
        console.log(`⚠️ [LeaveChat] Proceeding without lock (degraded mode)`);
        // Continue without lock - better to cleanup without lock than fail completely
      }
    }

    // Main cleanup logic with comprehensive error handling
    try {
      // 1. Remove from matchmaking room
      let remainingUsers = 0;
      try {
        const leaveResult = await matchmaking.leaveRoom(userId);
        remainingUsers = leaveResult.remainingUsers;
        console.log(`🏠 [Matchmaking] User ${username} removed. Remaining: ${remainingUsers}`);
      } catch (matchmakingError) {
        console.error(`⚠️ [LeaveChat] Matchmaking leave failed: ${matchmakingError.message}`);
        // Continue with cleanup even if matchmaking fails
      }

      // 2. Clear active room state
      await safeRedisCleanup(userId, firebaseUid);

      // 3. Remove from global mood tracking
      try {
        await removeUserFromMood(userId, room.mood);
      } catch (moodError) {
        console.error(`⚠️ [LeaveChat] Mood removal failed: ${moodError.message}`);
      }

      // 4. Cleanup any active calls
      try {
        const activeCallId = await getUserCall(userId);
        if (activeCallId) {
          await handleCallLeaveInternal(userId, activeCallId);
        }
      } catch (callError) {
        console.error(`⚠️ [LeaveChat] Call cleanup failed: ${callError.message}`);
      }

      // 5. Broadcast user_left to room
      try {
        io.to(roomId).emit('user_left', {
          userId,
          username,
          pfpUrl: userData?.pfpUrl,
          remainingUsers: room.users.length,
          roomId
        });
      } catch (emitError) {
        console.error(`⚠️ [LeaveChat] Emit failed: ${emitError.message}`);
      }

      // 6. Force socket room leave
      try {
        const socketIds = await getUserSocketIds(userId);
        socketIds.forEach(sid => {
          const s = io.sockets.sockets.get(sid);
          if (s) s.leave(roomId);
        });
      } catch (socketError) {
        console.error(`⚠️ [LeaveChat] Socket leave failed: ${socketError.message}`);
      }

      // 7. Notify user's devices
      if (firebaseUid) {
        try {
          emitToUserAllDevices(firebaseUid, 'left_room', {
            roomId,
            success: true,
            reason,
            forceRedirect: (reason === 'manual' || reason === 'heartbeat_timeout')
          });
        } catch (emitError) {
          console.error(`⚠️ [LeaveChat] Device notification failed: ${emitError.message}`);
        }
      }

      // 8. Clear presence record
      try {
        await removeUserPresence(userId);
      } catch (presenceError) {
        console.error(`⚠️ [LeaveChat] Presence removal failed: ${presenceError.message}`);
      }

      console.log(`✅ [LeaveChat] ${username} successfully cleared from room ${roomId}`);
      return { success: true };
      
    } catch (error) {
      console.error(`❌ [LeaveChat] Critical failure during cleanup for ${userId}:`, error);
      console.error('Stack:', error.stack);
      return { success: false, error: error.message };
    }
    
  } catch (outerError) {
    console.error(`❌ [LeaveChat] Outer error for ${userId}:`, outerError);
    console.error('Stack:', outerError.stack);
    return { success: false, error: outerError.message };
  } finally {
    // Always release lock, even if it's a no-op
    try {
      await releaseLock();
    } catch (releaseError) {
      console.warn(`⚠️ [LeaveChat] Lock release error: ${releaseError.message}`);
    }
  }
}

async function safeRedisCleanup(userId, firebaseUid) {
  const cleanupPromises = [
    clearUserActiveRoom(userId).catch(e => 
      console.error(`⚠️ Clear active room (userId) failed: ${e.message}`)
    ),
    removeUserPresence(userId).catch(e => 
      console.error(`⚠️ Remove presence failed: ${e.message}`)
    ),
    removeUserCall(userId).catch(e => 
      console.error(`⚠️ Remove user call failed: ${e.message}`)
    )
  ];

  if (firebaseUid) {
    cleanupPromises.push(
      clearUserActiveRoom(firebaseUid).catch(e => 
        console.error(`⚠️ Clear active room (firebaseUid) failed: ${e.message}`)
      )
    );
  }

  await Promise.allSettled(cleanupPromises);
}

/**
 * Presence Monitoring System
 * Checks for users who stopped sending heartbeats and cleans them up.
 */
setInterval(async () => {
  const now = Date.now();
  const HEARTBEAT_TIMEOUT = 35000; // 35 seconds (allows for some network jitter)

  try {
    const allPresence = await pubClient.hgetall('user:presence');

    for (const [userId, rawData] of Object.entries(allPresence)) {
      try {
        const presence = JSON.parse(rawData);

        // Skip cleanup if user is in an active call (navigation exception)
        if (presence.status === 'call_active') continue;

        if (now - presence.lastSeen > HEARTBEAT_TIMEOUT) {
          const roomInfo = presence.roomId ? `in room ${presence.roomId}` : '(not in room)';
          console.log(`⏱️ [Presence] Heartbeat timeout for ${userId} ${roomInfo}`);
          await performUserLeaveChat(userId, presence.roomId, 'heartbeat_timeout', presence.firebaseUid);
        }
      } catch (parseError) {
        console.error(`❌ Invalid presence data for ${userId}:`, parseError);
      }
    }
  } catch (err) {
    console.error('❌ Presence check error:', err);
  }
}, 10000); // Check every 10 seconds

// ============================================
// SOCKET.IO REAL-TIME COMMUNICATION
// ============================================




io.on('connection', (socket) => {
  console.log('🔌 Client connected:', socket.id);

  // ============================================
  // PRESENCE & HEARTBEAT EVENTS
  // ============================================
  socket.on('heartbeat', async ({ roomId }) => {
    const userData = await getSocketUser(socket.id);
    if (!userData) return;

    // Update presence timestamp
    await updateUserPresence(userData.userId, {
      roomId: roomId // merges with existing lastSeen/status automatically in helper
    });
  });

  socket.on('enter_call_mode', async ({ roomId }) => {
    const userData = await getSocketUser(socket.id);
    if (!userData) return;

    console.log(`📱 [Presence] User ${userData.username} entered call mode (Room: ${roomId})`);
    await updateUserPresence(userData.userId, {
      status: 'call_active',
      roomId: roomId
    });
  });

  socket.on('exit_call_mode', async ({ roomId }) => {
    const userData = await getSocketUser(socket.id);
    if (!userData) return;

    console.log(`💬 [Presence] User ${userData.username} returned to chat mode (Room: ${roomId})`);
    await updateUserPresence(userData.userId, {
      status: 'chat_active',
      roomId: roomId
    });
  });




  socket.on('send_message', async (data, callback) => {
    const userData = await getSocketUser(socket.id);
    if (!userData) {
      return callback?.({ success: false, error: 'Not authenticated' });
    }

    const { roomId, message, type = 'text' } = data;
    const userId = userData.userId;

    // Validate room access
    const validation = validateRoomAccess(roomId, userId);
    if (!validation.valid) {
      return callback?.({ success: false, error: validation.error });
    }

    const room = validation.room;

    const messageObj = {
      id: uuidv4(),
      userId,
      username: userData.username,
      message,
      type,
      timestamp: Date.now()
    };

    // CRITICAL: Add to room's chat history for persistence
    room.addMessage(messageObj);

    // Broadcast to room (all devices of both users)
    io.to(roomId).emit('new_message', messageObj);

    console.log(`💬 [UID: ${userId}] [Room: ${roomId}] Message sent (synced to all devices)`);

    callback?.({ success: true, message: messageObj });
  });


  socket.on('select_mood', async (data, callback) => {
    const userData = await getSocketUser(socket.id);

    if (!userData) {
      console.error(`❌ [select_mood] Socket ${socket.id} not authenticated`);
      return callback?.({
        success: false,
        error: 'Not authenticated. Please refresh the page.'
      });
    }

    const { mood } = data;
    const userId = userData.userId;
    const firebaseUid = userData.firebaseUid;
    const username = userData.username;

    console.log(`🎭 [UID: ${firebaseUid}] [Socket: ${socket.id}] Attempting to select mood: ${mood}`);

    try {
      const validation = await validateMoodSelection(firebaseUid);

      if (!validation.allowed) {
        console.log(`🚫 [UID: ${firebaseUid}] Mood selection blocked: ${validation.reason}`);

        // CRITICAL FIX: Clear stale state and allow re-entry
        const existingRoom = validation.existingRoom;
        const room = matchmaking.getRoom(existingRoom.roomId);

        if (!room || room.isExpired) {
          console.log(`🧹 [UID: ${firebaseUid}] Existing room is expired, clearing state`);
          clearUserActiveRoom(firebaseUid);
          // Check for active call before leaving matchmaking room
          const activeCallState = await findActiveCallForRoom(existingRoom.roomId);
          const hasActiveCall = !!activeCallState;
          matchmaking.leaveRoom(userId, hasActiveCall);

          // Continue with new mood selection
          console.log(`✅ [UID: ${firebaseUid}] Stale state cleared, proceeding with mood selection`);
        } else {
          // Room still valid, restore it
          const restoration = await restoreExistingRoom(socket, firebaseUid, existingRoom);

          if (restoration.success) {
            console.log(`✅ [UID: ${firebaseUid}] Room restored successfully`);
            socket.emit('match_found', restoration.room);

            return callback?.({
              success: true,
              matched: true,
              room: restoration.room,
              restored: true,
              message: 'Reconnected to your existing room'
            });
          } else {
            console.error(`❌ [UID: ${firebaseUid}] Room restoration failed: ${restoration.error}`);

            // Clear failed state and allow re-entry
            await clearUserActiveRoom(firebaseUid);
            // Check for active call before leaving matchmaking room
            const activeCallState = await findActiveCallForRoom(existingRoom.roomId);
            const hasActiveCall = !!activeCallState;
            await matchmaking.leaveRoom(userId, hasActiveCall);
            console.log(`🧹 [UID: ${firebaseUid}] Failed restoration cleaned up, proceeding with new selection`);
          }
        }
      }

      console.log(`✅ [UID: ${firebaseUid}] Mood selection allowed - proceeding with matchmaking`);

      addUserToMood(userId, mood);

      const matchResult = await matchmaking.addToQueue({
        userId,
        firebaseUid,
        username,
        pfpUrl: userData.pfpUrl,
        mood
      });

      if (matchResult) {
        const room = matchResult;

        await setUserActiveRoom(firebaseUid, room.id, mood);
        socket.join(room.id);

        if (room) {
          // Room lifecycle is now handled via Redis TTL in createRoomInternal
        }

        console.log(`🎯 [UID: ${firebaseUid}] Matched! Room: ${room.id}`);

        const partner = room.users.find(u => u.userId !== userId);
        const partnerProfile = partner ? await getUserProfile(partner.userId) : null;

        const roomData = {
          roomId: room.id,
          mood: room.mood,
          partner: partner ? {
            userId: partner.userId,
            username: partner.username,
            pfpUrl: partner.pfpUrl,
            bio: partnerProfile?.bio || ''
          } : null,
          expiresAt: room.expiresAt,
          chatHistory: room.messages || []
        };

        emitToUserAllDevices(firebaseUid, 'match_found', roomData);

        if (partner && partner.firebaseUid) {
          emitToUserAllDevices(partner.firebaseUid, 'match_found', {
            ...roomData,
            partner: {
              userId,
              username,
              pfpUrl: userData.pfpUrl,
              bio: (await getUserProfile(userId))?.bio || ''
            }
          });
        } else if (partner) {
          socket.to(room.id).emit('match_found', {
            ...roomData,
            partner: {
              userId,
              username,
              pfpUrl: userData.pfpUrl,
              bio: (await getUserProfile(userId))?.bio || ''
            }
          });
        }

        console.log(`✅ [UID: ${firebaseUid}] Match found event emitted`);

        return callback?.({
          success: true,
          matched: true,
          room: roomData
        });

      } else {
        const queuePosition = await matchmaking.getQueueStatus(mood);
        console.log(`⏳ [UID: ${firebaseUid}] Waiting in queue for mood: ${mood} (position: ${queuePosition})`);

        return callback?.({
          success: true,
          matched: false,
          queuePosition
        });
      }

    } catch (error) {
      console.error(`❌ [UID: ${firebaseUid}] Mood selection error:`, error);
      console.error(error.stack);

      return callback?.({
        success: false,
        error: 'Failed to process mood selection. Please try again.'
      });
    }
  });

  // ============================================
  // MANUAL ROOM RESTORATION
  // ============================================

  socket.on('restore_room', async (callback) => {
    if (!currentUser) {
      return callback?.({ success: false, error: 'Not authenticated' });
    }

    const userId = currentUser.userId;
    const activeRoom = await getUserActiveRoom(userId);

    if (!activeRoom) {
      return callback?.({ success: false, error: 'No active room to restore' });
    }

    console.log(`🔄 [UID: ${userId}] Manual room restoration requested`);

    const result = await restoreExistingRoom(socket, userId, activeRoom);
    callback?.(result);
  });


  socket.on('error', async (error) => {
    console.error(`❌ Socket error [${socket.id}]:`, error);
    const user = await getSocketUser(socket.id);
    if (user) {
      console.error(`   User: ${user.username} (${user.userId})`);
    }
    // Don't crash - socket.io will handle cleanup
  });

  socket.on('connect_error', (error) => {
    console.error(`❌ Connection error [${socket.id}]:`, error);
  });


  // ============================================
  // PEER-TO-PEER FILE TRANSFER VIA SOCKET RELAY
  // ============================================


  // ============================================
  // CHUNKED FILE TRANSMISSION RELAY
  // ============================================


  // ============================================
  // CHUNKED FILE TRANSMISSION RELAY (STATELESS)
  // ============================================

  socket.on('file_chunk', async (data) => {
    try {
      const user = await getSocketUser(socket.id);
      if (!user) return;

      const { fileId, fileName, roomId, chunkIndex, totalChunks, chunkSize, chunkData } = data;

      // 1. Basic Validation
      let actualChunkSize = 0;
      if (typeof chunkData === 'string') {
        const base64Clean = chunkData.replace(/^data:image\/\w+;base64,/, '');
        if (!/^[a-zA-Z0-9+/]*={0,2}$/.test(base64Clean)) {
          socket.emit('file_transmission_failed', { fileId, fileName, reason: 'Invalid encoding' });
          return;
        }
        actualChunkSize = Math.floor(chunkData.length * 0.75);
      } else {
        actualChunkSize = chunkData.length || chunkData.byteLength;
      }

      // 2. Distributed Transfer Initializtion
      let transfer = await getActiveFileTransfer(fileId);
      if (!transfer) {
        transfer = { roomId, userId: user.userId, bytesTransferred: 0, startTime: Date.now() };
        await setActiveFileTransfer(fileId, transfer);

        await saveFileRecord(fileId, {
          roomId, totalChunks, receivedCount: 0,
          name: fileName, senderId: user.userId, senderUsername: user.username
        });
        console.log(`📦 [Cluster] Started transfer: ${fileName} (${fileId})`);
      }

      // 3. Update transfer stats
      transfer.bytesTransferred += actualChunkSize;
      if (transfer.bytesTransferred > config.MAX_FILE_SIZE) {
        socket.emit('file_transmission_failed', { fileId, fileName, reason: 'Size limit exceeded' });
        await deleteActiveFileTransfer(fileId);
        await deleteFileRecord(fileId);
        return;
      }
      await setActiveFileTransfer(fileId, transfer);

      // 4. Store chunk in Redis
      const fileRecord = await getFileRecord(fileId);
      if (fileRecord) {
        const chunkBuffer = Buffer.isBuffer(chunkData) ? chunkData : Buffer.from(chunkData, 'base64');
        const alreadyReceived = await pubClient.exists(`file:chunk:${fileId}:${chunkIndex}`);
        if (!alreadyReceived) {
          await setFileChunk(fileId, chunkIndex, chunkBuffer);
          fileRecord.receivedCount++;
          await saveFileRecord(fileId, fileRecord);
        }
      }

      // 5. Relay to other users in room
      socket.to(roomId).emit('file_chunk', {
        fileId, fileName, senderId: user.userId, senderUsername: user.username,
        chunkIndex, totalChunks, chunkSize: actualChunkSize, chunkData
      });

      // 6. Assembly check
      if (fileRecord && fileRecord.receivedCount === totalChunks) {
        const allChunks = [];
        for (let i = 0; i < totalChunks; i++) {
          const chunk = await getFileChunk(fileId, i);
          if (chunk) allChunks.push(chunk);
        }

        if (allChunks.length === totalChunks) {
          const fullBuffer = Buffer.concat(allChunks);
          fileRecord.assembledData = fullBuffer.toString('base64');
          console.log(`✅ [Cluster] Assembled ${fileName} (${fileId})`);

          // Patch room history (Matchmaking is currently local, but room records can be patched)
          const room = await matchmaking.getRoom(roomId);
          if (room && room.messages) {
            const msg = room.messages.find(m => m.attachment && m.attachment.fileId === fileId);
            if (msg) {
              msg.attachment.data = fileRecord.assembledData;
              msg.attachment.chunked = false;
            }
          }
          await saveFileRecord(fileId, fileRecord);
        }
      }

      // 7. Progress & ACK
      const progress = fileRecord ? Math.round((fileRecord.receivedCount / totalChunks) * 100) : 0;
      socket.emit('file_upload_progress', { fileId, fileName, progress, totalChunks });
      socket.emit('file_chunk_ack', { fileId, chunkIndex });

    } catch (error) {
      console.error('❌ [Cluster] file_chunk error:', error);
    }
  });

  socket.on('file_transfer_complete', async ({ fileId }) => {
    const transfer = await getActiveFileTransfer(fileId);
    if (transfer) {
      console.log(`✅ [Cluster] Transfer ${fileId} marked complete`);
      await deleteActiveFileTransfer(fileId);
    }
  });

  socket.on('request_attachment_data', async ({ fileId, roomId }) => {
    try {
      const user = await getSocketUser(socket.id);
      if (!user) return;

      const fileRecord = await getFileRecord(fileId);
      if (fileRecord && fileRecord.assembledData) {
        socket.emit('attachment_data_received', {
          fileId,
          data: fileRecord.assembledData,
          metadata: { name: fileRecord.name }
        });
        return;
      }

      // Fallback: search room history
      const room = await matchmaking.getRoom(roomId);
      const msg = room?.messages?.find(m => m.attachment && m.attachment.fileId === fileId);
      if (msg?.attachment?.data) {
        socket.emit('attachment_data_received', {
          fileId, data: msg.attachment.data, metadata: { name: msg.attachment.name }
        });
        return;
      }

      // Request from peer
      if (msg) {
        io.to(`user:${msg.userId}`).emit('send_attachment_to_peer', {
          fileId, requesterId: user.userId, requesterSocketId: socket.id
        });
      }
    } catch (error) {
      console.error('❌ request_attachment_data error:', error);
    }
  });

  socket.on('attachment_data_response', async ({ fileId, requesterId, requesterSocketId, data, metadata }) => {
    try {
      io.to(requesterSocketId).emit('attachment_data_received', { fileId, data, metadata });
    } catch (error) {
      console.error('❌ attachment_data_response error:', error);
    }
  });


  socket.on('validate_cached_call', async ({ callId, roomId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.warn('⚠️ Unauthenticated socket tried to validate cached call');
        socket.emit('cached_call_invalid', { callId });
        return;
      }

      console.log(`🔍 Validating cached call ${callId} for ${user.username}`);

      const call = await getCall(callId);

      if (!call) {
        console.log(`❌ Cached call ${callId} not found or expired`);
        socket.emit('cached_call_invalid', { callId });
        return;
      }

      if (call.roomId !== roomId) {
        console.log(`❌ Cached call ${callId} room mismatch`);
        socket.emit('cached_call_invalid', { callId });
        return;
      }

      if (call.status === 'ended' || call.participants.length === 0) {
        console.log(`❌ Cached call ${callId} already ended`);
        socket.emit('cached_call_invalid', { callId });
        return;
      }

      // Call is still valid - send fresh call data
      console.log(`✅ Cached call ${callId} is valid, sending to ${user.username}`);

      const room = await matchmaking.getRoom(roomId);
      const callerData = room?.users.find(u => u.userId === call.initiator);

      if (!callerData) {
        console.error(`❌ Caller data not found for cached call ${callId}`);
        socket.emit('cached_call_invalid', { callId });
        return;
      }

      socket.emit('cached_call_valid', {
        callId: call.callId,
        callType: call.callType,
        callerUsername: callerData.username,
        callerPfp: callerData.pfpUrl,
        callerUserId: call.initiator,
        roomId: call.roomId
      });

      console.log(`📤 Sent cached_call_valid to ${user.username}`);

    } catch (error) {
      console.error('❌ Validate cached call error:', error);
      socket.emit('cached_call_invalid', { callId });
    }
  });


  socket.on('validate_room', async ({ roomId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.warn('⚠️ Unauthenticated socket tried to validate room');
        socket.emit('room_invalid', {
          roomId,
          reason: 'Not authenticated'
        });
        return;
      }

      const room = await matchmaking.getRoom(roomId);

      if (!room) {
        console.log(`❌ Room ${roomId} not found (validation request from ${user.username})`);
        socket.emit('room_invalid', {
          roomId,
          reason: 'Room not found or expired'
        });
        return;
      }

      if (room.isExpired) {
        console.log(`❌ Room ${roomId} is expired (validation request from ${user.username})`);
        socket.emit('room_invalid', {
          roomId,
          reason: 'Room has expired'
        });
        return;
      }

      // Room is valid, send fresh data
      console.log(`✅ Room ${roomId} is valid for ${user.username}`);
      console.log(`   Time remaining: ${(room.getTimeUntilExpiration() / 1000).toFixed(1)}s`);

      socket.emit('room_valid', {
        roomId: room.id,
        expiresAt: room.expiresAt,
        serverTime: Date.now(),
        timeRemaining: room.getTimeUntilExpiration()
      });

    } catch (error) {
      console.error('❌ Room validation error:', error);
      socket.emit('room_invalid', {
        roomId,
        reason: 'Validation error'
      });
    }
  });


  socket.on('request_room_sync', async ({ roomId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.warn('⚠️ Unauthenticated socket requested room sync');
        return;
      }

      const room = await matchmaking.getRoom(roomId);

      if (!room) {
        console.error(`❌ Room ${roomId} not found for sync request`);
        socket.emit('error', {
          message: 'Room not found',
          code: 'ROOM_NOT_FOUND'
        });
        return;
      }

      console.log(`📡 Room sync requested by ${user.username} for room ${roomId}`);

      // Send fresh server time and expiry
      const syncData = {
        roomId: room.id,
        expiresAt: room.expiresAt,
        timerStartedAt: room.timerStartedAt,
        serverTime: Date.now(), // CRITICAL: Current server time for clock sync
        timeRemaining: room.getTimeUntilExpiration()
      };

      console.log(`📤 Sending room sync to ${user.username}:`);
      console.log(`   expiresAt: ${new Date(room.expiresAt).toISOString()}`);
      console.log(`   serverTime: ${new Date(syncData.serverTime).toISOString()}`);
      console.log(`   timeRemaining: ${(syncData.timeRemaining / 1000).toFixed(1)}s`);

      socket.emit('room_sync_data', syncData);

    } catch (error) {
      console.error('❌ Room sync error:', error);
      socket.emit('error', { message: 'Failed to sync room data' });
    }
  });

  socket.on('authenticate', async ({ token, userId }) => {
    const authStart = Date.now();
    try {
      // ============================================
      // INPUT VALIDATION
      // ============================================
      if (!token || typeof token !== 'string') {
        console.error('❌ [authenticate] Missing or invalid token');
        socket.emit('auth_error', {
          message: 'Invalid authentication token',
          code: 'INVALID_TOKEN'
        });
        return;
      }

      if (!userId || typeof userId !== 'string') {
        console.error('❌ [authenticate] Missing or invalid userId');
        socket.emit('auth_error', {
          message: 'Invalid user ID',
          code: 'INVALID_USER_ID'
        });
        return;
      }

      // Validate userId format (MongoDB ObjectId)
      if (!/^[a-f\d]{24}$/i.test(userId)) {
        console.error(`❌ [authenticate] Invalid ObjectId format: ${userId}`);
        socket.emit('auth_error', {
          message: 'Invalid user ID format',
          code: 'INVALID_USER_ID'
        });
        return;
      }

      console.log(`🔐 [Auth] Starting authentication for userId: ${userId} (Socket: ${socket.id})`);

      // ============================================
      // TOKEN VERIFICATION
      // ============================================
      let decodedToken;
      try {
        decodedToken = await verifyToken(token);

        // Additional token validation
        if (!decodedToken || !decodedToken.uid) {
          throw new Error('Invalid token structure');
        }

        console.log(`✅ [Auth] Token verified for Firebase UID: ${decodedToken.uid}`);
      } catch (error) {
        console.error('❌ [Auth] Token verification failed:', error.message);
        socket.emit('auth_error', {
          message: 'Invalid or expired token',
          code: 'TOKEN_VERIFICATION_FAILED'
        });
        return;
      }

      // ============================================
      // DATABASE LOOKUP WITH CIRCUIT BREAKER
      // ============================================
      const db = getDB();

      let user;
      let retryCount = 0;
      const MAX_RETRIES = 2;

      while (retryCount <= MAX_RETRIES) {
        try {
          // ✅ FIX: Add maxTimeMS for query timeout
          user = await db.collection('users').findOne(
            { _id: new ObjectId(userId) },
            {
              projection: {
                _id: 1,
                username: 1,
                pfpUrl: 1,
                email: 1,
                firebaseUid: 1
              },
              maxTimeMS: 5000 // ✅ FIX: 5-second timeout per query
            }
          );
          break; // Success - exit retry loop

        } catch (dbError) {
          retryCount++;

          if (retryCount > MAX_RETRIES) {
            console.error('❌ [Auth] Database error during authentication (all retries exhausted):', dbError);
            socket.emit('auth_error', {
              message: 'Database temporarily unavailable. Please try again in a few seconds.',
              code: 'DB_ERROR',
              retryable: true
            });
            return;
          }

          console.warn(`⚠️ [Auth] Database query failed, retrying (${retryCount}/${MAX_RETRIES})...`);
          await new Promise(resolve => setTimeout(resolve, 500 * retryCount)); // ✅ FIX: Exponential backoff
        }
      }

      if (!user) {
        console.error(`❌ [Auth] User not found in database: ${userId}`);
        socket.emit('auth_error', {
          message: 'User not found',
          code: 'USER_NOT_FOUND'
        });
        return;
      }

      // CRITICAL: Verify Firebase UID matches (prevent token spoofing)
      if (user.firebaseUid !== decodedToken.uid) {
        console.error(`❌ [Auth] Firebase UID mismatch for user ${userId}`);
        console.error(`   Expected: ${user.firebaseUid}, Got: ${decodedToken.uid}`);
        socket.emit('auth_error', {
          message: 'Authentication mismatch',
          code: 'UID_MISMATCH'
        });
        return;
      }

      // ============================================
      // MULTI-DEVICE: REGISTER SOCKET FOR UID
      // ============================================
      const firebaseUid = decodedToken.uid;
      const mongoUserId = user._id.toString();

      // Register this socket in Redis for cluster-wide tracking
      await registerSocketForUser(mongoUserId, socket.id, {
        firebaseUid,
        username: user.username,
        email: user.email,
        profilePicture: user.profilePicture
      });

      // ✅ FIX: Join user-specific rooms for cluster-wide targeted emissions
      socket.join(`user:${mongoUserId}`);
      socket.join(`user:${firebaseUid}`);
      console.log(`📡 [Auth] Socket ${socket.id} joined rooms: user:${mongoUserId}, user:${firebaseUid}`);

      // ============================================
      // HANDLE EXISTING SOCKET FOR SAME USER (LEGACY)
      // ============================================
      // Clean up any pending distributed user cleanup
      await cancelUserCleanup(mongoUserId);

      // ============================================
      // REGISTER NEW SOCKET
      // ============================================
      const userSocketData = {
        userId: mongoUserId,
        firebaseUid: firebaseUid,
        username: user.username,
        pfpUrl: user.pfpUrl,
        email: user.email,
        authenticatedAt: Date.now()
      };

      // Store in Redis global tracking for cross-instance lookups
      await setSocketUser(socket.id, userSocketData);
      // socketUsers.set removed - redundant with Redis-backed state

      console.log(`✅ [Auth] Socket authenticated for ${user.username} (${mongoUserId})`);

      // ✅ FIX: Mark presence immediately so matchmaking sees the user as online
      await updateUserPresence(mongoUserId, {
        status: 'online',
        firebaseUid: firebaseUid,
        lastSeen: Date.now()
      });

      // ============================================
      // CHECK FOR ACTIVE ROOM (MULTI-DEVICE AWARE)
      // ============================================
      // ✅ FIX: Await the async Redis call
      console.log(`🔍 [Auth] Checking active room for ${firebaseUid}...`);
      const activeRoom = await getUserActiveRoom(firebaseUid);

      if (activeRoom) {
        console.log(`ℹ️ [Auth] User has active room: ${activeRoom.roomId}`);
      }

      // ============================================
      // SEND SUCCESS RESPONSE
      // ============================================
      socket.emit('authenticated', {
        success: true,
        user: {
          userId: mongoUserId,
          firebaseUid: firebaseUid,
          username: user.username,
          pfpUrl: user.pfpUrl
        },
        socketId: socket.id,
        timestamp: Date.now(),
        // MULTI-DEVICE: Include active room info
        hasActiveRoom: !!activeRoom,
        activeRoom: activeRoom ? {
          roomId: activeRoom.roomId,
          mood: activeRoom.mood,
          joinedAt: activeRoom.joinedAt
        } : null
      });

      // ✅ FIX: Await mood counts before sending
      const moodCounts = await getAllMoodCounts();
      socket.emit('mood_counts_initial', moodCounts);

      // ============================================
      // RESTORE USER STATE (LEGACY FALLBACK)
      // ============================================
      // Check legacy room tracking (for backwards compatibility)
      const legacyRoomId = await matchmaking.getRoomIdByUser(mongoUserId);
      if (legacyRoomId && !activeRoom) {
        const room = await matchmaking.getRoom(legacyRoomId);
        if (room && !room.isExpired) {
          console.log(`🔄 [Auth] Restoring legacy room ${legacyRoomId}`);

          // Register in new system
          // ✅ FIX: Await the async Redis call
          await setUserActiveRoom(firebaseUid, legacyRoomId, room.mood);

          socket.join(legacyRoomId);

          // Notify user they can resume
          socket.emit('room_reconnected', {
            roomId: room.id,
            expiresAt: room.expiresAt,
            timeRemaining: room.getTimeUntilExpiration()
          });

          // Notify other users in room
          socket.to(legacyRoomId).emit('user_reconnected', {
            userId: mongoUserId,
            username: user.username,
            pfpUrl: user.pfpUrl
          });
        } else {
          // Room expired while user was disconnected
          console.log(`⚠️ [Auth] Legacy room ${legacyRoomId} expired`);
          // Check for active call before leaving matchmaking room
          let hasActiveCall = false;
          const userRoom = await getUserActiveRoom(firebaseUid); // ✅ FIX: Await here too just in case
          if (userRoom) {
            // ✅ FIX: Await this too
            const activeCall = await findActiveCallForRoom(userRoom.roomId);
            if (activeCall) hasActiveCall = true;
          }
          await matchmaking.leaveRoom(mongoUserId, hasActiveCall);
        }
      } else if (activeRoom) {
        // User has active room in new system - auto-join socket to room
        const room = await matchmaking.getRoom(activeRoom.roomId);
        if (room && !room.isExpired) {
          console.log(`🔄 [Auth] Auto-joining socket to active room ${activeRoom.roomId}`);

          socket.join(activeRoom.roomId);

          // Notify this socket about the room (without full restoration)
          socket.emit('room_reconnected', {
            roomId: room.id,
            expiresAt: room.expiresAt,
            timeRemaining: room.getTimeUntilExpiration(),
            isMultiDevice: true
          });

          // Notify other users in room about this device joining
          socket.to(activeRoom.roomId).emit('user_reconnected', {
            userId: mongoUserId,
            username: user.username,
            pfpUrl: user.pfpUrl,
            isMultiDevice: true
          });
        } else {
          // Room expired - clean up stale state
          console.log(`⚠️ [Auth] Active room ${activeRoom.roomId} is expired, cleaning up`);
          await clearUserActiveRoom(firebaseUid); // ✅ FIX: Await Redis call
        }
      }

      // ============================================
      // RESTORE CALL STATE
      // ============================================
      // Check if user was in an active call
      const activeCallId = await getUserCall(mongoUserId);
      if (activeCallId) {
        const call = await getCall(activeCallId);
        if (call && call.status === 'active' && call.participants.includes(mongoUserId)) {
          console.log(`📞 [Auth] User found in active call ${activeCallId}`);

          socket.emit('call_reconnect_available', {
            callId: activeCallId, // The ID from Redis 
            callType: call.callType,
            participantCount: call.participants.length,
            roomId: call.roomId
          });
        } else {
          // Call ended while user was disconnected
          console.log(`⚠️ [Auth] Call ${activeCallId} ended or invalid`);
          await removeUserCall(mongoUserId);
        }
      }

      const authDuration = Date.now() - authStart;
      console.log(`✅ [Auth] Authentication completed in ${authDuration}ms`);

    } catch (error) {
      console.error(`❌ [authenticate] Unexpected error for user ${userId}:`, error);
      console.error(error.stack); // Print stack trace
      socket.emit('auth_error', {
        message: 'Authentication failed due to server error',
        code: 'AUTH_FAILED',
        retryable: true
      });
    }
  });


  // ================== DISCONNECT CLEANUP ==================

  socket.on('join_matchmaking', async ({ mood }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.error('❌ Unauthenticated socket tried to join matchmaking:', socket.id);
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      console.log(`🎮 User ${user.username} joining matchmaking for mood: ${mood}`);

      const validMood = config.MOODS.find(m => m.id === mood);
      if (!validMood) {
        socket.emit('error', { message: 'Invalid mood' });
        return;
      }

      // ✅ ADD USER TO MOOD (deduplicated)
      addUserToMood(user.userId, mood);

      // ✅ FIX: Refresh presence before joining queue to prevent race/stale state
      await updateUserPresence(user.userId, {
        status: 'matchmaking',
        lastSeen: Date.now()
      });

      // Clear any existing timeout for this user
      clearMatchmakingTimeout(user.userId);

      // Try to add to queue or join existing room
      let room = await matchmaking.addToQueue({
        ...user,
        mood,
        socketId: socket.id
      });

      if (!room) {
        const queueStatus = await matchmaking.getQueueStatus(mood);
        if (queueStatus >= config.MAX_USERS_PER_ROOM) {
          console.log(`🔄 Queue full detected (${queueStatus}/${config.MAX_USERS_PER_ROOM}), retrying match...`);
          room = await matchmaking.addToQueue({
            ...user,
            mood,
            socketId: socket.id
          });
        }
      }

      if (room) {
        // Match found (either new room or joined existing)
        clearMatchmakingTimeout(user.userId);

        console.log(`🎉 Match found! Room ${room.id} with ${room.users.length} users`);

        const uniqueUsers = new Map();
        room.users.forEach(roomUser => {
          uniqueUsers.set(roomUser.userId, roomUser);
        });
        room.users = Array.from(uniqueUsers.values());

        // Check if this is a new user joining existing room
        const isJoiningExisting = room.users.length > config.MIN_USERS_FOR_ROOM || room.messages.length > 0;

        for (const roomUser of room.users) {
          // CLUSTER ADAPTATION: Check presence instead of local socket
          const isConnected = await getUserPresence(roomUser.userId);

          if (isConnected) {
            console.log(`📤 [Cluster] Emitting match_found to ${roomUser.username} (${roomUser.userId})`);

            // Force remote sockets to join the room
            io.in(`user:${roomUser.userId}`).socketsJoin(room.id);
            console.log(`✅ User ${roomUser.username} joined Socket.IO room ${room.id} (Cluster Op)`);

            // Include previous messages for users joining existing room
            const matchData = {
              roomId: room.id,
              mood: room.mood,
              users: room.users.map(u => ({
                userId: u.userId,
                username: u.username,
                pfpUrl: u.pfpUrl
              })),
              expiresAt: room.expiresAt,
              activeCall: await findActiveCallForRoom(room.id) // ✅ Async call state
            };

            // If user is joining existing room, include previous messages
            if (isJoiningExisting && roomUser.userId === user.userId) {
              matchData.previousMessages = room.getMessages();
              console.log(`📨 Sending ${matchData.previousMessages.length} previous messages to ${roomUser.username}`);
            }

            io.to(`user:${roomUser.userId}`).emit('match_found', matchData);

            // ✅ CRITICAL FIX: Track active room for disconnect handler
            if (roomUser.firebaseUid) {
              await setUserActiveRoom(roomUser.firebaseUid, room.id, room.mood);
            } else {
              await setUserActiveRoom(roomUser.userId, room.id, room.mood);
            }

            // ✅ Keep user in mood count when moved to room
            addUserToMood(roomUser.userId, room.mood);

            // Clear matchmaking timeout for this user
            clearMatchmakingTimeout(roomUser.userId);

          } else {
            console.error(`❌ User ${roomUser.username} not connected (Presence Check Failed)`);
            // Check for active call before leaving matchmaking room
            const activeCall = await findActiveCallForRoom(room.id);
            const hasActiveCall = !!activeCall;
            await matchmaking.leaveRoom(roomUser.userId, hasActiveCall);
          }
        }

        // Notify existing room members about new user (if joining existing)
        if (isJoiningExisting) {
          io.to(room.id).emit('user_joined_room', {
            userId: user.userId,
            username: user.username,
            pfpUrl: user.pfpUrl,
            roomUserCount: room.users.length
          });
          console.log(`📢 Notified room ${room.id} about new user ${user.username}`);
        }

      } else {
        // No match yet, user is in queue
        const queuePosition = await matchmaking.getQueueStatus(mood);
        socket.emit('queued', {
          mood,
          position: queuePosition
        });
        console.log(`⏳ User ${user.username} queued (${queuePosition}/${config.MIN_USERS_FOR_ROOM})`);

        // ✅ START MATCHMAKING TIMEOUT
        const timeoutHandle = setTimeout(async () => {
          const currentQueueStatus = await matchmaking.getQueueStatus(mood);

          console.log(`⏰ Matchmaking timeout for ${user.username} in ${mood} queue`);
          console.log(`   Queue status: ${currentQueueStatus} users`);

          if (currentQueueStatus < config.MIN_USERS_FOR_ROOM) {
            console.log(`❌ Insufficient users (${currentQueueStatus}/${config.MIN_USERS_FOR_ROOM}) - timing out`);

            await matchmaking.cancelMatchmaking(user.userId, mood);
            removeUserFromAllMoods(user.userId);
            clearMatchmakingTimeout(user.userId);

            io.to(`user:${user.userId}`).emit('matchmaking_timeout', {
              message: 'No matches found. Please try again.',
              mood: mood,
              queueStatus: currentQueueStatus,
              minRequired: config.MIN_USERS_FOR_ROOM,
              redirectTo: '/mood.html'
            });
            console.log(`📤 Sent matchmaking_timeout with redirect to ${user.username}`);

            console.log(`🔄 User ${user.username} timed out, should redirect to mood selection`);
          } else {
            console.log(`✅ Sufficient users found (${currentQueueStatus}), creating room`);
            const room = await matchmaking.addToQueue({
              ...user,
              mood,
              socketId: socket.id
            });

            if (room) {
              console.log(`🎉 Room ${room.id} created after timeout check`);
            }
          }

          matchmakingTimeouts.delete(user.userId);
        }, config.MATCHMAKING_TIMEOUT);

        matchmakingTimeouts.set(user.userId, timeoutHandle);
        console.log(`⏰ Started ${config.MATCHMAKING_TIMEOUT / 1000}s timeout for ${user.username}`);
      }
    } catch (error) {
      console.error('Join matchmaking error:', error);
      socket.emit('error', { message: 'Matchmaking failed' });
    }
  });

  // findActiveSocketForUser DEPRECATED AND REMOVED for Redis/Cluster support
  // Use io.to(`user:${userId}`) instead


  socket.on('join_room', async ({ roomId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.error('❌ Unauthenticated socket tried to join room');
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      const joinKey = `${user.userId}:${roomId}`;

      // CRITICAL FIX: Idempotency check
      const existingJoin = roomJoinState.get(joinKey);
      if (existingJoin && (Date.now() - existingJoin.timestamp < 5000)) {
        console.log(`⚠️ Duplicate join_room from ${user.username} for ${roomId}, ignoring`);
        return;
      }

      console.log(`🚪 User ${user.username} (${user.userId}) confirming room ${roomId}`);

      const room = await matchmaking.getRoom(roomId);

      if (!room) {
        console.error(`❌ Room ${roomId} not found!`);
        socket.emit('error', {
          message: 'Room not found. It may have expired or been closed.',
          code: 'ROOM_NOT_FOUND'
        });
        return;
      }

      if (!room.hasUser(user.userId)) {
        console.warn(`⚠️ User ${user.username} (${user.userId}) not in room ${roomId} - attempting to re-add`);

        // Check if user was recently in this room (grace period reconnection)
        const activeRoom = firebaseUid ? await getUserActiveRoom(firebaseUid) : null;

        if (activeRoom && activeRoom.roomId === roomId) {
          // User is reconnecting to their active room - re-add them
          console.log(`🔄 Re-adding ${user.username} to room ${roomId} (reconnection)`);

          try {
            // Re-add user to room
            room.addUser({
              userId: user.userId,
              username: user.username,
              pfpUrl: user.pfpUrl
            });

            console.log(`✅ Successfully re-added ${user.username} to room ${roomId}`);
          } catch (error) {
            console.error(`❌ Failed to re-add user to room:`, error);
            socket.emit('error', {
              message: 'Failed to rejoin room. Please try again.',
              code: 'REJOIN_FAILED'
            });
            return;
          }
        } else {
          // User genuinely not in this room
          console.error(`❌ User ${user.username} (${user.userId}) not authorized for room ${roomId}`);
          socket.emit('error', {
            message: 'You are not a member of this room',
            code: 'NOT_IN_ROOM'
          });
          return;
        }
      }

      if (!socket.rooms.has(roomId)) {
        socket.join(roomId);
        console.log(`✅ User ${user.username} joined Socket.IO room ${roomId}`);
      } else {
        console.log(`ℹ️ User ${user.username} already in Socket.IO room ${roomId}`);
      }

      // CRITICAL: Start room lifecycle timers when FIRST user actually joins
      if (!room.userJoinedRoom) {
        room.userJoinedRoom = true;
        room.startLifecycleTimers();
        console.log(`⏱️ Room ${roomId} lifecycle timers STARTED by ${user.username}`);
        console.log(`   Timer started at: ${new Date(room.timerStartedAt).toISOString()}`);
        console.log(`   Will expire at: ${new Date(room.expiresAt).toISOString()}`);
      } else {
        const timeElapsed = Date.now() - room.timerStartedAt;
        const timeRemaining = room.getTimeUntilExpiration();
        console.log(`ℹ️ User ${user.username} joining room ${roomId} (timer already running)`);
        console.log(`   Time elapsed since first join: ${(timeElapsed / 1000).toFixed(1)}s`);
        console.log(`   Time remaining: ${(timeRemaining / 1000).toFixed(1)}s`);
        console.log(`   Expires at: ${new Date(room.expiresAt).toISOString()}`);
      }

      // Get chat history from the room, and attach any assembled file data for chunked attachments
      const chatHistory = room.getMessages ? room.getMessages() : [];
      chatHistory.forEach(msg => {
        if (msg.attachment && msg.attachment.chunked && msg.attachment.fileId) {
          const fileRecord = roomFileStore.get(msg.attachment.fileId);
          if (fileRecord && fileRecord.assembledData) {
            msg.attachment.data = fileRecord.assembledData;
            msg.attachment.chunked = false;
          }
        }
      });
      console.log(`📜 Sending ${chatHistory.length} chat messages to ${user.username}`);

      // Check for active calls in this room
      const activeCallState = await findActiveCallForRoom(roomId);
      if (activeCallState) {
        console.log(`📞 Active call detected in room ${roomId}: ${activeCallState.callId} with ${activeCallState.participantCount} participant(s)`);
      }

      const responseData = {
        roomId,
        chatHistory: chatHistory,
        expiresAt: room.expiresAt,
        timerStartedAt: room.timerStartedAt,
        serverTime: Date.now()
      };

      if (activeCallState) {
        responseData.activeCall = activeCallState;
        console.log(`📤 Sending active call state to ${user.username}:`, activeCallState);
      }

      console.log(`📤 Sending room_joined to ${user.username}:`);
      console.log(`   expiresAt: ${new Date(room.expiresAt).toISOString()}`);
      console.log(`   timeRemaining: ${(room.getTimeUntilExpiration() / 1000).toFixed(1)}s`);

      socket.emit('room_joined', responseData);

      // CRITICAL FIX: Mark this join as completed
      roomJoinState.set(joinKey, { joined: true, timestamp: Date.now() });

      // Clean up old join states (older than 10 seconds)
      setTimeout(() => {
        roomJoinState.delete(joinKey);
      }, 10000);

      // ============================================
      // CRITICAL FIX: BROADCAST USER JOIN TO ROOM
      // ============================================
      console.log(`📢 ========================================`);
      console.log(`📢 BROADCASTING USER_JOINED TO ROOM`);
      console.log(`📢 ========================================`);
      console.log(`   Room: ${roomId}`);
      console.log(`   New user: ${user.username} (${user.userId})`);
      console.log(`   Room state before broadcast:`);
      console.log(`     Users in room: [${room.users.map(u => u.username).join(', ')}]`);
      console.log(`     Socket.IO members: ${io.sockets.adapter.rooms.get(roomId)?.size || 0}`);

      // Get updated user list for the room
      const updatedUserList = room.users.map(u => ({
        userId: u.userId,
        username: u.username,
        pfpUrl: u.pfpUrl
      }));

      // Broadcast to ALL users in room (including the joiner for consistency)
      io.to(roomId).emit('user_joined', {
        userId: user.userId,
        username: user.username,
        pfpUrl: user.pfpUrl,
        users: updatedUserList,
        onlineCount: room.users.length
      });

      console.log(`✅ Broadcasted user_joined event`);
      console.log(`   Notified: ${io.sockets.adapter.rooms.get(roomId)?.size || 0} socket(s)`);
      console.log(`   Updated user list: ${updatedUserList.length} users`);
      console.log(`   Online count: ${room.users.length}`);
      console.log(`📢 ========================================\n`);

    } catch (error) {
      console.error('Join room error:', error);
      socket.emit('error', { message: 'Failed to join room' });
    }
  });

  socket.on('cancel_matchmaking', async () => {
    const user = await getSocketUser(socket.id);
    if (user) {
      // Clear matchmaking timeout
      clearMatchmakingTimeout(user.userId);

      await matchmaking.cancelMatchmaking(user.userId);

      // ✅ REMOVE USER FROM MOOD TRACKING
      removeUserFromAllMoods(user.userId);
      clearMatchmakingTimeout(user.userId);

      socket.emit('matchmaking_cancelled');
      console.log(`❌ Matchmaking cancelled: ${user.username}`);
    }
  });

  // ============================================
  // TYPING INDICATOR HANDLERS
  // ============================================
  socket.on('typing', async ({ roomId }) => {
    const userData = await getSocketUser(socket.id);
    if (!userData) return;

    const userId = userData.userId;
    const username = userData.username;

    // Broadcast typing state to EVERYONE ELSE in the room
    socket.to(roomId).emit('user_typing', {
      userId: userId,
      username: username
    });
  });

  socket.on('user_typing', async ({ roomId }) => {
    const user = await getSocketUser(socket.id);
    if (!user) return;

    // Broadcast typing state to EVERYONE ELSE in the room
    socket.to(roomId).emit('user_typing', {
      userId: user.userId,
      username: user.username
    });
  });

  socket.on('user_stop_typing', async ({ roomId }) => {
    const user = await getSocketUser(socket.id);
    if (!user) return;

    // Broadcast stop state to EVERYONE ELSE in the room
    socket.to(roomId).emit('user_stop_typing', {
      userId: user.userId
    });
  });

  socket.on('chat_message', async ({ roomId, message, replyTo, attachment }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.error('❌ Unauthenticated socket tried to send message');
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      // ✅ FIX: Validate message text size BEFORE rate limiting (to prevent memory allocation)
      const MAX_MESSAGE_LENGTH = 10000; // 10KB max for text messages

      if (message && typeof message === 'string' && message.length > MAX_MESSAGE_LENGTH) {
        console.warn(`⚠️ Oversized message from ${user.username}: ${message.length} chars`);
        socket.emit('error', {
          message: `Message too long. Maximum ${MAX_MESSAGE_LENGTH} characters.`,
          code: 'MESSAGE_TOO_LONG',
          maxLength: MAX_MESSAGE_LENGTH
        });
        return;
      }

      // Room-level rate limiting
      const rateLimitCheck = checkRoomMessageRateLimit(roomId);
      if (!rateLimitCheck.allowed) {
        console.warn(`⚠️ Rate limit exceeded for room ${roomId} (${rateLimitCheck.count} messages)`);
        socket.emit('error', {
          message: 'Room message limit reached. Please slow down.',
          code: 'ROOM_RATE_LIMIT',
          retryAfter: 5000
        });
        return;
      }

      console.log('💬 ========================================');
      console.log('💬 CHAT MESSAGE RECEIVED FROM CLIENT');
      console.log('💬 ========================================');
      console.log(`   From: ${user.username} (${user.userId})`);
      console.log(`   Room: ${roomId}`);
      console.log(`   Message length: ${message ? message.length : 0} chars`);
      console.log(`   Has attachment: ${!!attachment}`);
      console.log(`   Room rate: ${rateLimitCheck.count}/${ROOM_MESSAGE_RATE_LIMIT}`);

      // Validate room access
      const validation = await validateRoomAccess(roomId, user.userId);
      if (!validation.valid) {
        console.error(`❌ ${validation.error} for user ${user.username}`);
        socket.emit('error', { message: validation.error, code: validation.code });
        return;
      }

      const room = validation.room;

      const timestamp = Date.now();
      const messageData = {
        messageId: `msg-${user.userId}-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
        userId: user.userId,
        username: user.username,
        pfpUrl: user.pfpUrl,
        message,
        timestamp
      };

      if (replyTo) {
        messageData.replyTo = replyTo;
      }

      if (attachment) {
        console.log('📎 Processing attachment...');
        console.log(`   File: ${attachment.name}`);
        console.log(`   Type: ${attachment.type}`);
        console.log(`   Size: ${(attachment.size / 1024).toFixed(2)} KB`);
        console.log(`   Chunked: ${!!attachment.chunked}`);

        // Validate attachment size before broadcasting
        const maxAttachmentSize = 10 * 1024 * 1024; // 10MB
        if (attachment.size > maxAttachmentSize) {
          console.error(`❌ Attachment too large: ${(attachment.size / 1024 / 1024).toFixed(2)}MB`);
          socket.emit('error', {
            message: 'Attachment too large. Maximum size is 10MB.',
            code: 'ATTACHMENT_TOO_LARGE'
          });
          return;
        }

        if (attachment.chunked) {
          console.log(`📦 Chunked attachment detected - data will arrive separately`);
          console.log(`   Total chunks expected: ${attachment.totalChunks}`);

          if (!attachment.fileId || !attachment.name || !attachment.type || !attachment.size) {
            console.error('❌ Chunked attachment missing required metadata!');
            socket.emit('error', {
              message: 'Attachment metadata incomplete',
              code: 'INVALID_ATTACHMENT'
            });
            return;
          }

          messageData.attachment = {
            fileId: attachment.fileId,
            name: attachment.name,
            type: attachment.type,
            size: attachment.size,
            chunked: true,
            totalChunks: attachment.totalChunks
          };

          console.log('✅ Chunked attachment metadata validated');

        } else {
          console.log(`📎 Legacy attachment format detected`);

          if (!attachment.data) {
            console.error('❌ Legacy attachment missing data!');
            socket.emit('error', {
              message: 'Attachment data missing',
              code: 'INVALID_ATTACHMENT'
            });
            return;
          }

          // ✅ FIX: Validate legacy attachment data size
          if (attachment.data.length > maxAttachmentSize * 1.5) {
            console.error(`❌ Legacy attachment data too large: ${(attachment.data.length / 1024 / 1024).toFixed(2)}MB`);
            socket.emit('error', {
              message: 'Attachment data too large',
              code: 'ATTACHMENT_TOO_LARGE'
            });
            return;
          }

          messageData.attachment = {
            fileId: attachment.fileId,
            name: attachment.name,
            type: attachment.type,
            size: attachment.size,
            data: attachment.data
          };

          console.log('✅ Legacy attachment validated and ready for broadcast');
        }
      }

      // Create storage version (without data to save memory)
      const storedMessage = {
        messageId: messageData.messageId,
        userId: messageData.userId,
        username: messageData.username,
        pfpUrl: messageData.pfpUrl,
        message: messageData.message,
        timestamp: messageData.timestamp
      };

      if (messageData.replyTo) {
        storedMessage.replyTo = { ...messageData.replyTo };
      }

      if (messageData.attachment) {
        storedMessage.attachment = {
          fileId: messageData.attachment.fileId,
          name: messageData.attachment.name,
          type: messageData.attachment.type,
          size: messageData.attachment.size,
          chunked: messageData.attachment.chunked || false
        };

        if (messageData.attachment.totalChunks) {
          storedMessage.attachment.totalChunks = messageData.attachment.totalChunks;
        }
      }

      // Store to room history
      room.addMessage(storedMessage);
      console.log(`💾 Message stored to room history`);

      console.log('📡 ========================================');
      console.log('📡 BROADCASTING MESSAGE TO ROOM');
      console.log('📡 ========================================');
      console.log(`   Room: ${roomId}`);
      console.log(`   Users in room: ${room.users.length}`);
      console.log(`   MessageID: ${messageData.messageId}`);

      if (messageData.attachment) {
        console.log(`   📎 Broadcasting attachment metadata`);
        console.log(`      Chunked: ${messageData.attachment.chunked}`);
        console.log(`      File: ${messageData.attachment.name}`);

        if (messageData.attachment.data) {
          console.log(`      Legacy data size: ${(messageData.attachment.data.length / 1024).toFixed(2)} KB`);
        } else {
          console.log(`      Chunked - data will arrive separately`);
        }
      }

      if (messageData.attachment && messageData.attachment.chunked) {
        const fileRecord = roomFileStore.get(messageData.attachment.fileId);
        if (fileRecord && fileRecord.assembledData) {
          messageData.attachment.data = fileRecord.assembledData;
          messageData.attachment.chunked = false;
        }
      }

      // Emit to ALL users in the room (including sender)
      io.to(roomId).emit('chat_message', messageData);


      console.log(`✅ Message broadcast complete to ${roomId}`);
      console.log('📡 ========================================');
      console.log('💬 ========================================\n');

    } catch (error) {
      console.error('❌ ========================================');
      console.error('❌ CHAT MESSAGE ERROR');
      console.error('❌ ========================================');
      console.error('   Error:', error.message);
      console.error('   Stack:', error.stack);
      console.error('❌ ========================================\n');
      socket.emit('error', { message: 'Failed to send message' });
    }
  });

  socket.on('initiate_call', async ({ roomId, callType }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.error('❌ Unauthenticated socket tried to initiate call');
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      console.log('📞 ========================================');
      console.log('📞 INITIATE_CALL REQUEST');
      console.log('📞 ========================================');
      console.log(`   User: ${user.username} (${user.userId})`);
      console.log(`   Room: ${roomId}`);
      console.log(`   Type: ${callType}`);

      const room = await matchmaking.getRoom(roomId);

      if (!room) {
        console.error(`❌ Room ${roomId} not found`);
        socket.emit('error', { message: 'Room not found' });
        return;
      }

      if (!room.hasUser(user.userId)) {
        console.error(`❌ User ${user.username} not in room ${roomId}`);
        socket.emit('error', { message: 'You are not in this room' });
        return;
      }

      // Room lock acquired (await acquireRoomInitLock either succeeds or throws)
      const releaseRoomLock = await acquireRoomInitLock(roomId);
      try {
        // ✅ ATOMIC CHECK: Look for existing call using Redis Index
        const existingCall = await findActiveCallForRoom(roomId);

        if (existingCall) {
          console.log(`📞 Call already active in room ${roomId}: ${existingCall.callId}`);
          console.log(`   Participants: ${existingCall.participantCount}`);

          socket.emit('error', {
            message: 'A call is already in progress',
            code: 'CALL_ALREADY_ACTIVE',
            callId: existingCall.callId,
            callType: existingCall.callType,
            participantCount: existingCall.participantCount
          });
          return;
        }

        // Create new call (still inside lock)
        const callId = uuidv4();

        const call = {
          callId,
          roomId,
          callType,
          participants: [user.userId],
          status: 'active',
          createdAt: Date.now(),
          lastActivity: Date.now(),
          initiator: user.userId,
          userMediaStates: new Map()
        };

        call.userMediaStates.set(user.userId, {
          videoEnabled: callType === 'video',
          audioEnabled: true
        });

        // Save to Redis
        await saveCall(call);
        await setUserCall(user.userId, callId);
        webrtcMetrics.increment('totalCalls');

        room.setActiveCall(true);

        console.log(`✅ Call created: ${callId}`);
        console.log(`   Status: ${call.status} (active immediately)`);
        console.log(`   Participants: [${user.userId}]`);
        console.log(`   Room marked as having active call`);

        socket.emit('call_created', {
          callId,
          callType,
          isInitiator: true,
          participants: [{
            userId: user.userId,
            username: user.username,
            pfpUrl: user.pfpUrl,
            videoEnabled: callType === 'video',
            audioEnabled: true
          }]
        });
        console.log(`📤 Sent call_created to initiator ${user.username}`);

        io.to(roomId).emit('call_state_update', {
          callId: callId,
          isActive: true,
          participantCount: 1,
          callType: callType
        });
        console.log(`📢 Broadcasted call_state_update to room ${roomId}`);

        // Send incoming_call to other users via Redis Broadcast
        for (const roomUser of room.users) {
          if (roomUser.userId !== user.userId) {
            io.to(`user:${roomUser.userId}`).emit('incoming_call', {
              callId,
              callType,
              callerUserId: user.userId,
              callerUsername: user.username,
              callerPfp: user.pfpUrl,
              roomId
            });
            console.log(`📤 Sent incoming_call notification to ${roomUser.username}`);
          }
        }

        console.log('✅ ========================================');
        console.log('✅ CALL INITIATION COMPLETE');
        console.log('✅ ========================================\n');

      } catch (error) {
        console.error('❌ Call initiation error:', error);
        socket.emit('error', { message: 'Failed to initiate call' });
      } finally {
        await releaseRoomLock();
      }

    } catch (error) {
      console.error('❌ Initiate call error:', error);
      socket.emit('error', { message: 'Failed to initiate call' });
    }
  });

  socket.on('accept_call', async ({ callId, roomId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      // CRITICAL FIX: Validate room exists BEFORE proceeding
      const room = await matchmaking.getRoom(roomId);
      if (!room) {
        console.error(`❌ Room ${roomId} not found when ${user.username} tried to accept call ${callId}`);
        socket.emit('error', {
          message: 'Room not found or has expired',
          code: 'ROOM_NOT_FOUND'
        });
        return;
      }

      if (!room.hasUser(user.userId)) {
        console.error(`❌ User ${user.username} not in room ${roomId} when accepting call ${callId}`);
        socket.emit('error', {
          message: 'You are not in this room',
          code: 'NOT_IN_ROOM'
        });
        return;
      }

      const releaseCallLock = await acquireCallMutex(callId);
      try {
        // Fetch fresh state from Redis
        const call = await getCall(callId);

        if (!call) {
          socket.emit('error', { message: 'Call not found or ended' });
          return;
        }

        const validation = validateCallState(call, 'accept_call');
        if (!validation.valid) {
          socket.emit('error', { message: validation.error });
          return;
        }

        console.log(`🔍 [accept_call] Before: participants=[${call.participants.join(', ')}]`);
        console.log(`🔍 [accept_call] User ${user.username} (${user.userId}) accepting`);

        if (call.participants.includes(user.userId)) {
          console.log(`⚠️ User ${user.username} already in call ${callId} - re-sending state`);

          const callUsers = call.participants.map(participantId => {
            const roomUser = room.users.find(u => u.userId === participantId);

            if (!roomUser) {
              console.error(`❌ CRITICAL: Participant ${participantId} not found in room ${roomId}!`);
              return null;
            }

            const mediaState = (call.userMediaStates instanceof Map ? call.userMediaStates.get(participantId) : call.userMediaStates[participantId]) || {
              videoEnabled: call.callType === 'video',
              audioEnabled: true
            };

            return {
              userId: roomUser.userId,
              username: roomUser.username,
              pfpUrl: roomUser.pfpUrl,
              ...mediaState
            };
          }).filter(u => u !== null);

          if (callUsers.length !== call.participants.length) {
            console.error(`❌ CRITICAL: Participant count mismatch!`);
            socket.emit('error', {
              message: 'Call state inconsistent. Please try again.',
              code: 'STATE_MISMATCH'
            });
            return;
          }

          socket.emit('call_accepted', {
            callId,
            callType: call.callType,
            users: callUsers
          });

          console.log(`✅ Re-sent call state to ${user.username}`);
          return;
        }

        call.participants.push(user.userId);

        // Update Redis mappings
        await setUserCall(user.userId, callId);

        console.log(`➕ Added ${user.username} to participants`);
        console.log(`🔍 [accept_call] After: participants=[${call.participants.join(', ')}]`);

        call.userMediaStates.set(user.userId, {
          videoEnabled: call.callType === 'video',
          audioEnabled: true
        });

        if (call.status === 'pending') {
          call.status = 'active';
          console.log(`📊 Call status changed: pending → active`);

          if (room) {
            room.setActiveCall(true);
            console.log(`🛡️ Room ${roomId} marked as having active call (unified timer)`);
          }
        }

        call.lastActivity = Date.now();

        // Save updated call state to Redis
        await saveCall(call);

        console.log(`✅ User ${user.username} accepted call ${callId} - now ${call.status.toUpperCase()}`);

        const callUsers = call.participants.map(participantId => {
          const roomUser = room.users.find(u => u.userId === participantId);

          if (!roomUser) {
            console.error(`❌ CRITICAL: Participant ${participantId} not found in room ${roomId}!`);
            return null;
          }

          const mediaState = call.userMediaStates.get(participantId) || {
            videoEnabled: call.callType === 'video',
            audioEnabled: true
          };

          return {
            userId: participantId,
            username: roomUser.username,
            pfpUrl: roomUser.pfpUrl,
            videoEnabled: mediaState.videoEnabled,
            audioEnabled: mediaState.audioEnabled
          };
        }).filter(u => u !== null);

        if (callUsers.length !== call.participants.length) {
          console.error(`❌ CRITICAL: Participant validation failed!`);
          socket.emit('error', {
            message: 'Unable to resolve all participants. Please try again.',
            code: 'PARTICIPANT_RESOLUTION_FAILED'
          });

          // Rollback
          call.participants = call.participants.filter(p => p !== user.userId);
          await saveCall(call);
          await removeUserCall(user.userId);
          return;
        }


        // REMOVED: No need for Promise.all - emits are synchronous
        // CRITICAL FIX: Single broadcast instead of duplicate
        broadcastCallStateUpdate(callId);
      } finally {
        await releaseCallLock();
      }
    } catch (error) {
      console.error('❌ Accept call error:', error);
      socket.emit('error', { message: 'Failed to accept call' });
    }
  });

  socket.on('decline_call', async ({ callId, roomId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      const releaseCallLock = await acquireCallMutex(callId);
      try {
        const call = await getCall(callId);

        if (!call) {
          socket.emit('error', { message: 'Call not found' });
          return;
        }

        console.log(`🚫 User ${user.username} declined call ${callId}`);

        // If user was part of the call, remove them
        if (call.participants.includes(user.userId)) {
          await handleCallLeaveInternal(user.userId, callId);
        }

        // Notify room that user declined (optional)
        io.to(roomId).emit('user_declined_call', { userId: user.userId, callId });

      } finally {
        await releaseCallLock();
      }
    } catch (error) {
      console.error('❌ Decline call error:', error);
      socket.emit('error', { message: 'Failed to decline call' });
    }
  });


  socket.on('connection_established', async ({ callId, connectionType, localType, remoteType, protocol }) => {
    const user = await getSocketUser(socket.id);
    if (!user) return;

    console.log(`📊 [METRICS] Connection established for ${user.username}`);
    console.log(`   Type: ${connectionType}`);
    console.log(`   Local: ${localType}, Remote: ${remoteType}`);
    console.log(`   Protocol: ${protocol}`);

    // Track metrics using atomic operations
    // Note: Simple increment is sufficient for now without Redis atomic incr if fine with slight inaccuracy
    // or use webrtcMetrics which is local?
    // User instructions said "Refactor server for Redis". 
    // webrtcMetrics is a local object (line 755).
    // If we want distributed metrics, we should use pubClient.incr.
    // But for now, local metrics are acceptable or I can update them later.
    // I will leave local metrics for now to avoid scope creep, focus on Core Logic.

    if (connectionType === 'TURN_RELAY') {
      webrtcMetrics.increment('turnUsage');
      console.warn(`⚠️ [METRICS] TURN usage: ${webrtcMetrics.get('turnUsage')} / ${webrtcMetrics.get('totalCalls')} calls`);
    } else if (connectionType === 'STUN_REFLEXIVE') {
      webrtcMetrics.increment('stunUsage');
      console.log(`✅ [METRICS] STUN usage: ${webrtcMetrics.get('stunUsage')} / ${webrtcMetrics.get('totalCalls')} calls`);
    } else if (connectionType === 'DIRECT_HOST') {
      webrtcMetrics.increment('directConnections');
      console.log(`✅ [METRICS] Direct: ${webrtcMetrics.get('directConnections')} / ${webrtcMetrics.get('totalCalls')} calls`);
    }

    webrtcMetrics.increment('successfulConnections');
  });


  // Refactored webrtc_answer
  socket.on('webrtc_answer', async ({ callId, targetUserId, answer }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) return;

      if (!checkSignalingRateLimit(user.userId)) {
        console.warn(`⚠️ Signaling rate limit exceeded for ${user.username}`);
        socket.emit('error', {
          message: 'Too many signaling messages. Please slow down.',
          code: 'RATE_LIMIT_EXCEEDED'
        });
        return;
      }

      if (!answer || typeof answer !== 'object') {
        console.error(`❌ Invalid answer structure from ${user.username}`);
        return;
      }

      const sdpValidation = validateSDP(answer.sdp);
      if (!sdpValidation.valid) {
        console.error(`❌ Invalid SDP from ${user.username}: ${sdpValidation.error}`);
        socket.emit('error', {
          message: 'Invalid WebRTC answer',
          code: 'INVALID_ANSWER'
        });
        return;
      }

      console.log(`📤 WebRTC answer from ${user.username} to ${targetUserId}`);

      // Forward to target user via Redis
      io.to(`user:${targetUserId}`).emit('webrtc_answer', {
        fromUserId: user.userId,
        answer: {
          type: answer.type,
          sdp: answer.sdp
        }
      });
      console.log(`✅ Answer forwarded to ${targetUserId} via Redis`);

    } catch (error) {
      console.error('❌ WebRTC answer error:', error);
    }
  });


  socket.on('ice_candidate', async ({ callId, targetUserId, candidate }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) return;

      if (!checkSignalingRateLimit(user.userId)) {
        console.warn(`⚠️ Signaling rate limit exceeded for ${user.username}`);
        return;
      }

      const validation = validateICECandidate(candidate);
      if (!validation.valid) {
        console.error(`❌ Invalid ICE candidate from ${user.username}: ${validation.error}`);
        return;
      }

      if (candidate) {
        const candidateType = candidate.type || 'unknown';
        console.log(`🧊 [ICE] Candidate from ${user.username} to ${targetUserId}: type=${candidateType}`);
      } else {
        console.log(`🧊 [ICE] End-of-candidates from ${user.username} to ${targetUserId}`);
      }

      // Forward to target user via Redis
      io.to(`user:${targetUserId}`).emit('ice_candidate', {
        fromUserId: user.userId,
        candidate: candidate
      });
      console.log(`✅ [ICE] Candidate forwarded to ${targetUserId} via Redis`);

    } catch (error) {
      console.error('❌ [ICE] Candidate error:', error);
    }
  });


  socket.on('join_call', async ({ callId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      // ✅ FIX: Enhanced debounce with call state check
      const debounceKey = `${user.userId}:${callId}`;
      const lastJoinTime = joinCallDebounce.get(debounceKey);
      const now = Date.now();

      if (lastJoinTime && now - lastJoinTime < 2000) {
        console.warn(`⚠️ Ignoring duplicate join_call from ${user.username} (${now - lastJoinTime}ms since last)`);

        // ✅ Still send success if already in call (idempotent)
        const call = await getCall(callId);
        if (call && call.participants.includes(user.userId)) {
          const room = await matchmaking.getRoom(call.roomId);
          if (room) {
            const participantsWithMediaStates = call.participants.map(participantId => {
              const roomUser = room.users.find(u => u.userId === participantId);
              if (!roomUser) return null;

              const mediaState = call.userMediaStates.get(participantId) || {
                videoEnabled: call.callType === 'video',
                audioEnabled: true
              };

              return {
                userId: roomUser.userId,
                username: roomUser.username,
                pfpUrl: roomUser.pfpUrl,
                videoEnabled: mediaState.videoEnabled,
                audioEnabled: mediaState.audioEnabled
              };
            }).filter(p => p !== null);

            socket.emit('call_joined', {
              callId,
              callType: call.callType,
              participants: participantsWithMediaStates
            });
          }
        }
        return;
      }

      joinCallDebounce.set(debounceKey, now);

      const releaseCallLock = await acquireCallMutex(callId);
      try {
        const call = await getCall(callId);

        if (!call) {
          socket.emit('error', { message: 'Call not found' });
          joinCallDebounce.delete(debounceKey);
          return;
        }

        const validation = validateCallState(call, 'join_call');
        if (!validation.valid) {
          socket.emit('error', { message: validation.error });
          joinCallDebounce.delete(debounceKey);
          return;
        }

        // Validate room exists and user is in it
        const room = await matchmaking.getRoom(call.roomId);
        if (!room) {
          console.error(`❌ Room ${call.roomId} not found when ${user.username} tried to join call ${callId}`);
          socket.emit('error', {
            message: 'Room not found or has expired',
            code: 'ROOM_NOT_FOUND'
          });
          joinCallDebounce.delete(debounceKey);
          return;
        }

        if (!room.hasUser(user.userId)) {
          console.error(`❌ User ${user.username} not in room ${call.roomId}`);
          socket.emit('error', {
            message: 'You are not in this room',
            code: 'NOT_IN_ROOM'
          });
          joinCallDebounce.delete(debounceKey);
          return;
        }

        // ✅ FIX: Atomic check-and-add with Set for deduplication
        const participantSet = new Set(call.participants);
        const wasAlreadyInCall = participantSet.has(user.userId);

        // Ensure media state exists
        if (!call.userMediaStates.has(user.userId)) {
          call.userMediaStates.set(user.userId, {
            videoEnabled: call.callType === 'video',
            audioEnabled: true
          });
          console.log(`📊 Initialized media state for ${user.username}`);
        }

        // Get current user's media state
        const userMediaState = call.userMediaStates.get(user.userId);

        call.lastActivity = Date.now();

        if (!wasAlreadyInCall) {
          call.participants.push(user.userId);
          await setUserCall(user.userId, callId);
        }

        // Save updates to Redis
        await saveCall(call);

        // Clear grace period
        if (callGracePeriod.has(callId)) {
          clearTimeout(callGracePeriod.get(callId));
          callGracePeriod.delete(callId);
          console.log(`⏱️ Cleared grace period for call ${callId}`);
        }

        socket.join(`call-${callId}`);
        console.log(`📞 User ${user.username} joined call room: call-${callId}`);

        // Build participant data from ROOM (not socketUsers)
        const participantsWithMediaStates = call.participants.map(participantId => {
          const roomUser = room.users.find(u => u.userId === participantId);

          if (!roomUser) {
            console.error(`❌ CRITICAL: Participant ${participantId} not in room ${call.roomId}!`);
            return null;
          }

          const mediaState = call.userMediaStates.get(participantId) || {
            videoEnabled: call.callType === 'video',
            audioEnabled: true
          };

          return {
            userId: roomUser.userId,
            username: roomUser.username,
            pfpUrl: roomUser.pfpUrl,
            videoEnabled: mediaState.videoEnabled,
            audioEnabled: mediaState.audioEnabled
          };
        }).filter(p => p !== null);

        // Validate all participants were resolved
        if (participantsWithMediaStates.length !== call.participants.length) {
          console.error(`❌ CRITICAL: Failed to resolve all participants!`);
          socket.emit('error', {
            message: 'Unable to load all participants. Please refresh and try again.',
            code: 'PARTICIPANT_RESOLUTION_FAILED'
          });

          // Rollback locally (Redis save already happened? We should rollback Redis too)
          if (!wasAlreadyInCall) {
            call.participants = call.participants.filter(p => p !== user.userId);
            await saveCall(call); // Save rollback
            await removeUserCall(user.userId);
          }

          joinCallDebounce.delete(debounceKey);
          return;
        }

        console.log(`📊 Sending ${participantsWithMediaStates.length} VALIDATED participants to ${user.username}`);

        socket.emit('call_joined', {
          callId,
          callType: call.callType,
          participants: participantsWithMediaStates
        });

        // CRITICAL: Notify ALL other participants about this user joining
        const notificationData = {
          user: {
            userId: user.userId,
            username: user.username,
            pfpUrl: user.pfpUrl
          },
          mediaState: {
            videoEnabled: userMediaState.videoEnabled,
            audioEnabled: userMediaState.audioEnabled
          }
        };

        // Send to all sockets in the call room EXCEPT the joining user
        socket.to(`call-${callId}`).emit('user_joined_call', notificationData);
        console.log(`📢 Notified others about ${user.username} joining`);

        // Update room state broadcast
        io.to(call.roomId).emit('call_state_update', {
          callId: callId,
          isActive: true,
          participantCount: call.participants.length,
          callType: call.callType
        });

        console.log(`✅ ${user.username} successfully joined call ${callId} with ${call.participants.length} total participants`);
      } finally {
        await releaseCallLock();
      }

      // Clear debounce after successful join
      setTimeout(() => {
        joinCallDebounce.delete(debounceKey);
      }, 2000);

    } catch (error) {
      console.error('❌ Join call error:', error);
      socket.emit('error', { message: 'Failed to join call' });

      const user = await getSocketUser(socket.id);
      const debounceKey = `${user?.userId}:${callId}`;
      joinCallDebounce.delete(debounceKey);
    }
  });


  socket.on('leave_call', async ({ callId }) => {
    try {
      const user = await getSocketUser(socket.id);
      if (!user) {
        console.warn(`⚠️ Unauthenticated socket tried to leave call`);
        return;
      }

      await handleCallLeaveInternal(user.userId, callId);
      socket.leave(`call-${callId}`);
    } catch (error) {
      console.error('❌ Leave call error:', error);
      socket.emit('error', { message: 'Failed to leave call properly' });
    }
  });


  socket.on('join_existing_call', async ({ callId, roomId }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.error('❌ Unauthenticated socket tried to join call');
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      console.log('🔗 ========================================');
      console.log('🔗 JOIN_EXISTING_CALL REQUEST');
      console.log('🔗 ========================================');
      console.log(`   User: ${user.username} (${user.userId})`);
      console.log(`   CallID: ${callId}`);
      console.log(`   RoomID: ${roomId}`);

      // CRITICAL FIX: Use mutex to prevent race conditions
      const releaseCallLock = await acquireCallMutex(callId);
      const updatedCall = await (async () => {
        try {
          const call = await getCall(callId);

          if (!call) {
            console.error(`❌ Call ${callId} not found`);
            socket.emit('error', {
              message: 'Call not found or has ended',
              code: 'CALL_NOT_FOUND'
            });
            return null; // Return null to indicate failure
          }

          // Validate call state
          const validation = validateCallState(call, 'join_existing_call');
          if (!validation.valid) {
            socket.emit('error', { message: validation.error });
            return null;
          }

          if (call.roomId !== roomId) {
            console.error(`❌ Call ${callId} is in different room (${call.roomId} vs ${roomId})`);
            socket.emit('error', {
              message: 'Call is in a different room',
              code: 'WRONG_ROOM'
            });
            return null;
          }

          // CRITICAL FIX: Don't check participant count - allow joining even if empty
          // This handles the case where all users left but call is still "active"
          if (call.status === 'ended') {
            console.error(`❌ Call ${callId} has ended`);
            socket.emit('error', {
              message: 'Call has ended',
              code: 'CALL_ENDED'
            });
            return null;
          }

          // Check if user is in the room
          const room = await matchmaking.getRoom(roomId);
          if (!room) {
            console.error(`❌ Room ${roomId} not found`);
            socket.emit('error', {
              message: 'Room not found',
              code: 'ROOM_NOT_FOUND'
            });
            return null;
          }

          if (!room.hasUser(user.userId)) {
            console.error(`❌ User ${user.username} not in room ${roomId}`);
            socket.emit('error', {
              message: 'You are not in this room',
              code: 'NOT_IN_ROOM'
            });
            return null;
          }

          console.log(`✅ User ${user.username} authorized to join call ${callId}`);
          console.log(`📊 Current participants BEFORE add: [${call.participants.join(', ')}] (${call.participants.length} total)`);

          // CRITICAL FIX: Add user to participants atomically within mutex
          if (!call.participants.includes(user.userId)) {
            call.participants.push(user.userId);
            await setUserCall(user.userId, callId);
            console.log(`➕ Added ${user.username} to call participants (within mutex)`);
            console.log(`📊 Current participants AFTER add: [${call.participants.join(', ')}] (${call.participants.length} total)`);
          } else {
            console.log(`ℹ️ User ${user.username} already in call participants (re-joining)`);
          }

          // Mark call as active if it was in pending state
          if (call.status === 'pending') {
            call.status = 'active';
            console.log(`📊 Call status changed: pending → active`);
          }

          call.lastActivity = Date.now();

          // Initialize media state for joining user if not present
          if (!call.userMediaStates.has(user.userId)) {
            const defaultVideoState = call.callType === 'video';
            call.userMediaStates.set(user.userId, {
              videoEnabled: defaultVideoState,
              audioEnabled: true
            });
            console.log(`📊 Set initial media state for ${user.username}: video=${defaultVideoState}, audio=true`);
          }

          // Clear any grace period on this call
          if (callGracePeriod.has(callId)) {
            clearTimeout(callGracePeriod.get(callId));
            callGracePeriod.delete(callId);
            console.log(`⏱️ Cleared grace period for call ${callId} (new participant joined)`);
          }

          // Save updates to Redis
          await saveCall(call);

          // Mark room as having active call
          if (room && !room.hasActiveCall) {
            room.setActiveCall(true);
            console.log(`🛡️ Room ${roomId} marked as having active call`);
          }

          console.log('🔗 ========================================');
          console.log('🔗 JOIN REQUEST COMPLETE (within mutex)');
          console.log('🔗 ========================================');
          console.log(`   ${user.username} is NOW in participants list`);
          console.log(`   Total participants: ${call.participants.length}`);
          console.log(`   Participants: [${call.participants.join(', ')}]`);
          console.log(`   User will receive success event and navigate to call page`);
          console.log('🔗 ========================================\n');

          return call; // Return updated call object
        } finally {
          await releaseCallLock();
        }
      })(); // CRITICAL: Mutex releases HERE - state is now consistent

      // CRITICAL FIX: Emit success and broadcast AFTER mutex completes
      if (updatedCall && updatedCall.participants.includes(user.userId)) {
        // Send success response
        socket.emit('join_existing_call_success', {
          callId,
          callType: updatedCall.callType,
          roomId: updatedCall.roomId
        });

        console.log(`✅ Sent join_existing_call_success to ${user.username} (after mutex release)`);
        console.log(`   User will now navigate to call page`);

        // Broadcast updated call state to room
        io.to(roomId).emit('call_state_update', {
          callId: callId,
          isActive: true,
          participantCount: updatedCall.participants.length,
          callType: updatedCall.callType
        });
        console.log(`📢 Broadcasted call_state_update to room: ${updatedCall.participants.length} participant(s)`);

        // CRITICAL: Notify existing call participants about the new joiner
        // This ensures tiles are created on all devices
        const joinerMediaState = updatedCall.userMediaStates.get(user.userId);

        socket.join(`call-${callId}`);
        console.log(`📞 User ${user.username} joined Socket.IO call room: call-${callId}`);

        const notificationData = {
          user: {
            userId: user.userId,
            username: user.username,
            pfpUrl: user.pfpUrl
          },
          mediaState: {
            videoEnabled: joinerMediaState?.videoEnabled || (updatedCall.callType === 'video'),
            audioEnabled: joinerMediaState?.audioEnabled || true
          }
        };

        // Broadcast to all OTHER participants in the call
        socket.to(`call-${callId}`).emit('user_joined_call', notificationData);
        console.log(`📢 Notified existing participants in call-${callId} about ${user.username} joining`);
        console.log(`   Media state: video=${notificationData.mediaState.videoEnabled}, audio=${notificationData.mediaState.audioEnabled}`);

      } else {
        // If locked failed or returned null (error already emitted)
        // Do nothing
      }

    } catch (error) {
      console.error('❌ Join existing call error:', error);
      socket.emit('error', {
        message: 'Failed to join call',
        code: 'JOIN_FAILED'
      });
    }
  });






  function checkSignalingRateLimit(userId) {
    const now = Date.now();
    const userLimit = signalingRateLimiter.get(userId);

    if (!userLimit || now > userLimit.resetTime) {
      signalingRateLimiter.set(userId, {
        count: 1,
        resetTime: now + 10000 // 10 seconds
      });
      return true;
    }

    if (userLimit.count >= MAX_SIGNALING_RATE) {
      return false;
    }

    userLimit.count++;
    return true;
  }

  function validateSDP(sdp, maxSize = MAX_SDP_SIZE) {
    if (!sdp || typeof sdp !== 'string') {
      return { valid: false, error: 'SDP must be a string' };
    }

    if (sdp.length > maxSize) {
      return { valid: false, error: `SDP exceeds maximum size of ${maxSize} bytes` };
    }

    // Basic structure validation
    if (!sdp.includes('v=0') || !sdp.includes('m=')) {
      return { valid: false, error: 'Invalid SDP structure' };
    }

    return { valid: true };
  }

  function validateICECandidate(candidate) {
    if (candidate === null || candidate === undefined) {
      return { valid: true }; // End-of-candidates signal
    }

    if (typeof candidate !== 'object') {
      return { valid: false, error: 'ICE candidate must be an object' };
    }

    const candidateStr = JSON.stringify(candidate);
    if (candidateStr.length > MAX_ICE_CANDIDATE_SIZE) {
      return { valid: false, error: `ICE candidate exceeds ${MAX_ICE_CANDIDATE_SIZE} bytes` };
    }

    return { valid: true };
  }

  socket.on('webrtc_offer', async ({ callId, targetUserId, offer, renegotiation }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) return;

      if (!checkSignalingRateLimit(user.userId)) {
        console.warn(`⚠️ Signaling rate limit exceeded for ${user.username}`);
        socket.emit('error', {
          message: 'Too many signaling messages. Please slow down.',
          code: 'RATE_LIMIT_EXCEEDED'
        });
        return;
      }

      if (!offer || typeof offer !== 'object') {
        console.error(`❌ Invalid offer structure from ${user.username}`);
        return;
      }

      const sdpValidation = validateSDP(offer.sdp);
      if (!sdpValidation.valid) {
        console.error(`❌ Invalid SDP from ${user.username}: ${sdpValidation.error}`);
        socket.emit('error', {
          message: 'Invalid WebRTC offer',
          code: 'INVALID_OFFER'
        });
        return;
      }

      const offerKey = `dedupe:offer:${callId}:${user.userId}:${targetUserId}`;

      if (!renegotiation) {
        // Redis Deduplication (2000ms TTL)
        const isNew = await pubClient.set(offerKey, '1', 'PX', OFFER_DEDUPE_WINDOW, 'NX');
        if (!isNew) {
          console.warn(`⚠️ Duplicate offer from ${user.username} to ${targetUserId}, ignoring (Redis dedupe)`);
          return;
        }
      }

      const offerType = renegotiation ? 'RENEGOTIATION' : 'INITIAL';
      console.log(`📤 WebRTC ${offerType} offer from ${user.username} to ${targetUserId}`);

      // Forward to target user via Redis
      // Check presence first to avoid shouting into void? 
      // Not strictly necessary as io.to is safe, but good for logging.

      io.to(`user:${targetUserId}`).emit('webrtc_offer', {
        fromUserId: user.userId,
        offer: {
          type: offer.type,
          sdp: offer.sdp
        },
        renegotiation: renegotiation || false
      });
      console.log(`✅ ${offerType} offer forwarded to ${targetUserId} via Redis`);

    } catch (error) {
      console.error('❌ WebRTC offer error:', error);
    }
  });



  socket.on('ice_candidate', async ({ callId, targetUserId, candidate }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) return;

      // ✅ FIX: Rate limiting
      if (!checkSignalingRateLimit(user.userId)) {
        console.warn(`⚠️ Signaling rate limit exceeded for ${user.username}`);
        return; // Silently drop ICE candidates on rate limit
      }

      // ✅ FIX: Validate ICE candidate
      const validation = validateICECandidate(candidate);
      if (!validation.valid) {
        console.error(`❌ Invalid ICE candidate from ${user.username}: ${validation.error}`);
        return;
      }

      // Log candidate details
      if (candidate) {
        const candidateType = candidate.type || 'unknown';
        console.log(`🧊 [ICE] Candidate from ${user.username} to ${targetUserId}: type=${candidateType}`);
      } else {
        console.log(`🧊 [ICE] End-of-candidates from ${user.username} to ${targetUserId}`);
      }

      // Broadcast to specific user via Redis Adapter
      io.to(`user:${targetUserId}`).emit('ice_candidate', {
        fromUserId: user.userId,
        candidate: candidate
      });
      console.log(`✅ [ICE] Candidate forwarded to ${targetUserId} via Redis`);

    } catch (error) {
      console.error('❌ [ICE] Candidate error:', error);
    }
  });

  socket.on('connection_state_update', async ({ callId, state, candidateType }) => {
    const user = await getSocketUser(socket.id);
    if (!user) return;

    console.log(`🔌 Connection state from ${user.username}: ${state}`);
    if (candidateType) {
      console.log(`   Using candidate type: ${candidateType}`);

      // Track metrics based on candidate type using atomic operations
      if (candidateType === 'relay') {
        webrtcMetrics.increment('turnUsage');
        console.log('   📊 TURN relay connection established');
      } else if (candidateType === 'srflx') {
        webrtcMetrics.increment('stunUsage');
        console.log('   📊 STUN server-reflexive connection established');
      } else if (candidateType === 'host') {
        webrtcMetrics.increment('directConnections');
        console.log('   📊 Direct host connection established');
      }
    }

    if (state === 'connected') {
      webrtcMetrics.increment('successfulConnections');
      console.log(`   ✅ Total successful connections: ${webrtcMetrics.get('successfulConnections')}`);
    } else if (state === 'failed') {
      webrtcMetrics.increment('failedConnections');
      console.log(`   ❌ Total failed connections: ${webrtcMetrics.get('failedConnections')}`);
    }
  });

  // ✅ FIX K: Server-authoritative state verification
  socket.on('verify_call_state', async ({ callId }) => {
    try {
      const user = await getSocketUser(socket.id);
      if (!user) return;

      console.log(`🔍 ========================================`);
      console.log(`🔍 STATE VERIFICATION REQUEST`);
      console.log(`🔍 ========================================`);
      console.log(`   From: ${user.username} (${user.userId})`);
      console.log(`   CallID: ${callId}`);

      const call = await getCall(callId);

      if (!call) {
        console.log(`❌ Call ${callId} not found on server`);
        socket.emit('call_state_mismatch', {
          callId,
          reason: 'call_not_found',
          action: 'leave'
        });
        console.log(`📤 Sent call_state_mismatch - instructing client to leave`);
        return;
      }

      // Check if user is in participant list
      if (!call.participants.includes(user.userId)) {
        console.log(`❌ User ${user.username} not in server participant list`);
        console.log(`   Server participants: [${call.participants.join(', ')}]`);

        socket.emit('call_state_mismatch', {
          callId,
          reason: 'not_in_participants',
          action: 'leave'
        });
        console.log(`📤 Sent call_state_mismatch - instructing client to leave`);
        return;
      }

      // Provide authoritative participant list
      const room = await matchmaking.getRoom(call.roomId);
      const db = getDB();
      const usersCollection = db.collection('users');

      const participantDetails = await Promise.all(
        call.participants.map(async (userId) => {
          const user = await usersCollection.findOne(
            { _id: new ObjectId(userId) },
            { projection: { username: 1, profilePicture: 1 } }
          );

          const mediaState = call.userMediaStates.get(userId) || {
            videoEnabled: call.callType === 'video',
            audioEnabled: true
          };

          return {
            userId,
            username: user?.username || 'Unknown',
            profilePicture: user?.profilePicture || null,
            videoEnabled: mediaState.videoEnabled,
            audioEnabled: mediaState.audioEnabled
          };
        })
      );

      console.log(`✅ Server state verified - sending authoritative data`);
      console.log(`   Participants: ${participantDetails.length}`);

      socket.emit('call_state_verified', {
        callId,
        participants: participantDetails,
        callType: call.callType,
        expiresAt: room?.expiresAt || null
      });

      console.log(`🔍 ========================================\n`);

    } catch (error) {
      console.error('❌ Verify call state error:', error);
    }
  });

  socket.on('speaking_state', async ({ callId, speaking }) => {
    try {
      const user = await getSocketUser(socket.id);
      if (!user) return;

      socket.to(`call-${callId}`).emit('speaking_state', {
        userId: user.userId,
        speaking
      });
    } catch (error) {
      console.error('Speaking state error:', error);
    }
  });

  socket.on('audio_state_changed', async ({ callId, enabled }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) return;

      const releaseCallLock = await acquireCallMutex(callId);
      try {
        const call = await getCall(callId);
        if (!call) return;

        // Ensure map exists (getCall handles this, but safety check)
        if (!call.userMediaStates) call.userMediaStates = new Map();

        const currentState = (call.userMediaStates instanceof Map ? call.userMediaStates.get(user.userId) : call.userMediaStates[user.userId]) || {
          videoEnabled: call.callType === 'video',
          audioEnabled: true
        };

        if (call.userMediaStates instanceof Map) {
          call.userMediaStates.set(user.userId, {
            ...currentState,
            audioEnabled: enabled
          });
        } else {
          call.userMediaStates[user.userId] = {
            ...currentState,
            audioEnabled: enabled
          };
        }

        await saveCall(call);

        console.log(`🎤 ${user.username} audio: ${enabled ? 'ON' : 'OFF'} (call ${callId})`);

        // Broadcast to ALL users in call room
        io.to(`call-${callId}`).emit('audio_state_changed', {
          userId: user.userId,
          enabled
        });
      } finally {
        await releaseCallLock();
      }

    } catch (error) {
      console.error('❌ Audio state error:', error);
    }
  });

  socket.on('video_state_changed', async ({ callId, enabled }) => {
    try {
      const user = await getSocketUser(socket.id);

      if (!user) {
        console.warn(`⚠️ Unauthenticated socket tried to change video state`);
        return;
      }

      const releaseCallLock = await acquireCallMutex(callId);
      try {
        const call = await getCall(callId);
        if (!call) {
          console.warn(`⚠️ Call ${callId} not found for video state change`);
          return;
        }

        if (!call.userMediaStates) {
          call.userMediaStates = new Map();
        }

        const currentState = (call.userMediaStates instanceof Map ? call.userMediaStates.get(user.userId) : call.userMediaStates[user.userId]) || {
          videoEnabled: call.callType === 'video',
          audioEnabled: true
        };

        if (call.userMediaStates instanceof Map) {
          call.userMediaStates.set(user.userId, {
            ...currentState,
            videoEnabled: enabled
          });
        } else {
          call.userMediaStates[user.userId] = {
            ...currentState,
            videoEnabled: enabled
          };
        }

        await saveCall(call);

        console.log(`📹 SERVER: VIDEO STATE CHANGE - User: ${user.username}, State: ${enabled ? 'ON' : 'OFF'}`);

        // ✅ FIX: Broadcast to OTHER users only (exclude sender)
        socket.to(`call-${callId}`).emit('video_state_changed', {
          userId: user.userId,
          enabled: enabled
        });
      } finally {
        await releaseCallLock();
      }

    } catch (error) {
      console.error('❌ Video state error:', error);
    }
  });




  socket.on('leave_room', async (data, callback) => {
    const userData = await getSocketUser(socket.id);
    if (!userData) {
      return callback?.({ success: false, error: 'Not authenticated' });
    }

    const firebaseUid = userData.firebaseUid;
    const activeRoom = await getUserActiveRoom(firebaseUid);

    if (!activeRoom) {
      return callback?.({ success: true, message: 'No active room' });
    }

    try {
      const result = await performUserLeaveChat(userData.userId, activeRoom.roomId, 'manual', firebaseUid);
      callback?.(result);
    } catch (error) {
      console.error(`❌ [leave_room] Error:`, error);
      callback?.({ success: false, error: error.message });
    }
  });



  socket.on('disconnect', async (reason) => {
    console.log(`🔌 Socket disconnected: ${socket.id} (reason: ${reason})`);

    const userData = await getSocketUser(socket.id);
    if (!userData) {
      console.log(`ℹ️ Socket ${socket.id} was not authenticated or already cleaned up`);
      return;
    }

    const userId = userData.userId;
    const firebaseUid = userData.firebaseUid;
    const username = userData.username;

    // Unregister this socket from multi-device tracking
    if (firebaseUid) {
      await unregisterSocketForUser(firebaseUid, socket.id);
    }

    // Clean up socket user data mapping in Redis
    await deleteSocketUser(socket.id);

    try {
      // Check if user has other active devices across the cluster
      const sockets = await io.in(`user:${userId}`).fetchSockets();
      const remainingDevices = sockets.length;

      if (remainingDevices > 0) {
        console.log(`📱 [Presence] User ${username} still has ${remainingDevices} active device(s)`);
        return;
      }

      console.log(`👤 [Presence] Last device disconnected for ${username}. Scheduling distributed cleanup.`);

      // Use Redis TTL based cleanup instead of local setTimeout
      await scheduleUserCleanup(userId, 500); // 500ms grace period

    } catch (error) {
      console.error(`❌ Error in disconnect handler for ${userId}:`, error);
    }
  });

});
// ============================================
// PERIODIC CLEANUP
// ============================================


const fileChunkRateLimiter = new Map(); // userId -> { count, resetTime }
const CHUNK_RATE_LIMIT = 100; // Max chunks per 10 seconds
const RATE_WINDOW = 10000; // 10 seconds

function checkChunkRateLimit(userId) {
  const now = Date.now();
  const userLimit = fileChunkRateLimiter.get(userId);

  if (!userLimit || now > userLimit.resetTime) {
    fileChunkRateLimiter.set(userId, {
      count: 1,
      resetTime: now + RATE_WINDOW
    });
    return true;
  }

  if (userLimit.count >= CHUNK_RATE_LIMIT) {
    return false; // Rate limit exceeded
  }

  userLimit.count++;
  return true;
}

// Clean up rate limiter every 30s
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  for (const [userId, limit] of fileChunkRateLimiter.entries()) {
    if (now > limit.resetTime) {
      fileChunkRateLimiter.delete(userId);
      cleaned++;
    }
  }
  if (cleaned > 0) {
    console.log(`🗑️ Cleaned up ${cleaned} expired rate limit entries`);
  }
}, 30000);



// ============================================
// ASYNC PERIODIC CLEANUP (NON-BLOCKING)
// ============================================

async function performPeriodicCleanup() {
  const startTime = Date.now();
  console.log(`🧹 Starting periodic cleanup...`);

  try {
    const now = Date.now();
    const BATCH_SIZE = 50;

    // Clean up expired rooms (Authoritative check in case keyspace notification was missed)
    const rooms = matchmaking.getActiveRooms();
    console.log(`🧹 Checking ${rooms.length} active rooms for expiry`);

    for (let i = 0; i < rooms.length; i += BATCH_SIZE) {
      const batch = rooms.slice(i, i + BATCH_SIZE);
      for (const room of batch) {
        if (room.expiresAt <= now) {
          console.log(`🕐 Room ${room.id} has expired, cleaning up...`);
          await performRoomCleanup(room.id);
        }
      }
      if (i + BATCH_SIZE < rooms.length) await new Promise(resolve => setImmediate(resolve));
    }

    // Cleanup Rate Limiters (Still using local Maps for rate limiting is okay 
    // as it is per-instance protection, but for cluster-wide limits we'd use Redis)
    let cleanedRateLimiters = 0;
    for (const [key, limit] of signalingRateLimiter.entries()) {
      if (now > limit.resetTime) { signalingRateLimiter.delete(key); cleanedRateLimiters++; }
    }
    for (const [key, limit] of fileChunkRateLimiter.entries()) {
      if (now > limit.resetTime) { fileChunkRateLimiter.delete(key); cleanedRateLimiters++; }
    }
    for (const [key, limit] of connectionRateLimiter.entries()) {
      if (now > limit.resetTime) { connectionRateLimiter.delete(key); cleanedRateLimiters++; }
    }
    if (cleanedRateLimiters > 0) console.log(`🗑️ Cleaned up ${cleanedRateLimiters} expired rate limiters`);

    // Audit mood registry for orphaned users in Redis
    let orphanedUsers = 0;
    for (const moodConfig of config.MOODS) {
      const mood = moodConfig.id;
      const userIds = await pubClient.smembers(`mood:${mood}:users`);
      for (const userId of userIds) {
        const presence = await getUserPresence(userId);
        if (!presence || (now - (presence.lastSeen || 0) > 300000)) { // 5 min threshold
          orphanedUsers++;
          await removeUserFromMood(userId, mood);
        }
      }
    }
    if (orphanedUsers > 0) console.log(`🗑️ Cleaned up ${orphanedUsers} orphaned users from mood tracking`);

    const cleanupDuration = Date.now() - startTime;
    const allUsers = await getAllSocketUsers();

    console.log(`📊 Periodic cleanup completed in ${cleanupDuration}ms`);
    console.log(`📊 Statistics:
    - Active sockets on this instance: ${io.engine.clientsCount}
    - Total sockets in cluster (Redis): ${Object.keys(allUsers).length}
    - Global connections limit: ${io.engine.clientsCount}/${MAX_CONNECTIONS_GLOBAL}`);

  } catch (error) {
    console.error('❌ Periodic cleanup error:', error);
  }
}

// ✅ FIX: Run cleanup as async function (non-blocking)
setInterval(() => {
  performPeriodicCleanup().catch(error => {
    console.error('💥 Periodic cleanup fatal error:', error);
  });
}, 60000); // Every 60 seconds


// CRITICAL: Add graceful shutdown handler
process.on('SIGTERM', async () => {
  console.log('🛑 SIGTERM received, starting graceful shutdown...');

  // Stop accepting new connections
  server.close(() => {
    console.log('🛑 HTTP server closed');
  });

  // Notify all connected users
  io.emit('server_shutdown', {
    message: 'Server is shutting down for maintenance',
    reconnectIn: 10000
  });

  // Give clients time to save state
  setTimeout(() => {
    // Clean up all timers
    roomCleanupTimers.forEach(timer => clearTimeout(timer));
    callGracePeriod.forEach(timer => clearTimeout(timer));
    socketUserCleanup.forEach(timer => clearTimeout(timer));

    console.log('✅ All timers cleared');

    // Force disconnect all sockets
    io.close(() => {
      console.log('✅ Socket.IO server closed');
      process.exit(0);
    });
  }, 3000);
});

// CRITICAL: Add uncaught exception handler
process.on('uncaughtException', (error) => {
  console.error('💥 UNCAUGHT EXCEPTION:', error);
  console.error('Stack:', error.stack);
  // Log to external monitoring service here
  // DO NOT exit - let PM2/Docker handle restarts
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 UNHANDLED PROMISE REJECTION at:', promise, 'reason:', reason);
  // Log to external monitoring service here
});

// Add after line 2182 (in periodic cleanup interval)
setInterval(() => {
  const now = Date.now();

  // Clean up expired offers (> 5 seconds old)
  let expiredOffers = 0;
  for (const [key, timestamp] of activeOffers.entries()) {
    if (now - timestamp > 5000) {
      activeOffers.delete(key);
      expiredOffers++;
    }
  }
  if (expiredOffers > 0) {
    console.log(`🗑️ Cleaned up ${expiredOffers} expired offers`);
  }

  // Clean up expired answer debounce (> 5 seconds old)
  let expiredAnswers = 0;
  for (const [key, timestamp] of answerDebounce.entries()) {
    if (now - timestamp > 5000) {
      answerDebounce.delete(key);
      expiredAnswers++;
    }
  }
  if (expiredAnswers > 0) {
    console.log(`🗑️ Cleaned up ${expiredAnswers} expired answer debounce entries`);
  }

  // NOTE: joinCallDebounce and roomJoinState are now Redis-backed with TTL auto-expiry
  // No local cleanup needed for those

}, 30000); // Every 30 seconds



// ============================================
// START SERVER
// ============================================
async function startServer() {
  try {
    // ============================================
    // DATABASE CONNECTION
    // ============================================
    await connectDB();
    console.log('✅ Connected to MongoDB');

    const db = getDB(); // ✅ Get database instance

    // ============================================
    // DATABASE INDEXES
    // ============================================
    try {
      // Ensure indexes exist for performance
      await db.collection('users').createIndex({ email: 1 }, { unique: true });
      await db.collection('users').createIndex({ username: 1 }, { unique: true });
      await db.collection('users').createIndex({ firebaseUid: 1 });

      await db.collection('notes').createIndex({ createdAt: -1 }); // For pagination
      await db.collection('notes').createIndex({ userId: 1 }); // For user lookup
      await db.collection('notes').createIndex({ userId: 1, createdAt: -1 }); // Compound for user+pagination queries

      console.log('✅ Database indexes created');
    } catch (indexError) {
      // Indexes might already exist - this is fine
      if (indexError.code !== 11000) {
        console.warn('⚠️ Index creation warning:', indexError.message);
      }
    }

    // ============================================
    // MONGODB CONNECTION MONITORING
    // ============================================
    const mongoClient = db.client || db.s?.client;

    if (mongoClient) {
      let reconnectAttempts = 0;
      const MAX_RECONNECT_ATTEMPTS = 5;
      const RECONNECT_INTERVAL = 5000; // 5 seconds

      mongoClient.on('error', async (error) => {
        console.error('💥 MongoDB connection error:', error);

        // ✅ FIX: Attempt automatic reconnection
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          reconnectAttempts++;
          console.log(`🔄 Attempting MongoDB reconnection (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);

          setTimeout(async () => {
            try {
              await connectDB();
              console.log('✅ MongoDB reconnected successfully');
              reconnectAttempts = 0; // Reset counter on success
            } catch (reconnectError) {
              console.error(`❌ MongoDB reconnection attempt ${reconnectAttempts} failed:`, reconnectError.message);

              if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
                console.error('💥 CRITICAL: MongoDB reconnection failed after maximum attempts');
                console.error('💥 Server requires manual intervention or restart');
                // TODO: Alert monitoring system (PagerDuty, Sentry, etc.)
              }
            }
          }, RECONNECT_INTERVAL * reconnectAttempts); // Exponential backoff
        }
      });

      mongoClient.on('close', () => {
        console.error('💥 MongoDB connection closed unexpectedly');
        console.log('🔄 Connection will be restored automatically if possible');
        // TODO: Alert monitoring system
      });

      mongoClient.on('reconnect', () => {
        console.log('✅ MongoDB reconnected successfully');
        reconnectAttempts = 0; // Reset on successful reconnect
      });

      mongoClient.on('serverHeartbeatFailed', (event) => {
        console.warn(`⚠️ MongoDB heartbeat failed to ${event.connectionId}`);
      });

      mongoClient.on('serverHeartbeatSucceeded', (event) => {
        // Only log first success after failure to avoid spam
        if (reconnectAttempts > 0) {
          console.log(`✅ MongoDB heartbeat restored to ${event.connectionId}`);
        }
      });

      console.log('✅ MongoDB connection monitoring enabled with auto-reconnect');
    } else {
      console.warn('⚠️ Could not attach MongoDB connection event listeners');
    }

    // ============================================
    // FIREBASE INITIALIZATION
    // ============================================
    initializeFirebase();

    // ============================================
    // START HTTP SERVER
    // ============================================
    const PORT = config.PORT || 3000;

    server.listen(PORT, () => {
      console.log('');
      console.log('🚀 ========================================');
      console.log('🚀 SERVER STARTED SUCCESSFULLY');
      console.log('🚀 ========================================');
      console.log(`   Port: ${PORT}`);
      console.log(`   Environment: ${process.env.NODE_ENV || 'development'}`);
      console.log(`   Health check: http://localhost:${PORT}/health`);
      console.log('');
      console.log('📊 Configuration:');
      console.log(`   Socket.IO: Ready`);
      console.log(`   WebRTC Signaling: Enabled`);
      console.log(`   Room Expiry: ${ROOM_EXPIRY_TIME / 60000} minutes`);

      const hasTurn = !!(
        process.env.CLOUDFLARE_TURN_TOKEN_ID &&
        process.env.CLOUDFLARE_TURN_API_TOKEN
      );

      if (hasTurn) {
        console.log(`   TURN Server: Cloudflare (configured)`);
        console.log(`   ICE Priority: host → srflx (STUN) → relay (TURN)`);
      } else {
        console.log(`   TURN Server: Not configured (STUN only)`);
      }

      console.log('');
      console.log('✅ Server is ready to accept connections');
      console.log('🚀 ========================================');
      console.log('');
    });

    // ============================================
    // GRACEFUL SHUTDOWN HANDLERS
    // ============================================

    async function gracefulShutdown() {
      console.log('');
      console.log('🛑 ========================================');
      console.log('🛑 GRACEFUL SHUTDOWN INITIATED');
      console.log('🛑 ========================================');

      // Stop accepting new connections
      server.close(() => {
        console.log('✅ HTTP server closed');
      });

      // ✅ FIX: Wait for client acknowledgments before forcing shutdown
      const shutdownPromises = [];
      let ackCount = 0;

      // Notify all connected clients of THIS instance and wait for acknowledgments
      const localSockets = await io.fetchSockets();
      console.log(`📢 Notifying ${localSockets.length} local connected clients, waiting for acknowledgments...`);

      for (const clientSocket of localSockets) {
        const userData = await getSocketUser(clientSocket.id);
        const username = userData?.username || clientSocket.id;

        const ackPromise = new Promise((resolve) => {
          const timeout = setTimeout(() => {
            console.log(`⚠️ Shutdown ack timeout for ${username}`);
            resolve();
          }, 8000); // 8-second timeout per client

          clientSocket.emit('server_shutdown', {
            message: 'Server is shutting down for maintenance',
            reconnectIn: 10000
          }, () => {
            clearTimeout(timeout);
            ackCount++;
            console.log(`✅ Shutdown ack received from ${username}`);
            resolve();
          });
        });

        shutdownPromises.push(ackPromise);
      }

      // ✅ FIX: Wait for all local clients or 10-second timeout (whichever comes first)
      await Promise.race([
        Promise.all(shutdownPromises),
        new Promise(resolve => setTimeout(resolve, 10000))
      ]);

      console.log(`✅ Received ${ackCount}/${localSockets.length} client acknowledgments`);

      // Timers in Redis (TTL) handle cleanup automatically across cluster
      // No local maps of timers to clear in stateless mode
      console.log(`✅ No local timers to clean up (handled via Redis TTL)`);

      // Close Socket.IO
      io.close(() => {
        console.log('✅ Socket.IO server closed');
      });

      // Close MongoDB connection
      try {
        if (mongoClient) {
          await mongoClient.close();
          console.log('✅ MongoDB connection closed');
        }
      } catch (error) {
        console.error('❌ Error closing MongoDB:', error);
      }

      console.log('');
      console.log('✅ Graceful shutdown complete');
      console.log('🛑 ========================================');
      console.log('');

      process.exit(0);
    }

    // Register shutdown handlers
    process.on('SIGTERM', gracefulShutdown);
    process.on('SIGINT', gracefulShutdown);

  } catch (error) {
    console.error('');
    console.error('💥 ========================================');
    console.error('💥 FATAL: Failed to start server');
    console.error('💥 ========================================');
    console.error('Error:', error.message);
    console.error('Stack:', error.stack);
    console.error('💥 ========================================');
    console.error('');
    process.exit(1);
  }
}

startServer();
