import { v4 as uuidv4 } from 'uuid';
import config from './config.js';

let redis = null;
let io = null;
let redlock = null;

const ROOM_LIFETIME = 600000; // 10 minutes
const ROOM_WARNING_TIME = 600000; // 1 minute

/**
 * Initialize matchmaking with Redis client, Socket.IO, and Redlock
 */
export function init(redisClient, ioInstance, redlockInstance) {
  redis = redisClient;
  io = ioInstance;
  redlock = redlockInstance;
  console.log('📡 [Matchmaking] Initialized with Redis, Socket.IO, and Redlock');
}

/**
 * Acquire distributed lock for room operations
 */
async function acquireRoomMutex(roomId) {
  if (!redlock) return () => { };
  const lockKey = `locks:room:${roomId}`;
  const lockTTL = 5000; // 5 seconds

  try {
    const lock = await redlock.acquire([lockKey], lockTTL);
    console.log(`🔒 [Redlock][MMR] Acquired room lock for ${roomId}`);

    return async () => {
      try {
        if (typeof lock.release === 'function') {
          await lock.release();
        } else if (typeof lock.unlock === 'function') {
          await lock.unlock();
        }
        console.log(`🔓 [Redlock][MMR] Released room lock for ${roomId}`);
      } catch (error) {
        console.warn(`⚠️ [Redlock][MMR] Lock release failed for ${roomId}:`, error.message);
      }
    };
  } catch (error) {
    console.warn(`⚠️ [Redlock][MMR] Failed to acquire room lock for ${roomId}:`, error.message);
    return () => { };
  }
}

async function saveRoomToRedis(roomData) {
  // Create a shallow copy to avoid mutating the original object during serialization
  const data = { ...roomData };
  const id = data.id;

  // Serialize complex arrays/objects for Redis HSET
  if (data.users && typeof data.users !== 'string') data.users = JSON.stringify(data.users);
  if (data.messages && typeof data.messages !== 'string') data.messages = JSON.stringify(data.messages);

  // Ensure dates are numbers
  if (data.createdAt instanceof Date) data.createdAt = data.createdAt.getTime();
  if (data.lastActivity instanceof Date) data.lastActivity = data.lastActivity.getTime();
  if (data.expiresAt instanceof Date) data.expiresAt = data.expiresAt.getTime();

  try {
    await redis.hset(`room:data:${id}`, data);
    // Set Redis key expiry to match room expiry exactly
    if (data.expiresAt) {
      const ttl = Math.floor((data.expiresAt - Date.now()) / 1000);
      if (ttl > 0) {
        await redis.expire(`room:data:${id}`, ttl + 300); // 5 min buffer for data safety
      }
    }
  } catch (error) {
    console.error(`❌ [Redis] Save failure for room ${id}:`, error.stack);
    throw error;
  }
}

async function getRoomFromRedis(roomId) {
  try {
    console.log(`🔍 [Redis][getRoom] Fetching room:data:${roomId}...`);
    const data = await redis.hgetall(`room:data:${roomId}`);

    if (!data || !Object.keys(data).length) {
      console.log(`ℹ️ [Redis][getRoom] No data found for room ${roomId}`);
      return null;
    }

    console.log(`🔍 [Redis][getRoom] Data found for ${roomId}. Keys:`, Object.keys(data).join(', '));

    try {
      if (data.users) {
        console.log(`🔍 [Redis][getRoom] Parsing users for ${roomId}...`);
        data.users = JSON.parse(data.users);
      }
      if (data.messages) {
        console.log(`🔍 [Redis][getRoom] Parsing messages for ${roomId}...`);
        data.messages = JSON.parse(data.messages);
      }
    } catch (parseError) {
      console.error(`❌ [Redis][getRoom] JSON Parse error for room ${roomId}:`, parseError.message);
      return null;
    }

    if (data.createdAt) data.createdAt = parseInt(data.createdAt);
    if (data.lastActivity) data.lastActivity = parseInt(data.lastActivity);
    if (data.expiresAt) data.expiresAt = parseInt(data.expiresAt);
    if (data.timerStartedAt) data.timerStartedAt = parseInt(data.timerStartedAt);
    if (data.maxUsers) data.maxUsers = parseInt(data.maxUsers); // Ensure maxUsers is a number

    // Parse Booleans
    if (data.isExpired) data.isExpired = (data.isExpired === 'true');
    else data.isExpired = false; // Default to false if missing

    if (data.hasActiveCall) data.hasActiveCall = (data.hasActiveCall === 'true');
    else data.hasActiveCall = false;

    // Ensure arrays exist
    if (!data.users) data.users = [];
    if (!data.messages) data.messages = [];

    return data;
  } catch (error) {
    console.error(`❌ [Redis][getRoom] Command failed for room ${roomId}:`, error.stack);
    return null;
  }
}

/**
 * Enhanced Room class (Stateless helper)
 */
class Room {
  constructor(mood, users, id = null) {
    this.id = id || uuidv4();
    this.mood = mood;
    this.users = users;
    this.messages = [];
    this.createdAt = Date.now();
    this.lastActivity = Date.now();
    this.expiresAt = null;
    this.isExpired = false;
    this.hasActiveCall = false;
    this.maxUsers = config.MAX_USERS_PER_ROOM;
  }

  async save() {
    await saveRoomToRedis(this);
  }

  async updateActivity() {
    this.lastActivity = Date.now();
    await redis.hset(`room:data:${this.id}`, 'lastActivity', this.lastActivity);
  }

  async addMessage(message) {
    this.messages.push({ ...message, timestamp: Date.now() });
    if (this.messages.length > 200) this.messages = this.messages.slice(-100);
    await saveRoomToRedis(this);
    await this.updateActivity();
  }

  hasUser(userId) {
    return this.users.some(u => u.userId === userId);
  }

  async addUser(userData) {
    const releaseLock = await acquireRoomMutex(this.id);
    try {
      // Re-fetch to ensure we have the absolute latest user list
      const latestRoom = await getRoomFromRedis(this.id);
      if (latestRoom) {
        this.users = latestRoom.users;
      }

      const existingUser = this.users.find(u => u.userId === userData.userId);
      if (existingUser) {
        console.log(`ℹ️ User ${userData.userId} already in room ${this.id}`);
        return true;
      }

      if (this.users.length >= this.maxUsers) {
        console.error(`❌ Room ${this.id} is full`);
        return false;
      }

      this.users.push(userData);
      await redis.set(`user:room:${userData.userId}`, this.id, 'EX', 3600);
      await this.save();
      console.log(`✅ User ${userData.userId} added to room ${this.id}`);
      return true;
    } finally {
      await releaseLock();
    }
  }

  hasSpace() {
    return this.users.length < this.maxUsers && !this.isExpired;
  }

  // Legacy/Compatibility Methods
  setActiveCall(status) {
    this.hasActiveCall = status;
    this.save();
  }

  startLifecycleTimers() {
    if (this.expiresAt) return; // Already started
    this.timerStartedAt = Date.now();
    this.expiresAt = Date.now() + (config.ROOM_DURATION_MINUTES * 60 * 1000);
    this.save(); // Sync to Redis
  }

  getTimeUntilExpiration() {
    if (!this.expiresAt) return 0;
    return Math.max(0, this.expiresAt - Date.now());
  }

  getMessages() {
    return this.messages || [];
  }
}

/**
 * Find an existing room with space for the given mood
 */
async function findRoomWithSpace(mood, excludeUserId = null) {
  try {
    const keys = await redis.keys('room:data:*');
    for (const key of keys) {
      const roomId = key.replace('room:data:', '');
      const room = await getRoom(roomId);
      if (!room) continue;

      // Must match mood, have space, and not be expired
     if (room.mood === mood && room.hasSpace() && !room.isExpired) {
  console.log(`🔍 [Matchmaking] Room ${room.id} users: ${JSON.stringify(room.users.map(u => u.userId))}`);
  if (excludeUserId && room.users.some(u => u.userId === excludeUserId)) continue;
        console.log(`🔍 [Matchmaking] Found room ${roomId} with space for mood ${mood} (${room.users.length}/${room.maxUsers || config.MAX_USERS_PER_ROOM})`);
        return room;
      }
    }
    return null;
  } catch (error) {
    console.error(`❌ [Matchmaking] Error finding room with space:`, error.message);
    return null;
  }
}

/**
 * Add user to matchmaking queue
 */
export async function addToQueue(userData) {
  const { mood, userId, username } = userData;

  // 0. DUPLICATE PREVENTION: Check if user is already in a room
  const existingRoomId = await redis.get(`user:room:${userId}`);
  if (existingRoomId) {
    const existingRoom = await getRoom(existingRoomId);
    if (existingRoom && Array.isArray(existingRoom.users) && existingRoom.users.some(u => u.userId === userId)) {
      console.log(`⚠️ [Matchmaking] User ${username} (${userId}) already in room ${existingRoomId}, returning existing room`);
      return existingRoom;
    } else {
      // Stale mapping, clean it up
      console.log(`🧹 [Matchmaking] Cleaning stale room mapping for ${userId} (room ${existingRoomId})`);
      await redis.del(`user:room:${userId}`);
    }
  }

  // 1. Initial capacity check
  const keys = await redis.keys('room:data:*');
  if (keys.length >= config.MAX_ROOMS) {
    return { error: 'Server at capacity' };
  }

  // 2. DEDUPLICATE QUEUE: Remove user from queue if already present
  const queueKey = `matchmaking:queue:${mood}`;
  const allInQueue = await redis.lrange(queueKey, 0, -1);
  for (const item of allInQueue) {
    try {
      const parsed = JSON.parse(item);
      if (parsed.userId === userId) {
        await redis.lrem(queueKey, 1, item);
        console.log(`🧹 [Matchmaking] Removed duplicate queue entry for ${username}`);
      }
    } catch (e) { /* ignore parse errors */ }
  }

const availableRoom = await findRoomWithSpace(mood, userId);
  if (availableRoom) {
    console.log(`🚪 [Matchmaking] Adding ${username} to existing room ${availableRoom.id}`);
    const added = await availableRoom.addUser({
      userId: userData.userId,
      username: userData.username,
      pfpUrl: userData.pfpUrl,
      firebaseUid: userData.firebaseUid,
      socketId: userData.socketId
    });
    if (!added) {
      console.error(`❌ [Matchmaking] Failed to add ${username} to room ${availableRoom.id}`);
      return null;
    }
    await redis.set(`user:room:${userId}`, availableRoom.id, 'EX', 3600);
    console.log(`✅ [Matchmaking] ${username} joined room ${availableRoom.id} (${availableRoom.users.length}/${availableRoom.maxUsers || config.MAX_USERS_PER_ROOM})`);
    return availableRoom;
  }

  // 4. Add to Redis List (queue)
  await redis.rpush(queueKey, JSON.stringify(userData));

  const queueLength = await redis.llen(queueKey);
  console.log(`🎮 [Cluster] User ${username} queued for ${mood} (${queueLength}/${config.MIN_USERS_FOR_ROOM})`);

  // 5. Matchmaking Logic — create new room if enough users
  if (queueLength >= config.MIN_USERS_FOR_ROOM) {
    const roomUsers = [];
    for (let i = 0; i < config.MIN_USERS_FOR_ROOM; i++) {
      const u = await redis.lpop(queueKey);
      if (u) roomUsers.push(JSON.parse(u));
    }

    if (roomUsers.length === config.MIN_USERS_FOR_ROOM) {
      return await createRoomInternal(mood, roomUsers);
    } else {
      // Put back if race condition
      for (const u of roomUsers) await redis.lpush(queueKey, JSON.stringify(u));
    }
  }

  return null;
}

/**
 * Create a new room
 */
async function createRoomInternal(mood, users) {
  const room = new Room(mood, users);
  await saveRoomToRedis(room);

  for (const user of users) {
    await redis.set(`user:room:${user.userId}`, room.id, 'EX', 3600);
  }

  // Authoritative TTL in Redis
  await redis.set(`room:expiry:${room.id}`, 'active', 'PX', ROOM_LIFETIME);

  console.log(`🎉 [Cluster] Room ${room.id} created for mood ${mood}`);
  return room;
}

export async function getRoom(roomId) {
  const data = await getRoomFromRedis(roomId);
  if (!data) return null;
  const room = new Room(data.mood, data.users, data.id);
  Object.assign(room, data);
  return room;
}

export async function getRoomByUser(userId) {
  const roomId = await redis.get(`user:room:${userId}`);
  if (!roomId) return null;
  return await getRoom(roomId);
}

export async function getRoomIdByUser(userId) {
  return await redis.get(`user:room:${userId}`);
}

export async function leaveRoom(userId) {
  console.log(`🏠 [MMR] leaveRoom request for ${userId}`);
  const roomId = await getRoomIdByUser(userId);
  if (!roomId) {
    console.log(`🏠 [MMR] No room mapping found for ${userId}`);
    return { roomId: null, remainingUsers: 0 };
  }

  const releaseLock = await acquireRoomMutex(roomId);
  try {
    // CRITICAL: Always delete the user-to-room mapping immediately
    console.log(`🏠 [MMR] Deleting mapping user:room:${userId} (Room: ${roomId})`);
    await redis.del(`user:room:${userId}`);

    const roomData = await getRoomFromRedis(roomId);
    if (roomData) {
      const initialCount = roomData.users.length;
      const updatedUsers = roomData.users.filter(u => u.userId !== userId);
      const remainingUsers = updatedUsers.length;

      console.log(`🏠 [MMR] User filter for ${roomId}: ${initialCount} -> ${remainingUsers} users`);

      // AUTO-DESTROY LOGIC: Only destroy if NO users remain
      // This allows 1-person rooms to survive refreshes/reconnections
      if (remainingUsers === 0) {
        console.log(`💥 [MMR] Room ${roomId} is empty. Auto-destroying...`);
        await destroyRoomInternal(roomId);
        return { roomId, remainingUsers: 0, destroyed: true, users: [] };
      }

      // Save updated room state
      roomData.users = updatedUsers;
      await saveRoomToRedis(roomData);

      console.log(`🏠 [Matchmaking] User ${userId} removed from room ${roomId}. Remaining: ${remainingUsers}`);
      return { roomId, remainingUsers, destroyed: false, users: updatedUsers };
    }

    console.log(`🏠 [Matchmaking] Legacy marker for ${userId} cleared (room ${roomId} was already gone)`);
    return { roomId, remainingUsers: 0, destroyed: true, users: [] };
  } finally {
    await releaseLock();
  }
}

/**
 * Internal destroyRoom (no locking inside, used by functions that already have a lock)
 */
async function destroyRoomInternal(roomId) {
  const room = await getRoomFromRedis(roomId);
  if (room) {
    for (const user of room.users) {
      await redis.del(`user:room:${user.userId}`);
    }
  }
  await redis.del(`room:data:${roomId}`);
  await redis.del(`room:expiry:${roomId}`);
  console.log(`💥 [Cluster] Room ${roomId} destroyed`);
}

export async function destroyRoom(roomId) {
  const releaseLock = await acquireRoomMutex(roomId);
  try {
    await destroyRoomInternal(roomId);
  } finally {
    await releaseLock();
  }
}

export async function cancelMatchmaking(userId, mood) {
  const queueKey = `matchmaking:queue:${mood}`;
  const allInQueue = await redis.lrange(queueKey, 0, -1);
  for (const item of allInQueue) {
    const userData = JSON.parse(item);
    if (userData.userId === userId) {
      await redis.lrem(queueKey, 1, item);
      console.log(`🎮 [Cluster] Matchmaking cancelled for ${userId}`);
      return true;
    }
  }
  return false;
}

export async function getQueueStatus(mood) {
  return await redis.llen(`matchmaking:queue:${mood}`);
}

export async function getActiveRooms() {
  const keys = await redis.keys('room:data:*');
  const rooms = [];
  for (const key of keys) {
    const roomId = key.replace('room:data:', '');
    const room = await getRoom(roomId);
    if (room) rooms.push(room);
  }
  return rooms;
}

export async function getRoomStats() {
  const keys = await redis.keys('room:data:*');
  return {
    totalRooms: keys.length,
    // Detailed stats could be pulled via HGETALL on all keys but that's expensive
  };
}

export default {
  init,
  addToQueue,
  getRoom,
  getRoomByUser,
  getRoomIdByUser,
  leaveRoom,
  destroyRoom,
  cancelMatchmaking,
  getQueueStatus,
  getActiveRooms,
  getRoomStats
};
