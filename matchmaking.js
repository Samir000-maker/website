import { v4 as uuidv4 } from 'uuid';
import config from './config.js';

let redis = null;
let io = null;
let redlock = null;

/**
 * Initialize matchmaking with Redis client, Socket.IO, and Redlock
 */
export function init(redisClient, ioInstance, redlockInstance) {
  redis = redisClient;
  io = ioInstance;
  redlock = redlockInstance;
  console.log('📡 [Matchmaking] Initialized with Redis, Socket.IO, and Redlock');
}

async function createRoomInternalWithSize(mood, users, maxUsers) {
  const room = new Room(mood, users, null, maxUsers);
  room.maxUsers = parseInt(maxUsers, 10) || room.maxUsers;
  await saveRoomToRedis(room);

  for (const user of users) {
    await redis.set(`user:room:${user.userId}`, room.id);
  }

  console.log(`🎉 [Cluster] Room ${room.id} created for mood ${mood} (maxUsers=${room.maxUsers})`);
  return room;
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

    if (data.userJoinedRoom !== undefined) {
      data.userJoinedRoom = (data.userJoinedRoom === 'true' || data.userJoinedRoom === true);
    } else {
      data.userJoinedRoom = false;
    }

    // Ensure arrays exist
    if (!data.users) data.users = [];
    if (!data.messages) data.messages = [];

    return data;
  } catch (error) {
    console.error(`❌ [Redis][getRoom] Command failed for room ${roomId}:`, error.stack);
    return null;
  }
}

async function scanRoomDataKeys() {
  const keys = [];
  let cursor = '0';

  do {
    const [nextCursor, batch] = await redis.scan(cursor, 'MATCH', 'room:data:*', 'COUNT', 100);
    cursor = nextCursor;
    if (Array.isArray(batch) && batch.length) {
      keys.push(...batch);
    }
  } while (cursor !== '0');

  return keys;
}

/**
 * Enhanced Room class (Stateless helper)
 */
class Room {
  constructor(mood, users, id = null, maxUsersOverride = null) {
    this.id = id || uuidv4();
    this.mood = mood;
    this.users = users;
    this.messages = [];
    this.createdAt = Date.now();
    this.lastActivity = Date.now();
    this.expiresAt = null;
    this.isExpired = false;
    this.hasActiveCall = false;
    const override = parseInt(maxUsersOverride, 10);
    this.maxUsers = Number.isFinite(override) && override > 0 ? override : config.MAX_USERS_PER_ROOM;
    this.userJoinedRoom = false;
    this.timerStartedAt = null;
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
      await redis.set(`user:room:${userData.userId}`, this.id);
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

  async startLifecycleTimers() {
    const releaseLock = await acquireRoomMutex(this.id);
    try {
      const latestRoom = await getRoomFromRedis(this.id);
      if (!latestRoom) return false;
      Object.assign(this, latestRoom);

      if (this.userJoinedRoom) return false;
      this.userJoinedRoom = true;
      if (!this.timerStartedAt) this.timerStartedAt = Date.now();
      this.expiresAt = null;
      await this.save();
      return true;
    } finally {
      await releaseLock();
    }
  }

  getTimeUntilExpiration() {
    return 0;
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
    const keys = await scanRoomDataKeys();
    const candidates = [];
    for (const key of keys) {
      const roomId = key.replace('room:data:', '');
      const room = await getRoom(roomId);
      if (!room) continue;

      // Must match mood, have space, not be expired, and have at least 1 user already.
      if (room.mood !== mood) continue;
      if (!room.hasSpace() || room.isExpired) continue;
      if (!Array.isArray(room.users) || room.users.length < 1) continue;
      if (excludeUserId && room.users.some(u => u.userId === excludeUserId)) continue;

      candidates.push(room);
    }

    if (!candidates.length) return null;

    // Choose the oldest room (stable rule). This ensures we don't create new rooms while any old one has space.
    candidates.sort((a, b) => {
      const aCreated = Number.isFinite(a.createdAt) ? a.createdAt : 0;
      const bCreated = Number.isFinite(b.createdAt) ? b.createdAt : 0;
      if (aCreated !== bCreated) return aCreated - bCreated;
      const aUsers = Array.isArray(a.users) ? a.users.length : 0;
      const bUsers = Array.isArray(b.users) ? b.users.length : 0;
      return bUsers - aUsers;
    });

    const chosen = candidates[0];
    console.log(`🔍 [Matchmaking] Found room ${chosen.id} with space for mood ${mood} (${chosen.users.length}/${chosen.maxUsers || config.MAX_USERS_PER_ROOM})`);
    return chosen;
  } catch (error) {
    console.error('❌ [Matchmaking] Error finding room with space:', error);
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
  const keys = await scanRoomDataKeys();
  if (keys.length >= config.MAX_ROOMS) {
    return { error: 'Server at capacity' };
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
    await redis.set(`user:room:${userId}`, availableRoom.id);
    console.log(`✅ [Matchmaking] ${username} joined room ${availableRoom.id} (${availableRoom.users.length}/${availableRoom.maxUsers || config.MAX_USERS_PER_ROOM})`);
    return availableRoom;
  }

  // No queue-based matchmaking: create a room immediately for a single user.
  return await createRoomInternal(mood, [userData]);
}

/**
 * Create a new room
 */
async function createRoomInternal(mood, users) {
  const room = new Room(mood, users);
  await saveRoomToRedis(room);

  for (const user of users) {
    await redis.set(`user:room:${user.userId}`, room.id);
  }

  console.log(`🎉 [Cluster] Room ${room.id} created for mood ${mood}`);
  return room;
}

export async function getRoom(roomId) {
  const data = await getRoomFromRedis(roomId);
  if (!data) return null;
  const room = new Room(data.mood, data.users, data.id, data.maxUsers);
  Object.assign(room, data);
  return room;
}

export async function addToSocialQueue(userData, roomSize) {
  const mood = 'social_club';
  const maxUsers = parseInt(roomSize, 10) || 2;

  const existingRoomId = await redis.get(`user:room:${userData.userId}`);
  if (existingRoomId) {
    const existingRoom = await getRoom(existingRoomId);
    if (existingRoom && Array.isArray(existingRoom.users) && existingRoom.users.some(u => u.userId === userData.userId)) {
      return existingRoom;
    }
    await redis.del(`user:room:${userData.userId}`);
  }

  const queueKey = `matchmaking:queue:${mood}`;
  const allInQueue = await redis.lrange(queueKey, 0, -1);
  for (const item of allInQueue) {
    try {
      const parsed = JSON.parse(item);
      if (parsed.userId === userData.userId) {
        await redis.lrem(queueKey, 1, item);
      }
    } catch { }
  }

  const availableRoom = await findRoomWithSpace(mood, userData.userId);
  if (availableRoom) {
    availableRoom.maxUsers = maxUsers;
    const added = await availableRoom.addUser({
      userId: userData.userId,
      username: userData.username,
      pfpUrl: userData.pfpUrl,
      firebaseUid: userData.firebaseUid,
      socketId: userData.socketId
    });
    if (!added) return null;
    await redis.set(`user:room:${userData.userId}`, availableRoom.id);
    return availableRoom;
  }

  const room = await createRoomInternalWithSize(mood, [
    {
      userId: userData.userId,
      username: userData.username,
      pfpUrl: userData.pfpUrl,
      firebaseUid: userData.firebaseUid,
      socketId: userData.socketId
    }
  ], maxUsers);

  await redis.set(`user:room:${userData.userId}`, room.id);
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
    return { success: true, roomId: null, remainingUsers: 0 };
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

      if (remainingUsers < 1) {
        await destroyRoomInternal(roomId, 'empty_room');
        return { success: true, roomId, remainingUsers: 0, destroyed: true, users: [] };
      }

      roomData.users = updatedUsers;
      await saveRoomToRedis(roomData);
      console.log(`🏠 [MMR] User ${userId} removed from room ${roomId}. Remaining: ${remainingUsers}`);
      return { success: true, roomId, remainingUsers, destroyed: false, users: updatedUsers };
    }

    console.log(`🏠 [Matchmaking] Legacy marker for ${userId} cleared (room ${roomId} was already gone)`);
    return { success: true, roomId, remainingUsers: 0, destroyed: false, users: [] };
  } finally {
    await releaseLock();
  }
}

/**
 * Internal destroyRoom (no locking inside, used by functions that already have a lock)
 */
async function destroyRoomInternal(roomId, reason = 'manual') {
  const room = await getRoomFromRedis(roomId);
  if (room) {
    for (const user of room.users) {
      await redis.del(`user:room:${user.userId}`);
    }
  }
  await redis.del(`room:data:${roomId}`);
  console.log(`💥 [Cluster] Room ${roomId} destroyed (reason: ${reason})`);
}

export async function destroyRoom(roomId, reason = 'manual') {
  const releaseLock = await acquireRoomMutex(roomId);
  try {
    await destroyRoomInternal(roomId, reason);
  } finally {
    await releaseLock();
  }
}

export async function cancelMatchmaking(userId, mood) {
  const moods = mood ? [mood] : config.MOODS.map(m => m.id);
  let removed = false;

  for (const moodId of moods) {
    const queueKey = `matchmaking:queue:${moodId}`;
    const allInQueue = await redis.lrange(queueKey, 0, -1);
    for (const item of allInQueue) {
      try {
        const userData = JSON.parse(item);
        if (userData.userId === userId) {
          await redis.lrem(queueKey, 1, item);
          removed = true;
        }
      } catch { }
    }
  }

  if (removed) {
    console.log(`🎮 [Cluster] Matchmaking cancelled for ${userId}`);
  }
  return removed;
}

export async function getQueueStatus(mood) {
  return await redis.llen(`matchmaking:queue:${mood}`);
}

export async function getActiveRooms() {
  const keys = await scanRoomDataKeys();
  const rooms = [];
  for (const key of keys) {
    const roomId = key.replace('room:data:', '');
    const room = await getRoom(roomId);
    if (room) rooms.push(room);
  }
  return rooms;
}

export async function getRoomStats() {
  const keys = await scanRoomDataKeys();
  return {
    totalRooms: keys.length,
    // Detailed stats could be pulled via HGETALL on all keys but that's expensive
  };
}

export default {
  init,
  addToQueue,
  addToSocialQueue,
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
