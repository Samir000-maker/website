import { v4 as uuidv4 } from 'uuid';
import config from './config.js';

let redis = null;
let io = null;

const ROOM_LIFETIME = 600000; // 10 minutes
const ROOM_WARNING_TIME = 60000; // 1 minute

/**
 * Initialize matchmaking with Redis client and Socket.IO
 */
export function init(redisClient, ioInstance) {
  redis = redisClient;
  io = ioInstance;
  console.log('📡 [Matchmaking] Initialized with Redis and Socket.IO');
}

async function saveRoomToRedis(roomData) {
  const data = { ...roomData };
  const id = data.id;
  console.log(`💾 [Redis] Saving room ${id}...`);
  if (data.users && typeof data.users !== 'string') data.users = JSON.stringify(data.users);
  if (data.messages && typeof data.messages !== 'string') data.messages = JSON.stringify(data.messages);

  try {
    const result = await redis.hset(`room:data:${id}`, data);
    console.log(`💾 [Redis] Room ${id} saved. Result:`, result);
  } catch (error) {
    console.error(`❌ [Redis] Save failure for room ${id}:`, error.stack);
    throw error;
  }
}

async function getRoomFromRedis(roomId) {
  try {
    const data = await redis.hgetall(`room:data:${roomId}`);
    if (!data || !Object.keys(data).length) return null;
    if (data.users) data.users = JSON.parse(data.users);
    if (data.messages) data.messages = JSON.parse(data.messages);
    if (data.createdAt) data.createdAt = parseInt(data.createdAt);
    if (data.lastActivity) data.lastActivity = parseInt(data.lastActivity);
    if (data.expiresAt) data.expiresAt = parseInt(data.expiresAt);
    return data;
  } catch (error) {
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

  hasSpace() {
    return this.users.length < this.maxUsers && !this.isExpired;
  }
}

/**
 * Add user to matchmaking queue
 */
export async function addToQueue(userData) {
  const { mood, userId, username } = userData;

  // 1. Initial capacity check
  const keys = await redis.keys('room:data:*');
  if (keys.length >= config.MAX_ROOMS) {
    return { error: 'Server at capacity' };
  }

  // 2. Add to Redis List
  const queueKey = `matchmaking:queue:${mood}`;
  await redis.rpush(queueKey, JSON.stringify(userData));

  const queueLength = await redis.llen(queueKey);
  console.log(`🎮 [Cluster] User ${username} queued for ${mood} (${queueLength}/${config.MIN_USERS_FOR_ROOM})`);

  // 3. Matchmaking Logic
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

  // CRITICAL: Always delete the user-to-room mapping even if room data is gone
  console.log(`🏠 [MMR] Deleting mapping user:room:${userId} (Room: ${roomId})`);
  const delResult = await redis.del(`user:room:${userId}`);
  console.log(`🏠 [MMR] Mapping delete result:`, delResult);

  const room = await getRoom(roomId);
  if (room) {
    const initialCount = room.users.length;
    room.users = room.users.filter(u => u.userId !== userId);
    const finalCount = room.users.length;

    console.log(`🏠 [MMR] User filter: ${initialCount} -> ${finalCount} users`);

    await saveRoomToRedis(room);
    console.log(`🏠 [Matchmaking] User ${userId} removed from room ${roomId}. Remaining: ${room.users.length}`);
    return { roomId, remainingUsers: room.users.length };
  }

  console.log(`🏠 [Matchmaking] Legacy marker for ${userId} cleared (room ${roomId} was already gone)`);
  return { roomId, remainingUsers: 0 };
}

export async function destroyRoom(roomId) {
  const room = await getRoom(roomId);
  if (room) {
    for (const user of room.users) {
      await redis.del(`user:room:${user.userId}`);
    }
  }
  await redis.del(`room:data:${roomId}`);
  await redis.del(`room:expiry:${roomId}`);
  console.log(`💥 [Cluster] Room ${roomId} destroyed`);
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
