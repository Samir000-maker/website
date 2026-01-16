import { v4 as uuidv4 } from 'uuid';
import config from './config.js';

/**
 * Enhanced Matchmaking System with Room Lifecycle Management
 * 
 * Features:
 * - 50-second room expiration (configurable)
 * - Automatic cleanup
 * - Ephemeral message storage
 * - Grace periods for reconnection
 * - Memory-efficient state management
 */

// In-memory queues: { mood: [user1, user2, ...] }
const matchmakingQueues = new Map();

// Active rooms: { roomId: RoomData }
const activeRooms = new Map();

// User to room mapping for quick lookup
const userRoomMap = new Map();

// Room activity tracking
const roomActivity = new Map(); // roomId -> lastActivityTimestamp

// Cleanup interval
const CLEANUP_INTERVAL = 60 * 1000; // 1 minute
const ROOM_LIFETIME = 50 * 1000; // 50 SECONDS
const ROOM_WARNING_TIME = 30 * 1000; // 30 seconds (20 second warning before expiry)

/**
 * Enhanced Room class with lifecycle management
 */
class Room {
  constructor(mood, users) {
    this.id = uuidv4();
    this.mood = mood;
    this.users = users;
    this.messages = []; // Ephemeral message storage
    this.createdAt = Date.now();
    this.lastActivity = Date.now();
    this.expiresAt = null; // Will be set when timer starts
    this.cleanupTimer = null;
    this.warningTimer = null;
    this.isExpired = false;
    this.io = null; // Socket.IO instance (set externally)
    this.hasActiveCall = false; // Track if room has active call
    this.userJoinedRoom = false; // Track if users actually joined
    this.timerStartedAt = null; // Track when timer actually started

    console.log(`🏠 Room ${this.id} created at ${new Date(this.createdAt).toISOString()}`);
    console.log(`   Users: ${users.map(u => u.username).join(', ')}`);
    console.log(`   ⏰ Timer will start when first user joins (NOT started yet)`);
  }

  setupLifecycleTimers() {
    if (this.cleanupTimer || this.warningTimer) {
      console.warn(`⚠️ Timers already set for room ${this.id}, skipping`);
      return;
    }

    // Set expiration time NOW
    this.timerStartedAt = Date.now();
    this.expiresAt = this.timerStartedAt + ROOM_LIFETIME;

    const timeUntilWarning = ROOM_WARNING_TIME;
    const timeUntilExpiry = ROOM_LIFETIME;
    
    console.log(`⏱️ Room ${this.id} timers STARTED at ${new Date(this.timerStartedAt).toISOString()}`);
    console.log(`   ⏰ Will expire at: ${new Date(this.expiresAt).toISOString()}`);
    console.log(`   ⚠️ Warning in: ${timeUntilWarning / 1000}s (at ${new Date(this.timerStartedAt + timeUntilWarning).toISOString()})`);
    console.log(`   💥 Expiry in: ${timeUntilExpiry / 1000}s (at ${new Date(this.expiresAt).toISOString()})`);
    
    // Warning timer (fires at ROOM_WARNING_TIME)
    this.warningTimer = setTimeout(() => {
      this.emitWarning();
    }, timeUntilWarning);

    // Cleanup timer (fires at ROOM_LIFETIME)
    this.cleanupTimer = setTimeout(() => {
      this.expire();
    }, timeUntilExpiry);
  }

  /**
   * Update activity timestamp (extends room life)
   */
  updateActivity() {
    this.lastActivity = Date.now();
    roomActivity.set(this.id, this.lastActivity);
  }

  /**
   * Mark room as having active call
   */
  setActiveCall(isActive) {
    this.hasActiveCall = isActive;
    console.log(`📞 Room ${this.id} active call status: ${isActive}`);
  }

  startLifecycleTimers() {
    if (this.cleanupTimer || this.warningTimer) {
      console.log(`⚠️ Timers already running for room ${this.id}, skipping`);
      return;
    }

    this.setupLifecycleTimers();
  }

  emitWarning() {
    if (this.io && !this.isExpired) {
      const remainingTime = (ROOM_LIFETIME - ROOM_WARNING_TIME) / 1000;
      console.log(`⚠️ Room ${this.id} WARNING: ${remainingTime} seconds remaining`);
      console.log(`   Current time: ${new Date().toISOString()}`);
      console.log(`   Expires at: ${new Date(this.expiresAt).toISOString()}`);

      this.io.to(this.id).emit('room_expiring_soon', {
        roomId: this.id,
        expiresIn: ROOM_LIFETIME - ROOM_WARNING_TIME,
        expiresAt: this.expiresAt
      });
    }
  }

  /**
   * Expire and destroy room
   */
  expire() {
    if (this.isExpired) {
      console.log(`⏭️ Room ${this.id} already expired, skipping`);
      return;
    }

    this.isExpired = true;

    console.log(`💥 Room ${this.id} EXPIRED`);
    console.log(`   Created: ${new Date(this.createdAt).toISOString()}`);
    console.log(`   Timer started: ${new Date(this.timerStartedAt).toISOString()}`);
    console.log(`   Expired: ${new Date().toISOString()}`);
    console.log(`   Total lifetime: ${(Date.now() - this.timerStartedAt) / 1000}s`);

    // Notify all users
    if (this.io) {
      this.io.to(this.id).emit('room_expired', {
        roomId: this.id,
        message: `This conversation has ended after ${ROOM_LIFETIME / 1000} seconds`
      });
    }

    // Destroy room
    this.destroy();
  }

  /**
   * Add message to room
   */
  addMessage(message) {
    this.messages.push({
      ...message,
      timestamp: Date.now()
    });

    // Limit messages to prevent memory issues
    if (this.messages.length > 200) {
      this.messages = this.messages.slice(-100);
    }

    this.updateActivity();
  }

  /**
   * Get messages
   */
  getMessages() {
    return [...this.messages];
  }

  /**
   * Remove user from room
   */
  removeUser(userId) {
    this.users = this.users.filter(u => u.userId !== userId);
    this.updateActivity();
    return this.users.length;
  }

  /**
   * Check if user is in room
   */
  hasUser(userId) {
    return this.users.some(u => u.userId === userId);
  }

  /**
   * Get time until expiration
   */
  getTimeUntilExpiration() {
    if (!this.expiresAt) {
      console.warn(`⚠️ Room ${this.id} has no expiration time set yet`);
      return ROOM_LIFETIME; // Return full lifetime if timer not started
    }
    return Math.max(0, this.expiresAt - Date.now());
  }

  /**
   * Destroy room and clean up
   */
  destroy() {
    console.log(`🗑️ Room ${this.id} destroying...`);
    
    // Clear timers
    if (this.cleanupTimer) {
      clearTimeout(this.cleanupTimer);
      this.cleanupTimer = null;
    }

    if (this.warningTimer) {
      clearTimeout(this.warningTimer);
      this.warningTimer = null;
    }

    // Clear messages (ephemeral)
    this.messages = [];

    // Remove from activity tracking
    roomActivity.delete(this.id);
    
    console.log(`✅ Room ${this.id} destroyed`);
  }
}

/**
 * Add user to matchmaking queue
 */
export function addToQueue(userData) {
  const { mood, userId, username } = userData;

  if (!mood || !userId || !username) {
    console.error('Invalid user data provided to addToQueue:', userData);
    return null;
  }

  // Remove user from any existing queue
  removeFromAllQueues(userId);

  // Initialize queue for this mood if it doesn't exist
  if (!matchmakingQueues.has(mood)) {
    matchmakingQueues.set(mood, []);
  }

  const queue = matchmakingQueues.get(mood);
  queue.push(userData);

  console.log(`🎮 User ${username} queued for ${mood} (${queue.length}/${config.MAX_USERS_PER_ROOM})`);

  // Check if we have enough users to create a room
  if (queue.length >= config.MAX_USERS_PER_ROOM) {
    const roomUsers = queue.splice(0, config.MAX_USERS_PER_ROOM);
    const room = createRoom(mood, roomUsers);
    return room;
  }

  return null;
}

/**
 * Create a new room
 */
function createRoom(mood, users) {
  const room = new Room(mood, users);

  // Store room
  activeRooms.set(room.id, room);

  // Map users to room
  users.forEach(user => {
    userRoomMap.set(user.userId, room.id);
  });

  // Track activity
  roomActivity.set(room.id, room.lastActivity);

  const usernames = users.map(u => u.username).join(', ');
  console.log(`🎉 Room ${room.id} created with users: ${usernames}`);
  console.log(`   ⏰ Timer will start when first user joins room`);

  return room;
}

/**
 * Set Socket.IO instance for room notifications
 */
export function setSocketIO(io) {
  activeRooms.forEach(room => {
    room.io = io;
  });

  console.log('✅ Socket.IO instance set for all active rooms');
}

/**
 * Get room by ID
 */
export function getRoom(roomId) {
  const room = activeRooms.get(roomId);

  if (room && room.isExpired) {
    return null;
  }

  return room || null;
}

/**
 * Get room by user ID
 */
export function getRoomByUser(userId) {
  const roomId = userRoomMap.get(userId);
  if (!roomId) return null;

  const room = activeRooms.get(roomId);

  if (room && room.isExpired) {
    return null;
  }

  return room || null;
}

/**
 * Get room ID by user ID
 */
export function getRoomIdByUser(userId) {
  return userRoomMap.get(userId) || null;
}

/**
 * Remove user from room
 */
export function leaveRoom(userId, hasActiveCall = false) {
  const roomId = userRoomMap.get(userId);
  if (!roomId) {
    return { roomId: null, remainingUsers: 0, destroyed: false };
  }

  const room = activeRooms.get(roomId);
  if (!room) {
    userRoomMap.delete(userId);
    return { roomId: null, remainingUsers: 0, destroyed: false };
  }

  // Remove user from room
  const remainingUsers = room.removeUser(userId);
  userRoomMap.delete(userId);

  console.log(`👋 User ${userId} left room ${roomId}, ${remainingUsers} remaining`);

  // Do NOT destroy room if there's an active call
  if (hasActiveCall) {
    console.log(`🛡️ Room ${roomId} preserved due to active call`);
    return { roomId, remainingUsers, destroyed: false };
  }

  // Destroy room if only 1 or 0 users remain (and no active call)
  if (remainingUsers <= 1) {
    console.log(`🗑️ Room ${roomId} has ${remainingUsers} user(s), destroying...`);
    destroyRoom(roomId, 'Too few users');
    return { roomId, remainingUsers, destroyed: true };
  }

  return { roomId, remainingUsers, destroyed: false };
}

/**
 * Destroy a room immediately
 */
export function destroyRoom(roomId, reason = 'Unknown') {
  const room = activeRooms.get(roomId);
  if (!room) {
    console.log(`⚠️ Room ${roomId} not found, already destroyed`);
    return;
  }

  console.log(`🗑️ Destroying room ${roomId}, reason: ${reason}`);

  // Remove all users from room map
  room.users.forEach(user => {
    userRoomMap.delete(user.userId);
  });

  // Destroy room (clears timers, messages, etc.)
  room.destroy();

  // Remove from active rooms
  activeRooms.delete(roomId);
}

/**
 * Remove user from all matchmaking queues
 */
function removeFromAllQueues(userId) {
  for (const [mood, queue] of matchmakingQueues.entries()) {
    const index = queue.findIndex(u => u.userId === userId);
    if (index !== -1) {
      const removed = queue.splice(index, 1)[0];
      console.log(`❌ Removed ${removed.username} from ${mood} queue`);
    }
  }
}

/**
 * Cancel user's matchmaking
 */
export function cancelMatchmaking(userId) {
  removeFromAllQueues(userId);
}

/**
 * Get queue status
 */
export function getQueueStatus(mood) {
  const queue = matchmakingQueues.get(mood);
  return queue ? queue.length : 0;
}

/**
 * Get all active rooms (for monitoring)
 */
export function getActiveRooms() {
  return Array.from(activeRooms.values())
    .filter(room => !room.isExpired)
    .map(room => ({
      id: room.id,
      mood: room.mood,
      userCount: room.users.length,
      messageCount: room.messages.length,
      createdAt: room.createdAt,
      timerStartedAt: room.timerStartedAt,
      expiresAt: room.expiresAt,
      timeRemaining: room.getTimeUntilExpiration(),
      isExpired: room.isExpired
    }));
}

/**
 * Cleanup expired rooms (safety mechanism)
 */
export function cleanupExpiredRooms() {
  const now = Date.now();
  let cleaned = 0;

  for (const [roomId, room] of activeRooms.entries()) {
    // Check if room expired
    if (room.expiresAt && room.expiresAt < now || room.isExpired) {
      console.log(`🧹 Cleanup: Room ${roomId} expired`);
      destroyRoom(roomId, 'Expired (cleanup)');
      cleaned++;
    }
  }

  if (cleaned > 0) {
    console.log(`🧹 Cleaned up ${cleaned} expired room(s)`);
  }
}

/**
 * Get room statistics
 */
export function getRoomStats() {
  const rooms = Array.from(activeRooms.values()).filter(r => !r.isExpired);

  return {
    totalRooms: rooms.length,
    totalUsers: rooms.reduce((sum, room) => sum + room.users.length, 0),
    totalMessages: rooms.reduce((sum, room) => sum + room.messages.length, 0),
    averageRoomAge: rooms.length > 0 
      ? rooms.reduce((sum, room) => sum + (Date.now() - room.createdAt), 0) / rooms.length 
      : 0,
    oldestRoom: rooms.length > 0 
      ? Math.min(...rooms.map(r => r.createdAt)) 
      : null
  };
}

// Run cleanup every minute
setInterval(cleanupExpiredRooms, CLEANUP_INTERVAL);

// Log statistics every 5 minutes
setInterval(() => {
  const stats = getRoomStats();
  console.log('📊 Room Statistics:', stats);
}, 5 * 60 * 1000);

export default {
  addToQueue,
  setSocketIO,
  getRoom,
  getRoomByUser,
  getRoomIdByUser,
  leaveRoom,
  destroyRoom,
  cancelMatchmaking,
  getQueueStatus,
  getActiveRooms,
  cleanupExpiredRooms,
  getRoomStats
};
