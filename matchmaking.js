import { v4 as uuidv4 } from 'uuid';
import config from './config.js';

/**
 * Enhanced Matchmaking System with Room Lifecycle Management
 * 
 * Features:
 * - 10-minute room expiration
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
const ROOM_LIFETIME = 50 * 1000; // 7 SECONDS FOR TESTING
const ROOM_WARNING_TIME = 18 * 1000; // 5 seconds (2 second warning)g)

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
  this.expiresAt = Date.now() + ROOM_LIFETIME;
  this.cleanupTimer = null;
  this.warningTimer = null;
  this.isExpired = false;
  this.io = null; // Socket.IO instance (set externally)
  this.hasActiveCall = false; // CRITICAL: Track if room has active call
  this.userJoinedRoom = false; // NEW: Track if users actually joined

  // DON'T setup timers here - wait until users confirm join
  // this.setupLifecycleTimers();
}

setupLifecycleTimers() {
  // Calculate warning time (happens BEFORE expiry)
  const timeUntilWarning = ROOM_WARNING_TIME;
  const timeUntilExpiry = ROOM_LIFETIME;
  
  console.log(`⏱️ Room ${this.id} timers: warning in ${timeUntilWarning / 1000}s, expiry in ${timeUntilExpiry / 1000}s`);
  
  // Warning timer (fires at ROOM_WARNING_TIME)
  this.warningTimer = setTimeout(() => {
    this.emitWarning();
  }, timeUntilWarning);

  // Cleanup timer (fires at ROOM_LIFETIME)
  this.cleanupTimer = setTimeout(() => {
    this.expire();
  }, timeUntilExpiry);

  console.log(`⏱️ Room ${this.id} lifecycle timers set (expires in ${ROOM_LIFETIME / 1000}s)`);
}

  /**
   * Update activity timestamp (extends room life)
   */
  updateActivity() {
    this.lastActivity = Date.now();
    roomActivity.set(this.id, this.lastActivity);

    // NOTE: We do NOT extend expiration time
    // Room always expires 10 minutes after creation
  }

  /**
   * CRITICAL: Mark room as having active call
   */
  setActiveCall(isActive) {
    this.hasActiveCall = isActive;
  }

 extendExpiry(additionalMinutes) {
  // DISABLED: Calls now use same timer as chat
}

startLifecycleTimers() {
  if (this.cleanupTimer || this.warningTimer) {
    console.log(`⚠️ Timers already set for room ${this.id}, skipping`);
    return;
  }

  // Reset expiration time to NOW + lifetime
  this.expiresAt = Date.now() + ROOM_LIFETIME;
  
  this.setupLifecycleTimers();
}

emitWarning() {
  if (this.io && !this.isExpired) {
    const warningTime = (ROOM_LIFETIME - ROOM_WARNING_TIME) / 1000;
    console.log(`⚠️ Room ${this.id} will expire in ${warningTime} seconds`);

    this.io.to(this.id).emit('room_expiring_soon', {
      roomId: this.id,
      expiresIn: ROOM_LIFETIME - ROOM_WARNING_TIME // Time remaining after warning
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

  console.log(`⏱️ Room ${this.id} expired after ${ROOM_LIFETIME / 1000} seconds`);

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

    // Update activity
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
    return Math.max(0, this.expiresAt - Date.now());
  }

  /**
   * Destroy room and clean up
   */
  destroy() {
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

  // Add user to queue
  queue.push(userData);


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
 * Note: Does NOT destroy room if there are active calls
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

  // CRITICAL FIX: Do NOT destroy room if there's an active call
  if (hasActiveCall) {
    return { roomId, remainingUsers, destroyed: false };
  }

  // Destroy room if only 1 or 0 users remain (and no active call)
  if (remainingUsers <= 1) {
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
  if (!room) return;

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
    if (room.expiresAt < now || room.isExpired) {
      destroyRoom(roomId, 'Expired (cleanup)');
      cleaned++;
    }
  }

  if (cleaned > 0) {
    
    
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
