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
const ROOM_LIFETIME = 10 * 60 * 1000; // 10 minutes
const ROOM_WARNING_TIME = 9 * 60 * 1000; // 9 minutes (1 minute warning)

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
    
    // Setup timers
    this.setupLifecycleTimers();
  }

  /**
   * Setup lifecycle timers
   */
  setupLifecycleTimers() {
    // Warning timer (1 minute before expiration)
    this.warningTimer = setTimeout(() => {
      this.emitWarning();
    }, ROOM_WARNING_TIME);

    // Cleanup timer (expiration)
    this.cleanupTimer = setTimeout(() => {
      this.expire();
    }, ROOM_LIFETIME);

    console.log(`⏱️ Room ${this.id} lifecycle timers set (expires in 10 min)`);
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
  const timeRemaining = Math.floor((this.expiresAt - Date.now()) / 1000);
  console.log(`📞 Room ${this.id} active call status: ${isActive}`);
  console.log(`   Time remaining until expiry: ${timeRemaining}s`);
}

  /**
   * CRITICAL: Extend room expiry (for active calls)
   */
extendExpiry(additionalMinutes) {
  // CRITICAL FIX: Don't extend if additionalMinutes is 0
  if (additionalMinutes <= 0) {
    console.warn(`⚠️ Attempted to extend room ${this.id} by ${additionalMinutes} minutes - ignoring`);
    return;
  }
  
  const extension = additionalMinutes * 60 * 1000;
  const oldExpiry = this.expiresAt;
  this.expiresAt = Date.now() + extension;
  
  console.log(`⏰ Extended room ${this.id} expiry by ${additionalMinutes} minutes`);
  console.log(`   Old expiry: ${new Date(oldExpiry).toLocaleTimeString()}`);
  console.log(`   New expiry: ${new Date(this.expiresAt).toLocaleTimeString()}`);
  
  // Clear existing timers
  if (this.cleanupTimer) {
    clearTimeout(this.cleanupTimer);
  }
  if (this.warningTimer) {
    clearTimeout(this.warningTimer);
  }
  
  // Setup new timers based on NEW expiry time
  const timeUntilExpiry = this.expiresAt - Date.now();
  const timeUntilWarning = Math.max(0, timeUntilExpiry - 60000); // 1 min before expiry
  
  if (timeUntilWarning > 0) {
    this.warningTimer = setTimeout(() => {
      this.emitWarning();
    }, timeUntilWarning);
  }
  
  if (timeUntilExpiry > 0) {
    this.cleanupTimer = setTimeout(() => {
      this.expire();
    }, timeUntilExpiry);
  }
  
  console.log(`⏱️ Room ${this.id} lifecycle timers set (expires in ${Math.round(timeUntilExpiry / 1000)}s)`);
}


  /**
   * Emit warning to room users
   */
  emitWarning() {
    // CRITICAL: Don't warn if call is active
    if (this.hasActiveCall) {
      console.log(`⏭️ Skipping warning for room ${this.id} - active call in progress`);
      return;
    }
    
    if (this.io && !this.isExpired) {
      console.log(`⚠️ Room ${this.id} expiring in 1 minute`);
      
      this.io.to(this.id).emit('room_expiring_soon', {
        roomId: this.id,
        expiresIn: 60000 // 1 minute
      });
    }
  }

  /**
   * Expire and destroy room
   */
expire() {
  if (this.isExpired) return;
  
  // CRITICAL: Don't expire if call is active - just keep the same timer running
  if (this.hasActiveCall) {
    console.log(`⏭️ Room ${this.id} has active call - letting timer continue normally`);
    console.log(`   Room will still expire at: ${new Date(this.expiresAt).toLocaleTimeString()}`);
    return;
  }
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
    
    console.log(`🗑️ Room ${this.id} destroyed and cleaned up`);
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
  
  console.log(`👤 User ${username} joined ${mood} queue (${queue.length}/${config.MAX_USERS_PER_ROOM})`);

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
  console.log(`🎉 Room ${room.id} created with ${users.length} users (${mood}): ${usernames}`);
  console.log(`⏱️ Room will expire at ${new Date(room.expiresAt).toLocaleTimeString()}`);
  
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
  
  console.log(`👋 User ${userId} left room ${roomId}. Remaining: ${remainingUsers}`);

  // CRITICAL FIX: Do NOT destroy room if there's an active call
  if (hasActiveCall) {
    console.log(`🛡️ Room ${roomId} has active call - PRESERVING room despite ${remainingUsers} users`);
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

  console.log(`💥 Destroying room ${roomId}. Reason: ${reason}`);

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
      console.log(`Removed user ${removed.username} (${userId}) from ${mood} queue`);
    }
  }
}

/**
 * Cancel user's matchmaking
 */
export function cancelMatchmaking(userId) {
  removeFromAllQueues(userId);
  console.log(`🚫 Matchmaking cancelled for user: ${userId}`);
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
    console.log(`🧹 Cleaned up ${cleaned} expired rooms`);
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
