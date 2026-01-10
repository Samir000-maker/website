import { v4 as uuidv4 } from 'uuid';
import config from './config.js';

/**
 * Matchmaking System
 * Manages queues of users waiting to be matched by mood
 * Creates rooms when 4 users with the same mood are available
 */

// In-memory queues: { mood: [user1, user2, ...] }
const matchmakingQueues = new Map();

// Active rooms: { roomId: RoomData }
const activeRooms = new Map();

// User to room mapping for quick lookup
const userRoomMap = new Map();

/**
 * Room data structure
 */
class Room {
  constructor(mood, users) {
    this.id = uuidv4();
    this.mood = mood;
    this.users = users; // Array of user objects
    this.messages = []; // Ephemeral in-RAM message storage
    this.createdAt = Date.now();
    this.expiresAt = Date.now() + (config.ROOM_DURATION_MINUTES * 60 * 1000);
    this.timer = null;
  }

  addMessage(message) {
    this.messages.push({
      ...message,
      timestamp: Date.now()
    });
  }

  getMessages() {
    return this.messages;
  }

  removeUser(userId) {
    this.users = this.users.filter(u => u.userId !== userId);
    return this.users.length;
  }

  hasUser(userId) {
    return this.users.some(u => u.userId === userId);
  }

  destroy() {
    // Clear timer
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    
    // Clear messages (ephemeral)
    this.messages = [];
    
    console.log(`🗑️  Room ${this.id} destroyed`);
  }
}

/**
 * Add user to matchmaking queue
 * @param {Object} userData - User data {userId, username, pfpUrl, mood, socketId}
 * @returns {Object|null} Room data if match found, null if queued
 */
export function addToQueue(userData) {
  const { mood, userId, username } = userData;

  // Validate required fields
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
    // Get first 4 users from queue
    const roomUsers = queue.splice(0, config.MAX_USERS_PER_ROOM);
    
    // Create room
    const room = createRoom(mood, roomUsers);
    
    return room;
  }

  return null;
}

/**
 * Create a new room
 * @param {string} mood - Mood type
 * @param {Array} users - Array of user objects
 * @returns {Room} Created room
 */
function createRoom(mood, users) {
  const room = new Room(mood, users);
  
  // Store room
  activeRooms.set(room.id, room);
  
  // Map users to room
  users.forEach(user => {
    userRoomMap.set(user.userId, room.id);
  });
  
  // Set destruction timer
  room.timer = setTimeout(() => {
    destroyRoom(room.id, 'Timer expired');
  }, config.ROOM_DURATION_MINUTES * 60 * 1000);
  
  const usernames = users.map(u => u.username).join(', ');
  console.log(`🎉 Room ${room.id} created with ${users.length} users (${mood}): ${usernames}`);
  
  return room;
}

/**
 * Get room by ID
 * @param {string} roomId - Room ID
 * @returns {Room|null} Room or null if not found
 */
export function getRoom(roomId) {
  return activeRooms.get(roomId) || null;
}

/**
 * Get room by user ID
 * @param {string} userId - User ID
 * @returns {Room|null} Room or null if not found
 */
export function getRoomByUser(userId) {
  const roomId = userRoomMap.get(userId);
  if (!roomId) return null;
  return activeRooms.get(roomId) || null;
}

/**
 * Get room ID by user ID (without removing user)
 * @param {string} userId - User ID
 * @returns {string|null} Room ID or null if not found
 */
export function getRoomIdByUser(userId) {
  return userRoomMap.get(userId) || null;
}

/**
 * Remove user from room
 * @param {string} userId - User ID
 * @returns {Object} {roomId, remainingUsers, destroyed}
 */
export function leaveRoom(userId) {
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

  // Destroy room if only 1 or 0 users remain
  if (remainingUsers <= 1) {
    destroyRoom(roomId, 'Too few users');
    return { roomId, remainingUsers, destroyed: true };
  }

  return { roomId, remainingUsers, destroyed: false };
}

/**
 * Destroy a room
 * @param {string} roomId - Room ID
 * @param {string} reason - Reason for destruction
 */
export function destroyRoom(roomId, reason = 'Unknown') {
  const room = activeRooms.get(roomId);
  if (!room) return;

  console.log(`💥 Destroying room ${roomId}. Reason: ${reason}`);

  // Remove all users from room map
  room.users.forEach(user => {
    userRoomMap.delete(user.userId);
  });

  // Destroy room (clears messages, timer, etc.)
  room.destroy();

  // Remove from active rooms
  activeRooms.delete(roomId);
}

/**
 * Remove user from all matchmaking queues
 * @param {string} userId - User ID
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
 * @param {string} userId - User ID
 */
export function cancelMatchmaking(userId) {
  removeFromAllQueues(userId);
  console.log(`🚫 Matchmaking cancelled for user: ${userId}`);
}

/**
 * Get queue status
 * @param {string} mood - Mood type
 * @returns {number} Number of users in queue
 */
export function getQueueStatus(mood) {
  const queue = matchmakingQueues.get(mood);
  return queue ? queue.length : 0;
}

/**
 * Get all active rooms (for monitoring)
 * @returns {Array} Array of room summaries
 */
export function getActiveRooms() {
  return Array.from(activeRooms.values()).map(room => ({
    id: room.id,
    mood: room.mood,
    userCount: room.users.length,
    messageCount: room.messages.length,
    createdAt: room.createdAt,
    expiresAt: room.expiresAt
  }));
}

/**
 * Cleanup expired rooms (safety mechanism)
 */
export function cleanupExpiredRooms() {
  const now = Date.now();
  
  for (const [roomId, room] of activeRooms.entries()) {
    if (room.expiresAt < now) {
      destroyRoom(roomId, 'Expired (cleanup)');
    }
  }
}

// Run cleanup every minute
setInterval(cleanupExpiredRooms, 60 * 1000);

export default {
  addToQueue,
  getRoom,
  getRoomByUser,
  getRoomIdByUser,
  leaveRoom,
  destroyRoom,
  cancelMatchmaking,
  getQueueStatus,
  getActiveRooms
};
