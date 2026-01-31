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

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const callMutexes = new Map();
const socketUserCleanup = new Map();
const roomJoinState = new Map();
const userToSocketId = new Map();
const socketUsers = new Map();
// Add at top with other Maps
const joinCallDebounce = new Map(); // userId -> timestamp

const roomCallInitLocks = new Map(); // roomId -> Promise
const userActiveRooms = new Map();
const userSockets = new Map();
const userOperationLocks = new Map();


// ============================================
// REAL-TIME MOOD USER COUNTERS (DEDUPLICATED)
// ============================================
const moodUserRegistry = new Map(); // mood -> Set<userId>
const moodUserCounts = new Map(); // mood -> count (derived)
const moodCountBroadcastDebounce = new Map(); // mood -> timeout
const userCurrentMood = new Map(); // userId -> mood (for cleanup)

// Initialize registries for all moods
config.MOODS.forEach(mood => {
  moodUserRegistry.set(mood.id, new Set());
  moodUserCounts.set(mood.id, 0);
});

/**
 * Add user to mood tracking (idempotent, deduplicated)
 */
function addUserToMood(userId, mood) {
  if (!moodUserRegistry.has(mood)) {
    console.error(`❌ Invalid mood: ${mood}`);
    return;
  }
  
  // Remove from old mood if exists
  const oldMood = userCurrentMood.get(userId);
  if (oldMood && oldMood !== mood) {
    removeUserFromMood(userId, oldMood);
  }
  
  const registry = moodUserRegistry.get(mood);
  const sizeBefore = registry.size;
  
  // Add to Set (automatically deduplicates)
  registry.add(userId);
  
  // Update derived count
  const newCount = registry.size;
  moodUserCounts.set(mood, newCount);
  
  // Track user's current mood
  userCurrentMood.set(userId, mood);
  
  // Only broadcast if count actually changed
  if (newCount !== sizeBefore) {
    debouncedBroadcastMoodCount(mood);
    console.log(`📊 Mood count: ${mood} = ${newCount} (added ${userId})`);
  } else {
    console.log(`ℹ️ User ${userId} already in ${mood} - no change`);
  }
}

async function acquireUserLock(userId) {
  while (userOperationLocks.has(userId)) {
    await userOperationLocks.get(userId);
  }
  
  let releaseLock;
  const lockPromise = new Promise(resolve => {
    releaseLock = resolve;
  });
  
  userOperationLocks.set(userId, lockPromise);
  
  return () => {
    userOperationLocks.delete(userId);
    releaseLock();
  };
}

function getUserActiveRoom(userId) {
  return userActiveRooms.get(userId) || null;
}

function setUserActiveRoom(userId, roomId, mood) {
  const roomData = {
    roomId,
    joinedAt: Date.now(),
    mood
  };
  
  userActiveRooms.set(userId, roomData);
  console.log(`🔐 [UID: ${userId}] Set active room: ${roomId} (mood: ${mood})`);
  
  return roomData;
}

function clearUserActiveRoom(userId) {
  const activeRoom = userActiveRooms.get(userId);
  if (activeRoom) {
    userActiveRooms.delete(userId);
    console.log(`🔓 [UID: ${userId}] Cleared active room: ${activeRoom.roomId}`);
  }
  return activeRoom;
}

function registerSocketForUser(userId, socketId) {
  if (!userSockets.has(userId)) {
    userSockets.set(userId, new Set());
  }
  
  userSockets.get(userId).add(socketId);
  console.log(`📱 [UID: ${userId}] Registered socket ${socketId} (total devices: ${userSockets.get(userId).size})`);
}

function unregisterSocketForUser(userId, socketId) {
  const sockets = userSockets.get(userId);
  if (sockets) {
    sockets.delete(socketId);
    console.log(`📱 [UID: ${userId}] Unregistered socket ${socketId} (remaining devices: ${sockets.size})`);
    
    if (sockets.size === 0) {
      userSockets.delete(userId);
      console.log(`📱 [UID: ${userId}] All devices disconnected`);
    }
  }
}


function getUserSocketIds(userId) {
  return Array.from(userSockets.get(userId) || []);
}


function emitToUserAllDevices(userId, event, data) {
  const socketIds = getUserSocketIds(userId);
  socketIds.forEach(socketId => {
    const socket = io.sockets.sockets.get(socketId);
    if (socket && socket.connected) {
      socket.emit(event, data);
    }
  });
  
  if (socketIds.length > 0) {
    console.log(`📢 [UID: ${userId}] Emitted '${event}' to ${socketIds.length} device(s)`);
  }
}



async function validateMoodSelection(userId) {
  const releaseLock = await acquireUserLock(userId);
  
  try {
    // Check if user already in active room
    const activeRoom = getUserActiveRoom(userId);
    
    if (activeRoom) {
      // Verify room still exists and is valid
      const room = matchmaking.getRoom(activeRoom.roomId);
      
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
  
  const room = matchmaking.getRoom(existingRoom.roomId);
  
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
    isRestored: true // Flag to indicate this is a restoration
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
 * Remove user from mood tracking (idempotent)
 */
function removeUserFromMood(userId, mood) {
  if (!moodUserRegistry.has(mood)) {
    console.error(`❌ Invalid mood: ${mood}`);
    return;
  }
  
  const registry = moodUserRegistry.get(mood);
  const sizeBefore = registry.size;
  
  // Remove from Set
  const wasPresent = registry.delete(userId);
  
  if (wasPresent) {
    // Update derived count
    const newCount = registry.size;
    moodUserCounts.set(mood, newCount);
    
    // Clear user's mood tracking
    if (userCurrentMood.get(userId) === mood) {
      userCurrentMood.delete(userId);
    }
    
    debouncedBroadcastMoodCount(mood);
    console.log(`📊 Mood count: ${mood} = ${newCount} (removed ${userId})`);
  } else {
    console.log(`ℹ️ User ${userId} was not in ${mood} - no change`);
  }
}

/**
 * Remove user from ALL moods (for disconnect/cleanup)
 */
function removeUserFromAllMoods(userId) {
  const currentMood = userCurrentMood.get(userId);
  if (currentMood) {
    removeUserFromMood(userId, currentMood);
  }
}

function debouncedBroadcastMoodCount(mood) {
  if (moodCountBroadcastDebounce.has(mood)) {
    clearTimeout(moodCountBroadcastDebounce.get(mood));
  }
  
  const timeout = setTimeout(() => {
    const count = moodUserCounts.get(mood) || 0;
    io.emit('mood_count_update', { mood, count });
    moodCountBroadcastDebounce.delete(mood);
  }, 1000);
  
  moodCountBroadcastDebounce.set(mood, timeout);
}

function getAllMoodCounts() {
  const counts = {};
  moodUserCounts.forEach((count, mood) => {
    counts[mood] = count;
  });
  return counts;
}


const activeFileTransfers = new Map(); // fileId -> { roomId, userId, bytesTransferred, startTime }
const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB per file
const MAX_TRANSFER_TIME = 5 * 60 * 1000; // 5 minutes
const MAX_CONCURRENT_TRANSFERS = 20000; // ✅ Global limit
const MAX_MEMORY_FOR_TRANSFERS = 1000 * 1024 * 1024; // ✅ 500MB total cap

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
function validateRoomAccess(roomId, userId) {
  const room = matchmaking.getRoom(roomId);
  
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


function broadcastCallStateUpdate(callId) {
  const call = activeCalls.get(callId);
  if (!call) return;

  const room = matchmaking.getRoom(call.roomId);
  if (!room) return;

  io.to(call.roomId).emit('call_state_update', {
    callId: callId,
    isActive: call.participants.length > 0,
    participantCount: call.participants.length,
    callType: call.callType
  });
  
  console.log(`📢 Call state update: ${callId} - ${call.participants.length} participants`);
}

function getUserDataForParticipant(participantId, socketUsers, room) {
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
  } else {
    console.warn(`⚠️ No room provided for user lookup`);
  }
  
  // Fallback to active socket connections
  for (const [socketId, socketUser] of socketUsers.entries()) {
    if (socketUser.userId === participantId) {
      console.log(`✅ Found in active sockets: ${socketUser.username}`);
      return {
        userId: socketUser.userId,
        username: socketUser.username,
        pfpUrl: socketUser.pfpUrl
      };
    }
  }
  
  // CRITICAL: Do NOT use fallback - return null to signal error
  console.error(`❌ CRITICAL: No user data found for ${participantId} anywhere!`);
  return null;
}

async function withCallMutex(callId, operation) {
  // Wait for any pending operation
  while (callMutexes.has(callId)) {
    try {
      await callMutexes.get(callId);
    } catch (err) {
      // Previous operation failed, continue
    }
  }
  
  // Create new mutex with atomic cleanup
  let resolve, reject;
  const mutexPromise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  
  callMutexes.set(callId, mutexPromise);
  
  try {
    const result = await operation();
    resolve(result);
    return result;
  } catch (error) {
    console.error(`❌ Mutex operation failed for call ${callId}:`, error);
    reject(error);
    throw error;
  } finally {
    // CRITICAL: Delay deletion to let waiters see completion
    setImmediate(() => {
      callMutexes.delete(callId);
      console.log(`🔓 Released mutex for call ${callId}`);
    });
  }
}


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
      console.warn(`⚠️ Room ${roomId} rate limit exceeded: ${roomLimit.count} messages in ${ROOM_RATE_WINDOW/1000}s`);
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
  pingInterval: 25000
});

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

// ============================================
// ENHANCED STATE MANAGEMENT
// ============================================

// Active calls with persistence
const activeCalls = new Map(); // callId -> { callId, roomId, callType, participants[], status, createdAt, lastActivity }
const userCalls = new Map(); // userId -> callId
const callGracePeriod = new Map(); // callId -> timeout

// Room cleanup tracking
const roomCleanupTimers = new Map(); // roomId -> timeout
// const ROOM_EXPIRY_TIME = 10 * 60 * 1000; // 10 minutes
const ROOM_EXPIRY_TIME = 900000 // 10 minutes
const ROOM_CLEANUP_GRACE = 30 * 1000; // 30 seconds grace period after expiry

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

function scheduleRoomCleanup(roomId, expiresAt) {
  // CRITICAL: Clear AND delete old timer
  if (roomCleanupTimers.has(roomId)) {
    const oldTimer = roomCleanupTimers.get(roomId);
    clearTimeout(oldTimer);
    roomCleanupTimers.delete(roomId); // Prevent reference leak
  }

  const now = Date.now();
  const timeUntilExpiry = expiresAt - now;

  if (timeUntilExpiry <= 0) {
    console.log(`⏰ Room ${roomId} already expired, cleaning up immediately`);
    performRoomCleanup(roomId);
    return;
  }

  console.log(`⏰ Scheduled cleanup for room ${roomId} in ${Math.round(timeUntilExpiry / 1000)}s`);

  const timer = setTimeout(() => {
    console.log(`⏰ Room ${roomId} expiry timer triggered`);
    performRoomCleanup(roomId);
    roomCleanupTimers.delete(roomId); // Self-cleanup
  }, timeUntilExpiry + ROOM_CLEANUP_GRACE);

  roomCleanupTimers.set(roomId, timer);
}

async function performRoomCleanup(roomId) {
  const room = matchmaking.getRoom(roomId);
  
  if (!room) {
    console.log(`🗑️ Room ${roomId} not found, already cleaned up`);
    roomCleanupTimers.delete(roomId);
    return;
  }

  console.log(`🗑️ ========================================`);
  console.log(`🗑️ CLEANING UP ROOM: ${roomId}`);
  console.log(`🗑️ ========================================`);
  console.log(`   Users: ${room.users.length}`);

  // CRITICAL FIX: Cancel ANY pending room lifecycle timers
  if (room.expiryTimer) {
    clearTimeout(room.expiryTimer);
    room.expiryTimer = null;
    console.log(`⏰ Canceled room's internal expiry timer`);
  }
  if (room.warningTimer) {
    clearTimeout(room.warningTimer);
    room.warningTimer = null;
    console.log(`⏰ Canceled room's internal warning timer`);
  }

  // Notify all users to clean up their IndexedDB files
  room.users.forEach(user => {
    const userSocket = findActiveSocketForUser(user.userId);
    if (userSocket) {
      userSocket.emit('room_expired', {
        roomId,
        message: 'Chat room has expired',
        cleanupFiles: true
      });
      userSocket.leave(roomId);
    }
  });

  // ✅ FIX: Clean up calls with mutex protection
  const callsToCleanup = [];
  activeCalls.forEach((call, callId) => {
    if (call.roomId === roomId) {
      callsToCleanup.push(callId);
    }
  });

  console.log(`🗑️ Found ${callsToCleanup.length} call(s) to clean up in expired room`);

  // ✅ Clean up each call with mutex to prevent race with join/leave
  for (const callId of callsToCleanup) {
    try {
      await withCallMutex(callId, async () => {
        const call = activeCalls.get(callId);
        
        if (!call) {
          console.log(`ℹ️ Call ${callId} already cleaned up`);
          return;
        }
        
        console.log(`🗑️ Cleaning up call ${callId} in expired room (inside mutex)`);
        console.log(`   Participants: [${call.participants.join(', ')}]`);
        
        // Notify participants that call ended due to room expiry
        call.participants.forEach(userId => {
          const userSocket = findActiveSocketForUser(userId);
          if (userSocket) {
            userSocket.emit('call_ended', {
              callId,
              reason: 'Room expired'
            });
            userSocket.leave(`call-${callId}`);
          }
          userCalls.delete(userId);
        });
        
        // Delete call state
        activeCalls.delete(callId);
        call.userMediaStates.clear();
        
        console.log(`✅ Call ${callId} cleaned up safely (mutex protected)`);
      });
      
      // Clear grace period if exists
      if (callGracePeriod.has(callId)) {
        clearTimeout(callGracePeriod.get(callId));
        callGracePeriod.delete(callId);
      }
      
    } catch (error) {
      console.error(`❌ Error cleaning up call ${callId}:`, error);
      // Continue with other calls
    }
  }

  // Remove the room from matchmaking
  matchmaking.destroyRoom(roomId);

  // Clear the cleanup timer
  roomCleanupTimers.delete(roomId);

  console.log(`✅ Room ${roomId} fully cleaned up and destroyed`);
  console.log(`🗑️ ========================================\n`);
}

function cancelRoomCleanup(roomId) {
  if (roomCleanupTimers.has(roomId)) {
    clearTimeout(roomCleanupTimers.get(roomId));
    roomCleanupTimers.delete(roomId);
    console.log(`❌ Cancelled cleanup timer for room ${roomId}`);
  }
}

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
    activeCalls: activeCalls.size,
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

// ============================================
// SOCKET.IO REAL-TIME COMMUNICATION
// ============================================



io.on('connection', (socket) => {
  console.log('🔌 Client connected:', socket.id);
  
  
  
  socket.on('send_message', async (data, callback) => {
  if (!currentUser) {
    return callback?.({ success: false, error: 'Not authenticated' });
  }
  
  const { roomId, message, type = 'text' } = data;
  const userId = currentUser.userId;
  
  // Validate room access
  const validation = validateRoomAccess(roomId, userId);
  if (!validation.valid) {
    return callback?.({ success: false, error: validation.error });
  }
  
  const room = validation.room;
  
  // Create message object
  const messageObj = {
    id: uuidv4(),
    userId,
    username: currentUser.username,
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
    if (!currentUser) {
      return callback?.({ success: false, error: 'Not authenticated' });
    }
    
    const { mood } = data;
    const userId = currentUser.userId;
    
    console.log(`🎭 [UID: ${userId}] [Socket: ${socket.id}] Attempting to select mood: ${mood}`);
    
    try {
      // CRITICAL: Validate if user can select mood
      const validation = await validateMoodSelection(userId);
      
      if (!validation.allowed) {
        console.log(`🚫 [UID: ${userId}] Mood selection blocked: ${validation.reason}`);
        
        // Attempt to restore existing room
        const restoration = await restoreExistingRoom(socket, userId, validation.existingRoom);
        
        return callback?.({
          success: false,
          error: validation.reason,
          existingRoom: validation.existingRoom,
          restored: restoration.success,
          room: restoration.room
        });
      }
      
      // User is allowed to select mood - proceed with matchmaking
      console.log(`✅ [UID: ${userId}] Mood selection allowed`);
      
      // Add to mood registry (deduplicated by UID)
      addUserToMood(userId, mood);
      
      // Join matchmaking queue
      const result = await matchmaking.joinQueue(userId, mood, currentUser);
      
      if (result.matched) {
        // Match found - register active room
        setUserActiveRoom(userId, result.roomId, mood);
        
        // Join socket to room
        socket.join(result.roomId);
        
        console.log(`🎯 [UID: ${userId}] Matched! Room: ${result.roomId}`);
        
        // Get partner info
        const room = matchmaking.getRoom(result.roomId);
        const partner = room.users.find(u => u.userId !== userId);
        const partnerProfile = partner ? await getUserProfile(partner.userId) : null;
        
        const roomData = {
          roomId: result.roomId,
          mood: room.mood,
          partner: partner ? {
            userId: partner.userId,
            username: partner.username,
            profilePictureUrl: partner.profilePictureUrl,
            bio: partnerProfile?.bio || ''
          } : null,
          expiresAt: room.expiresAt,
          chatHistory: room.chatHistory || []
        };
        
        // Emit to all devices of this user
        emitToUserAllDevices(userId, 'match_found', roomData);
        
        // Also emit to partner's all devices
        if (partner) {
          emitToUserAllDevices(partner.userId, 'match_found', {
            ...roomData,
            partner: {
              userId,
              username: currentUser.username,
              profilePictureUrl: currentUser.profilePictureUrl,
              bio: (await getUserProfile(userId))?.bio || ''
            }
          });
        }
        
        callback?.({ success: true, matched: true, room: roomData });
        
      } else {
        // Waiting in queue
        console.log(`⏳ [UID: ${userId}] Waiting in queue for mood: ${mood}`);
        callback?.({ success: true, matched: false, queuePosition: result.queuePosition });
      }
      
    } catch (error) {
      console.error(`❌ [UID: ${userId}] Mood selection error:`, error);
      callback?.({ success: false, error: error.message });
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
    const activeRoom = getUserActiveRoom(userId);
    
    if (!activeRoom) {
      return callback?.({ success: false, error: 'No active room to restore' });
    }
    
    console.log(`🔄 [UID: ${userId}] Manual room restoration requested`);
    
    const result = await restoreExistingRoom(socket, userId, activeRoom);
    callback?.(result);
  });
  
  
    socket.on('error', (error) => {
    console.error(`❌ Socket error [${socket.id}]:`, error);
    const user = socketUsers.get(socket.id);
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


socket.on('file_chunk', (data) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      console.error('❌ Unauthenticated socket tried to send file chunk');
      return;
    }
    
    // ✅ Rate limit check
    if (!checkChunkRateLimit(user.userId)) {
      console.warn(`⚠️ Rate limit exceeded for ${user.username} on file chunks`);
      socket.emit('file_transmission_failed', { 
        fileId: data.fileId,
        fileName: data.fileName,
        reason: 'Rate limit exceeded. Please slow down.' 
      });
      return;
    }
    
    const { fileId, fileName, roomId, chunkIndex, totalChunks, chunkSize, chunkData } = data;
    
    // CRITICAL: Validate chunk data exists and is a string
    if (!chunkData || typeof chunkData !== 'string') {
      console.error(`❌ Invalid chunk data at index ${chunkIndex}`);
      socket.emit('file_transmission_failed', { 
        fileId, 
        fileName,
        reason: 'Invalid chunk data' 
      });
      return;
    }
    
    // ✅ FIX: Validate chunk data size matches claimed chunkSize
    // Base64 encoding increases size by ~33%, so base64 length should be ~1.33x binary size
    const expectedBase64Length = Math.ceil(chunkSize * 1.37); // 37% overhead for padding
    const maxAllowedLength = expectedBase64Length * 1.1; // Allow 10% variance for padding
    
    if (chunkData.length > maxAllowedLength) {
      console.error(`❌ Chunk data size mismatch for ${fileId} chunk ${chunkIndex}`);
      console.error(`   Claimed size: ${chunkSize} bytes`);
      console.error(`   Expected base64 length: ~${expectedBase64Length}`);
      console.error(`   Actual length: ${chunkData.length}`);
      console.error(`   Difference: ${chunkData.length - expectedBase64Length} characters`);
      
      socket.emit('file_transmission_failed', { 
        fileId,
        fileName,
        reason: 'Chunk data size exceeds claimed size. Possible malicious upload.'
      });
      
      // Clean up this transfer
      activeFileTransfers.delete(fileId);
      return;
    }
    
    // ✅ FIX: Additional validation - chunk data should be valid base64
    if (!/^[A-Za-z0-9+/]*={0,2}$/.test(chunkData)) {
      console.error(`❌ Invalid base64 data in chunk ${chunkIndex} of ${fileId}`);
      socket.emit('file_transmission_failed', { 
        fileId,
        fileName,
        reason: 'Invalid file data encoding'
      });
      activeFileTransfers.delete(fileId);
      return;
    }
    
    // ✅ FIX: Use actual chunkData.length for memory tracking (not claimed chunkSize)
    const actualChunkSize = Math.floor(chunkData.length * 0.75); // Approximate binary size from base64
    
    // Check global concurrent transfer limit
    if (!activeFileTransfers.has(fileId)) {
      if (activeFileTransfers.size >= MAX_CONCURRENT_TRANSFERS) {
        console.warn(`⚠️ Max concurrent transfers (${MAX_CONCURRENT_TRANSFERS}) reached`);
        socket.emit('file_transmission_failed', { 
          fileId,
          fileName,
          reason: 'Server at capacity. Please try again in a moment.'
        });
        return;
      }
      
      // Check global memory limit
      const currentMemory = getCurrentTransferMemory();
      if (currentMemory >= MAX_MEMORY_FOR_TRANSFERS) {
        console.warn(`⚠️ Transfer memory limit reached: ${(currentMemory / 1024 / 1024).toFixed(2)}MB`);
        socket.emit('file_transmission_failed', { 
          fileId,
          fileName,
          reason: 'Server memory at capacity. Please try again shortly.'
        });
        return;
      }
      
      // Initialize transfer tracking
      activeFileTransfers.set(fileId, {
        roomId,
        userId: user.userId,
        bytesTransferred: 0,
        startTime: Date.now()
      });
      
      console.log(`📦 New file transfer started: ${fileName} (${fileId})`);
      console.log(`   Active transfers: ${activeFileTransfers.size}/${MAX_CONCURRENT_TRANSFERS}`);
      console.log(`   Memory in use: ${(currentMemory / 1024 / 1024).toFixed(2)}MB/${(MAX_MEMORY_FOR_TRANSFERS / 1024 / 1024).toFixed(2)}MB`);
    }
    
    // Update bytes transferred with ACTUAL size
    const transfer = activeFileTransfers.get(fileId);
    if (transfer) {
      transfer.bytesTransferred += actualChunkSize;
      
      // Check if individual file exceeds limit
      if (transfer.bytesTransferred > MAX_FILE_SIZE) {
        console.error(`❌ File ${fileId} exceeded size limit: ${(transfer.bytesTransferred / 1024 / 1024).toFixed(2)}MB`);
        activeFileTransfers.delete(fileId);
        socket.emit('file_transmission_failed', { 
          fileId,
          fileName,
          reason: 'File size limit exceeded'
        });
        return;
      }
    }
    
    // Validate room access
    const room = matchmaking.getRoom(roomId);
    if (!room) {
      console.error(`❌ Room ${roomId} not found for file chunk`);
      activeFileTransfers.delete(fileId);
      socket.emit('file_transmission_failed', { 
        fileId,
        fileName,
        reason: 'Room not found or expired'
      });
      return;
    }
    
    if (!room.hasUser(user.userId)) {
      console.error(`❌ User ${user.username} not in room ${roomId}`);
      activeFileTransfers.delete(fileId);
      socket.emit('file_transmission_failed', { 
        fileId,
        fileName,
        reason: 'You are not in this room'
      });
      return;
    }
    
    // Relay chunk to all OTHER users in room
    socket.to(roomId).emit('file_chunk', {
      fileId,
      fileName,
      senderId: user.userId,
      senderUsername: user.username,
      chunkIndex,
      totalChunks,
      chunkSize: actualChunkSize, // ✅ Send validated size
      chunkData
    });
    
    // Log progress for large files
    if (totalChunks > 10 && chunkIndex % Math.floor(totalChunks / 10) === 0) {
      const progress = ((chunkIndex / totalChunks) * 100).toFixed(1);
      console.log(`📦 File ${fileName} progress: ${progress}% (chunk ${chunkIndex}/${totalChunks})`);
    }
    
  } catch (error) {
    console.error('❌ File chunk relay error:', error);
    if (data?.fileId) {
      activeFileTransfers.delete(data.fileId);
    }
  }
});

// ✅ FIX: Enhanced cleanup on transfer complete
socket.on('file_transfer_complete', ({ fileId }) => {
  if (activeFileTransfers.has(fileId)) {
    const transfer = activeFileTransfers.get(fileId);
    const transferTime = Date.now() - transfer.startTime;
    const sizeMB = (transfer.bytesTransferred / 1024 / 1024).toFixed(2);
    
    activeFileTransfers.delete(fileId);
    
    console.log(`✅ File transfer ${fileId} completed`);
    console.log(`   Size: ${sizeMB}MB, Time: ${(transferTime / 1000).toFixed(1)}s`);
    console.log(`   Active transfers: ${activeFileTransfers.size}/${MAX_CONCURRENT_TRANSFERS}`);
  }
});

socket.on('file_chunk_ack', (data) => {
  // ACKs are sent TO sender, not relayed to room
  // This handler can log or track reliability metrics if needed
  const { fileId, chunkIndex } = data;
  
  // Optional: Track chunk delivery success rate
  // console.log(`✅ Chunk ${chunkIndex} of ${fileId} acknowledged by receiver`);
});

socket.on('request_attachment_data', async ({ fileId, roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      socket.emit('error', { message: 'Not authenticated' });
      return;
    }
    
    console.log('📂 ========================================');
    console.log('📂 ATTACHMENT DATA REQUEST');
    console.log('📂 ========================================');
    console.log(`   Requester: ${user.username} (${user.userId})`);
    console.log(`   FileID: ${fileId}`);
    console.log(`   Room: ${roomId}`);
    
    const room = matchmaking.getRoom(roomId);
    
    if (!room) {
      console.error(`❌ Room ${roomId} not found`);
      socket.emit('attachment_data_unavailable', { 
        fileId,
        reason: 'Room expired or not found' 
      });
      return;
    }
    
    if (!room.hasUser(user.userId)) {
      console.error(`❌ User not in room ${roomId}`);
      socket.emit('error', { message: 'Not in room' });
      return;
    }
    
    // Find the message with this attachment in room history
    const messages = room.getMessages ? room.getMessages() : [];
    const messageWithFile = messages.find(msg => 
      msg.attachment && msg.attachment.fileId === fileId
    );
    
    if (!messageWithFile) {
      console.error(`❌ Message with file ${fileId} not found in room history`);
      socket.emit('attachment_data_unavailable', { 
        fileId,
        reason: 'File not in room history' 
      });
      return;
    }
    
    const senderId = messageWithFile.userId;
    console.log(`📤 Requesting file from sender: ${senderId}`);
    
    // Find sender's active socket
    const senderSocket = findActiveSocketForUser(senderId);
    
    if (!senderSocket) {
      console.error(`❌ Sender ${senderId} not connected`);
      socket.emit('attachment_data_unavailable', { 
        fileId,
        reason: 'File owner not online' 
      });
      return;
    }
    
    // Request file data from sender
    senderSocket.emit('send_attachment_to_peer', {
      fileId,
      requesterId: user.userId,
      requesterSocketId: socket.id
    });
    
    console.log(`✅ File request forwarded to sender`);
    console.log('📂 ========================================\n');
    
  } catch (error) {
    console.error('❌ Request attachment error:', error);
    socket.emit('error', { message: 'Failed to request attachment' });
  }
});

socket.on('attachment_data_response', ({ fileId, requesterId, requesterSocketId, data, metadata }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;
    
    console.log('📤 ========================================');
    console.log('📤 ATTACHMENT DATA RESPONSE');
    console.log('📤 ========================================');
    console.log(`   Sender: ${user.username}`);
    console.log(`   FileID: ${fileId}`);
    console.log(`   Data size: ${data ? (data.length / 1024).toFixed(2) : 0} KB`);
    console.log(`   Target socket: ${requesterSocketId}`);
    
    // Forward to requester
    const requesterSocket = io.sockets.sockets.get(requesterSocketId);
    
    if (requesterSocket) {
      requesterSocket.emit('attachment_data_received', {
        fileId,
        data,
        metadata
      });
      console.log(`✅ File data forwarded to requester`);
    } else {
      console.error(`❌ Requester socket ${requesterSocketId} not found`);
    }
    
    console.log('📤 ========================================\n');
    
  } catch (error) {
    console.error('❌ Attachment response error:', error);
  }
});

// CRITICAL FIX: Handle file transfer completion cleanup
socket.on('file_transfer_complete', ({ fileId }) => {
  if (activeFileTransfers.has(fileId)) {
    activeFileTransfers.delete(fileId);
    console.log(`✅ File transfer ${fileId} completed and cleaned up`);
  }
});
  
  socket.on('validate_cached_call', async ({ callId, roomId }) => {
    try {
        const user = socketUsers.get(socket.id);
        
        if (!user) {
            console.warn('⚠️ Unauthenticated socket tried to validate cached call');
            socket.emit('cached_call_invalid', { callId });
            return;
        }
        
        console.log(`🔍 Validating cached call ${callId} for ${user.username}`);
        
        const call = activeCalls.get(callId);
        
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
        
        const room = matchmaking.getRoom(roomId);
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


socket.on('validate_room', ({ roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      console.warn('⚠️ Unauthenticated socket tried to validate room');
      socket.emit('room_invalid', { 
        roomId,
        reason: 'Not authenticated' 
      });
      return;
    }
    
    const room = matchmaking.getRoom(roomId);
    
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


socket.on('request_room_sync', ({ roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      console.warn('⚠️ Unauthenticated socket requested room sync');
      return;
    }
    
    const room = matchmaking.getRoom(roomId);
    
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

    console.log(`🔐 Authenticating socket for userId: ${userId}`);

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
      
      console.log(`✅ Token verified for Firebase UID: ${decodedToken.uid}`);
    } catch (error) {
      console.error('❌ Token verification failed:', error.message);
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
          console.error('❌ Database error during authentication (all retries exhausted):', dbError);
          socket.emit('auth_error', { 
            message: 'Database temporarily unavailable. Please try again in a few seconds.',
            code: 'DB_ERROR',
            retryable: true
          });
          return;
        }
        
        console.warn(`⚠️ Database query failed, retrying (${retryCount}/${MAX_RETRIES})...`);
        await new Promise(resolve => setTimeout(resolve, 500 * retryCount)); // ✅ FIX: Exponential backoff
      }
    }

    if (!user) {
      console.error(`❌ User not found in database: ${userId}`);
      socket.emit('auth_error', { 
        message: 'User not found', 
        code: 'USER_NOT_FOUND' 
      });
      return;
    }

    // CRITICAL: Verify Firebase UID matches (prevent token spoofing)
    if (user.firebaseUid !== decodedToken.uid) {
      console.error(`❌ Firebase UID mismatch for user ${userId}`);
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
    
    // Register this socket under Firebase UID for multi-device tracking
    registerSocketForUser(firebaseUid, socket.id);
    
    console.log(`📱 [UID: ${firebaseUid}] Registered socket ${socket.id}`);
    console.log(`📱 [UID: ${firebaseUid}] Total active devices: ${getUserSocketIds(firebaseUid).length}`);

    // ============================================
    // HANDLE EXISTING SOCKET FOR SAME USER (LEGACY)
    // ============================================
    // Note: With multi-device support, we DON'T disconnect old sockets
    // Instead, we allow multiple concurrent sessions
    const oldSocketId = userToSocketId.get(mongoUserId);
    
    if (oldSocketId && oldSocketId !== socket.id) {
      console.log(`🔄 User ${user.username} has multiple active sessions`);
      console.log(`   Previous socket: ${oldSocketId}`);
      console.log(`   New socket: ${socket.id}`);
      
      // Clean up old socket mapping from legacy tracking
      const oldSocketData = socketUsers.get(oldSocketId);
      if (oldSocketData) {
        console.log(`ℹ️ Updating socket tracking for ${user.username}`);
      }
      
      // Clean up any pending socket cleanup timers
      if (socketUserCleanup.has(oldSocketId)) {
        clearTimeout(socketUserCleanup.get(oldSocketId));
        socketUserCleanup.delete(oldSocketId);
        console.log(`⏰ Cancelled pending cleanup for old socket ${oldSocketId}`);
      }
    }

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

    // Store in both directions for O(1) lookups
    socketUsers.set(socket.id, userSocketData);
    userToSocketId.set(mongoUserId, socket.id); // Update to latest socket

    console.log('✅ Socket authenticated successfully');
    console.log(`   User: ${user.username} (${mongoUserId})`);
    console.log(`   Firebase UID: ${firebaseUid}`);
    console.log(`   Socket: ${socket.id}`);
    console.log(`   Active sockets: ${socketUsers.size}`);

    // ============================================
    // CHECK FOR ACTIVE ROOM (MULTI-DEVICE AWARE)
    // ============================================
    const activeRoom = getUserActiveRoom(firebaseUid);
    
    if (activeRoom) {
      console.log(`ℹ️ [UID: ${firebaseUid}] User has active room: ${activeRoom.roomId}`);
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

    // ✅ SEND INITIAL MOOD COUNTS
    socket.emit('mood_counts_initial', getAllMoodCounts());

    // ============================================
    // RESTORE USER STATE (LEGACY FALLBACK)
    // ============================================
    // Check legacy room tracking (for backwards compatibility)
    const legacyRoomId = matchmaking.getRoomIdByUser(mongoUserId);
    if (legacyRoomId && !activeRoom) {
      const room = matchmaking.getRoom(legacyRoomId);
      if (room && !room.isExpired) {
        console.log(`🔄 [Legacy] User ${user.username} was in room ${legacyRoomId}, registering in new system`);
        
        // Register in new system
        setUserActiveRoom(firebaseUid, legacyRoomId, room.mood);
        
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
        console.log(`⚠️ User ${user.username} was in expired room ${legacyRoomId}`);
        matchmaking.leaveRoom(mongoUserId);
      }
    } else if (activeRoom) {
      // User has active room in new system - auto-join socket to room
      const room = matchmaking.getRoom(activeRoom.roomId);
      if (room && !room.isExpired) {
        console.log(`🔄 [Multi-Device] Auto-joining socket ${socket.id} to existing room ${activeRoom.roomId}`);
        
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
        console.log(`⚠️ [UID: ${firebaseUid}] Active room ${activeRoom.roomId} is expired, cleaning up`);
        clearUserActiveRoom(firebaseUid);
      }
    }

    // ============================================
    // RESTORE CALL STATE
    // ============================================
    // Check if user was in an active call
    const activeCallId = userCalls.get(mongoUserId);
    if (activeCallId) {
      const call = activeCalls.get(activeCallId);
      if (call && call.status === 'active' && call.participants.includes(mongoUserId)) {
        console.log(`📞 User ${user.username} was in call ${activeCallId}, notifying of reconnection opportunity`);
        
        socket.emit('call_reconnect_available', {
          callId: activeCallId,
          callType: call.callType,
          participantCount: call.participants.length,
          roomId: call.roomId
        });
      } else {
        // Call ended while user was disconnected
        console.log(`⚠️ User ${user.username} was in ended call ${activeCallId}`);
        userCalls.delete(mongoUserId);
      }
    }

    // ============================================
    // OPTIONAL: AUTO-RESTORE ROOM ON RECONNECT
    // ============================================
    // If client wants automatic room restoration on auth, they can listen for
    // the 'authenticated' event and check hasActiveRoom, then call restore_room
    // This gives the client control over when to show the room UI

  } catch (error) {
    console.error(`❌ [authenticate] Unexpected error for user ${userId}:`, error);
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
    const user = socketUsers.get(socket.id);
    
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

    // Clear any existing timeout for this user
    clearMatchmakingTimeout(user.userId);

    // Try to add to queue or join existing room
    let room = matchmaking.addToQueue({
      ...user,
      mood,
      socketId: socket.id
    });

    if (!room) {
      const queueStatus = matchmaking.getQueueStatus(mood);
      if (queueStatus >= config.MAX_USERS_PER_ROOM) {
        console.log(`🔄 Queue full detected (${queueStatus}/${config.MAX_USERS_PER_ROOM}), retrying match...`);
        room = matchmaking.addToQueue({
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

      room.users.forEach(roomUser => {
        const userSocket = findActiveSocketForUser(roomUser.userId);
        
        if (userSocket) {
          console.log(`📤 Emitting match_found to ${roomUser.username} on socket ${userSocket.id}`);
          
          userSocket.join(room.id);
          console.log(`✅ User ${roomUser.username} joined Socket.IO room ${room.id}`);
          
          // Include previous messages for users joining existing room
          const matchData = {
            roomId: room.id,
            mood: room.mood,
            users: room.users.map(u => ({
              userId: u.userId,
              username: u.username,
              pfpUrl: u.pfpUrl
            })),
            expiresAt: room.expiresAt
          };

          // If user is joining existing room, include previous messages
          if (isJoiningExisting && roomUser.userId === user.userId) {
            matchData.previousMessages = room.getMessages();
            console.log(`📨 Sending ${matchData.previousMessages.length} previous messages to ${roomUser.username}`);
          }

          userSocket.emit('match_found', matchData);
          
          // ✅ Keep user in mood count when moved to room
          addUserToMood(roomUser.userId, room.mood);
          
          // Clear matchmaking timeout for this user
          clearMatchmakingTimeout(roomUser.userId);
          
        } else {
          console.error(`❌ No active socket found for user ${roomUser.username} (${roomUser.userId})`);
          matchmaking.leaveRoom(roomUser.userId);
        }
      });

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
      const queuePosition = matchmaking.getQueueStatus(mood);
      socket.emit('queued', { 
        mood, 
        position: queuePosition 
      });
      console.log(`⏳ User ${user.username} queued (${queuePosition}/${config.MIN_USERS_FOR_ROOM})`);

      // ✅ START MATCHMAKING TIMEOUT
      const timeoutHandle = setTimeout(() => {
        // Check if user is still in queue
        const currentQueueStatus = matchmaking.getQueueStatus(mood);
        
        console.log(`⏰ Matchmaking timeout for ${user.username} in ${mood} queue`);
        console.log(`   Queue status: ${currentQueueStatus} users`);
        
        // If fewer than MIN_USERS_FOR_ROOM, timeout and redirect
        if (currentQueueStatus < config.MIN_USERS_FOR_ROOM) {
          console.log(`❌ Insufficient users (${currentQueueStatus}/${config.MIN_USERS_FOR_ROOM}) - timing out`);
          
          // Cancel matchmaking
          matchmaking.cancelMatchmaking(user.userId);
          removeUserFromAllMoods(user.userId);
          clearMatchmakingTimeout(user.userId);
          
          // Notify client to redirect
          const userSocket = findActiveSocketForUser(user.userId);
          if (userSocket) {
            userSocket.emit('matchmaking_timeout', {
              message: 'No matches found. Please try again.',
              mood: mood,
              queueStatus: currentQueueStatus,
              minRequired: config.MIN_USERS_FOR_ROOM
            });
          }
          
          console.log(`🔄 User ${user.username} timed out, redirecting to mood selection`);
        } else {
          // Enough users found, try to create room
          console.log(`✅ Sufficient users found (${currentQueueStatus}), creating room`);
          const room = matchmaking.addToQueue({
            ...user,
            mood,
            socketId: socket.id
          });
          
          if (room) {
            console.log(`🎉 Room ${room.id} created after timeout check`);
          }
        }
        
        // Clean up timeout
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

function findActiveSocketForUser(userId) {
  // CRITICAL FIX: O(1) lookup instead of O(n)
  const socketId = userToSocketId.get(userId);
  if (!socketId) {
    return null;
  }
  
  const socketInstance = io.sockets.sockets.get(socketId);
  if (socketInstance && socketInstance.connected) {
    return socketInstance;
  }
  
  // Socket disconnected but index not cleaned - remove stale entry
  userToSocketId.delete(userId);
  return null;
}

socket.on('join_room', ({ roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
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

    const room = matchmaking.getRoom(roomId);
    
    if (!room) {
      console.error(`❌ Room ${roomId} not found!`);
      socket.emit('error', { 
        message: 'Room not found. It may have expired or been closed.',
        code: 'ROOM_NOT_FOUND'
      });
      return;
    }

    if (!room.hasUser(user.userId)) {
      console.error(`❌ User ${user.username} (${user.userId}) not in room ${roomId}`);
      socket.emit('error', { 
        message: 'You are not a member of this room',
        code: 'NOT_IN_ROOM'
      });
      return;
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

    // Get chat history from the room
    const chatHistory = room.getMessages ? room.getMessages() : [];
    console.log(`📜 Sending ${chatHistory.length} chat messages to ${user.username}`);

    // Check for active calls in this room
    let activeCallState = null;
    activeCalls.forEach((call, callId) => {
      if (call.roomId === roomId && call.participants.length > 0) {
        activeCallState = {
          callId: callId,
          isActive: true,
          participantCount: call.participants.length,
          callType: call.callType
        };
        console.log(`📞 Active call detected in room ${roomId}: ${callId} with ${call.participants.length} participant(s)`);
      }
    });

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

socket.on('cancel_matchmaking', () => {
  const user = socketUsers.get(socket.id);
  if (user) {
    // Clear matchmaking timeout
    clearMatchmakingTimeout(user.userId);
    
    matchmaking.cancelMatchmaking(user.userId);
    
    // ✅ REMOVE USER FROM MOOD TRACKING
    removeUserFromAllMoods(user.userId);
    clearMatchmakingTimeout(user.userId);
    
    socket.emit('matchmaking_cancelled');
    console.log(`❌ Matchmaking cancelled: ${user.username}`);
  }
});

socket.on('chat_message', ({ roomId, message, replyTo, attachment }) => {
  try {
    const user = socketUsers.get(socket.id);
    
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
    const validation = validateRoomAccess(roomId, user.userId);
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
    const user = socketUsers.get(socket.id);
    
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

    const room = matchmaking.getRoom(roomId);
    
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

    // ✅ FIX: Room-level lock to prevent duplicate call creation
    // Wait for any pending initiate_call on this room
    while (roomCallInitLocks.has(roomId)) {
      try {
        await roomCallInitLocks.get(roomId);
      } catch (err) {
        // Previous initiation failed, continue
      }
    }
    
    // Create new lock
    let resolve, reject;
    const lockPromise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    
    roomCallInitLocks.set(roomId, lockPromise);
    
    try {
      // ✅ ATOMIC CHECK: Look for existing call inside lock
      let existingCallId = null;
      let existingCall = null;
      
      for (const [callId, call] of activeCalls.entries()) {
        if (call.roomId === roomId && call.participants.length > 0) {
          existingCallId = callId;
          existingCall = call;
          break;
        }
      }

      if (existingCallId) {
        console.log(`📞 Call already active in room ${roomId}: ${existingCallId}`);
        console.log(`   Participants: ${existingCall.participants.length}`);
        
        socket.emit('error', { 
          message: 'A call is already in progress',
          code: 'CALL_ALREADY_ACTIVE',
          callId: existingCallId,
          callType: existingCall.callType,
          participantCount: existingCall.participants.length
        });
        
        resolve(); // Release lock
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

      activeCalls.set(callId, call);
      userCalls.set(user.userId, callId);
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

      // Send incoming_call to other users
      room.users.forEach(roomUser => {
        if (roomUser.userId !== user.userId) {
          const targetSocket = findActiveSocketForUser(roomUser.userId);
          if (targetSocket) {
            targetSocket.emit('incoming_call', {
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
      });

      console.log('✅ ========================================');
      console.log('✅ CALL INITIATION COMPLETE');
      console.log('✅ ========================================\n');
      
      resolve(); // Release lock
      
    } catch (error) {
      console.error('❌ Call initiation error:', error);
      reject(error);
      socket.emit('error', { message: 'Failed to initiate call' });
    } finally {
      // Clean up lock
      setImmediate(() => {
        roomCallInitLocks.delete(roomId);
      });
    }

  } catch (error) {
    console.error('❌ Initiate call error:', error);
    socket.emit('error', { message: 'Failed to initiate call' });
  }
});

socket.on('accept_call', async ({ callId, roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      socket.emit('error', { message: 'Not authenticated' });
      return;
    }

    // CRITICAL FIX: Validate room exists BEFORE proceeding
    const room = matchmaking.getRoom(roomId);
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

    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      
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
      userCalls.set(user.userId, callId);
      
      console.log(`➕ Added ${user.username} to participants`);
      console.log(`🔍 [accept_call] After: participants=[${call.participants.join(', ')}]`);
      
      call.userMediaStates.set(user.userId, {
        videoEnabled: call.callType === 'video',
        audioEnabled: true
      });
      
      if (call.status === 'pending') {
        call.status = 'active';
        console.log(`📊 Call status changed: pending → active`);
        
        // REMOVED: Room extension logic - calls use unified timer
        if (room) {
          room.setActiveCall(true);
          console.log(`🛡️ Room ${roomId} marked as having active call (unified timer)`);
        }
      }
      
      call.lastActivity = Date.now();

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
        call.participants = call.participants.filter(p => p !== user.userId);
        userCalls.delete(user.userId);
        call.userMediaStates.delete(user.userId);
        return;
      }

// Emit synchronously - Socket.IO handles queueing
callUsers.forEach(roomUser => {
  const targetSocket = findActiveSocketForUser(roomUser.userId);
  if (targetSocket) {
    targetSocket.emit('call_accepted', {
      callId,
      callType: call.callType,
      users: callUsers
    });
    console.log(`📤 Sent call_accepted to ${roomUser.username}`);
  } else {
    console.error(`❌ No active socket for ${roomUser.username}`);
  }
});

// REMOVED: No need for Promise.all - emits are synchronous

      // CRITICAL FIX: Single broadcast instead of duplicate
      broadcastCallStateUpdate(callId);
    });
    
    
    

  } catch (error) {
    console.error('❌ Accept call error:', error);
    socket.emit('error', { message: 'Failed to accept call' });
  }
});

socket.on('decline_call', async ({ callId, roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      socket.emit('error', { message: 'Not authenticated' });
      return;
    }

    // ✅ FIX: Use mutex to prevent race with accept_call/join_call
    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      
      if (!call) {
        socket.emit('error', { message: 'Call not found' });
        return;
      }

      console.log(`❌ User ${user.username} declined call ${callId}`);
      console.log(`   Current participants: [${call.participants.join(', ')}]`);

      const initiatorSocket = findActiveSocketForUser(call.initiator);
      if (initiatorSocket) {
        initiatorSocket.emit('call_declined', {
          callId,
          userId: user.userId,
          username: user.username
        });
        console.log(`📤 Sent call_declined to initiator`);
      }

      // ✅ Clean up call if only initiator remains
      if (call.participants.length === 1 && call.participants[0] === call.initiator) {
        console.log(`🗑️ Cleaning up declined call ${callId} (only initiator remained)`);
        activeCalls.delete(callId);
        userCalls.delete(call.initiator);
        
        // Mark room as call-free
        const room = matchmaking.getRoom(call.roomId);
        if (room) {
          room.setActiveCall(false);
          console.log(`📞 Room ${call.roomId} marked as call-free`);
        }
      } else {
        console.log(`ℹ️ Call ${callId} not cleaned up - ${call.participants.length} participants remain`);
      }
    });

  } catch (error) {
    console.error('❌ Decline call error:', error);
    socket.emit('error', { message: 'Failed to decline call' });
  }
});


socket.on('connection_established', ({ callId, connectionType, localType, remoteType, protocol }) => {
  const user = socketUsers.get(socket.id);
  if (!user) return;

  console.log(`📊 [METRICS] Connection established for ${user.username}`);
  console.log(`   Type: ${connectionType}`);
  console.log(`   Local: ${localType}, Remote: ${remoteType}`);
  console.log(`   Protocol: ${protocol}`);

  // Track metrics using atomic operations
  if (connectionType === 'TURN_RELAY') {
    webrtcMetrics.increment('turnUsage');
    console.warn(`⚠️ [METRICS] TURN usage: ${webrtcMetrics.get('turnUsage')} / ${webrtcMetrics.get('totalCalls')} calls (${((webrtcMetrics.get('turnUsage') / webrtcMetrics.get('totalCalls')) * 100).toFixed(1)}%)`);
  } else if (connectionType === 'STUN_REFLEXIVE') {
    webrtcMetrics.increment('stunUsage');
    console.log(`✅ [METRICS] STUN usage: ${webrtcMetrics.get('stunUsage')} / ${webrtcMetrics.get('totalCalls')} calls (${((webrtcMetrics.get('stunUsage') / webrtcMetrics.get('totalCalls')) * 100).toFixed(1)}%)`);
  } else if (connectionType === 'DIRECT_HOST') {
    webrtcMetrics.increment('directConnections');
    console.log(`✅ [METRICS] Direct: ${webrtcMetrics.get('directConnections')} / ${webrtcMetrics.get('totalCalls')} calls (${((webrtcMetrics.get('directConnections') / webrtcMetrics.get('totalCalls')) * 100).toFixed(1)}%)`);
  }

  webrtcMetrics.increment('successfulConnections');
});


socket.on('join_call', async ({ callId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
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
      const call = activeCalls.get(callId);
      if (call && call.participants.includes(user.userId)) {
        const room = matchmaking.getRoom(call.roomId);
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

    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      
      const validation = validateCallState(call, 'join_call');
      if (!validation.valid) {
        socket.emit('error', { message: validation.error });
        joinCallDebounce.delete(debounceKey);
        return;
      }

      // Validate room exists and user is in it
      const room = matchmaking.getRoom(call.roomId);
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
      
      if (!wasAlreadyInCall) {
        participantSet.add(user.userId);
        call.participants = Array.from(participantSet); // ✅ Guaranteed unique
        userCalls.set(user.userId, callId);
        
        console.log(`➕ Added ${user.username} to call ${callId} participants`);
        console.log(`   Participants: [${call.participants.join(', ')}] (${call.participants.length} total)`);
      } else {
        console.log(`ℹ️ User ${user.username} already in call ${callId} participants (re-joining)`);
      }

      call.lastActivity = Date.now();
      
      // Ensure media state exists
      if (!call.userMediaStates.has(user.userId)) {
        call.userMediaStates.set(user.userId, {
          videoEnabled: call.callType === 'video',
          audioEnabled: true
        });
        console.log(`📊 Initialized media state for ${user.username}`);
      }

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
        console.error(`   Expected: ${call.participants.length}, Got: ${participantsWithMediaStates.length}`);
        socket.emit('error', { 
          message: 'Unable to load all participants. Please refresh and try again.',
          code: 'PARTICIPANT_RESOLUTION_FAILED'
        });
        
        // ✅ FIX: Rollback if validation fails
        if (!wasAlreadyInCall) {
          call.participants = call.participants.filter(p => p !== user.userId);
          userCalls.delete(user.userId);
          call.userMediaStates.delete(user.userId);
        }
        
        joinCallDebounce.delete(debounceKey);
        return;
      }

      console.log(`📊 Sending ${participantsWithMediaStates.length} VALIDATED participants to ${user.username}`);

      // Send complete state to joining user
      socket.emit('call_joined', {
        callId,
        callType: call.callType,
        participants: participantsWithMediaStates
      });

      // Get current user's media state
      const userMediaState = call.userMediaStates.get(user.userId);

      // ✅ FIX: Only broadcast if this is a NEW join (not re-join)
      if (!wasAlreadyInCall) {
        socket.to(`call-${callId}`).emit('user_joined_call', {
          user: {
            userId: user.userId,
            username: user.username,
            pfpUrl: user.pfpUrl,
            videoEnabled: userMediaState.videoEnabled,
            audioEnabled: userMediaState.audioEnabled
          }
        });
        console.log(`📢 Broadcasted user_joined_call to other participants`);
      }

      console.log(`✅ ${user.username} successfully joined call ${callId} with ${call.participants.length} total participants`);
    });
    
    // Clear debounce after successful join
    setTimeout(() => {
      joinCallDebounce.delete(debounceKey);
    }, 2000);

  } catch (error) {
    console.error('❌ Join call error:', error);
    socket.emit('error', { message: 'Failed to join call' });
    
    const debounceKey = `${socketUsers.get(socket.id)?.userId}:${callId}`;
    joinCallDebounce.delete(debounceKey);
  }
});


socket.on('leave_call', async ({ callId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      console.warn(`⚠️ Unauthenticated socket tried to leave call`);
      return;
    }

    // ✅ FIX: Use mutex to prevent race condition with join_call
    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      
      if (!call) {
        console.warn(`⚠️ Call ${callId} not found when ${user.username} tried to leave`);
        socket.leave(`call-${callId}`);
        return;
      }

      // Check if user is actually in the call before removing
      const participantIndex = call.participants.indexOf(user.userId);
      if (participantIndex === -1) {
        console.warn(`⚠️ User ${user.username} not in call ${callId} participants, ignoring leave`);
        socket.leave(`call-${callId}`);
        return;
      }

      // ✅ Atomic removal inside mutex - prevents race conditions
      call.participants.splice(participantIndex, 1);
      userCalls.delete(user.userId);
      call.userMediaStates.delete(user.userId);

      console.log(`📵 User ${user.username} left call ${callId}`);
      console.log(`📊 Remaining participants: [${call.participants.join(', ')}] (${call.participants.length} total)`);

      socket.leave(`call-${callId}`);

      // Notify others that user left
      io.to(`call-${callId}`).emit('user_left_call', {
        userId: user.userId,
        username: user.username
      });
      console.log(`📢 Notified others in call-${callId} that ${user.username} left`);

      const room = matchmaking.getRoom(call.roomId);
      if (room) {
        io.to(call.roomId).emit('call_state_update', {
          callId: callId,
          isActive: call.participants.length > 0,
          participantCount: call.participants.length
        });
        console.log(`📢 Broadcasted call_state_update to room ${call.roomId}: active=${call.participants.length > 0}, count=${call.participants.length}`);
      } else {
        console.warn(`⚠️ Room ${call.roomId} not found when broadcasting call state update`);
      }

      if (call.participants.length === 0) {
        console.log(`🕐 Call ${callId} has 0 participants - starting 5s grace period for cleanup`);
        
        if (room) {
          room.setActiveCall(false);
          console.log(`📞 Room ${call.roomId} marked as call-free`);
        }
        
        // Clear any existing grace period before setting new one
        if (callGracePeriod.has(callId)) {
          clearTimeout(callGracePeriod.get(callId));
          console.log(`⏰ Cleared existing grace period for call ${callId}`);
        }
        
        const graceTimeout = setTimeout(() => {
          const currentCall = activeCalls.get(callId);
          
          if (!currentCall) {
            console.log(`ℹ️ Call ${callId} already cleaned up`);
            callGracePeriod.delete(callId);
            return;
          }
          
          if (currentCall.participants.length === 0) {
            console.log(`🗑️ Call ${callId} still empty after grace period - cleaning up`);
            activeCalls.delete(callId);
            callGracePeriod.delete(callId);
            
            if (room) {
              io.to(currentCall.roomId).emit('call_ended_notification', {
                callId: callId
              });
              console.log(`📢 Call ${callId} fully ended, notified room ${currentCall.roomId}`);
            }
          } else {
            console.log(`✅ Call ${callId} has ${currentCall.participants.length} participant(s) again - cleanup cancelled`);
            callGracePeriod.delete(callId);
            
            if (room) {
              room.setActiveCall(true);
              console.log(`📞 Room ${call.roomId} marked as having active call again`);
            }
          }
        }, 5000);
        
        callGracePeriod.set(callId, graceTimeout);
        
      } else {
        console.log(`✅ Call ${callId} still active with ${call.participants.length} participant(s)`);
        
        if (room && !room.hasActiveCall) {
          room.setActiveCall(true);
          console.log(`📞 Room ${call.roomId} re-marked as having active call`);
        }
      }
    }); // ✅ Mutex released here

  } catch (error) {
    console.error('❌ Leave call error:', error);
    socket.emit('error', { message: 'Failed to leave call properly' });
  }
});


socket.on('join_existing_call', async ({ callId, roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
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
    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      
      if (!call) {
        console.error(`❌ Call ${callId} not found`);
        socket.emit('error', { 
          message: 'Call not found or has ended',
          code: 'CALL_NOT_FOUND'
        });
        return;
      }

      // Validate call state
      const validation = validateCallState(call, 'join_existing_call');
      if (!validation.valid) {
        socket.emit('error', { message: validation.error });
        return;
      }

      if (call.roomId !== roomId) {
        console.error(`❌ Call ${callId} is in different room (${call.roomId} vs ${roomId})`);
        socket.emit('error', { 
          message: 'Call is in a different room',
          code: 'WRONG_ROOM'
        });
        return;
      }

      // CRITICAL FIX: Don't check participant count - allow joining even if empty
      // This handles the case where all users left but call is still "active"
      if (call.status === 'ended') {
        console.error(`❌ Call ${callId} has ended`);
        socket.emit('error', { 
          message: 'Call has ended',
          code: 'CALL_ENDED'
        });
        return;
      }

      // Check if user is in the room
      const room = matchmaking.getRoom(roomId);
      if (!room) {
        console.error(`❌ Room ${roomId} not found`);
        socket.emit('error', { 
          message: 'Room not found',
          code: 'ROOM_NOT_FOUND'
        });
        return;
      }

      if (!room.hasUser(user.userId)) {
        console.error(`❌ User ${user.username} not in room ${roomId}`);
        socket.emit('error', { 
          message: 'You are not in this room',
          code: 'NOT_IN_ROOM'
        });
        return;
      }

      console.log(`✅ User ${user.username} authorized to join call ${callId}`);
      console.log(`📊 Current participants BEFORE add: [${call.participants.join(', ')}] (${call.participants.length} total)`);

      // CRITICAL FIX: Add user to participants atomically within mutex
      if (!call.participants.includes(user.userId)) {
        call.participants.push(user.userId);
        userCalls.set(user.userId, callId);
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
    }); // CRITICAL: Mutex releases HERE - state is now consistent

    // CRITICAL FIX: Emit success and broadcast AFTER mutex completes
    const call = activeCalls.get(callId);
    if (call && call.participants.includes(user.userId)) {
      // Send success response
      socket.emit('join_existing_call_success', {
        callId,
        callType: call.callType,
        roomId: call.roomId
      });

      console.log(`✅ Sent join_existing_call_success to ${user.username} (after mutex release)`);
      console.log(`   User will now navigate to call page`);
      
      // Broadcast updated call state to room
      io.to(roomId).emit('call_state_update', {
        callId: callId,
        isActive: true,
        participantCount: call.participants.length,
        callType: call.callType
      });
      console.log(`📢 Broadcasted call_state_update to room: ${call.participants.length} participant(s)`);
    } else {
      console.error(`❌ CRITICAL: User ${user.username} not in participants after mutex!`);
      socket.emit('error', { 
        message: 'Failed to add you to the call',
        code: 'JOIN_FAILED'
      });
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

// Replace webrtc_offer handler (line 2826)
socket.on('webrtc_offer', ({ callId, targetUserId, offer }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    // ✅ FIX: Rate limiting
    if (!checkSignalingRateLimit(user.userId)) {
      console.warn(`⚠️ Signaling rate limit exceeded for ${user.username}`);
      socket.emit('error', { 
        message: 'Too many signaling messages. Please slow down.',
        code: 'RATE_LIMIT_EXCEEDED'
      });
      return;
    }

    // ✅ FIX: Validate offer structure
    if (!offer || typeof offer !== 'object') {
      console.error(`❌ Invalid offer structure from ${user.username}`);
      return;
    }

    // ✅ FIX: Validate SDP size and structure
    const sdpValidation = validateSDP(offer.sdp);
    if (!sdpValidation.valid) {
      console.error(`❌ Invalid SDP from ${user.username}: ${sdpValidation.error}`);
      socket.emit('error', { 
        message: 'Invalid WebRTC offer',
        code: 'INVALID_OFFER'
      });
      return;
    }

    const offerKey = `${callId}:${user.userId}:${targetUserId}`;
    const now = Date.now();
    
    // Check for duplicate offer within time window
    if (activeOffers.has(offerKey)) {
      const lastOfferTime = activeOffers.get(offerKey);
      if (now - lastOfferTime < OFFER_DEDUPE_WINDOW) {
        console.warn(`⚠️ Duplicate offer from ${user.username} to ${targetUserId} within ${now - lastOfferTime}ms, ignoring`);
        return;
      }
    }
    
    // Track this offer
    activeOffers.set(offerKey, now);
    
    console.log(`📤 WebRTC offer from ${user.username} to ${targetUserId}`);
    console.log(`   Offer SDP length: ${offer.sdp.length} bytes (validated)`);

    const targetSocket = findActiveSocketForUser(targetUserId);
    
    if (targetSocket) {
      targetSocket.emit('webrtc_offer', {
        fromUserId: user.userId,
        offer: {
          type: offer.type,
          sdp: offer.sdp
        }
      });
      console.log(`✅ Offer forwarded to ${targetUserId}`);
    } else {
      console.warn(`⚠️ Target user ${targetUserId} not found for offer`);
    }
    
    // Clean up old offers after window expires
    setTimeout(() => {
      activeOffers.delete(offerKey);
    }, OFFER_DEDUPE_WINDOW);

  } catch (error) {
    console.error('❌ WebRTC offer error:', error);
  }
});

// Replace webrtc_answer handler (line 2876)
socket.on('webrtc_answer', ({ callId, targetUserId, answer }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    // ✅ FIX: Rate limiting
    if (!checkSignalingRateLimit(user.userId)) {
      console.warn(`⚠️ Signaling rate limit exceeded for ${user.username}`);
      socket.emit('error', { 
        message: 'Too many signaling messages. Please slow down.',
        code: 'RATE_LIMIT_EXCEEDED'
      });
      return;
    }

    // ✅ FIX: Validate answer structure
    if (!answer || typeof answer !== 'object') {
      console.error(`❌ Invalid answer structure from ${user.username}`);
      return;
    }

    // ✅ FIX: Validate SDP size and structure
    const sdpValidation = validateSDP(answer.sdp);
    if (!sdpValidation.valid) {
      console.error(`❌ Invalid SDP from ${user.username}: ${sdpValidation.error}`);
      socket.emit('error', { 
        message: 'Invalid WebRTC answer',
        code: 'INVALID_ANSWER'
      });
      return;
    }

    // Deduplication
    const answerKey = `${user.userId}:${targetUserId}`;
    const now = Date.now();
    
    if (answerDebounce.has(answerKey)) {
      const lastAnswerTime = answerDebounce.get(answerKey);
      if (now - lastAnswerTime < ANSWER_DEDUPE_WINDOW) {
        console.warn(`⚠️ Duplicate answer from ${user.username} to ${targetUserId} within ${now - lastAnswerTime}ms, ignoring`);
        return;
      }
    }
    
    // Track this answer
    answerDebounce.set(answerKey, now);
    
    console.log(`📤 WebRTC answer from ${user.username} to ${targetUserId}`);
    console.log(`   Answer SDP length: ${answer.sdp.length} bytes (validated)`);

    const targetSocket = findActiveSocketForUser(targetUserId);
    
    if (targetSocket) {
      targetSocket.emit('webrtc_answer', {
        fromUserId: user.userId,
        answer: {
          type: answer.type,
          sdp: answer.sdp
        }
      });
      console.log(`✅ Answer forwarded to ${targetUserId}`);
    } else {
      console.warn(`⚠️ Target user ${targetUserId} not found for answer`);
    }
    
    // Clean up after window expires
    setTimeout(() => {
      answerDebounce.delete(answerKey);
    }, ANSWER_DEDUPE_WINDOW);

  } catch (error) {
    console.error('❌ WebRTC answer error:', error);
  }
});


socket.on('ice_candidate', ({ callId, targetUserId, candidate }) => {
  try {
    const user = socketUsers.get(socket.id);
    
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

    const targetSocket = findActiveSocketForUser(targetUserId);
    
    if (targetSocket) {
      targetSocket.emit('ice_candidate', {
        fromUserId: user.userId,
        candidate: candidate
      });
      console.log(`✅ [ICE] Candidate forwarded to ${targetUserId}`);
    } else {
      console.warn(`⚠️ [ICE] Target user ${targetUserId} not found for ICE candidate`);
    }

  } catch (error) {
    console.error('❌ [ICE] Candidate error:', error);
  }
});

  socket.on('connection_state_update', ({ callId, state, candidateType }) => {
    const user = socketUsers.get(socket.id);
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

  socket.on('speaking_state', ({ callId, speaking }) => {
    try {
      const user = socketUsers.get(socket.id);
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
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      if (!call) return;

      if (!call.userMediaStates) call.userMediaStates = new Map();
      
      const currentState = call.userMediaStates.get(user.userId) || { 
        videoEnabled: call.callType === 'video', 
        audioEnabled: true 
      };
      
      call.userMediaStates.set(user.userId, {
        ...currentState,
        audioEnabled: enabled
      });
      
      console.log(`🎤 ${user.username} audio: ${enabled ? 'ON' : 'OFF'} (call ${callId})`);

      // Broadcast to ALL users in call room
      io.to(`call-${callId}`).emit('audio_state_changed', {
        userId: user.userId,
        enabled
      });
    });

  } catch (error) {
    console.error('❌ Audio state error:', error);
  }
});

socket.on('video_state_changed', async ({ callId, enabled }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      console.warn(`⚠️ Unauthenticated socket tried to change video state`);
      return;
    }

    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      if (!call) {
        console.warn(`⚠️ Call ${callId} not found for video state change`);
        return;
      }
      
      if (!call.userMediaStates) {
        call.userMediaStates = new Map();
        console.log(`📊 Initialized userMediaStates Map for call ${callId}`);
      }
      
      const currentState = call.userMediaStates.get(user.userId) || { 
        videoEnabled: call.callType === 'video', 
        audioEnabled: true 
      };
      
      call.userMediaStates.set(user.userId, {
        ...currentState,
        videoEnabled: enabled
      });
      
      console.log(`📹 ========================================`);
      console.log(`📹 SERVER: VIDEO STATE CHANGE`);
      console.log(`📹 ========================================`);
      console.log(`   User: ${user.username} (${user.userId})`);
      console.log(`   Call: ${callId}`);
      console.log(`   New state: ${enabled ? 'ON' : 'OFF'}`);
      console.log(`   Server state updated`);

      // ✅ FIX: Broadcast to OTHER users only (exclude sender)
      socket.to(`call-${callId}`).emit('video_state_changed', {
        userId: user.userId,
        enabled: enabled
      });
      
      console.log(`📤 Broadcasted to OTHER participants (sender excluded)`);
      console.log(`📹 ========================================\n`);
    });

  } catch (error) {
    console.error('❌ Video state error:', error);
  }
});




socket.on('leave_room', async (data, callback) => {
    if (!currentUser) {
      return callback?.({ success: false, error: 'Not authenticated' });
    }
    
    const { roomId } = data;
    const userId = currentUser.userId;
    
    console.log(`🚪 [UID: ${userId}] Leaving room ${roomId}`);
    
    const releaseLock = await acquireUserLock(userId);
    
    try {
      // Validate room access
      const validation = validateRoomAccess(roomId, userId);
      if (!validation.valid) {
        return callback?.({ success: false, error: validation.error });
      }
      
      const room = validation.room;
      
      // Remove user from room
      room.removeUser(userId);
      
      // CRITICAL: Clear active room state
      clearUserActiveRoom(userId);
      
      // Remove from mood tracking
      removeUserFromMood(userId, room.mood);
      
      // Leave socket room on ALL devices
      const allSocketIds = getUserSocketIds(userId);
      allSocketIds.forEach(socketId => {
        const userSocket = io.sockets.sockets.get(socketId);
        if (userSocket) {
          userSocket.leave(roomId);
        }
      });
      
      // Notify partner
      const partner = room.users.find(u => u.userId !== userId);
      if (partner) {
        // Emit to all partner's devices
        emitToUserAllDevices(partner.userId, 'partner_left', {
          roomId,
          userId,
          username: currentUser.username
        });
      }
      
      // Notify all user's devices
      emitToUserAllDevices(userId, 'room_left', { roomId });
      
      console.log(`✅ [UID: ${userId}] Left room ${roomId} - can now join new rooms`);
      
      callback?.({ success: true });
      
    } catch (error) {
      console.error(`❌ [UID: ${userId}] Leave room error:`, error);
      callback?.({ success: false, error: error.message });
    } finally {
      releaseLock();
    }
  });

socket.on('disconnect', async (reason) => {
    console.log(`🔌 Socket disconnected: ${socket.id} (reason: ${reason})`);
    
    if (!currentUser) return;
    
    const userId = currentUser.userId;
    
    // Unregister this socket
    unregisterSocketForUser(userId, socket.id);
    
    // Clean up socket user data
    socketUsers.delete(socket.id);
    
    // Check if user has other active devices
    const remainingDevices = getUserSocketIds(userId).length;
    
    if (remainingDevices > 0) {
      console.log(`📱 [UID: ${userId}] Still has ${remainingDevices} active device(s) - preserving room state`);
      return; // Don't clean up room - user still connected on other device
    }
    
    console.log(`📱 [UID: ${userId}] Last device disconnected - starting grace period`);
    
    // Start grace period timer
    const cleanup = setTimeout(async () => {
      console.log(`⏰ [UID: ${userId}] Grace period expired - cleaning up`);
      
      // Remove from mood tracking
      removeUserFromAllMoods(userId);
      
      // Get active room
      const activeRoom = getUserActiveRoom(userId);
      
      if (activeRoom) {
        const room = matchmaking.getRoom(activeRoom.roomId);
        if (room && !room.isExpired) {
          // Remove from room
          room.removeUser(userId);
          
          // Notify partner
          const partner = room.users.find(u => u.userId !== userId);
          if (partner) {
            emitToUserAllDevices(partner.userId, 'partner_disconnected', {
              roomId: activeRoom.roomId,
              userId,
              username: currentUser.username
            });
          }
        }
        
        // Clear active room state
        clearUserActiveRoom(userId);
      }
      
      // Final cleanup
      userToSocketId.delete(userId);
      socketUserCleanup.delete(userId);
      
      console.log(`🧹 [UID: ${userId}] Full cleanup completed`);
      
    }, DISCONNECT_GRACE_PERIOD);
    
    socketUserCleanup.set(userId, cleanup);
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
  
  
  let cleanedMoodDebounce = 0;
for (const [mood, timeout] of moodCountBroadcastDebounce.entries()) {
  // If no users in this mood, clear the debounce
  const count = moodUserCounts.get(mood) || 0;
  if (count === 0) {
    clearTimeout(timeout);
    moodCountBroadcastDebounce.delete(mood);
    cleanedMoodDebounce++;
  }
}
if (cleanedMoodDebounce > 0) {
  console.log(`🗑️ Cleaned up ${cleanedMoodDebounce} mood count debounce timers`);
}
  
  try {
    const now = Date.now();
    
    // ✅ FIX: Process in batches to yield to event loop
    const BATCH_SIZE = 50;
    
    // Clean up expired rooms
    const rooms = matchmaking.getActiveRooms();
    console.log(`🧹 Checking ${rooms.length} active rooms`);
    
    for (let i = 0; i < rooms.length; i += BATCH_SIZE) {
      const batch = rooms.slice(i, i + BATCH_SIZE);
      
      for (const room of batch) {
        if (room.expiresAt <= now) {
          console.log(`🕐 Room ${room.id} has expired (${((now - room.expiresAt) / 1000).toFixed(1)}s ago)`);
          await performRoomCleanup(room.id); // ✅ AWAIT since performRoomCleanup is now async (Fix #16)
        }
      }
      
      // ✅ Yield to event loop after each batch
      if (i + BATCH_SIZE < rooms.length) {
        await new Promise(resolve => setImmediate(resolve));
      }
    }
    
    // Clean up orphaned call grace periods
    const orphanedGracePeriods = [];
    for (const [callId, timeout] of callGracePeriod.entries()) {
      if (!activeCalls.has(callId)) {
        orphanedGracePeriods.push(callId);
        clearTimeout(timeout);
      }
    }
    if (orphanedGracePeriods.length > 0) {
      orphanedGracePeriods.forEach(id => callGracePeriod.delete(id));
      console.log(`🗑️ Cleaned up ${orphanedGracePeriods.length} orphaned call grace periods`);
    }
    
    // ✅ Yield to event loop
    await new Promise(resolve => setImmediate(resolve));
    
    // Clean up orphaned room cleanup timers
    const orphanedRoomTimers = [];
    for (const [roomId, timeout] of roomCleanupTimers.entries()) {
      if (!matchmaking.getRoom(roomId)) {
        orphanedRoomTimers.push(roomId);
        clearTimeout(timeout);
      }
    }
    if (orphanedRoomTimers.length > 0) {
      orphanedRoomTimers.forEach(id => roomCleanupTimers.delete(id));
      console.log(`🗑️ Cleaned up ${orphanedRoomTimers.length} orphaned room timers`);
    }
    
    // ✅ Yield to event loop
    await new Promise(resolve => setImmediate(resolve));
    
    // Clean up stale socketUserCleanup entries
    const staleCleanups = [];
    for (const [socketId, timeout] of socketUserCleanup.entries()) {
      if (!socketUsers.has(socketId) && !io.sockets.sockets.has(socketId)) {
        staleCleanups.push(socketId);
        clearTimeout(timeout);
      }
    }
    if (staleCleanups.length > 0) {
      staleCleanups.forEach(id => socketUserCleanup.delete(id));
      console.log(`🗑️ Cleaned up ${staleCleanups.length} stale socket cleanup entries`);
    }
    
    // ✅ Yield to event loop
    await new Promise(resolve => setImmediate(resolve));
    
    // Clean up orphaned mutex entries
    const orphanedMutexes = [];
    for (const [callId] of callMutexes.entries()) {
      if (!activeCalls.has(callId)) {
        orphanedMutexes.push(callId);
      }
    }
    if (orphanedMutexes.length > 0) {
      orphanedMutexes.forEach(id => callMutexes.delete(id));
      console.log(`🗑️ Cleaned up ${orphanedMutexes.length} orphaned call mutexes`);
    }
    
    // ✅ FIX #16: Clean up orphaned room call init locks
    let orphanedRoomLocks = 0;
    for (const roomId of roomCallInitLocks.keys()) {
      if (!matchmaking.getRoom(roomId)) {
        roomCallInitLocks.delete(roomId);
        orphanedRoomLocks++;
      }
    }
    if (orphanedRoomLocks > 0) {
      console.log(`🗑️ Cleaned up ${orphanedRoomLocks} orphaned room call init locks`);
    }
    
    // Clean up expired signaling rate limit entries
    let expiredSignaling = 0;
    for (const [userId, limit] of signalingRateLimiter.entries()) {
      if (now > limit.resetTime) {
        signalingRateLimiter.delete(userId);
        expiredSignaling++;
      }
    }
    if (expiredSignaling > 0) {
      console.log(`🗑️ Cleaned up ${expiredSignaling} expired signaling rate limit entries`);
    }
    
    // ✅ Yield to event loop
    await new Promise(resolve => setImmediate(resolve));
    
    // Clean up stale file transfers
    let staleTransfers = 0;
    for (const [fileId, transfer] of activeFileTransfers.entries()) {
      if (now - transfer.startTime > MAX_TRANSFER_TIME) {
        activeFileTransfers.delete(fileId);
        staleTransfers++;
      }
    }
    if (staleTransfers > 0) {
      console.log(`🗑️ Cleaned up ${staleTransfers} stale file transfers`);
    }
    
    // Clean up expired offers
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
    
    // Clean up expired answer debounce
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
    
    // Clean up expired join debounce
    let expiredJoins = 0;
    for (const [key, timestamp] of joinCallDebounce.entries()) {
      if (now - timestamp > 5000) {
        joinCallDebounce.delete(key);
        expiredJoins++;
      }
    }
    if (expiredJoins > 0) {
      console.log(`🗑️ Cleaned up ${expiredJoins} expired join debounce entries`);
    }
    
    // Clean up expired room join states
    let expiredRoomJoins = 0;
    for (const [key, data] of roomJoinState.entries()) {
      if (now - data.timestamp > 15000) {
        roomJoinState.delete(key);
        expiredRoomJoins++;
      }
    }
    if (expiredRoomJoins > 0) {
      console.log(`🗑️ Cleaned up ${expiredRoomJoins} expired room join states`);
    }
    
    // ✅ Clean up room message rate limiter
    let expiredRoomRates = 0;
    for (const [roomId, limit] of roomMessageRateLimiter.entries()) {
      if (now > limit.resetTime) {
        roomMessageRateLimiter.delete(roomId);
        expiredRoomRates++;
      }
    }
    if (expiredRoomRates > 0) {
      console.log(`🗑️ Cleaned up ${expiredRoomRates} expired room rate limit entries`);
    }
    
    // ✅ FIX #10: Verify connectionsByIP accuracy
    let orphanedIPs = 0;
    for (const [ip, ipData] of connectionsByIP.entries()) {
      const validConnections = new Set();
      for (const socketId of ipData.connections) {
        if (io.sockets.sockets.has(socketId)) {
          validConnections.add(socketId);
        }
      }
      
      if (validConnections.size !== ipData.connections.size) {
        console.log(`🔧 Fixed connection count for IP ${ip}: ${ipData.connections.size} → ${validConnections.size}`);
        ipData.connections = validConnections;
        ipData.count = validConnections.size;
        
        if (ipData.count === 0) {
          connectionsByIP.delete(ip);
          orphanedIPs++;
        }
      }
    }
    if (orphanedIPs > 0) {
      console.log(`🗑️ Removed ${orphanedIPs} orphaned IP entries`);
    }
    
    // ✅ FIX #10: Clean up expired connection rate limiters
    let cleanedRateLimiters = 0;
    for (const [ip, limiter] of connectionRateLimiter.entries()) {
      if (now > limiter.resetTime) {
        connectionRateLimiter.delete(ip);
        cleanedRateLimiters++;
      }
    }
    if (cleanedRateLimiters > 0) {
      console.log(`🗑️ Cleaned up ${cleanedRateLimiters} expired connection rate limiters`);
    }
    
    
    // ✅ Audit mood registry for orphaned users
let orphanedUsers = 0;
for (const [mood, userSet] of moodUserRegistry.entries()) {
  const validUsers = new Set();
  
  for (const userId of userSet) {
    // Check if user still has active socket
    const hasActiveSocket = Array.from(socketUsers.values()).some(u => u.userId === userId);
    
    if (hasActiveSocket) {
      validUsers.add(userId);
    } else {
      orphanedUsers++;
      console.log(`🗑️ Removing orphaned user ${userId} from ${mood}`);
    }
  }
  
  // Replace with validated set
  if (validUsers.size !== userSet.size) {
    moodUserRegistry.set(mood, validUsers);
    const newCount = validUsers.size;
    moodUserCounts.set(mood, newCount);
    debouncedBroadcastMoodCount(mood);
  }
}

if (orphanedUsers > 0) {
  console.log(`🗑️ Cleaned up ${orphanedUsers} orphaned user(s) from mood tracking`);
}

// Clean up orphaned userCurrentMood entries
let orphanedMoodMappings = 0;
for (const [userId] of userCurrentMood.entries()) {
  const hasActiveSocket = Array.from(socketUsers.values()).some(u => u.userId === userId);
  
  if (!hasActiveSocket) {
    userCurrentMood.delete(userId);
    orphanedMoodMappings++;
  }
}

if (orphanedMoodMappings > 0) {
  console.log(`🗑️ Cleaned up ${orphanedMoodMappings} orphaned mood mapping(s)`);
}

// Clean up mood count broadcast debounce timers
let cleanedMoodDebounce = 0;
for (const [mood, timeout] of moodCountBroadcastDebounce.entries()) {
  const count = moodUserCounts.get(mood) || 0;
  if (count === 0) {
    clearTimeout(timeout);
    moodCountBroadcastDebounce.delete(mood);
    cleanedMoodDebounce++;
  }
}
if (cleanedMoodDebounce > 0) {
  console.log(`🗑️ Cleaned up ${cleanedMoodDebounce} mood count debounce timers`);
}
    
    
    // Log memory stats
    const cleanupDuration = Date.now() - startTime;
    console.log(`📊 Periodic cleanup completed in ${cleanupDuration}ms`);
    console.log(`📊 Memory stats:
    - Active sockets: ${socketUsers.size}
    - Global connections: ${io.engine.clientsCount}/${MAX_CONNECTIONS_GLOBAL}
    - Unique IPs connected: ${connectionsByIP.size}
    - Connection rate limiters: ${connectionRateLimiter.size}
    - Active calls: ${activeCalls.size}
    - Call grace periods: ${callGracePeriod.size}
    - Room cleanup timers: ${roomCleanupTimers.size}
    - Room call init locks: ${roomCallInitLocks.size}
    - Active offers: ${activeOffers.size}
    - Call mutexes: ${callMutexes.size}
    - Join debounce: ${joinCallDebounce.size}
    - Room join states: ${roomJoinState.size}
    - User-to-socket index: ${userToSocketId.size}
    - Signaling rate limiter: ${signalingRateLimiter.size}
    - File chunk rate limiter: ${fileChunkRateLimiter.size}
    - Room message rate limiter: ${roomMessageRateLimiter.size}
    - Active file transfers: ${activeFileTransfers.size}/${MAX_CONCURRENT_TRANSFERS}
    - File transfer memory: ${(getCurrentTransferMemory() / 1024 / 1024).toFixed(2)}MB/${(MAX_MEMORY_FOR_TRANSFERS / 1024 / 1024).toFixed(2)}MB`);
    
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
  
  // Clean up expired join debounce (> 5 seconds old)
  let expiredJoins = 0;
  for (const [key, timestamp] of joinCallDebounce.entries()) {
    if (now - timestamp > 5000) {
      joinCallDebounce.delete(key);
      expiredJoins++;
    }
  }
  if (expiredJoins > 0) {
    console.log(`🗑️ Cleaned up ${expiredJoins} expired join debounce entries`);
  }
  
  // Clean up expired room join states (> 15 seconds old)
  let expiredRoomJoins = 0;
  for (const [key, data] of roomJoinState.entries()) {
    if (now - data.timestamp > 15000) {
      roomJoinState.delete(key);
      expiredRoomJoins++;
    }
  }
  if (expiredRoomJoins > 0) {
    console.log(`🗑️ Cleaned up ${expiredRoomJoins} expired room join states`);
  }
  
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
      
      // Notify all connected clients and wait for acknowledgments
      for (const [socketId, user] of socketUsers.entries()) {
        const clientSocket = io.sockets.sockets.get(socketId);
        if (clientSocket && clientSocket.connected) {
          const ackPromise = new Promise((resolve) => {
            const timeout = setTimeout(() => {
              console.log(`⚠️ Shutdown ack timeout for ${user.username}`);
              resolve();
            }, 8000); // 8-second timeout per client
            
            clientSocket.emit('server_shutdown', { 
              message: 'Server is shutting down for maintenance',
              reconnectIn: 10000 
            }, () => {
              clearTimeout(timeout);
              ackCount++;
              console.log(`✅ Shutdown ack received from ${user.username}`);
              resolve();
            });
          });
          
          shutdownPromises.push(ackPromise);
        }
      }
      
      console.log(`📢 Notified ${socketUsers.size} connected clients, waiting for acknowledgments...`);
      
      // ✅ FIX: Wait for all clients or 10-second timeout (whichever comes first)
      await Promise.race([
        Promise.all(shutdownPromises),
        new Promise(resolve => setTimeout(resolve, 10000))
      ]);
      
      console.log(`✅ Received ${ackCount}/${socketUsers.size} client acknowledgments`);
      
      // Clean up all timers
      let timerCount = 0;
      roomCleanupTimers.forEach(timer => {
        clearTimeout(timer);
        timerCount++;
      });
      callGracePeriod.forEach(timer => {
        clearTimeout(timer);
        timerCount++;
      });
      socketUserCleanup.forEach(timer => {
        clearTimeout(timer);
        timerCount++;
      });
      
      console.log(`✅ Cleaned up ${timerCount} timers`);
      
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
