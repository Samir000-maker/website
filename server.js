
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
const userToSocketId = new Map()
const socketUsers = new Map();
// Add at top with other Maps
const joinCallDebounce = new Map(); // userId -> timestamp



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
  // Wait for any pending operation on this call
  while (callMutexes.has(callId)) {
    await callMutexes.get(callId);
  }
  
  // Create new mutex with guaranteed cleanup
  const mutexPromise = (async () => {
    try {
      return await operation();
    } catch (error) {
      // CRITICAL FIX: Log error but still clean up mutex
      console.error(`❌ Mutex operation failed for call ${callId}:`, error);
      throw error; // Re-throw after logging
    } finally {
      // CRITICAL: Always delete mutex, even on error
      callMutexes.delete(callId);
      console.log(`🔓 Released mutex for call ${callId}`);
    }
  })();
  
  callMutexes.set(callId, mutexPromise);
  return mutexPromise;
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
// CLOUDFLARE TURN SERVER CONFIGURATION
// ============================================

async function generateCloudTurnCredentials() {
  const TURN_TOKEN_ID = process.env.CLOUDFLARE_TURN_TOKEN_ID || '726fccae33334279a71e962da3d8e01c';
  const TURN_API_TOKEN = process.env.CLOUDFLARE_TURN_API_TOKEN || 'a074b71f6fa5853f4daff0fafb57c9bee54faae54f4012fab0b55720910440cf';

  if (!TURN_TOKEN_ID || !TURN_API_TOKEN) {
    console.warn('⚠️ TURN credentials not configured - operating with STUN only');
    return null;
  }

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
        })
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error('❌ Failed to generate TURN credentials:', response.status, errorText);
      return null;
    }

    const data = await response.json();
    
    console.log('📦 Raw TURN response:', JSON.stringify(data, null, 2));
    
    // Cloudflare returns: { iceServers: { urls: [...], username: '...', credential: '...' } }
    if (data.iceServers) {
      const turnConfig = data.iceServers;
      
      // Convert to RTCIceServer format
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
      
      return [iceServer]; // Return as array
    } else {
      console.error('❌ Unexpected TURN response structure:', data);
      return null;
    }
  } catch (error) {
    console.error('❌ Error generating TURN credentials:', error.message);
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

// WebRTC metrics
const webrtcMetrics = {
  totalCalls: 0,
  successfulConnections: 0,
  failedConnections: 0,
  turnUsage: 0,
  stunUsage: 0,
  directConnections: 0
};


const activeOffers = new Map(); // callId:userId -> offerTimestamp
const OFFER_DEDUPE_WINDOW = 2000; // 2 seconds

// ============================================
// ROOM CLEANUP SYSTEM
// ============================================

function scheduleRoomCleanup(roomId, expiresAt) {
  // Clear existing timer if any
  if (roomCleanupTimers.has(roomId)) {
    clearTimeout(roomCleanupTimers.get(roomId));
  }

  const now = Date.now();
  const timeUntilExpiry = expiresAt - now;

  if (timeUntilExpiry <= 0) {
    // Room already expired
    console.log(`⏰ Room ${roomId} already expired, cleaning up immediately`);
    performRoomCleanup(roomId);
    return;
  }

  console.log(`⏰ Scheduled cleanup for room ${roomId} in ${Math.round(timeUntilExpiry / 1000)}s`);

  const timer = setTimeout(() => {
    console.log(`⏰ Room ${roomId} expiry timer triggered`);
    performRoomCleanup(roomId);
  }, timeUntilExpiry + ROOM_CLEANUP_GRACE);

  roomCleanupTimers.set(roomId, timer);
}

function performRoomCleanup(roomId) {
  const room = matchmaking.getRoom(roomId);
  
  if (!room) {
    console.log(`🗑️ Room ${roomId} not found, already cleaned up`);
    roomCleanupTimers.delete(roomId);
    return;
  }

  console.log(`🗑️ Cleaning up room ${roomId} (${room.users.length} users)`);

  // CRITICAL FIX: Cancel ANY pending room lifecycle timers in the room object itself
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

  // Notify all users in the room
  room.users.forEach(user => {
    const userSocket = findActiveSocketForUser(user.userId);
    if (userSocket) {
      userSocket.emit('room_expired', {
        roomId,
        message: 'Chat room has expired'
      });
      userSocket.leave(roomId);
    }
  });

  // Clean up any active calls in this room
  activeCalls.forEach((call, callId) => {
    if (call.roomId === roomId) {
      console.log(`🗑️ Cleaning up call ${callId} in expired room`);
      
      call.participants.forEach(userId => {
        userCalls.delete(userId);
      });
      
      activeCalls.delete(callId);
      
      if (callGracePeriod.has(callId)) {
        clearTimeout(callGracePeriod.get(callId));
        callGracePeriod.delete(callId);
      }
    }
  });

  // Remove the room from matchmaking
  matchmaking.destroyRoom(roomId);

  // Clear the cleanup timer
  roomCleanupTimers.delete(roomId);

  console.log(`✅ Room ${roomId} fully cleaned up and destroyed`);
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
    webrtcMetrics,
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
    const existingUser = await db.collection('users').findOne(
      { username: trimmedUsername },
      { projection: { _id: 1 } }
    );

    if (existingUser) {
      const suggestions = [];
      for (let i = 0; i < 3; i++) {
        const suffix = Math.floor(Math.random() * 999) + 1;
        const suggestion = `${trimmedUsername}_${suffix}`;
        const suggestionExists = await db.collection('users').findOne(
          { username: suggestion },
          { projection: { _id: 1 } }
        );
        if (!suggestionExists) {
          suggestions.push(suggestion);
        }
      }
      return res.json({ available: false, suggestions });
    }

    res.json({ available: true });
  } catch (error) {
    console.error('Check username error:', error);
    res.status(500).json({ 
      available: false,
      error: 'Internal server error'
    });
  }
});


// NEW ENDPOINT: Check if user profile exists and has username
app.post('/api/users/check-profile', authenticateFirebase, async (req, res) => {
  try {
    const firebaseUser = req.firebaseUser;
    const db = getDB();

    // Find user in database by email
    const user = await db.collection('users').findOne(
      { email: firebaseUser.email },
      { projection: { username: 1, pfpUrl: 1, _id: 1 } }
    );

    if (!user) {
      // User doesn't exist in database yet
      return res.json({
        exists: false,
        hasUsername: false
      });
    }

    // Check if user has username
    const hasUsername = !!(user.username && user.username.trim());

    return res.json({
      exists: true,
      hasUsername: hasUsername,
      username: user.username || null,
      userId: user._id.toString()
    });

  } catch (error) {
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
    
    // Check if user already exists by email
    const existingUser = await db.collection('users').findOne({ 
      email: firebaseUser.email 
    });

    // Check if username is taken by someone else
    if (existingUser && existingUser.username !== trimmedUsername) {
      const usernameExists = await db.collection('users').findOne({
        username: trimmedUsername
      });
      if (usernameExists) {
        return res.status(400).json({ error: 'Username already taken' });
      }
    } else if (!existingUser) {
      // New user - check if username is available
      const usernameExists = await db.collection('users').findOne({
        username: trimmedUsername
      });
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
      // Update existing user
      await db.collection('users').updateOne(
        { _id: existingUser._id },
        { $set: userData }
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
      const result = await db.collection('users').insertOne(userData);
      res.json({ 
        success: true, 
        userId: result.insertedId.toString(),
        message: 'Profile created' 
      });
    }
  } catch (error) {
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
      const user = await db.collection('users').findOne({ 
        email: firebaseUser.email 
      });

      if (!user) {
        return res.status(404).json({ error: 'User not found' });
      }

      const pfpUrl = await uploadProfilePicture(
        req.file.buffer,
        req.file.mimetype,
        user._id.toString()
      );

      await db.collection('users').updateOne(
        { _id: user._id },
        { $set: { pfpUrl, updatedAt: new Date() } }
      );

      const updatedUser = { ...user, pfpUrl };
      await updateUserProfileCache(user._id.toString(), updatedUser);

      res.json({ success: true, pfpUrl });
    } catch (error) {
      console.error('Upload PFP error:', error);
      res.status(500).json({ error: 'Failed to upload profile picture' });
    }
  }
);

app.get('/api/users/me', authenticateFirebase, async (req, res) => {
  try {
    const firebaseUser = req.firebaseUser;
    const db = getDB();
    const user = await db.collection('users').findOne(
      { email: firebaseUser.email },
      { projection: { password: 0 } }
    );

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.json(user);
  } catch (error) {
    console.error('Get profile error:', error);
    res.status(500).json({ error: 'Internal server error' });
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
    const user = await db.collection('users').findOne({ 
      email: firebaseUser.email 
    });

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

    const result = await db.collection('notes').insertOne(note);
    res.json({ success: true, noteId: result.insertedId });
  } catch (error) {
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

    const db = getDB();
    const notes = await db.collection('notes')
      .find({})
      .sort({ createdAt: -1 })
      .skip(page * limit)
      .limit(limit)
      .project({ 
        username: 1, 
        pfpUrl: 1, 
        text: 1, 
        mood: 1, 
        createdAt: 1 
      })
      .toArray();

    const total = await db.collection('notes').countDocuments();

    res.json({ 
      notes, 
      page, 
      limit, 
      total,
      hasMore: (page + 1) * limit < total
    });
  } catch (error) {
    console.error('Get notes error:', error);
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
    console.log('🔐 Authenticating socket for userId:', userId);

    let decodedToken;
    try {
      decodedToken = await verifyToken(token);
    } catch (error) {
      console.error('❌ Token verification failed:', error);
      socket.emit('auth_error', { message: 'Invalid or expired token' });
      return;
    }

    const db = getDB();
    const user = await db.collection('users').findOne({ 
      _id: new ObjectId(userId) 
    });

    if (!user) {
      socket.emit('auth_error', { message: 'User not found' });
      return;
    }

    socketUsers.set(socket.id, {
      userId: user._id.toString(),
      username: user.username,
      pfpUrl: user.pfpUrl,
      email: user.email
    });

    // CRITICAL FIX: Maintain reverse lookup index
    userToSocketId.set(user._id.toString(), socket.id);

    socket.emit('authenticated', { 
      success: true, 
      user: {
        userId: user._id.toString(),
        username: user.username,
        pfpUrl: user.pfpUrl
      }
    });

    console.log('✅ Socket authenticated:', user.username, socket.id);
  } catch (error) {
    console.error('Socket auth error:', error);
    socket.emit('auth_error', { message: 'Authentication failed' });
  }
});


// ================== DISCONNECT CLEANUP ==================
socket.on('disconnect', () => {
  const user = socketUsers.get(socket.id);

  // Remove reverse lookup
  if (user?.userId) {
    userToSocketId.delete(user.userId);
    console.log(`🗑️ Cleaned up userToSocketId for ${user.username}`);
  }

  // Remove socket mapping
  socketUsers.delete(socket.id);

  console.log('🔌 Socket disconnected:', socket.id);
});

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

    // CRITICAL FIX: Atomic matchmaking with immediate retry
    let room = matchmaking.addToQueue({
      ...user,
      mood,
      socketId: socket.id
    });

    // CRITICAL: If we got queued but queue is now full, try matching again
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
      console.log(`🎉 Match found! Room ${room.id} with ${room.users.length} users`);

      // CRITICAL FIX: Deduplicate users in room (prevent double-add race condition)
      const uniqueUsers = new Map();
      room.users.forEach(roomUser => {
        uniqueUsers.set(roomUser.userId, roomUser);
      });
      room.users = Array.from(uniqueUsers.values());

      room.users.forEach(roomUser => {
        const userSocket = findActiveSocketForUser(roomUser.userId);
        
        if (userSocket) {
          console.log(`📤 Emitting match_found to ${roomUser.username} on socket ${userSocket.id}`);
          
          userSocket.join(room.id);
          console.log(`✅ User ${roomUser.username} joined Socket.IO room ${room.id}`);
          
          userSocket.emit('match_found', {
            roomId: room.id,
            mood: room.mood,
            users: room.users.map(u => ({
              userId: u.userId,
              username: u.username,
              pfpUrl: u.pfpUrl
            })),
            expiresAt: room.expiresAt
          });
        } else {
          console.error(`❌ No active socket found for user ${roomUser.username} (${roomUser.userId})`);
          // CRITICAL: Remove user from room if socket not found
          matchmaking.leaveRoom(roomUser.userId);
        }
      });

    } else {
      const queuePosition = matchmaking.getQueueStatus(mood);
      socket.emit('queued', { 
        mood, 
        position: queuePosition 
      });
      console.log(`⏳ User ${user.username} queued (${queuePosition}/${config.MAX_USERS_PER_ROOM})`);
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
    
  } catch (error) {
    console.error('Join room error:', error);
    socket.emit('error', { message: 'Failed to join room' });
  }
});

  socket.on('cancel_matchmaking', () => {
    const user = socketUsers.get(socket.id);
    if (user) {
      matchmaking.cancelMatchmaking(user.userId);
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

    console.log('💬 ========================================');
    console.log('💬 CHAT MESSAGE RECEIVED FROM CLIENT');
    console.log('💬 ========================================');
    console.log(`   From: ${user.username} (${user.userId})`);
    console.log(`   Room: ${roomId}`);
    console.log(`   Message: ${message ? message.substring(0, 50) : '[no text]'}`);
    console.log(`   Has attachment: ${!!attachment}`);

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

    // Handle attachment
    if (attachment) {
      console.log('📎 Processing attachment...');
      console.log(`   File: ${attachment.name}`);
      console.log(`   Type: ${attachment.type}`);
      console.log(`   Size: ${(attachment.size / 1024).toFixed(2)} KB`);
      
      if (!attachment.data) {
        console.error('❌ Attachment missing data!');
        socket.emit('error', { 
          message: 'Attachment data missing',
          code: 'INVALID_ATTACHMENT' 
        });
        return;
      }
      
      // Validate file size
      const maxSize = attachment.type.startsWith('image/') ? 10 * 1024 * 1024 : 
                      attachment.type.startsWith('video/') ? 50 * 1024 * 1024 : 
                      20 * 1024 * 1024;
      
      if (attachment.size > maxSize) {
        console.error(`❌ File too large: ${(attachment.size / 1024 / 1024).toFixed(2)} MB`);
        socket.emit('error', { 
          message: 'File too large',
          code: 'FILE_TOO_LARGE'
        });
        return;
      }
      
      // Include attachment WITH data for broadcast
      messageData.attachment = {
        fileId: attachment.fileId,
        name: attachment.name,
        type: attachment.type,
        size: attachment.size,
        data: attachment.data // CRITICAL: Keep for broadcast
      };
      
      console.log('✅ Attachment validated and ready for broadcast');
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
        size: messageData.attachment.size
        // NO data field - saves memory
      };
    }

    // Store to room history
    room.addMessage(storedMessage);
    console.log(`💾 Message stored to room history`);
    
    // ============================================
    // CRITICAL FIX: BROADCAST TO ALL USERS
    // ============================================
    console.log('📡 ========================================');
    console.log('📡 BROADCASTING MESSAGE TO ROOM');
    console.log('📡 ========================================');
    console.log(`   Room: ${roomId}`);
    console.log(`   Users in room: ${room.users.length}`);
    console.log(`   MessageID: ${messageData.messageId}`);
    
    if (messageData.attachment) {
      console.log(`   📎 Broadcasting WITH attachment data`);
      console.log(`      File: ${messageData.attachment.name}`);
      console.log(`      Data size: ${(messageData.attachment.data.length / 1024).toFixed(2)} KB`);
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

    // Check for existing active calls in this room
    let existingCallId = null;
    let existingCall = null;
    activeCalls.forEach((call, callId) => {
      if (call.roomId === roomId && call.participants.length > 0) {
        existingCallId = callId;
        existingCall = call;
      }
    });

    if (existingCallId) {
      console.log(`📞 Call already active in room ${roomId}: ${existingCallId}`);
      console.log(`   Participants: ${existingCall.participants.length}`);
      console.log(`   Suggesting user join instead of creating new call`);
      
      socket.emit('error', { 
        message: 'A call is already in progress',
        code: 'CALL_ALREADY_ACTIVE',
        callId: existingCallId,
        callType: existingCall.callType,
        participantCount: existingCall.participants.length
      });
      return;
    }

    // ... rest of initiate_call handler remains the same
    // Create new call
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
    webrtcMetrics.totalCalls++;

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
    console.log(`📤 Sent call_created to initiator ${user.username} - they will navigate`);

    io.to(roomId).emit('call_state_update', {
      callId: callId,
      isActive: true,
      participantCount: 1,
      callType: callType
    });
    console.log(`📢 Broadcasted call_state_update to room ${roomId}`);

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

    const emitPromises = callUsers.map(roomUser => {
        return new Promise((resolve) => {
          const targetSocket = findActiveSocketForUser(roomUser.userId);
          if (targetSocket) {
            targetSocket.emit('call_accepted', {
              callId,
              callType: call.callType,
              users: callUsers
            });
            console.log(`📤 Sent call_accepted to ${roomUser.username}`);
            resolve();
          } else {
            console.error(`❌ No active socket for ${roomUser.username}`);
            resolve();
          }
        });
      });

      await Promise.all(emitPromises);

      // CRITICAL FIX: Single broadcast instead of duplicate
      broadcastCallStateUpdate(callId);
    });

      io.to(roomId).emit('call_state_update', {
        callId: callId,
        isActive: true,
        participantCount: call.participants.length,
        callType: call.callType
      });
      console.log(`📢 Broadcasted call_state_update: active=true, count=${call.participants.length}`);
    
    
    

  } catch (error) {
    console.error('❌ Accept call error:', error);
    socket.emit('error', { message: 'Failed to accept call' });
  }
});

  socket.on('decline_call', ({ callId, roomId }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) {
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      const call = activeCalls.get(callId);
      
      if (!call) {
        socket.emit('error', { message: 'Call not found' });
        return;
      }

      console.log(`❌ User ${user.username} declined call ${callId}`);

      const initiatorSocket = findActiveSocketForUser(call.initiator);
      if (initiatorSocket) {
        initiatorSocket.emit('call_declined', {
          callId,
          userId: user.userId,
          username: user.username
        });
      }

      if (call.participants.length === 1) {
        activeCalls.delete(callId);
        userCalls.delete(call.initiator);
      }

    } catch (error) {
      console.error('Decline call error:', error);
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

  // Track metrics
  if (connectionType === 'TURN_RELAY') {
    webrtcMetrics.turnUsage++;
    console.warn(`⚠️ [METRICS] TURN usage: ${webrtcMetrics.turnUsage} / ${webrtcMetrics.totalCalls} calls (${((webrtcMetrics.turnUsage / webrtcMetrics.totalCalls) * 100).toFixed(1)}%)`);
  } else if (connectionType === 'STUN_REFLEXIVE') {
    webrtcMetrics.stunUsage++;
    console.log(`✅ [METRICS] STUN usage: ${webrtcMetrics.stunUsage} / ${webrtcMetrics.totalCalls} calls (${((webrtcMetrics.stunUsage / webrtcMetrics.totalCalls) * 100).toFixed(1)}%)`);
  } else if (connectionType === 'DIRECT_HOST') {
    webrtcMetrics.directConnections++;
    console.log(`✅ [METRICS] Direct: ${webrtcMetrics.directConnections} / ${webrtcMetrics.totalCalls} calls (${((webrtcMetrics.directConnections / webrtcMetrics.totalCalls) * 100).toFixed(1)}%)`);
  }

  webrtcMetrics.successfulConnections++;
});


socket.on('join_call', async ({ callId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      socket.emit('error', { message: 'Not authenticated' });
      return;
    }

    // CRITICAL FIX: Debounce rapid join_call requests
    const lastJoinTime = joinCallDebounce.get(user.userId);
    const now = Date.now();
    if (lastJoinTime && now - lastJoinTime < 2000) {
      console.warn(`⚠️ Ignoring duplicate join_call from ${user.username} (${now - lastJoinTime}ms since last)`);
      return;
    }
    joinCallDebounce.set(user.userId, now);

    await withCallMutex(callId, async () => {
      const call = activeCalls.get(callId);
      
      const validation = validateCallState(call, 'join_call');
      if (!validation.valid) {
        socket.emit('error', { message: validation.error });
        joinCallDebounce.delete(user.userId);
        return;
      }

      // CRITICAL FIX: Validate room exists and user is in it
      const room = matchmaking.getRoom(call.roomId);
      if (!room) {
        console.error(`❌ Room ${call.roomId} not found when ${user.username} tried to join call ${callId}`);
        socket.emit('error', { 
          message: 'Room not found or has expired',
          code: 'ROOM_NOT_FOUND'
        });
        joinCallDebounce.delete(user.userId);
        return;
      }

      if (!room.hasUser(user.userId)) {
        console.error(`❌ User ${user.username} not in room ${call.roomId}`);
        socket.emit('error', { 
          message: 'You are not in this room',
          code: 'NOT_IN_ROOM'
        });
        joinCallDebounce.delete(user.userId);
        return;
      }

      // ============================================
      // CRITICAL FIX FOR JOIN BUTTON ISSUE
      // ============================================
      // Add user to participants if not already there
      // This allows the "Join" button to work even if user dismissed the incoming call popup
      if (!call.participants.includes(user.userId)) {
        console.log(`➕ [JOIN_BUTTON_FIX] Adding ${user.username} to call ${callId} participants`);
        console.log(`   Participants before: [${call.participants.join(', ')}]`);
        call.participants.push(user.userId);
        userCalls.set(user.userId, callId);
        console.log(`   Participants after: [${call.participants.join(', ')}]`);
      } else {
        console.log(`ℹ️ User ${user.username} already in call ${callId} participants (re-joining)`);
      }
      // ============================================

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
        joinCallDebounce.delete(user.userId);
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

      // Broadcast to OTHER users that this user joined
      socket.to(`call-${callId}`).emit('user_joined_call', {
        user: {
          userId: user.userId,
          username: user.username,
          pfpUrl: user.pfpUrl,
          videoEnabled: userMediaState.videoEnabled,
          audioEnabled: userMediaState.audioEnabled
        }
      });

      console.log(`✅ ${user.username} successfully joined call ${callId} with ${call.participants.length} total participants`);
      
      // Clear debounce after successful join
      setTimeout(() => {
        joinCallDebounce.delete(user.userId);
      }, 2000);
    });

  } catch (error) {
    console.error('❌ Join call error:', error);
    socket.emit('error', { message: 'Failed to join call' });
    joinCallDebounce.delete(user.userId);
  }
});


socket.on('leave_call', ({ callId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      console.warn(`⚠️ Unauthenticated socket tried to leave call`);
      return;
    }

    const call = activeCalls.get(callId);
    
    if (!call) {
      console.warn(`⚠️ Call ${callId} not found when ${user.username} tried to leave`);
      socket.leave(`call-${callId}`);
      return;
    }

    // CRITICAL FIX: Check if user is actually in the call before removing
    const participantIndex = call.participants.indexOf(user.userId);
    if (participantIndex === -1) {
      console.warn(`⚠️ User ${user.username} not in call ${callId} participants, ignoring leave`);
      socket.leave(`call-${callId}`);
      return;
    }

    // CRITICAL FIX: Atomic removal - prevent race conditions with multiple leave events
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
      
      // CRITICAL FIX: Clear any existing grace period before setting new one
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


  // WebRTC Signaling
socket.on('webrtc_offer', ({ callId, targetUserId, offer }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

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
    console.log(`   Offer SDP length: ${offer.sdp?.length || 0} bytes`);

    const targetSocket = findActiveSocketForUser(targetUserId);
    
    if (targetSocket) {
      targetSocket.emit('webrtc_offer', {
        fromUserId: user.userId,
        offer
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

const answerDebounce = new Map(); // userId:targetUserId -> timestamp
const ANSWER_DEDUPE_WINDOW = 2000; // 2 seconds

// Replace existing webrtc_answer handler
socket.on('webrtc_answer', ({ callId, targetUserId, answer }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    // ✅ DEDUPLICATION: Prevent duplicate answers
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
    console.log(`   Answer SDP length: ${answer.sdp?.length || 0} bytes`);

    const targetSocket = findActiveSocketForUser(targetUserId);
    
    if (targetSocket) {
      targetSocket.emit('webrtc_answer', {
        fromUserId: user.userId,
        answer
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


// server.js - add after line 1647
socket.on('connection_established', ({ callId, connectionType, localType, remoteType, protocol }) => {
  const user = socketUsers.get(socket.id);
  if (!user) return;

  console.log(`📊 [METRICS] Connection established for ${user.username}`);
  console.log(`   Type: ${connectionType}`);
  
  // Track in database or external analytics
  if (connectionType === 'TURN_RELAY') {
    // Send alert - unexpected TURN usage
    console.error(`🚨 ALERT: TURN relay used when direct connection should work`);
  } else {
    console.log(`✅ Optimal connection: ${connectionType}`);
  }
});

socket.on('ice_candidate', ({ callId, targetUserId, candidate }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

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
        candidate
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
      
      // Track metrics based on candidate type
      if (candidateType === 'relay') {
        webrtcMetrics.turnUsage++;
        console.log('   📊 TURN relay connection established');
      } else if (candidateType === 'srflx') {
        webrtcMetrics.stunUsage++;
        console.log('   📊 STUN server-reflexive connection established');
      } else if (candidateType === 'host') {
        webrtcMetrics.directConnections++;
        console.log('   📊 Direct host connection established');
      }
    }

    if (state === 'connected') {
      webrtcMetrics.successfulConnections++;
      console.log(`   ✅ Total successful connections: ${webrtcMetrics.successfulConnections}`);
    } else if (state === 'failed') {
      webrtcMetrics.failedConnections++;
      console.log(`   ❌ Total failed connections: ${webrtcMetrics.failedConnections}`);
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




  socket.on('leave_room', () => {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    const result = matchmaking.leaveRoom(user.userId);
    
    if (result.roomId) {
      socket.leave(result.roomId);

      if (!result.destroyed) {
        io.to(result.roomId).emit('user_left', {
          userId: user.userId,
          username: user.username,
          remainingUsers: result.remainingUsers
        });
      }

      // If room is destroyed, cancel cleanup timer and perform immediate cleanup
      if (result.destroyed) {
        console.log(`🗑️ Room ${result.roomId} destroyed by user leaving`);
        cancelRoomCleanup(result.roomId);
        performRoomCleanup(result.roomId);
      }

      socket.emit('left_room', { roomId: result.roomId });
      console.log(`👋 ${user.username} left room ${result.roomId}`);
    }
  });

socket.on('disconnect', () => {
  const user = socketUsers.get(socket.id);
  
  
    if (user?.userId) {
    joinCallDebounce.delete(user.userId);
    console.log(`🗑️ Cleaned up joinCallDebounce for ${user.username}`);
  }
  
    const offersToDelete = [];
  for (const [key, _] of activeOffers.entries()) {
    if (key.includes(user?.userId)) {
      offersToDelete.push(key);
    }
  }
  offersToDelete.forEach(key => {
    activeOffers.delete(key);
  });
  if (offersToDelete.length > 0) {
    console.log(`🗑️ Cleaned up ${offersToDelete.length} pending offers for ${user?.username || socket.id}`);
  }
  
  if (user) {
    console.log(`🔌 User ${user.username} disconnected`);

    // CRITICAL FIX: Schedule immediate socketUsers cleanup with grace period
    const cleanupTimeout = setTimeout(() => {
      // Check if user reconnected with different socket
      const hasOtherSocket = Array.from(socketUsers.entries()).some(
        ([sid, u]) => sid !== socket.id && u.userId === user.userId
      );
      
      if (!hasOtherSocket) {
        console.log(`🗑️ Cleaning up socketUsers entry for ${user.username} (${socket.id})`);
        socketUsers.delete(socket.id);
      } else {
        console.log(`✅ User ${user.username} has active connection, keeping old socket entry`);
      }
      
      socketUserCleanup.delete(socket.id);
    }, 15000); // 15s grace period
    
    socketUserCleanup.set(socket.id, cleanupTimeout);

    const callId = userCalls.get(user.userId);
    if (callId) {
      const call = activeCalls.get(callId);
      if (call && call.status === 'active') {
        console.log(`⏱️ User ${user.username} in active call ${callId} - grace period for reconnection`);
        
        const graceTimeout = setTimeout(() => {
          console.log(`⏱️ Grace period expired for ${user.username} in call ${callId}`);
          
          const reconnected = Array.from(socketUsers.values()).some(u => u.userId === user.userId);
          
          if (!reconnected) {
            console.log(`❌ User ${user.username} did not rejoin call - removing from call`);
            
            call.participants = call.participants.filter(p => p !== user.userId);
            userCalls.delete(user.userId);
            call.userMediaStates.delete(user.userId);

            io.to(`call-${callId}`).emit('user_left_call', {
              userId: user.userId,
              username: user.username
            });

            if (call.participants.length === 0) {
              console.log(`🕐 Call ${callId} empty - scheduling cleanup`);
              
              const room = matchmaking.getRoom(call.roomId);
              if (room) {
                room.setActiveCall(false);
              }
              
              setTimeout(() => {
                const currentCall = activeCalls.get(callId);
                if (currentCall && currentCall.participants.length === 0) {
                  activeCalls.delete(callId);
                  console.log(`🗑️ Call ${callId} fully cleaned up`);
                  
                  if (room) {
                    io.to(call.roomId).emit('call_ended_notification', {
                      callId: callId
                    });
                    console.log(`📢 Call ${callId} ended, room ${call.roomId} still active`);
                  }
                }
              }, 5000);
            }
          } else {
            console.log(`✅ User ${user.username} rejoined call`);
          }
        }, 10000);
        
        callGracePeriod.set(callId, graceTimeout);
      }
    }
    
    matchmaking.cancelMatchmaking(user.userId);
    
    const roomId = matchmaking.getRoomIdByUser(user.userId);
    
    if (roomId) {
      const room = matchmaking.getRoom(roomId);
      
      let hasActiveCall = false;
      activeCalls.forEach((call) => {
        if (call.roomId === roomId && call.participants.length > 0) {
          hasActiveCall = true;
          console.log(`🛡️ Room ${roomId} has active call ${call.callId}`);
        }
      });
      
      if (hasActiveCall) {
        console.log(`🛡️ User ${user.username} disconnected - room ${roomId} PRESERVED (active call)`);
      } else {
        console.log(`⏳ User ${user.username} disconnected - grace period for room ${roomId}`);
        
        setTimeout(() => {
          const stillDisconnected = !Array.from(socketUsers.values()).some(u => u.userId === user.userId);
          
          if (stillDisconnected) {
            console.log(`👋 User ${user.username} did not reconnect, removing from room ${roomId}`);
            
            let stillHasActiveCall = false;
            activeCalls.forEach((call) => {
              if (call.roomId === roomId && call.participants.length > 0) {
                stillHasActiveCall = true;
              }
            });
            
            const result = matchmaking.leaveRoom(user.userId, stillHasActiveCall);
            
            if (result.roomId && !result.destroyed) {
              io.to(result.roomId).emit('user_left', {
                userId: user.userId,
                username: user.username,
                remainingUsers: result.remainingUsers
              });
            }
            
            if (result.destroyed) {
              console.log(`🗑️ Room ${result.roomId} destroyed after user disconnect`);
              cancelRoomCleanup(result.roomId);
              performRoomCleanup(result.roomId);
            }
          } else {
            console.log(`✅ User ${user.username} reconnected, keeping in room ${roomId}`);
          }
        }, 5000);
      }
    }

  } else {
    console.log('🔌 Unauthenticated client disconnected:', socket.id);
    // CRITICAL: Still need to clean up socketUsers entry
    socketUsers.delete(socket.id);
  }
});
});

// ============================================
// PERIODIC CLEANUP
// ============================================

// Replace existing periodic cleanup with improved version
setInterval(() => {
  const now = Date.now();
  const rooms = matchmaking.getActiveRooms();
  
  console.log(`🧹 Periodic cleanup check: ${rooms.length} active rooms`);
  
  rooms.forEach(room => {
    if (room.expiresAt <= now) {
      console.log(`🕐 Periodic cleanup: Room ${room.id} has expired (${((now - room.expiresAt) / 1000).toFixed(1)}s ago)`);
      performRoomCleanup(room.id);
    }
  });
  
  // CRITICAL FIX: Clean up orphaned call grace periods
  const orphanedGracePeriods = [];
  callGracePeriod.forEach((timeout, callId) => {
    if (!activeCalls.has(callId)) {
      orphanedGracePeriods.push(callId);
      clearTimeout(timeout);
    }
  });
  if (orphanedGracePeriods.length > 0) {
    orphanedGracePeriods.forEach(id => callGracePeriod.delete(id));
    console.log(`🗑️ Cleaned up ${orphanedGracePeriods.length} orphaned call grace periods`);
  }
  
  // CRITICAL FIX: Clean up orphaned room cleanup timers
  const orphanedRoomTimers = [];
  roomCleanupTimers.forEach((timeout, roomId) => {
    if (!matchmaking.getRoom(roomId)) {
      orphanedRoomTimers.push(roomId);
      clearTimeout(timeout);
    }
  });
  if (orphanedRoomTimers.length > 0) {
    orphanedRoomTimers.forEach(id => roomCleanupTimers.delete(id));
    console.log(`🗑️ Cleaned up ${orphanedRoomTimers.length} orphaned room timers`);
  }
  
  // CRITICAL FIX: Clean up stale socketUserCleanup entries
  const staleCleanups = [];
  socketUserCleanup.forEach((timeout, socketId) => {
    if (!socketUsers.has(socketId) && !io.sockets.sockets.has(socketId)) {
      staleCleanups.push(socketId);
      clearTimeout(timeout);
    }
  });
  if (staleCleanups.length > 0) {
    staleCleanups.forEach(id => socketUserCleanup.delete(id));
    console.log(`🗑️ Cleaned up ${staleCleanups.length} stale socket cleanup entries`);
  }
  
  // CRITICAL FIX: Clean up orphaned mutex entries (should never happen but safety net)
  const orphanedMutexes = [];
  callMutexes.forEach((promise, callId) => {
    if (!activeCalls.has(callId)) {
      orphanedMutexes.push(callId);
    }
  });
  if (orphanedMutexes.length > 0) {
    orphanedMutexes.forEach(id => callMutexes.delete(id));
    console.log(`🗑️ Cleaned up ${orphanedMutexes.length} orphaned call mutexes`);
  }
  
  // Log memory stats
  console.log(`📊 Memory stats:
    - Active sockets: ${socketUsers.size}
    - Active calls: ${activeCalls.size}
    - Call grace periods: ${callGracePeriod.size}
    - Room cleanup timers: ${roomCleanupTimers.size}
    - Active offers: ${activeOffers.size}
    - Call mutexes: ${callMutexes.size}
    - Join debounce: ${joinCallDebounce.size}
    - Room join states: ${roomJoinState.size}
    - User-to-socket index: ${userToSocketId.size}`);
    
}, 60000);;


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

// ============================================
// START SERVER
// ============================================

async function startServer() {
  try {
    await connectDB();
    initializeFirebase();

    const PORT = config.PORT;
    server.listen(PORT, () => {
      console.log(`🚀 Server running on port ${PORT}`);
      console.log(`📊 Health check: http://localhost:${PORT}/health`);
      console.log(`🔌 Socket.IO ready for connections`);
      console.log(`📞 WebRTC signaling enabled with ICE prioritization`);
      console.log(`⏰ Room cleanup: ${ROOM_EXPIRY_TIME / 60000} minutes`);
      
      const hasTurn = !!(process.env.CLOUDFLARE_TURN_TOKEN_ID || '726fccae33334279a71e962da3d8e01c');
      if (hasTurn) {
        console.log(`✅ Cloudflare TURN server configured`);
        console.log(`   Priority: host → srflx (STUN) → relay (TURN)`);
      } else {
        console.log(`⚠️ TURN server NOT configured - STUN only mode`);
      }
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
