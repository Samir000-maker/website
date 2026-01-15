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
  
  // Create new mutex
  const mutexPromise = (async () => {
    try {
      return await operation();
    } finally {
      callMutexes.delete(callId);
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
const ROOM_EXPIRY_TIME = 10 * 60 * 1000; // 10 minutes
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
    console.log('📡 ICE servers requested by client');
    const iceServers = await getIceServers();
    
    res.json({ 
      iceServers,
      timestamp: Date.now(),
      ttl: 86400
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

const socketUsers = new Map();

io.on('connection', (socket) => {
  console.log('🔌 Client connected:', socket.id);

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

      const room = matchmaking.addToQueue({
        ...user,
        mood,
        socketId: socket.id
      });

      if (room) {
        console.log(`🎉 Match found! Room ${room.id} with ${room.users.length} users`);

        // Schedule room cleanup
        scheduleRoomCleanup(room.id, room.expiresAt);

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
    for (const [socketId, userData] of socketUsers.entries()) {
      if (userData.userId === userId) {
        const socketInstance = io.sockets.sockets.get(socketId);
        if (socketInstance && socketInstance.connected) {
          return socketInstance;
        }
      }
    }
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

    console.log(`🚪 User ${user.username} confirming room ${roomId}`);

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

    // Get chat history from the room
    const chatHistory = room.getMessages ? room.getMessages() : [];
    console.log(`📜 Sending ${chatHistory.length} chat messages to ${user.username}`);

    // CRITICAL FIX: Check for active calls in this room and send state
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

    // Send room joined confirmation with call state
    const responseData = { 
      roomId,
      chatHistory: chatHistory 
    };

    if (activeCallState) {
      responseData.activeCall = activeCallState;
      console.log(`📤 Sending active call state to ${user.username}:`, activeCallState);
    }

    socket.emit('room_joined', responseData);
    
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

  socket.on('chat_message', ({ roomId, message, replyTo }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) {
        console.error('❌ Unauthenticated socket tried to send message');
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

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

      room.addMessage(messageData);
      io.to(roomId).emit('chat_message', messageData);
      
      console.log(`💬 Message from ${user.username} in room ${roomId}: "${message.substring(0, 30)}..."`);
      
    } catch (error) {
      console.error('Chat message error:', error);
      socket.emit('error', { message: 'Failed to send message' });
    }
  });

socket.on('initiate_call', async ({ roomId, callType }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      socket.emit('error', { message: 'Not authenticated' });
      return;
    }

    const room = matchmaking.getRoom(roomId);
    
    if (!room) {
      socket.emit('error', { message: 'Room not found' });
      return;
    }

    if (!room.hasUser(user.userId)) {
      socket.emit('error', { message: 'You are not in this room' });
      return;
    }

    // Check for existing active calls in this room
    let existingCallId = null;
    activeCalls.forEach((call, callId) => {
      if (call.roomId === roomId && call.participants.length > 0) {
        existingCallId = callId;
      }
    });

    if (existingCallId) {
      console.log(`📞 Call already active in room ${roomId}: ${existingCallId}`);
      socket.emit('error', { 
        message: 'A call is already in progress',
        code: 'CALL_ALREADY_ACTIVE',
        callId: existingCallId
      });
      return;
    }

    const callId = uuidv4();
    
    const call = {
      callId,
      roomId,
      callType,
      participants: [user.userId],
      status: 'pending',
      createdAt: Date.now(),
      lastActivity: Date.now(),
      initiator: user.userId,
      userMediaStates: new Map()
    };

    // Initialize media state for initiator
    call.userMediaStates.set(user.userId, {
      videoEnabled: callType === 'video',
      audioEnabled: true
    });

    activeCalls.set(callId, call);
    userCalls.set(user.userId, callId);
    webrtcMetrics.totalCalls++;

    console.log(`📞 Call initiated: ${callId} by ${user.username} (${callType})`);
    console.log(`📊 Initial state: participants=[${user.userId}], status=pending`);

    // Notify all other users in room
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
          console.log(`📤 Sent incoming_call to ${roomUser.username}`);
        }
      }
    });

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

    // CRITICAL FIX: Validate user is in the room
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

      // Check if already in call
      if (call.participants.includes(user.userId)) {
        console.log(`⚠️ User ${user.username} already in call ${callId} - re-sending state`);
        
        // CRITICAL FIX: Build participant data with VALIDATED room data
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
        }).filter(u => u !== null); // Remove any null entries

        if (callUsers.length !== call.participants.length) {
          console.error(`❌ CRITICAL: Participant count mismatch! Expected ${call.participants.length}, got ${callUsers.length}`);
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

      // Add participant atomically
      call.participants.push(user.userId);
      userCalls.set(user.userId, callId);
      
      console.log(`➕ Added ${user.username} to participants`);
      console.log(`🔍 [accept_call] After: participants=[${call.participants.join(', ')}]`);
      
      // Initialize media state
      call.userMediaStates.set(user.userId, {
        videoEnabled: call.callType === 'video',
        audioEnabled: true
      });
      
      // Mark as active when first user accepts
      if (call.status === 'pending') {
        call.status = 'active';
        console.log(`📊 Call status changed: pending → active`);
        
        // CRITICAL: Mark room as having active call and extend expiry
        if (room) {
          room.setActiveCall(true);
          room.extendExpiry(15); // Extend by 15 minutes
          console.log(`🛡️ Room ${roomId} marked as having active call and extended by 15 minutes`);
        }
      }
      
      call.lastActivity = Date.now();

      console.log(`✅ User ${user.username} accepted call ${callId} - now ${call.status.toUpperCase()}`);

      // CRITICAL FIX: Build validated participant list from ROOM data
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
        console.error(`❌ CRITICAL: Participant validation failed! Expected ${call.participants.length}, got ${callUsers.length}`);
        console.error(`   Room users: ${room.users.map(u => u.userId).join(', ')}`);
        console.error(`   Call participants: ${call.participants.join(', ')}`);
        socket.emit('error', { 
          message: 'Unable to resolve all participants. Please try again.',
          code: 'PARTICIPANT_RESOLUTION_FAILED'
        });
        // Rollback the participant addition
        call.participants = call.participants.filter(p => p !== user.userId);
        userCalls.delete(user.userId);
        call.userMediaStates.delete(user.userId);
        return;
      }

      console.log(`📊 Broadcasting to ${callUsers.length} participants:`);
      callUsers.forEach((u, idx) => {
        console.log(`   [${idx}] ${u.username} (${u.userId})`);
      });

      // Emit to ALL participants simultaneously
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
            console.error(`❌ No active socket for ${roomUser.username} (${roomUser.userId})`);
            resolve();
          }
        });
      });

      await Promise.all(emitPromises);

      // Broadcast call state to entire room
      io.to(roomId).emit('call_state_update', {
        callId: callId,
        isActive: true,
        participantCount: call.participants.length,
        callType: call.callType
      });
      console.log(`📢 Broadcasted call_state_update: active=true, count=${call.participants.length}`);
    });

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

// Add at top with other Maps
const joinCallDebounce = new Map(); // userId -> timestamp

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

      // CRITICAL FIX: Check if user is in participants list
      if (!call.participants.includes(user.userId)) {
        console.error(`❌ User ${user.username} not authorized for call ${callId}`);
        console.error(`   Participants: [${call.participants.join(', ')}]`);
        socket.emit('error', { 
          message: 'You are not in this call',
          code: 'NOT_IN_CALL'
        });
        joinCallDebounce.delete(user.userId);
        return;
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

      // CRITICAL FIX: Build participant data from ROOM (not socketUsers)
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

      // CRITICAL: Validate all participants were resolved
      if (participantsWithMediaStates.length !== call.participants.length) {
        console.error(`❌ CRITICAL: Failed to resolve all participants!`);
        console.error(`   Expected: ${call.participants.length}, Got: ${participantsWithMediaStates.length}`);
        console.error(`   Room users: ${room.users.map(u => `${u.username}(${u.userId})`).join(', ')}`);
        console.error(`   Call participants: ${call.participants.join(', ')}`);
        socket.emit('error', { 
          message: 'Unable to load all participants. Please refresh and try again.',
          code: 'PARTICIPANT_RESOLUTION_FAILED'
        });
        joinCallDebounce.delete(user.userId);
        return;
      }

      console.log(`📊 Sending ${participantsWithMediaStates.length} VALIDATED participants to ${user.username}`);
      participantsWithMediaStates.forEach((p, idx) => {
        console.log(`   [${idx}] ${p.username} (${p.userId}): video=${p.videoEnabled}, audio=${p.audioEnabled}`);
      });

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
      // Still let user leave the room socket
      socket.leave(`call-${callId}`);
      return;
    }

    // CRITICAL FIX: Check if user is actually in the call before removing
    if (!call.participants.includes(user.userId)) {
      console.warn(`⚠️ User ${user.username} not in call ${callId} participants, ignoring leave`);
      socket.leave(`call-${callId}`);
      return;
    }

    // Remove user from call
    call.participants = call.participants.filter(p => p !== user.userId);
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

    // CRITICAL FIX: Get room and broadcast call state
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

    // CRITICAL FIX: Only delete call after grace period if truly empty
    if (call.participants.length === 0) {
      console.log(`🕐 Call ${callId} has 0 participants - starting 5s grace period for cleanup`);
      
      // CRITICAL: Mark room as no longer having active call
      if (room) {
        room.setActiveCall(false);
        console.log(`📞 Room ${call.roomId} marked as call-free`);
      }
      
      setTimeout(() => {
        const currentCall = activeCalls.get(callId);
        
        if (!currentCall) {
          console.log(`ℹ️ Call ${callId} already cleaned up`);
          return;
        }
        
        if (currentCall.participants.length === 0) {
          console.log(`🗑️ Call ${callId} still empty after grace period - cleaning up`);
          activeCalls.delete(callId);
          
          // Notify room that call has fully ended
          if (room) {
            io.to(currentCall.roomId).emit('call_ended_notification', {
              callId: callId
            });
            console.log(`📢 Call ${callId} fully ended, notified room ${currentCall.roomId}`);
          }
        } else {
          console.log(`✅ Call ${callId} has ${currentCall.participants.length} participant(s) again - cleanup cancelled`);
          
          // CRITICAL: If call has participants again, mark room as having active call
          if (room) {
            room.setActiveCall(true);
            console.log(`📞 Room ${call.roomId} marked as having active call again`);
          }
        }
      }, 5000); // 5 second grace period
      
    } else {
      console.log(`✅ Call ${callId} still active with ${call.participants.length} participant(s)`);
      
      // Make sure room knows call is still active
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


socket.on('join_existing_call', ({ callId, roomId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      socket.emit('error', { message: 'Not authenticated' });
      return;
    }

    const call = activeCalls.get(callId);
    
    if (!call) {
      console.error(`❌ Call ${callId} not found for join request from ${user.username}`);
      socket.emit('error', { message: 'Call not found or has ended' });
      return;
    }

    if (call.participants.length === 0) {
      console.error(`❌ Call ${callId} has no active participants`);
      socket.emit('error', { message: 'Call has no active participants' });
      return;
    }

    console.log(`🔄 User ${user.username} joining existing call ${callId} with ${call.participants.length} participants`);

    // Add user to participants if not already in
    if (!call.participants.includes(user.userId)) {
      call.participants.push(user.userId);
      userCalls.set(user.userId, callId);
      console.log(`➕ Added ${user.username} to call ${callId}`);
    } else {
      console.log(`ℹ️ User ${user.username} already in call ${callId}`);
    }

    call.lastActivity = Date.now();

    // Initialize media state for joining user if not present
    if (!call.userMediaStates) {
      call.userMediaStates = new Map();
      console.log(`📊 Initialized userMediaStates Map for call ${callId}`);
    }
    
    if (!call.userMediaStates.has(user.userId)) {
      const defaultVideoState = call.callType === 'video';
      call.userMediaStates.set(user.userId, {
        videoEnabled: defaultVideoState,
        audioEnabled: true
      });
      console.log(`📊 Set initial media state for ${user.username}: video=${defaultVideoState}, audio=true`);
    }

    // Navigate user to call page
    socket.emit('join_existing_call_success', {
      callId,
      callType: call.callType,
      roomId: call.roomId
    });

    console.log(`✅ User ${user.username} successfully joining call ${callId}`);

  } catch (error) {
    console.error('Join existing call error:', error);
    socket.emit('error', { message: 'Failed to join call' });
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

socket.on('webrtc_answer', ({ callId, targetUserId, answer }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

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

  } catch (error) {
    console.error('❌ WebRTC answer error:', error);
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
      
      console.log(`📹 ${user.username} video: ${enabled ? 'ON' : 'OFF'} (call ${callId})`);
      console.log(`📹 Server state updated: userId=${user.userId}, videoEnabled=${enabled}`);

      // Broadcast to ALL users in call room
      io.to(`call-${callId}`).emit('video_state_changed', {
        userId: user.userId,
        enabled: enabled
      });
      
      console.log(`📤 Broadcasted video_state_changed to all participants in call-${callId}`);
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
  
  if (user) {
    console.log(`🔌 User ${user.username} disconnected`);

    // Handle call disconnection with grace period
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

            // Only clean up call if empty
            if (call.participants.length === 0) {
              console.log(`🕐 Call ${callId} empty - scheduling cleanup`);
              
              // Mark room as no longer having active call
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
        }, 10000); // 10 second grace period
        
        callGracePeriod.set(callId, graceTimeout);
      }
    }
    
    // Matchmaking cleanup
    matchmaking.cancelMatchmaking(user.userId);
    
    // CRITICAL FIX: Room cleanup - check for active calls
    const roomId = matchmaking.getRoomIdByUser(user.userId);
    
    if (roomId) {
      const room = matchmaking.getRoom(roomId);
      
      // Check if there's an active call in this room
      let hasActiveCall = false;
      activeCalls.forEach((call) => {
        if (call.roomId === roomId && call.participants.length > 0) {
          hasActiveCall = true;
          console.log(`🛡️ Room ${roomId} has active call ${call.callId}`);
        }
      });
      
      if (hasActiveCall) {
        console.log(`🛡️ User ${user.username} disconnected - room ${roomId} PRESERVED (active call)`);
        // Do NOT remove user or start destruction
      } else {
        console.log(`⏳ User ${user.username} disconnected - grace period for room ${roomId}`);
        
        setTimeout(() => {
          const stillDisconnected = !Array.from(socketUsers.values()).some(u => u.userId === user.userId);
          
          if (stillDisconnected) {
            console.log(`👋 User ${user.username} did not reconnect, removing from room ${roomId}`);
            
            // Check AGAIN for active calls before destroying
            let stillHasActiveCall = false;
            activeCalls.forEach((call) => {
              if (call.roomId === roomId && call.participants.length > 0) {
                stillHasActiveCall = true;
              }
            });
            
            // Pass the hasActiveCall flag to leaveRoom
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
        }, 5000); // 5 second grace
      }
    }

    socketUsers.delete(socket.id);
  } else {
    console.log('🔌 Unauthenticated client disconnected:', socket.id);
  }
});
});

// ============================================
// PERIODIC CLEANUP
// ============================================

// Run cleanup every minute to catch any missed expirations
setInterval(() => {
  const now = Date.now();
  const rooms = matchmaking.getActiveRooms();
  
  rooms.forEach(room => {
    if (room.expiresAt <= now) {
      console.log(`🕐 Periodic cleanup: Room ${room.id} has expired`);
      performRoomCleanup(room.id);
    }
  });
}, 60000);

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
