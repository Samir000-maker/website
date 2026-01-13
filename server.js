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
    
    // FIX: Check the actual response structure
    console.log('📦 Raw TURN response:', JSON.stringify(data, null, 2));
    
    // Cloudflare returns { iceServers: { iceServers: [...], ttl: ... } }
    // We need to extract the actual ice servers array
    if (data.iceServers && data.iceServers.iceServers) {
      console.log('✅ Cloudflare TURN credentials generated successfully');
      console.log(`   TTL: ${data.iceServers.ttl}s, ICE Servers: ${data.iceServers.iceServers.length || 0}`);
      return data.iceServers.iceServers; // Return the actual array
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

async function getIceServers() {
  // Start with STUN servers (always available, no authentication needed)
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

  // Try to add Cloudflare TURN servers
  const turnCreds = await generateCloudTurnCredentials();
  
  if (turnCreds && turnCreds.iceServers && turnCreds.iceServers.ice_servers) {
    // Add TURN servers from Cloudflare
    turnCreds.iceServers.ice_servers.forEach(server => {
      iceServers.push(server);
      
      // Log TURN server details for debugging
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

      socket.emit('room_joined', { 
        roomId,
        chatHistory: chatHistory 
      });
      
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

  socket.on('initiate_call', ({ roomId, callType }) => {
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

      const callId = uuidv4();
      
      const call = {
        callId,
        roomId,
        callType,
        participants: [user.userId],
        status: 'pending',
        createdAt: Date.now(),
        lastActivity: Date.now(),
        initiator: user.userId
      };

      activeCalls.set(callId, call);
      userCalls.set(user.userId, callId);
      webrtcMetrics.totalCalls++;

      console.log(`📞 Call initiated: ${callId} by ${user.username} (${callType})`);

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
          }
        }
      });

    } catch (error) {
      console.error('Initiate call error:', error);
      socket.emit('error', { message: 'Failed to initiate call' });
    }
  });

  socket.on('accept_call', ({ callId, roomId }) => {
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

      if (!call.participants.includes(user.userId)) {
        call.participants.push(user.userId);
        userCalls.set(user.userId, callId);
      }
      
      call.status = 'active';
      call.lastActivity = Date.now();

      console.log(`✅ User ${user.username} accepted call ${callId} - now ACTIVE`);

      const room = matchmaking.getRoom(roomId);
      if (!room) {
        socket.emit('error', { message: 'Room not found' });
        return;
      }

      const callUsers = room.users.filter(u => call.participants.includes(u.userId));
      
      callUsers.forEach(roomUser => {
        const targetSocket = findActiveSocketForUser(roomUser.userId);
        if (targetSocket) {
          targetSocket.emit('call_accepted', {
            callId,
            callType: call.callType,
            users: callUsers.map(u => ({
              userId: u.userId,
              username: u.username,
              pfpUrl: u.pfpUrl
            }))
          });
        }
      });

    } catch (error) {
      console.error('Accept call error:', error);
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

socket.on('join_call', ({ callId }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) {
      socket.emit('error', { message: 'Not authenticated' });
      return;
    }

    const call = activeCalls.get(callId);
    
    if (!call) {
      console.error(`❌ Call ${callId} not found for user ${user.username}`);
      socket.emit('error', { message: 'Call not found' });
      return;
    }

    if (!call.participants.includes(user.userId)) {
      console.error(`❌ User ${user.username} not in call ${callId}`);
      socket.emit('error', { message: 'You are not in this call' });
      return;
    }

    // Update call activity
    call.lastActivity = Date.now();

    // Initialize user's media state if not present
    if (!call.userMediaStates) {
      call.userMediaStates = new Map();
    }
    
    // Set default media state for this user
    if (!call.userMediaStates.has(user.userId)) {
      call.userMediaStates.set(user.userId, {
        videoEnabled: call.callType === 'video',
        audioEnabled: true
      });
    }

    // Clear grace period
    if (callGracePeriod.has(callId)) {
      clearTimeout(callGracePeriod.get(callId));
      callGracePeriod.delete(callId);
      console.log(`⏱️ Cleared grace period for call ${callId}`);
    }

    socket.join(`call-${callId}`);
    console.log(`📞 User ${user.username} joined call room: call-${callId}`);

    const room = matchmaking.getRoom(call.roomId);
    const participants = room ? room.users.filter(u => call.participants.includes(u.userId)) : [];

    // Include media states in participant data
    const participantsWithMediaStates = participants.map(p => {
      const mediaState = call.userMediaStates.get(p.userId) || {
        videoEnabled: call.callType === 'video',
        audioEnabled: true
      };
      return {
        userId: p.userId,
        username: p.username,
        pfpUrl: p.pfpUrl,
        videoEnabled: mediaState.videoEnabled,
        audioEnabled: mediaState.audioEnabled
      };
    });

    console.log(`📊 Participants with media states:`, participantsWithMediaStates.map(p => 
      `${p.username} (video=${p.videoEnabled}, audio=${p.audioEnabled})`
    ).join(', '));

    socket.emit('call_joined', {
      callId,
      callType: call.callType,
      participants: participantsWithMediaStates
    });

    // Get current user's media state
    const userMediaState = call.userMediaStates.get(user.userId);

    socket.to(`call-${callId}`).emit('user_joined_call', {
      user: {
        userId: user.userId,
        username: user.username,
        pfpUrl: user.pfpUrl,
        videoEnabled: userMediaState.videoEnabled,
        audioEnabled: userMediaState.audioEnabled
      }
    });

  } catch (error) {
    console.error('Join call error:', error);
    socket.emit('error', { message: 'Failed to join call' });
  }
});

  socket.on('leave_call', ({ callId }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      const call = activeCalls.get(callId);
      
      if (!call) return;

      call.participants = call.participants.filter(p => p !== user.userId);
      userCalls.delete(user.userId);

      console.log(`📵 User ${user.username} left call ${callId}`);

      socket.leave(`call-${callId}`);

      io.to(`call-${callId}`).emit('user_left_call', {
        userId: user.userId,
        username: user.username
      });

      if (call.participants.length === 0) {
        activeCalls.delete(callId);
        console.log(`🗑️ Call ${callId} ended (no participants)`);
      }

    } catch (error) {
      console.error('Leave call error:', error);
    }
  });

  // WebRTC Signaling
  socket.on('webrtc_offer', ({ callId, targetUserId, offer }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      console.log(`📤 WebRTC offer from ${user.username} to ${targetUserId}`);

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

    } catch (error) {
      console.error('WebRTC offer error:', error);
    }
  });

  socket.on('webrtc_answer', ({ callId, targetUserId, answer }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      console.log(`📤 WebRTC answer from ${user.username} to ${targetUserId}`);

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
      console.error('WebRTC answer error:', error);
    }
  });

  socket.on('ice_candidate', ({ callId, targetUserId, candidate }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      const candidateType = candidate.type || 'unknown';
      console.log(`🧊 ICE candidate from ${user.username} to ${targetUserId}: type=${candidateType}`);

      const targetSocket = findActiveSocketForUser(targetUserId);
      
      if (targetSocket) {
        targetSocket.emit('ice_candidate', {
          fromUserId: user.userId,
          candidate
        });
      } else {
        console.warn(`⚠️ Target user ${targetUserId} not found for ICE candidate`);
      }

    } catch (error) {
      console.error('ICE candidate error:', error);
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

socket.on('audio_state_changed', ({ callId, enabled }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    const call = activeCalls.get(callId);
    if (call) {
      if (!call.userMediaStates) call.userMediaStates = new Map();
      const currentState = call.userMediaStates.get(user.userId) || {};
      call.userMediaStates.set(user.userId, {
        ...currentState,
        audioEnabled: enabled
      });
      console.log(`🎤 ${user.username} audio: ${enabled ? 'ON' : 'OFF'}`);
    }

    socket.to(`call-${callId}`).emit('audio_state_changed', {
      userId: user.userId,
      enabled
    });

  } catch (error) {
    console.error('Audio state error:', error);
  }
});

socket.on('video_state_changed', ({ callId, enabled }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    const call = activeCalls.get(callId);
    if (call) {
      if (!call.userMediaStates) call.userMediaStates = new Map();
      const currentState = call.userMediaStates.get(user.userId) || {};
      call.userMediaStates.set(user.userId, {
        ...currentState,
        videoEnabled: enabled
      });
      console.log(`📹 ${user.username} video: ${enabled ? 'ON' : 'OFF'}`);
    }

    socket.to(`call-${callId}`).emit('video_state_changed', {
      userId: user.userId,
      enabled
    });

  } catch (error) {
    console.error('Video state error:', error);
  }
});

socket.on('video_state_changed', ({ callId, enabled }) => {
  try {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    const call = activeCalls.get(callId);
    if (call) {
      if (!call.userMediaStates) call.userMediaStates = new Map();
      const currentState = call.userMediaStates.get(user.userId) || {};
      call.userMediaStates.set(user.userId, {
        ...currentState,
        videoEnabled: enabled
      });
      console.log(`📹 ${user.username} video: ${enabled ? 'ON' : 'OFF'}`);
    }

    socket.to(`call-${callId}`).emit('video_state_changed', {
      userId: user.userId,
      enabled
    });

  } catch (error) {
    console.error('Video state error:', error);
  }
});

  socket.on('video_state_changed', ({ callId, enabled }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      socket.to(`call-${callId}`).emit('video_state_changed', {
        userId: user.userId,
        enabled
      });

    } catch (error) {
      console.error('Video state error:', error);
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

              io.to(`call-${callId}`).emit('user_left_call', {
                userId: user.userId,
                username: user.username
              });

              if (call.participants.length === 0) {
                activeCalls.delete(callId);
                callGracePeriod.delete(callId);
                console.log(`🗑️ Call ${callId} ended (all participants left)`);
              }
            } else {
              console.log(`✅ User ${user.username} rejoined call`);
            }
          }, 10000);
          
          callGracePeriod.set(callId, graceTimeout);
        }
      }
      
      // Matchmaking cleanup
      matchmaking.cancelMatchmaking(user.userId);
      
      // Room cleanup with grace period
      const roomId = matchmaking.getRoomIdByUser(user.userId);
      
      if (roomId) {
        console.log(`⏳ User ${user.username} disconnected but still in room ${roomId} (grace period)`);
        
        setTimeout(() => {
          const stillDisconnected = !Array.from(socketUsers.values()).some(u => u.userId === user.userId);
          
          if (stillDisconnected) {
            console.log(`👋 User ${user.username} did not reconnect, removing from room ${roomId}`);
            const result = matchmaking.leaveRoom(user.userId);
            
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
