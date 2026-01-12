// CRITICAL FIXES FOR PRODUCTION - ALL PATCHES APPLIED
// 1. Message queuing for backgrounded clients
// 2. Extended grace periods for call navigation
// 3. Background mode tracking
// 4. Message sync on reconnection
// 5. High concurrency optimizations

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
// TURN SERVER CONFIGURATION
// ============================================

async function generateTurnCredentials() {
  const TURN_TOKEN_ID = process.env.CLOUDFLARE_TURN_TOKEN_ID;
  const TURN_API_TOKEN = process.env.CLOUDFLARE_TURN_API_TOKEN;

  if (!TURN_TOKEN_ID || !TURN_API_TOKEN) {
    console.warn('⚠️ TURN credentials not configured - TURN server will not be available');
    return null;
  }

  try {
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
      console.error('❌ Failed to generate TURN credentials:', response.status);
      return null;
    }

    const credentials = await response.json();
    console.log('✅ TURN credentials generated successfully');
    return credentials;
  } catch (error) {
    console.error('❌ Error generating TURN credentials:', error);
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

  const turnCreds = await generateTurnCredentials();
  if (turnCreds && turnCreds.iceServers) {
    iceServers.push(...turnCreds.iceServers.ice_servers);
    console.log('✅ TURN server added to ICE configuration');
  } else {
    console.warn('⚠️ Operating without TURN server - may fail in restrictive networks');
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
  pingInterval: 25000,
  maxHttpBufferSize: 1e8, // 100 MB for high concurrency
  perMessageDeflate: {
    threshold: 1024 // Compress messages > 1KB
  }
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

const matchmakingRateLimit = new Map();

// PRODUCTION: Enhanced state tracking
const activeCalls = new Map();
const userCalls = new Map();
const callGracePeriod = new Map();
const socketUsers = new Map();
const backgroundClients = new Map(); // NEW: Track backgrounded clients
const messageQueues = new Map(); // NEW: Queue messages for background clients

const webrtcMetrics = {
  totalCalls: 0,
  successfulConnections: 0,
  failedConnections: 0,
  turnUsage: 0,
  stunUsage: 0
};

// ============================================
// HELPER FUNCTIONS
// ============================================

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

function handleRoomCleanup(userId, socketId) {
  matchmaking.cancelMatchmaking(userId);
  
  const roomId = matchmaking.getRoomIdByUser(userId);
  
  if (roomId) {
    console.log(`⏳ User disconnected from room ${roomId} (grace period - keeping room alive)`);
    
    // CRITICAL: Give 15 seconds for page navigation (discovery → chat)
    setTimeout(() => {
      const stillDisconnected = !Array.from(socketUsers.values()).some(u => u.userId === userId);
      
      if (stillDisconnected) {
        console.log(`👋 User ${userId} did not reconnect after 15s, checking room status`);
        
        const room = matchmaking.getRoom(roomId);
        if (!room) {
          console.log(`⚠️ Room ${roomId} no longer exists`);
          return;
        }
        
        // Check if ANY user is still connected via socket
        const hasConnectedUsers = room.users.some(roomUser => {
          return Array.from(socketUsers.values()).some(socketUser => 
            socketUser.userId === roomUser.userId
          );
        });
        
        if (hasConnectedUsers) {
          console.log(`✅ Room ${roomId} has connected users, keeping alive`);
          return;
        }
        
        // Only leave room if NO users are connected
        console.log(`🚪 No users connected to room ${roomId}, user ${userId} leaving`);
        const result = matchmaking.leaveRoom(userId);
        
        if (result.roomId && !result.destroyed) {
          io.to(result.roomId).emit('user_left', {
            userId: userId,
            username: 'User',
            remainingUsers: result.remainingUsers
          });
        }
      } else {
        console.log(`✅ User ${userId} reconnected, keeping in room ${roomId}`);
      }
    }, 15000); // INCREASED from 5000 to 15000
  }
  
  socketUsers.delete(socketId);
  backgroundClients.delete(socketId);
  messageQueues.delete(socketId);
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
    connectedClients: socketUsers.size,
    backgroundClients: backgroundClients.size,
    webrtcMetrics,
    turnConfigured: !!(process.env.CLOUDFLARE_TURN_TOKEN_ID && process.env.CLOUDFLARE_TURN_API_TOKEN),
    server: 'running'
  });
});

app.get('/api/ice-servers', authenticateFirebase, async (req, res) => {
  try {
    console.log('📡 ICE servers requested');
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
    const existingUser = await db.collection('users').findOne({ 
      email: firebaseUser.email 
    });

    if (existingUser && existingUser.username !== trimmedUsername) {
      const usernameExists = await db.collection('users').findOne({
        username: trimmedUsername
      });
      if (usernameExists) {
        return res.status(400).json({ error: 'Username already taken' });
      }
    }

    const userData = {
      email: firebaseUser.email,
      username: trimmedUsername,
      pfpUrl: pfpUrl || getDefaultProfilePicture(),
      updatedAt: new Date()
    };

    if (existingUser) {
      await db.collection('users').updateOne(
        { _id: existingUser._id },
        { $set: userData }
      );
      await invalidateUserProfileCache(existingUser._id.toString());
      res.json({ 
        success: true, 
        userId: existingUser._id,
        message: 'Profile updated' 
      });
    } else {
      userData.createdAt = new Date();
      const result = await db.collection('users').insertOne(userData);
      res.json({ 
        success: true, 
        userId: result.insertedId,
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

  // NEW: Background mode tracking
  socket.on('background_mode', ({ active }) => {
    const user = socketUsers.get(socket.id);
    if (!user) return;

    console.log(`${active ? '🌙' : '☀️'} User ${user.username} ${active ? 'backgrounded' : 'foregrounded'}`);

    if (active) {
      backgroundClients.set(socket.id, {
        userId: user.userId,
        username: user.username,
        since: Date.now()
      });
      
      if (!messageQueues.has(socket.id)) {
        messageQueues.set(socket.id, []);
      }
    } else {
      backgroundClients.delete(socket.id);
      
      const queue = messageQueues.get(socket.id);
      if (queue && queue.length > 0) {
        console.log(`📬 Sending ${queue.length} queued messages to ${user.username}`);
        socket.emit('missed_messages', { messages: queue, count: queue.length });
        messageQueues.delete(socket.id);
      }
    }
  });

  // NEW: Message sync for reconnection
  socket.on('sync_messages', ({ roomId, lastTimestamp }) => {
    const user = socketUsers.get(socket.id);
    if (!user || !roomId) return;

    console.log(`🔄 Syncing messages for ${user.username} from ${lastTimestamp}`);

    const room = matchmaking.getRoom(roomId);
    if (!room) return;

    const messages = room.getMessages().filter(msg => msg.timestamp > lastTimestamp);

    if (messages.length > 0) {
      console.log(`📨 Sending ${messages.length} synced messages to ${user.username}`);
      socket.emit('missed_messages', { messages, count: messages.length });
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

      // CRITICAL: Mark user as actively in room
      socket.data = socket.data || {};
      socket.data.activeRoomId = roomId;
      socket.data.joinedAt = Date.now();

      socket.emit('room_joined', { 
        roomId,
        users: room.users.map(u => ({
          userId: u.userId,
          username: u.username,
          pfpUrl: u.pfpUrl
        }))
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

      const messageData = {
        userId: user.userId,
        username: user.username,
        pfpUrl: user.pfpUrl,
        message,
        timestamp: Date.now(),
        messageId: uuidv4()
      };

      if (replyTo) {
        messageData.replyTo = replyTo;
      }

      room.addMessage(messageData);
      
      // PRODUCTION FIX: Send to each user, queue for backgrounded clients
      room.users.forEach(roomUser => {
        const userSocket = findActiveSocketForUser(roomUser.userId);
        
        if (userSocket) {
          if (backgroundClients.has(userSocket.id)) {
            // Queue for background client
            if (!messageQueues.has(userSocket.id)) {
              messageQueues.set(userSocket.id, []);
            }
            messageQueues.get(userSocket.id).push(messageData);
            console.log(`📦 Queued message for backgrounded user: ${roomUser.username}`);
          } else {
            // Send immediately
            userSocket.emit('chat_message', messageData);
          }
        }
      });
      
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

      if (callGracePeriod.has(callId)) {
        clearTimeout(callGracePeriod.get(callId));
        callGracePeriod.delete(callId);
        console.log(`⏱️ Cleared grace period for call ${callId}`);
      }

      socket.join(`call-${callId}`);
      console.log(`📞 User ${user.username} joined call room: call-${callId}`);

      const room = matchmaking.getRoom(call.roomId);
      const participants = room ? room.users.filter(u => call.participants.includes(u.userId)) : [];

      socket.emit('call_joined', {
        callId,
        callType: call.callType,
        participants: participants.map(p => ({
          userId: p.userId,
          username: p.username,
          pfpUrl: p.pfpUrl
        }))
      });

      socket.to(`call-${callId}`).emit('user_joined_call', {
        user: {
          userId: user.userId,
          username: user.username,
          pfpUrl: user.pfpUrl
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

      const targetSocket = findActiveSocketForUser(targetUserId);
      
      if (targetSocket) {
        targetSocket.emit('ice_candidate', {
          fromUserId: user.userId,
          candidate
        });
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
      if (candidateType === 'relay') {
        webrtcMetrics.turnUsage++;
      } else if (candidateType === 'srflx') {
        webrtcMetrics.stunUsage++;
      }
    }

    if (state === 'connected') {
      webrtcMetrics.successfulConnections++;
    } else if (state === 'failed') {
      webrtcMetrics.failedConnections++;
    }
  });

  socket.on('audio_state_changed', ({ callId, enabled }) => {
    try {
      const user = socketUsers.get(socket.id);
      if (!user) return;

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

      socket.emit('left_room', { roomId: result.roomId });
      console.log(`👋 ${user.username} left room ${result.roomId}`);
    }
  });

  // PRODUCTION FIX: Enhanced disconnect handler
  socket.on('disconnect', () => {
    const user = socketUsers.get(socket.id);
    
    if (user) {
      console.log(`🔌 User ${user.username} disconnected`);

      // Check if in active call - give EXTENDED grace
      const callId = userCalls.get(user.userId);
      if (callId) {
        const call = activeCalls.get(callId);
        if (call && call.status === 'active') {
          console.log(`⏱️ User ${user.username} in active call ${callId} - EXTENDED grace period (20s)`);
          
          const graceTimeout = setTimeout(() => {
            console.log(`⏱️ Extended grace period expired for ${user.username} in call ${callId}`);
            
            const reconnected = Array.from(socketUsers.values()).some(u => u.userId === user.userId);
            
            if (!reconnected) {
              console.log(`❌ User ${user.username} did not rejoin call`);
              
              call.participants = call.participants.filter(p => p !== user.userId);
              userCalls.delete(user.userId);

              io.to(`call-${callId}`).emit('user_left_call', {
                userId: user.userId,
                username: user.username
              });

              if (call.participants.length === 0) {
                activeCalls.delete(callId);
                callGracePeriod.delete(callId);
              }
              
              handleRoomCleanup(user.userId, socket.id);
            } else {
              console.log(`✅ User ${user.username} rejoined call`);
            }
          }, 20000); // 20 seconds for call navigation
          
          callGracePeriod.set(callId, graceTimeout);
          return;
        }
      }
      
      // Check if backgrounded - give EXTENDED grace
      if (backgroundClients.has(socket.id)) {
        console.log(`🌙 User ${user.username} disconnected while backgrounded - 60s grace`);
        
        setTimeout(() => {
          if (backgroundClients.has(socket.id)) {
            console.log(`⏱️ Background grace expired for ${user.username}`);
            handleRoomCleanup(user.userId, socket.id);
          } else {
            console.log(`✅ User ${user.username} reconnected from background`);
          }
        }, 60000); // 60 seconds for backgrounded app
        
        return;
      }
      
      // Normal cleanup
      handleRoomCleanup(user.userId, socket.id);
    } else {
      console.log('🔌 Unauthenticated client disconnected:', socket.id);
    }
  });
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
      console.log(`📞 WebRTC signaling enabled`);
      console.log(`✅ Production fixes applied: Message queuing, background handling, extended grace periods`);
      
      const hasTurn = !!(process.env.CLOUDFLARE_TURN_TOKEN_ID && process.env.CLOUDFLARE_TURN_API_TOKEN);
      if (hasTurn) {
        console.log(`✅ TURN server configured (Cloudflare)`);
      } else {
        console.log(`⚠️ TURN server NOT configured - set CLOUDFLARE_TURN_TOKEN_ID and CLOUDFLARE_TURN_API_TOKEN`);
      }
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
