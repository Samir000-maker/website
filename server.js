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

// Initialize Express app
const app = express();
const server = createServer(app);

// Initialize Socket.IO with CORS
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

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Configure multer for file uploads (memory storage)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: config.MAX_FILE_SIZE
  },
  fileFilter: (req, file, cb) => {
    // Accept images only
    if (!file.mimetype.startsWith('image/')) {
      return cb(new Error('Only image files are allowed'), false);
    }
    cb(null, true);
  }
});

// Rate limiting map for matchmaking
const matchmakingRateLimit = new Map();

// Call management
const activeCalls = new Map(); // callId -> { callId, roomId, callType, participants[], createdAt }
const userCalls = new Map(); // userId -> callId

// ============================================
// API ROUTES
// ============================================

/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    activeRooms: matchmaking.getActiveRooms().length,
    activeCalls: activeCalls.size,
    server: 'running'
  });
});

/**
 * Check username availability
 * POST /api/check-username
 * Body: { username: string }
 * NOTE: Does NOT require authentication
 */
app.post('/api/check-username', async (req, res) => {
  try {
    const { username } = req.body;

    console.log('Checking username:', username);

    // Validate username
    if (!username || typeof username !== 'string') {
      return res.status(400).json({ 
        available: false,
        error: 'Username is required' 
      });
    }

    const trimmedUsername = username.trim().toLowerCase();

    // Check length (3-20 characters)
    if (trimmedUsername.length < 3 || trimmedUsername.length > 20) {
      return res.status(400).json({ 
        available: false,
        error: 'Username must be between 3 and 20 characters' 
      });
    }

    // Check format - allow letters, numbers, underscores, and hyphens
    if (!/^[a-zA-Z0-9_-]+$/.test(trimmedUsername)) {
      return res.status(400).json({ 
        available: false,
        error: 'Username can only contain letters, numbers, underscores, and hyphens' 
      });
    }

    const db = getDB();
    
    // Check if username exists (case-insensitive)
    const existingUser = await db.collection('users').findOne(
      { username: trimmedUsername },
      { projection: { _id: 1 } }
    );

    if (existingUser) {
      console.log('Username taken:', trimmedUsername);
      
      // Generate suggestions
      const suggestions = [];
      for (let i = 0; i < 3; i++) {
        const suffix = Math.floor(Math.random() * 999) + 1;
        const suggestion = `${trimmedUsername}_${suffix}`;
        
        // Check if suggestion is available
        const suggestionExists = await db.collection('users').findOne(
          { username: suggestion },
          { projection: { _id: 1 } }
        );
        
        if (!suggestionExists) {
          suggestions.push(suggestion);
        }
      }

      return res.json({ 
        available: false, 
        suggestions 
      });
    }

    // Username is available
    console.log('Username available:', trimmedUsername);
    res.json({ available: true });
    
  } catch (error) {
    console.error('Check username error:', error);
    res.status(500).json({ 
      available: false,
      error: 'Internal server error',
      message: error.message
    });
  }
});

/**
 * Create/Update user profile
 * POST /api/users/profile
 * Body: { username: string, pfpUrl?: string }
 */
app.post('/api/users/profile', authenticateFirebase, async (req, res) => {
  try {
    const { username, pfpUrl } = req.body;
    const firebaseUser = req.firebaseUser;

    console.log('Creating/updating profile for:', firebaseUser.email, 'with username:', username);

    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    // Validate username format
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
    
    // Check if user already exists
    const existingUser = await db.collection('users').findOne({ 
      email: firebaseUser.email 
    });

    if (existingUser && existingUser.username !== trimmedUsername) {
      // User is trying to change username - check availability
      const usernameExists = await db.collection('users').findOne({
        username: trimmedUsername
      });

      if (usernameExists) {
        return res.status(400).json({ 
          error: 'Username already taken' 
        });
      }
    }

    const userData = {
      email: firebaseUser.email,
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

      // Invalidate cache
      await invalidateUserProfileCache(existingUser._id.toString());

      console.log('Profile updated for:', trimmedUsername);

      res.json({ 
        success: true, 
        userId: existingUser._id,
        message: 'Profile updated' 
      });
    } else {
      // Create new user
      userData.createdAt = new Date();
      
      const result = await db.collection('users').insertOne(userData);

      console.log('Profile created for:', trimmedUsername);

      res.json({ 
        success: true, 
        userId: result.insertedId,
        message: 'Profile created' 
      });
    }
  } catch (error) {
    if (error.code === 11000) {
      // Duplicate key error (username already exists)
      return res.status(400).json({ error: 'Username already taken' });
    }

    console.error('Create profile error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * Upload profile picture
 * POST /api/users/upload-pfp
 * Form data: file (image)
 */
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

      // Get user
      const user = await db.collection('users').findOne({ 
        email: firebaseUser.email 
      });

      if (!user) {
        return res.status(404).json({ error: 'User not found' });
      }

      // Upload to Cloudflare R2
      const pfpUrl = await uploadProfilePicture(
        req.file.buffer,
        req.file.mimetype,
        user._id.toString()
      );

      // Update user profile
      await db.collection('users').updateOne(
        { _id: user._id },
        { $set: { pfpUrl, updatedAt: new Date() } }
      );

      // Update cache
      const updatedUser = { ...user, pfpUrl };
      await updateUserProfileCache(user._id.toString(), updatedUser);

      res.json({ 
        success: true, 
        pfpUrl 
      });
    } catch (error) {
      console.error('Upload PFP error:', error);
      res.status(500).json({ error: 'Failed to upload profile picture' });
    }
  }
);

/**
 * Get user profile
 * GET /api/users/me
 */
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

/**
 * Post a worldwide note
 * POST /api/notes
 * Body: { text: string, mood?: string }
 */
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
    
    // Get user
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

    res.json({ 
      success: true, 
      noteId: result.insertedId 
    });
  } catch (error) {
    console.error('Post note error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * Get worldwide notes (paginated)
 * GET /api/notes?page=0&limit=25
 */
app.get('/api/notes', optionalFirebaseAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 0;
    const limit = Math.min(
      parseInt(req.query.limit) || config.NOTES_PAGE_SIZE, 
      config.NOTES_PAGE_SIZE
    );

    const db = getDB();
    
    // Use indexed query (sorted by createdAt descending)
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

/**
 * Get available moods
 * GET /api/moods
 */
app.get('/api/moods', (req, res) => {
  res.json({ moods: config.MOODS });
});

// ============================================
// SOCKET.IO REAL-TIME COMMUNICATION
// ============================================

// Store socket to user mapping
const socketUsers = new Map();

io.on('connection', (socket) => {
  console.log('🔌 Client connected:', socket.id);

  /**
   * Authenticate socket connection
   */
  socket.on('authenticate', async ({ token, userId }) => {
    try {
      console.log('🔐 Authenticating socket for userId:', userId);

      // Verify Firebase token
      let decodedToken;
      try {
        decodedToken = await verifyToken(token);
      } catch (error) {
        console.error('Token verification failed:', error);
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

      // Store user data for this socket
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

  /**
   * Join matchmaking queue
   */
  socket.on('join_matchmaking', async ({ mood }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) {
        console.error('❌ Unauthenticated socket tried to join matchmaking:', socket.id);
        socket.emit('error', { message: 'Not authenticated' });
        return;
      }

      console.log(`🎮 User ${user.username} joining matchmaking for mood: ${mood}`);

      // Validate mood
      const validMood = config.MOODS.find(m => m.id === mood);
      if (!validMood) {
        socket.emit('error', { message: 'Invalid mood' });
        return;
      }

      // Add to matchmaking queue with CURRENT socket ID
      const room = matchmaking.addToQueue({
        ...user,
        mood,
        socketId: socket.id
      });

      if (room) {
        // Match found! Notify all users and have them join the Socket.IO room
        console.log(`🎉 Match found! Room ${room.id} with ${room.users.length} users`);

        room.users.forEach(roomUser => {
          // Find the CURRENT active socket for each user
          const userSocket = findActiveSocketForUser(roomUser.userId);
          
          if (userSocket) {
            console.log(`📤 Emitting match_found to ${roomUser.username} on socket ${userSocket.id}`);
            
            // Join Socket.IO room IMMEDIATELY
            userSocket.join(room.id);
            console.log(`✅ User ${roomUser.username} joined Socket.IO room ${room.id}`);
            
            // Then emit match_found event
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
        // Queued, waiting for more users
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

  /**
   * Helper function to find active socket for a user
   */
  function findActiveSocketForUser(userId) {
    // Search through all connected sockets for this user
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

  /**
   * Join room (when user enters chat)
   */
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

      // Join Socket.IO room if not already joined
      if (!socket.rooms.has(roomId)) {
        socket.join(roomId);
        console.log(`✅ User ${user.username} joined Socket.IO room ${roomId}`);
      } else {
        console.log(`ℹ️ User ${user.username} already in Socket.IO room ${roomId}`);
      }

      // Confirm room joined
      socket.emit('room_joined', { roomId });
      
    } catch (error) {
      console.error('Join room error:', error);
      socket.emit('error', { message: 'Failed to join room' });
    }
  });

  /**
   * Cancel matchmaking
   */
  socket.on('cancel_matchmaking', () => {
    const user = socketUsers.get(socket.id);
    if (user) {
      matchmaking.cancelMatchmaking(user.userId);
      socket.emit('matchmaking_cancelled');
      console.log(`❌ Matchmaking cancelled: ${user.username}`);
    }
  });

  /**
   * Send chat message
   */
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

      // Build message data object
      const messageData = {
        userId: user.userId,
        username: user.username,
        pfpUrl: user.pfpUrl,
        message,
        timestamp: Date.now()
      };

      // Add replyTo if it exists
      if (replyTo) {
        messageData.replyTo = replyTo;
        console.log(`📧 Message is a reply to: ${replyTo.username}`);
      }

      // Add message to room (ephemeral storage)
      room.addMessage(messageData);

      console.log(`💬 Message in room ${roomId} from ${user.username}: ${message}`);

      // Broadcast to all users in room (including sender)
      io.to(roomId).emit('chat_message', messageData);
      
    } catch (error) {
      console.error('Chat message error:', error);
      socket.emit('error', { message: 'Failed to send message' });
    }
  });

  /**
   * Initiate call
   */
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

      // Create call
      const callId = uuidv4();
      const call = {
        callId,
        roomId,
        callType, // 'audio' or 'video'
        participants: [user.userId],
        createdAt: Date.now(),
        initiator: user.userId
      };

      activeCalls.set(callId, call);
      userCalls.set(user.userId, callId);

      console.log(`📞 Call initiated: ${callId} by ${user.username} (${callType})`);

      // Notify other users in room
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

  /**
   * Accept call
   */
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

      // Add user to call
      if (!call.participants.includes(user.userId)) {
        call.participants.push(user.userId);
        userCalls.set(user.userId, callId);
      }

      console.log(`✅ User ${user.username} accepted call ${callId}`);

      // Get room to fetch all users
      const room = matchmaking.getRoom(roomId);
      if (!room) {
        socket.emit('error', { message: 'Room not found' });
        return;
      }

      // Notify all participants
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

  /**
   * Decline call
   */
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

      // Notify initiator
      const initiatorSocket = findActiveSocketForUser(call.initiator);
      if (initiatorSocket) {
        initiatorSocket.emit('call_declined', {
          callId,
          userId: user.userId,
          username: user.username
        });
      }

      // Clean up call if no one accepted
      if (call.participants.length === 1) {
        activeCalls.delete(callId);
        userCalls.delete(call.initiator);
      }

    } catch (error) {
      console.error('Decline call error:', error);
      socket.emit('error', { message: 'Failed to decline call' });
    }
  });

  /**
   * Join call (WebRTC room)
   */
  socket.on('join_call', ({ callId }) => {
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
        socket.emit('error', { message: 'You are not in this call' });
        return;
      }

      // Join Socket.IO room for the call
      socket.join(`call-${callId}`);
      console.log(`📞 User ${user.username} joined call room: call-${callId}`);

      // Get room to fetch participant details
      const room = matchmaking.getRoom(call.roomId);
      const participants = room ? room.users.filter(u => call.participants.includes(u.userId)) : [];

      // Send current participants
      socket.emit('call_joined', {
        callId,
        callType: call.callType,
        participants: participants.map(p => ({
          userId: p.userId,
          username: p.username,
          pfpUrl: p.pfpUrl
        }))
      });

      // Notify other participants
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

  /**
   * Leave call
   */
  socket.on('leave_call', ({ callId }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      const call = activeCalls.get(callId);
      
      if (!call) return;

      // Remove user from call
      call.participants = call.participants.filter(p => p !== user.userId);
      userCalls.delete(user.userId);

      console.log(`📵 User ${user.username} left call ${callId}`);

      // Leave Socket.IO room
      socket.leave(`call-${callId}`);

      // Notify other participants
      io.to(`call-${callId}`).emit('user_left_call', {
        userId: user.userId,
        username: user.username
      });

      // Clean up call if empty
      if (call.participants.length === 0) {
        activeCalls.delete(callId);
        console.log(`🗑️ Call ${callId} ended (no participants)`);
      }

    } catch (error) {
      console.error('Leave call error:', error);
    }
  });

  /**
   * WebRTC Signaling: Offer
   */
  socket.on('webrtc_offer', ({ callId, targetUserId, offer }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      const targetSocket = findActiveSocketForUser(targetUserId);
      
      if (targetSocket) {
        targetSocket.emit('webrtc_offer', {
          fromUserId: user.userId,
          offer
        });
        console.log(`📤 WebRTC offer forwarded from ${user.username} to ${targetUserId}`);
      }

    } catch (error) {
      console.error('WebRTC offer error:', error);
    }
  });

  /**
   * WebRTC Signaling: Answer
   */
  socket.on('webrtc_answer', ({ callId, targetUserId, answer }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      const targetSocket = findActiveSocketForUser(targetUserId);
      
      if (targetSocket) {
        targetSocket.emit('webrtc_answer', {
          fromUserId: user.userId,
          answer
        });
        console.log(`📤 WebRTC answer forwarded from ${user.username} to ${targetUserId}`);
      }

    } catch (error) {
      console.error('WebRTC answer error:', error);
    }
  });

  /**
   * WebRTC Signaling: ICE Candidate
   */
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
        console.log(`🧊 ICE candidate forwarded from ${user.username} to ${targetUserId}`);
      }

    } catch (error) {
      console.error('ICE candidate error:', error);
    }
  });

  /**
   * Audio state changed
   */
  socket.on('audio_state_changed', ({ callId, enabled }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      // Broadcast to other participants
      socket.to(`call-${callId}`).emit('audio_state_changed', {
        userId: user.userId,
        enabled
      });

    } catch (error) {
      console.error('Audio state error:', error);
    }
  });

  /**
   * Video state changed
   */
  socket.on('video_state_changed', ({ callId, enabled }) => {
    try {
      const user = socketUsers.get(socket.id);
      
      if (!user) return;

      // Broadcast to other participants
      socket.to(`call-${callId}`).emit('video_state_changed', {
        userId: user.userId,
        enabled
      });

    } catch (error) {
      console.error('Video state error:', error);
    }
  });

  /**
   * Leave room
   */
  socket.on('leave_room', () => {
    const user = socketUsers.get(socket.id);
    
    if (!user) return;

    const result = matchmaking.leaveRoom(user.userId);
    
    if (result.roomId) {
      // Leave Socket.IO room
      socket.leave(result.roomId);

      // Notify other users
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

  /**
   * Handle disconnection
   */
  socket.on('disconnect', () => {
    const user = socketUsers.get(socket.id);
    
    if (user) {
      console.log(`🔌 User ${user.username} disconnected`);

      // Remove from matchmaking
      matchmaking.cancelMatchmaking(user.userId);
      
      // Check if user is in a call
      const callId = userCalls.get(user.userId);
      if (callId) {
        const call = activeCalls.get(callId);
        if (call) {
          // Remove from call
          call.participants = call.participants.filter(p => p !== user.userId);
          userCalls.delete(user.userId);

          // Notify other participants
          io.to(`call-${callId}`).emit('user_left_call', {
            userId: user.userId,
            username: user.username
          });

          // Clean up call if empty
          if (call.participants.length === 0) {
            activeCalls.delete(callId);
            console.log(`🗑️ Call ${callId} ended (disconnection)`);
          }
        }
      }
      
      // Check if user is in a room
      const roomId = matchmaking.getRoomIdByUser(user.userId);
      
      if (roomId) {
        // User is in a room - grace period
        console.log(`⏳ User ${user.username} disconnected but still in room ${roomId} (grace period)`);
        
        // Set a grace period timer (5 seconds)
        setTimeout(() => {
          // Check if user reconnected
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
          } else {
            console.log(`✅ User ${user.username} reconnected, keeping in room ${roomId}`);
          }
        }, 5000); // 5 second grace period
      }

      socketUsers.delete(socket.id);
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
    // Connect to MongoDB
    await connectDB();

    // Initialize Firebase
    initializeFirebase();

    // Start server
    const PORT = config.PORT;
    server.listen(PORT, () => {
      console.log(`🚀 Server running on port ${PORT}`);
      console.log(`📊 Health check: http://localhost:${PORT}/health`);
      console.log(`🔌 Socket.IO ready for connections`);
      console.log(`📞 WebRTC signaling enabled`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
