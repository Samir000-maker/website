// ───────────────────────────────
// 1. Console Silencing
// ───────────────────────────────
(function () {
  try {
    window.__VIBE_SILENCE_CONSOLE__ = false;
  } catch { }
})();

// ───────────────────────────────
// 2. Firebase Init + Presence & Heartbeat
// ───────────────────────────────
(function () {
  if (!firebase.apps.length) {
    firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__);
  }
  firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL);

  let socketInstance = null;
  let currentRoomId = null;
  let heartbeatInterval = null;
  let socketReconnectFailures = 0;
  let lastVisibilityHiddenAt = 0;
  let controlledReloadInProgress = false;

  const SIGNALING_QUEUE_KEY = 'vibe_chat_socket_queue_v1';
  let socketEmitQueue = [];

  function loadSocketEmitQueue() {
    try {
      const raw = sessionStorage.getItem(SIGNALING_QUEUE_KEY);
      const parsed = raw ? JSON.parse(raw) : [];
      if (Array.isArray(parsed)) {
        socketEmitQueue = parsed;
      }
    } catch {
      socketEmitQueue = [];
    }
  }

  function persistSocketEmitQueue() {
    try {
      sessionStorage.setItem(SIGNALING_QUEUE_KEY, JSON.stringify(socketEmitQueue.slice(-500)));
    } catch { }
  }

  function enqueueSocketEmit(eventName, payload) {
    socketEmitQueue.push({
      eventName,
      payload,
      queuedAt: Date.now()
    });
    persistSocketEmitQueue();
  }

  function flushSocketEmitQueue() {
    if (!socketInstance?.connected) return;
    if (!socketEmitQueue.length) return;

    const now = Date.now();
    const ttlMs = 2 * 60 * 1000;
    const queue = socketEmitQueue;
    socketEmitQueue = [];
    persistSocketEmitQueue();

    let flushed = 0;
    for (const item of queue) {
      if (!item || !item.eventName) continue;
      if (item.queuedAt && (now - item.queuedAt) > ttlMs) continue;
      try {
        socketInstance.emit(item.eventName, item.payload);
        flushed++;
      } catch {
        enqueueSocketEmit(item.eventName, item.payload);
      }
    }

    if (flushed > 0) {
      console.log(`📤 Flushed ${flushed} queued socket message(s)`);
    }
  }

  function safeSocketEmit(eventName, payload, options = {}) {
    const { queueWhenDisconnected = true } = options;
    if (socketInstance?.connected) {
      try {
        socketInstance.emit(eventName, payload);
        return true;
      } catch {
        if (queueWhenDisconnected) enqueueSocketEmit(eventName, payload);
        return false;
      }
    }
    if (queueWhenDisconnected) enqueueSocketEmit(eventName, payload);
    return false;
  }

  function safeSocketEmitWithAck(eventName, payload, ack, options = {}) {
    const { queueWhenDisconnected = true } = options;
    if (socketInstance?.connected) {
      try {
        socketInstance.emit(eventName, payload, ack);
        return true;
      } catch {
        if (queueWhenDisconnected) enqueueSocketEmit(eventName, payload);
        return false;
      }
    }
    if (queueWhenDisconnected) enqueueSocketEmit(eventName, payload);
    return false;
  }

  function startHeartbeat() {
    if (heartbeatInterval) stopHeartbeat();
    console.log('💓 [Presence] Starting heartbeat interval');
    const heartbeatRoomId = currentRoomId;

    if (socketInstance && socketInstance.connected && heartbeatRoomId) {
      safeSocketEmit('heartbeat', {
        roomId: heartbeatRoomId,
        location: 'chat',
        path: window.location.pathname
      });
    }

    heartbeatInterval = setInterval(() => {
      if (socketInstance && socketInstance.connected) {
        const roomId = currentRoomId;
        if (roomId) {
          safeSocketEmit('heartbeat', {
            roomId,
            location: 'chat',
            path: window.location.pathname
          });
        }
      }
    }, 10000);
  }

  function stopHeartbeat() {
    if (heartbeatInterval) {
      console.log('💓 [Presence] Stopping heartbeat interval');
      clearInterval(heartbeatInterval);
      heartbeatInterval = null;
    }
  }

  function getStableSocketSessionId() {
    if (window.MoodApp?.Session && typeof window.MoodApp.Session.getSocketSessionId === 'function') {
      return window.MoodApp.Session.getSocketSessionId();
    }
    try {
      let sessionId = sessionStorage.getItem('vibe_socket_session_id');
      if (sessionId) return sessionId;
      sessionId = `sess_${Date.now()}_${Math.random().toString(36).slice(2, 12)}`;
      sessionStorage.setItem('vibe_socket_session_id', sessionId);
      return sessionId;
    } catch {
      return `sess_${Date.now()}_${Math.random().toString(36).slice(2, 12)}`;
    }
  }

  function getScopedSocketSessionId(scope) {
    const base = getStableSocketSessionId();
    const safeScope = String(scope || 'default').toLowerCase().replace(/[^a-z0-9_-]/g, '');
    return `${base}:${safeScope || 'default'}`;
  }

  function triggerSocketRecoveryReload(reason = 'chat_socket_recovery') {
    if (controlledReloadInProgress) return;
    controlledReloadInProgress = true;

    try {
      const key = 'chat_socket_recovery_state';
      const now = Date.now();
      const state = JSON.parse(sessionStorage.getItem(key) || '{}');
      const withinWindow = state.lastReloadAt && (now - state.lastReloadAt) < (5 * 60 * 1000);
      const count = withinWindow ? (state.count || 0) + 1 : 1;

      if (count > 2) {
        controlledReloadInProgress = false;
        console.error(`❌ [Socket] Reload suppressed by loop guard (${reason})`);
        return;
      }

      sessionStorage.setItem(key, JSON.stringify({ count, lastReloadAt: now, reason }));
    } catch { }

    window.location.reload();
  }

  function reconnectSocketIfNeeded(trigger = 'resume') {
    if (!socketInstance) return;
    if (!socketInstance.connected) {
      console.log(`🔄 [Socket] Reconnect requested (${trigger})`);
      socketInstance.connect();
    } else if (currentRoomId) {
      safeSocketEmit('heartbeat', {
        roomId: currentRoomId,
        location: 'chat',
        path: window.location.pathname
      });
    }
  }

  window.__chatGlobals = {
    get socketInstance() { return socketInstance; },
    set socketInstance(v) { socketInstance = v; },
    get currentRoomId() { return currentRoomId; },
    set currentRoomId(v) { currentRoomId = v; },
    get controlledReloadInProgress() { return controlledReloadInProgress; },
    set controlledReloadInProgress(v) { controlledReloadInProgress = v; },
    startHeartbeat,
    stopHeartbeat,
    loadSocketEmitQueue,
    flushSocketEmitQueue,
    enqueueSocketEmit,
    safeSocketEmit,
    safeSocketEmitWithAck,
    getStableSocketSessionId,
    getScopedSocketSessionId,
    triggerSocketRecoveryReload,
    reconnectSocketIfNeeded,
    socketReconnectFailures: 0,
    lastVisibilityHiddenAt: 0,
    SIGNALING_QUEUE_KEY,
    socketEmitQueue
  };
})();

// ───────────────────────────────
// 3. Main Chat App Script
// ───────────────────────────────
(async () => {
  try {
    try {
      const _AuthEarly = window.MoodApp && window.MoodApp.Auth;
      if (_AuthEarly && typeof _AuthEarly.requireAuth === 'function') {
        await _AuthEarly.requireAuth();
      }
    } catch { }

    const MoodApp = window.MoodApp || {};
    const _Auth = MoodApp.Auth;
    const _API = MoodApp.API;
    const _Toast = MoodApp.Toast;
    const _Utils = MoodApp.Utils;
    const _Session = MoodApp.Session;

    const toast = (m, t = 'success') => {
      if (_Toast) {
        const toastType = (t === 'info' && typeof _Toast.info !== 'function') ? 'success' : t;
        if (typeof _Toast[toastType] === 'function') {
          return _Toast[toastType](m);
        }
      }
      console[t === 'error' ? 'error' : 'log'](m);
    };

    // ============================================
    // STORAGE KEYS
    // ============================================
    const CHAT_STATE_KEY = 'chat_state';
    const CHAT_MESSAGES_KEY = 'chat_messages';
    const CHAT_TIMESTAMP_KEY = 'chat_timestamp';
    const ACTIVE_CALL_KEY = 'activeCall';

    const CACHED_CALL_KEY = 'cachedIncomingCall';
    const CALL_CACHE_TIMEOUT = 60000;
    let cachedCallCheckInterval = null;

    // ============================================
    // CALL STATE MONITORING
    // ============================================
    let activeCallConnectionState = null;

    function getActiveChatRoomId() {
      return roomData?.roomId || currentRoomId || null;
    }

    function shouldPreservePendingActiveCall() {
      try {
        return sessionStorage.getItem('returningFromCall') === 'true'
          || sessionStorage.getItem('backgroundCallMode') === 'true'
          || (window.parent && window.parent !== window);
      } catch {
        return false;
      }
    }

    function resetHeaderCallButtons(reason = 'no active call') {
      try { console.log(`📞 Resetting call header buttons (${reason})`); } catch { }
      activeCallConnectionState = null;
      activeCallInRoom = null;
      updateCallButtonStates();
      updateCallButtonState(false);
    }

    function clearStoredActiveCall(reason = 'unknown') {
      try {
        console.warn(`📞 Clearing stale activeCall (${reason})`);
        localStorage.removeItem(ACTIVE_CALL_KEY);
      } catch { }
      resetHeaderCallButtons(reason);
    }

    function updateActiveCallState() {
      const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);

      if (!activeCallStr) {
        if (activeCallConnectionState !== null) {
          console.log('📞 No active call - clearing state');
          resetHeaderCallButtons('storage empty');
        }
        return;
      }

      const clearStaleActiveCall = (reason) => {
        try {
          console.warn(`📞 Clearing stale activeCall (${reason || 'unknown'})`);
          localStorage.removeItem(ACTIVE_CALL_KEY);
        } catch { }
        resetHeaderCallButtons(reason || 'stale activeCall');
      };

      try {
        const callData = JSON.parse(activeCallStr);
        if (!callData?.callId || !callData?.roomId) {
          console.warn('📞 Invalid activeCall payload - clearing');
          clearStaleActiveCall('invalid payload');
          return;
        }

        const activeRoomId = getActiveChatRoomId();
        if (activeRoomId && callData.roomId !== activeRoomId) {
          clearStaleActiveCall(`room mismatch (${callData.roomId} !== ${activeRoomId})`);
          return;
        }

        if (!activeRoomId && !shouldPreservePendingActiveCall()) {
          clearStaleActiveCall('chat room not resolved yet');
          return;
        }

        const storedAt = typeof callData?.storedAt === 'number' ? callData.storedAt : 0;
        const ageMs = storedAt ? (Date.now() - storedAt) : Number.POSITIVE_INFINITY;
        const MAX_STALE_MS = 2 * 60 * 1000;
        if (!storedAt || ageMs > MAX_STALE_MS) {
          const state = callData.connectionState || 'connecting';
          const isActiveState = (state === 'initializing' || state === 'connecting' || state === 'connected');
          if (isActiveState && !storedAt) {
            try {
              callData.storedAt = Date.now();
              localStorage.setItem(ACTIVE_CALL_KEY, JSON.stringify(callData));
            } catch { }
          } else {
            clearStaleActiveCall(`stale (${Math.round(ageMs / 1000)}s)`);
            return;
          }
        }

        const newState = callData.connectionState || 'connecting';

        if (activeCallConnectionState !== newState) {
          console.log(`📞 Active call state changed: ${activeCallConnectionState} → ${newState}`);
          activeCallConnectionState = newState;

          if (activeCallConnectionState === 'initializing' || activeCallConnectionState === 'connecting' || activeCallConnectionState === 'connected') {
            const modal = document.getElementById('incomingCallModal');
            if (modal && !modal.classList.contains('hidden')) {
              console.log('🧹 Hiding incoming call modal - user is now in an active/initiating callState');
              modal.classList.add('hidden');
              pendingCallData = null;
            }
          }

          updateCallButtonStates();

          const callInCurrentRoom = callData.roomId === getActiveChatRoomId();

          if (callInCurrentRoom) {
            if (!activeCallInRoom || activeCallInRoom.callId !== callData.callId) {
              console.log('💾 Synchronizing activeCallInRoom from localStorage update');
              activeCallInRoom = {
                callId: callData.callId,
                callType: callData.callType,
                participantCount: Array.isArray(callData.participants) ? callData.participants.length : 0
              };
            }

            updateCallButtonState(true, activeCallInRoom);
          } else {
            if (activeCallConnectionState === null) {
              updateCallButtonState(false);
            }
          }
        }
      } catch (e) {
        console.error('❌ Failed to parse active call data:', e);
        clearStaleActiveCall('parse error');
      }
    }

    function updateCallButtonStates() {
      if (!audioCallBtn || !videoCallBtn) return;

      if (activeCallConnectionState === 'connecting' || activeCallConnectionState === 'initializing') {
        console.log('🚫 Disabling call buttons - call connecting');
        audioCallBtn.disabled = true;
        audioCallBtn.classList.remove('call-ready');
        audioCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
        audioCallBtn.title = 'Call connecting...';

        videoCallBtn.disabled = true;
        videoCallBtn.classList.remove('call-ready');
        videoCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
        videoCallBtn.title = 'Call connecting...';

      } else if (activeCallConnectionState === 'connected') {
        console.log('✅ Call connected - disabling initiate buttons');
        audioCallBtn.disabled = true;
        videoCallBtn.disabled = true;
        audioCallBtn.classList.remove('call-ready');
        videoCallBtn.classList.remove('call-ready');
        audioCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
        videoCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
        audioCallBtn.title = 'Call in progress';
        videoCallBtn.title = 'Call in progress';

        const callIframeContainer = document.getElementById('callIframeContainer');
        if (callIframeContainer && callIframeContainer.classList.contains('hidden')) {
          console.log('🎬 Call connected - automatically showing call UI');
          callIframeContainer.classList.remove('hidden');
        }

      } else {
        if (isSocketAuthenticated && hasJoinedRoom && roomData?.roomId && !isInitiatingCall) {
          audioCallBtn.disabled = false;
          videoCallBtn.disabled = false;
          audioCallBtn.classList.remove('opacity-50', 'cursor-not-allowed', 'animate-pulse', 'call-btn-loading', 'calling');
          videoCallBtn.classList.remove('opacity-50', 'cursor-not-allowed', 'animate-pulse', 'call-btn-loading', 'calling');
          audioCallBtn.classList.add('call-ready');
          videoCallBtn.classList.add('call-ready');
          audioCallBtn.removeAttribute('aria-disabled');
          videoCallBtn.removeAttribute('aria-disabled');
          audioCallBtn.title = 'Start audio call';
          videoCallBtn.title = 'Start video call';
        } else {
          audioCallBtn.disabled = true;
          videoCallBtn.disabled = true;
          audioCallBtn.classList.remove('call-ready');
          videoCallBtn.classList.remove('call-ready');
          audioCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
          videoCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
          audioCallBtn.setAttribute('aria-disabled', 'true');
          videoCallBtn.setAttribute('aria-disabled', 'true');
          audioCallBtn.title = isInitiatingCall ? 'Calling...' : 'Preparing chat...';
          videoCallBtn.title = isInitiatingCall ? 'Calling...' : 'Preparing chat...';
        }
      }
    }

    window.addEventListener('storage', (e) => {
      if (e.key === 'callStateChanged' || e.key === 'activeCall') {
        console.log('🔄 Storage event detected - updating call state');
        updateActiveCallState();
      }
    });

    // ============================================
    // TYPING INDICATOR STATE & FUNCTIONS
    // ============================================
    const typingUsers = new Map();
    let typingTimeout = null;
    let isCurrentlyTyping = false;

    function updateTypingUI() {
      const typingIndicator = document.getElementById('typingIndicator');
      const typingText = document.getElementById('typingText');

      if (typingIndicator && typingText) {
        if (typingUsers.size > 0) {
          const names = Array.from(typingUsers.values());
          if (names.length === 1) {
            typingText.textContent = `${names[0]} is typing...`;
          } else if (names.length === 2) {
            typingText.textContent = `${names[0]} and ${names[1]} are typing...`;
          } else {
            typingText.textContent = "Multiple people are typing...";
          }
          typingIndicator.classList.remove('hidden');
        } else {
          typingIndicator.classList.add('hidden');
        }
      }
    }

    const stopTyping = () => {
      if (isCurrentlyTyping && socketInstance?.connected) {
        isCurrentlyTyping = false;
        socketInstance.emit('user_stop_typing', { roomId: roomData?.roomId });
      }
    };

    const handleTyping = () => {
      if (!isCurrentlyTyping && socketInstance?.connected) {
        isCurrentlyTyping = true;
        socketInstance.emit('user_typing', { roomId: roomData?.roomId });
      }

      if (typingTimeout) clearTimeout(typingTimeout);
      typingTimeout = setTimeout(stopTyping, 2000);
    };

    window.addEventListener('message', (event) => {
      const action = event.data?.action || event.data?.type;

      if (action === 'hideCall' || action === 'REQUEST_SHOW_CHAT' || action === 'FORCE_HIDE_CALL') {
        console.log(`📨 Message from call iframe: ${action}`);
        const callIframeContainer = document.getElementById('callIframeContainer');
        if (callIframeContainer) {
          callIframeContainer.classList.add('hidden');
          console.log('✅ Call iframe hidden - chat visible');
        }

        if (action === 'FORCE_HIDE_CALL') {
          clearStoredActiveCall('call iframe closed');
        }
      } else if (action === 'REQUEST_SHOW_CALL') {
        console.log('📨 Message from call iframe: REQUEST_SHOW_CALL');
        const callIframeContainer = document.getElementById('callIframeContainer');
        if (callIframeContainer) {
          callIframeContainer.classList.remove('hidden');
          console.log('✅ Call iframe shown');
        }
      }
    });

    setInterval(updateActiveCallState, 1000);

    let pendingAttachment = null;
    let isImageZoomed = false;
    // ============================================
    // STATE MANAGEMENT
    // ============================================
    let replyingTo = null;
    let isInitiatingCall = false;
    let callInitiationTimeoutId = null;
    let pendingCallData = null;
    let navigatingToCall = false;
    let messagesCache = [];
    let activeCallInRoom = null;
    let backButtonHandled = false;
    let presenceInvalidCount = 0;
    let lastPresenceInvalidAt = 0;
    let timerInterval = null;
    let currentUser = null;
    let roomData = null;
    const urlParams = new URLSearchParams(window.location.search);
    const chatMode = urlParams.get('mode');
    const isSocialClubMode = chatMode === 'social-club';

    let userToSocketId = new Map();
    let hasJoinedRoom = false;
    let pendingInitialRoomSync = false;
    let freshRoomCreationInProgress = false;

    function clearStaleRoomAndCreateFresh(source = 'stale_room') {
      if (freshRoomCreationInProgress) return;
      if (isSocialClubMode) return;
      if (!selectedMood) return;

      freshRoomCreationInProgress = true;

      try {
        localStorage.removeItem('currentRoom');
      } catch { }

      roomData = null;
      currentRoomId = null;
      hasJoinedRoom = false;
      pendingInitialRoomSync = false;

      try {
        showChatLoadingOverlay('Creating your room…', 'You can invite someone anytime.');
      } catch { }

      reportChatPresenceContext('stale_room_recreate', {
        allowRedirect: false,
        keepalive: true,
        source
      }).catch(() => { });

      safeSocketEmit('join_matchmaking', { mood: selectedMood });
    }

    const chatHeaderTitleEl = document.getElementById('chatHeaderTitle');
    if (chatHeaderTitleEl && isSocialClubMode) {
      chatHeaderTitleEl.textContent = 'Social Club';
    }
    let serverClockOffset = 0;
    let serverExpiresAt = null;
    const CLOCK_SKEW_NOTICE_MS = 5000;
    const CLOCK_SKEW_WARNING_MS = 30000;

    const selectedMood = (() => {
      try {
        const v = localStorage.getItem('selectedMood');
        return (v && typeof v === 'string') ? v : null;
      } catch {
        return null;
      }
    })();

    const showChatLoadingOverlay = (titleText = 'Preparing your room…', subtitleText = 'Connecting you now…') => {
      try {
        let overlay = document.getElementById('chatLoadingOverlay');
        if (!overlay) {
          overlay = document.createElement('div');
          overlay.id = 'chatLoadingOverlay';
          overlay.style.cssText = 'position:fixed;inset:0;z-index:99999;background:rgba(11,12,16,0.92);backdrop-filter:blur(10px);display:flex;align-items:center;justify-content:center;padding:24px;';
          overlay.innerHTML = `
            <div style="max-width:420px;width:100%;border:1px solid rgba(255,255,255,0.08);border-radius:18px;padding:22px 20px;background:linear-gradient(135deg, rgba(21,22,28,0.85) 0%, rgba(11,12,16,0.85) 100%);box-shadow:0 25px 70px rgba(0,0,0,0.45);">
              <div style="display:flex;align-items:center;gap:12px;">
                <div style="width:40px;height:40px;border-radius:12px;background:rgba(51,191,204,0.15);border:1px solid rgba(51,191,204,0.25);display:flex;align-items:center;justify-content:center;">
                  <span class="material-symbols-outlined" style="color:#33bfcc;">forum</span>
                </div>
                <div style="flex:1;min-width:0;">
                  <div id="chatLoadingTitle" style="color:#f0f6fc;font-weight:800;font-size:1.05rem;line-height:1.2;">${titleText}</div>
                  <div id="chatLoadingSubtitle" style="color:#8b949e;font-size:0.9rem;margin-top:4px;">${subtitleText}</div>
                </div>
              </div>
              <div style="margin-top:16px;height:10px;border-radius:999px;background:rgba(255,255,255,0.06);overflow:hidden;">
                <div style="height:100%;width:40%;background:linear-gradient(90deg,#33bfcc,#a855f7);border-radius:999px;animation:chat-loading-bar 1.2s ease-in-out infinite alternate;"></div>
              </div>
            </div>
            <style>@keyframes chat-loading-bar{from{transform:translateX(-30%);}to{transform:translateX(130%);}}</style>
          `;
          document.body.appendChild(overlay);
        } else {
          const titleEl = document.getElementById('chatLoadingTitle');
          const subEl = document.getElementById('chatLoadingSubtitle');
          if (titleEl) titleEl.textContent = titleText;
          if (subEl) subEl.textContent = subtitleText;
          overlay.style.display = 'flex';
        }
      } catch { }
    };

    const hideChatLoadingOverlay = () => {
      try {
        const overlay = document.getElementById('chatLoadingOverlay');
        if (overlay) overlay.style.display = 'none';
      } catch { }
    };

    let roomRecoveryInProgress = false;
    let roomRecoveryAttempts = 0;
    let lastRoomRecoveryAttemptAt = 0;
    const recentLifecycleMessages = new Map();
    const LIFECYCLE_MESSAGE_DEDUPE_MS = 8000;
    let isConfirmationDialogOpen = false;
    let confirmationDialogResolve = null;
    let leaveSequenceInProgress = false;
    // ============================================
    // EMOJI PICKER STATE
    // ============================================
    let emojiData = null;
    let emojiCache = null;
    let currentEmojiCategory = 'all';
    let emojiSearchTerm = '';
    let isEmojiPickerOpen = false;

    function shouldShowLifecycleMessage(kind, userId) {
      const key = `${kind}:${userId || 'unknown'}`;
      const now = Date.now();
      const lastAt = recentLifecycleMessages.get(key) || 0;
      if (now - lastAt < LIFECYCLE_MESSAGE_DEDUPE_MS) {
        return false;
      }
      recentLifecycleMessages.set(key, now);

      for (const [k, t] of recentLifecycleMessages.entries()) {
        if (now - t > LIFECYCLE_MESSAGE_DEDUPE_MS * 4) {
          recentLifecycleMessages.delete(k);
        }
      }
      return true;
    }

    let floatingCallPopup = null;
    let floatingCallSocket = null;
    let currentSpeaker = null;
    let isInBackgroundCall = false;

    // ============================================
    // DOM ELEMENTS (Initialized early)
    // ============================================
    const sidebar = document.getElementById('sidebar');
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebarClose = document.getElementById('sidebarClose');
    const sidebarOverlay = document.getElementById('sidebarOverlay');
    const messagesList = document.getElementById('messagesList');
    const messageForm = document.getElementById('messageForm');
    const messageInput = document.getElementById('messageInput');
    const sendMessageBtn = document.getElementById('sendMessageBtn');
    const composerBar = document.getElementById('composerBar');
    const usersList = document.getElementById('usersList');
    const onlineCount = document.getElementById('onlineCount');
    const leaveBtn = document.getElementById('leaveBtn');

    const chatMainEl = document.querySelector('.chat-main');
    const headerEl = document.querySelector('header');
    const initialViewportHeight = (window.visualViewport && window.visualViewport.height) ? window.visualViewport.height : window.innerHeight;

    const updateKeyboardOffset = () => {
      try {
        const vv = window.visualViewport;
        if (!vv) {
          if (chatMainEl) {
            chatMainEl.style.height = '';
            chatMainEl.style.maxHeight = '';
            chatMainEl.style.flex = '';
          }
          document.body.style.height = '';
          document.body.style.overflow = '';
          document.documentElement.style.overflow = '';
          return;
        }

        const headerH = headerEl ? Math.ceil(headerEl.getBoundingClientRect().height) : 0;
        const targetH = Math.max(0, Math.floor(vv.height - headerH));
        if (chatMainEl) {
          chatMainEl.style.height = targetH + 'px';
          chatMainEl.style.maxHeight = targetH + 'px';
        }

        const keyboardOpen = vv.height < (initialViewportHeight - 80);
        if (keyboardOpen) {
          document.body.style.height = vv.height + 'px';
          document.body.style.overflow = 'hidden';
          document.documentElement.style.overflow = 'hidden';
          if (chatMainEl) {
            chatMainEl.style.flex = '0 0 auto';
          }
        } else {
          document.body.style.height = '';
          document.body.style.overflow = '';
          document.documentElement.style.overflow = '';
          if (chatMainEl) {
            chatMainEl.style.flex = '';
          }
        }

        if (document.activeElement === messageInput) {
          try {
            if (typeof messageInput.scrollIntoView === 'function') {
              messageInput.scrollIntoView({ block: 'end' });
            }
          } catch (e) { }

          const scrollToBottom = () => {
            if (messagesList) {
              messagesList.scrollTop = messagesList.scrollHeight;
            } else {
              window.scrollTo(0, document.body.scrollHeight);
            }
          };

          try {
            requestAnimationFrame(() => {
              scrollToBottom();
              setTimeout(scrollToBottom, 50);
              setTimeout(scrollToBottom, 250);
            });
          } catch (e) {
            scrollToBottom();
          }
        }
      } catch (e) {
        if (chatMainEl) {
          chatMainEl.style.height = '';
          chatMainEl.style.maxHeight = '';
          chatMainEl.style.flex = '';
        }
        document.body.style.height = '';
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
      }
    };

    if (window.visualViewport) {
      window.visualViewport.addEventListener('resize', updateKeyboardOffset);
      window.visualViewport.addEventListener('scroll', updateKeyboardOffset);
    }

    window.addEventListener('resize', updateKeyboardOffset);
    window.addEventListener('orientationchange', updateKeyboardOffset);

    if (messageInput) {
      messageInput.addEventListener('focus', () => {
        updateKeyboardOffset();
        setTimeout(updateKeyboardOffset, 50);
        setTimeout(updateKeyboardOffset, 250);
      });
      messageInput.addEventListener('blur', () => {
        if (chatMainEl) {
          chatMainEl.style.height = '';
        }
        setTimeout(updateKeyboardOffset, 50);
      });
    }

    if (sendMessageBtn && messageForm) {
      const fastSubmit = (e) => {
        try {
          e.preventDefault();
          e.stopPropagation();
        } catch { }
        try {
          if (typeof messageForm.requestSubmit === 'function') {
            messageForm.requestSubmit();
          } else {
            messageForm.dispatchEvent(new Event('submit', { cancelable: true, bubbles: true }));
          }
        } catch { }
      };
      sendMessageBtn.addEventListener('pointerdown', fastSubmit, { passive: false });
      sendMessageBtn.addEventListener('touchstart', fastSubmit, { passive: false });
    }

    updateKeyboardOffset();
    const cancelReplyBtn = document.getElementById('cancelReply');
    const audioCallBtn = document.getElementById('audioCallBtn');
    const videoCallBtn = document.getElementById('videoCallBtn');
    const joinCallBtn = document.getElementById('joinCallBtn');
    const incomingCallModal = document.getElementById('incomingCallModal');
    const acceptCallBtn = document.getElementById('acceptCallBtn');
    const declineCallBtn = document.getElementById('declineCallBtn');

    // ============================================
    // UTILITY FUNCTIONS
    // ============================================

    // ============================================
    // AUTHENTICATION STATE MANAGEMENT
    // ============================================
    let isSocketAuthenticated = false;
    let authenticationPromise = null;

    // ============================================
    // EMOJI PICKER FUNCTIONS
    // ============================================

    function findActiveSocketForUser(userId) {
      const socketId = userToSocketId.get(userId);
      if (!socketId) {
        return null;
      }

      const socketInstance = io.sockets.sockets.get(socketId);
      if (socketInstance && socketInstance.connected) {
        return socketInstance;
      }

      userToSocketId.delete(userId);
      return null;
    }

    async function fetchEmojis() {
      console.log('😀 ========================================');
      console.log('😀 FETCHING EMOJIS FROM API');
      console.log('😀 ========================================');

      if (emojiData) {
        console.log('✅ Using cached emoji data');
        console.log('😀 ========================================\n');
        return emojiData;
      }

      try {
        console.log('📡 Fetching from: https://emojihub.yurace.pro/api/all');
        const response = await fetch('https://emojihub.yurace.pro/api/all');

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        emojiData = await response.json();

        console.log(`✅ Fetched ${emojiData.length} emojis`);
        console.log(`   Categories: ${[...new Set(emojiData.map(e => e.category))].length}`);
        console.log('😀 ========================================\n');

        return emojiData;

      } catch (error) {
        console.error('❌ ========================================');
        console.error('❌ EMOJI FETCH FAILED');
        console.error('❌ ========================================');
        console.error('   Error:', error.message);
        console.error('   Stack:', error.stack);
        console.error('❌ ========================================\n');
        throw error;
      }
    }

    const emojiBtn = document.querySelector('.emoji-btn');
    if (emojiBtn) {
      emojiBtn.addEventListener('click', openEmojiPicker, { passive: true });
      console.log('✅ Emoji button listener attached');
    }

    const emojiPickerOverlay = document.getElementById('emojiPickerOverlay');
    if (emojiPickerOverlay) {
      emojiPickerOverlay.addEventListener('click', (e) => {
        if (e.target === emojiPickerOverlay) {
          closeEmojiPicker();
        }
      }, { passive: true });
      console.log('✅ Emoji overlay listener attached');
    }

    const emojiSearchInput = document.getElementById('emojiSearchInput');
    if (emojiSearchInput) {
      const debouncedSearch = debounceEmojiSearch((value) => {
        emojiSearchTerm = value;
        renderEmojiGrid();
      }, 150);

      emojiSearchInput.addEventListener('input', (e) => {
        debouncedSearch(e.target.value);
      }, { passive: true });

      console.log('✅ Emoji search listener attached');
    }

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && isEmojiPickerOpen) {
        closeEmojiPicker();
      }
    });

    function processEmojiData(rawData) {
      console.log('⚙️ Processing emoji data for optimized access...');

      const categories = {};
      const searchIndex = [];

      rawData.forEach((emoji, index) => {
        const emojiChar = emoji.htmlCode
          .map(code => String.fromCodePoint(parseInt(code.replace('&#', '').replace(';', ''))))
          .join('');

        const processed = {
          id: index,
          char: emojiChar,
          name: emoji.name,
          category: emoji.category,
          group: emoji.group,
          searchText: `${emoji.name} ${emoji.category} ${emoji.group}`.toLowerCase()
        };

        if (!categories[emoji.category]) {
          categories[emoji.category] = [];
        }
        categories[emoji.category].push(processed);

        searchIndex.push(processed);
      });

      console.log(`✅ Processed ${searchIndex.length} emojis into ${Object.keys(categories).length} categories`);

      return { categories, searchIndex, all: searchIndex };
    }

    function filterEmojis(searchTerm, category) {
      if (!emojiCache) return [];

      let emojis = category === 'all'
        ? emojiCache.all
        : emojiCache.categories[category] || [];

      if (searchTerm) {
        const search = searchTerm.toLowerCase().trim();
        emojis = emojis.filter(emoji => emoji.searchText.includes(search));
      }

      return emojis;
    }

    function renderEmojiCategories() {
      const container = document.getElementById('emojiCategories');
      if (!container || !emojiCache) return;

      const categories = ['all', ...Object.keys(emojiCache.categories).sort()];

      container.innerHTML = categories.map(cat => {
        const isActive = cat === currentEmojiCategory;
        const displayName = cat === 'all' ? 'All' : cat;
        return `
      <button 
        class="emoji-category-tab ${isActive ? 'active' : ''}" 
        data-category="${cat}"
      >
        ${displayName}
      </button>
    `;
      }).join('');

      container.querySelectorAll('.emoji-category-tab').forEach(btn => {
        btn.addEventListener('click', () => {
          currentEmojiCategory = btn.dataset.category;
          renderEmojiCategories();
          renderEmojiGrid();
        }, { passive: true });
      });
    }

    function renderEmojiGrid() {
      const container = document.getElementById('emojiGrid');
      if (!container) return;

      const filteredEmojis = filterEmojis(emojiSearchTerm, currentEmojiCategory);

      console.log(`🎨 Rendering ${filteredEmojis.length} emojis (category: ${currentEmojiCategory}, search: "${emojiSearchTerm}")`);

      if (filteredEmojis.length === 0) {
        container.innerHTML = '<div class="emoji-empty">No emojis found</div>';
        return;
      }

      const toRender = filteredEmojis.slice(0, 200);

      container.innerHTML = toRender.map(emoji => `
    <div 
      class="emoji-item" 
      data-emoji="${emoji.char}"
      title="${emoji.name}"
    >
      ${emoji.char}
    </div>
  `).join('');

      container.querySelectorAll('.emoji-item').forEach(item => {
        item.addEventListener('click', () => {
          insertEmojiIntoInput(item.dataset.emoji);
        }, { passive: true });
      });
    }

    function insertEmojiIntoInput(emoji) {
      if (!messageInput) {
        console.error('❌ Message input not found');
        return;
      }

      console.log(`😀 Inserting emoji: ${emoji}`);

      const start = messageInput.selectionStart;
      const end = messageInput.selectionEnd;
      const currentValue = messageInput.value;

      const newValue = currentValue.substring(0, start) + emoji + currentValue.substring(end);
      messageInput.value = newValue;

      const newCursorPos = start + emoji.length;
      messageInput.setSelectionRange(newCursorPos, newCursorPos);

      messageInput.focus();

      console.log(`✅ Emoji inserted at position ${start}, cursor now at ${newCursorPos}`);

      closeEmojiPicker();
    }

    async function openEmojiPicker() {
      console.log('😀 ========================================');
      console.log('😀 OPENING EMOJI PICKER');
      console.log('😀 ========================================');

      const overlay = document.getElementById('emojiPickerOverlay');
      if (!overlay) {
        console.error('❌ Emoji picker overlay not found');
        return;
      }

      if (isEmojiPickerOpen) {
        console.log('⚠️ Picker already open');
        console.log('😀 ========================================\n');
        return;
      }

      isEmojiPickerOpen = true;

      const grid = document.getElementById('emojiGrid');
      if (grid) {
        grid.innerHTML = `
      <div class="emoji-loading">
        <div class="emoji-loading-spinner"></div>
        <div>Loading emojis...</div>
      </div>
    `;
      }

      overlay.classList.add('active');
      console.log('✅ Overlay shown');

      try {
        if (!emojiCache) {
          console.log('📡 Fetching emoji data...');
          const rawData = await fetchEmojis();
          emojiCache = processEmojiData(rawData);
          console.log('✅ Emoji cache ready');
        }

        renderEmojiCategories();
        renderEmojiGrid();

        console.log('✅ Emoji picker rendered successfully');

      } catch (error) {
        console.error('❌ Failed to load emojis:', error);

        if (grid) {
          grid.innerHTML = `
        <div class="emoji-error">
          <div style="font-size: 2rem; margin-bottom: 8px;">😞</div>
          <div>Failed to load emojis</div>
          <div style="font-size: 0.75rem; margin-top: 4px; opacity: 0.7;">Please try again</div>
        </div>
      `;
        }
      }

      console.log('😀 ========================================\n');
    }

    function closeEmojiPicker() {
      console.log('😀 Closing emoji picker');

      const overlay = document.getElementById('emojiPickerOverlay');
      if (overlay) {
        overlay.classList.remove('active');
      }

      isEmojiPickerOpen = false;

      emojiSearchTerm = '';
      const searchInput = document.getElementById('emojiSearchInput');
      if (searchInput) {
        searchInput.value = '';
      }

      currentEmojiCategory = 'all';

      console.log('✅ Emoji picker closed');
    }

    function debounceEmojiSearch(func, wait) {
      let timeout;
      return function executedFunction(...args) {
        const later = () => {
          clearTimeout(timeout);
          func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
      };
    }

    function setAuthenticationState(authenticated) {
      console.log(`🔐 ========================================`);
      console.log(`🔐 AUTHENTICATION STATE CHANGE`);
      console.log(`🔐 ========================================`);
      console.log(`   Previous state: ${isSocketAuthenticated}`);
      console.log(`   New state: ${authenticated}`);

      isSocketAuthenticated = authenticated;

      if (authenticated) {
        console.log(`✅ Socket fully authenticated - call features will enable after room join`);
        if (hasJoinedRoom && roomData?.roomId && !isInitiatingCall) {
          enableCallButtons();
        } else {
          disableCallButtons(false);
        }
      } else {
        console.log(`🔒 Socket not authenticated - disabling call features`);
        disableCallButtons(true);
      }

      console.log(`🔐 ========================================\n`);
    }

    // ============================================
    // INDEXEDDB SETUP FOR FILE ATTACHMENTS
    // ============================================
    const DB_NAME = 'MoodLogChatFiles';
    const DB_VERSION = 1;
    const FILE_STORE_NAME = 'attachments';

    let dbInstance = null;

    function initFileDB() {
      return new Promise((resolve, reject) => {
        if (dbInstance) {
          resolve(dbInstance);
          return;
        }

        console.log('📂 Initializing IndexedDB for file attachments...');
        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onerror = () => {
          console.error('❌ IndexedDB open failed:', request.error);
          reject(request.error);
        };

        request.onsuccess = () => {
          dbInstance = request.result;
          console.log('✅ IndexedDB initialized successfully');
          resolve(dbInstance);
        };

        request.onupgradeneeded = (event) => {
          console.log('🔧 IndexedDB upgrade needed - creating object stores');
          const db = event.target.result;

          if (!db.objectStoreNames.contains(FILE_STORE_NAME)) {
            const store = db.createObjectStore(FILE_STORE_NAME, { keyPath: 'id' });
            store.createIndex('roomId', 'roomId', { unique: false });
            store.createIndex('messageId', 'messageId', { unique: false });
            store.createIndex('timestamp', 'timestamp', { unique: false });
            console.log('✅ Object store created:', FILE_STORE_NAME);
          }
        };
      });
    }

    async function storeFileToIndexedDB(file, roomId, messageId = null, customFileId = null) {
      console.log('💾 ========================================');
      console.log('💾 STORING FILE TO INDEXEDDB');
      console.log('💾 ========================================');
      console.log(`   File: ${file.name}`);
      console.log(`   Size: ${(file.size / 1024 / 1024).toFixed(2)} MB`);
      console.log(`   Type: ${file.type}`);
      console.log(`   Room: ${roomId}`);
      if (customFileId) console.log(`   Custom ID: ${customFileId}`);

      try {
        const db = await initFileDB();

        const fileId = customFileId || `file_${roomId}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

        const chunks = [];
        let offset = 0;

        while (offset < file.size) {
          const chunk = file.slice(offset, offset + CHUNK_SIZE);
          const arrayBuffer = await readChunkAsArrayBuffer(chunk);
          chunks.push(arrayBuffer);
          offset += CHUNK_SIZE;

          if (file.size > 5 * 1024 * 1024) {
            const progress = ((offset / file.size) * 100).toFixed(1);
            console.log(`   Progress: ${progress}%`);
          }
        }

        const blob = new Blob(chunks, { type: file.type });

        chunks.length = 0;

        const fileRecord = {
          id: fileId,
          roomId: roomId,
          messageId: messageId,
          name: file.name,
          type: file.type,
          size: file.size,
          blob: blob,
          timestamp: Date.now()
        };

        await new Promise((resolve, reject) => {
          const transaction = db.transaction([FILE_STORE_NAME], 'readwrite');
          const store = transaction.objectStore(FILE_STORE_NAME);
          const request = store.add(fileRecord);

          request.onsuccess = () => {
            console.log(`✅ File stored to IndexedDB: ${fileId}`);
            console.log(`   Disk-backed storage (not in RAM)`);
            resolve(fileId);
          };

          request.onerror = () => {
            console.error('❌ Failed to store file:', request.error);
            reject(request.error);
          };
        });

        console.log('💾 ========================================\n');
        return fileId;

      } catch (error) {
        console.error('❌ Error storing file to IndexedDB:', error);
        console.log('💾 ========================================\n');
        throw error;
      }
    }

    function readChunkAsArrayBuffer(chunk) {
      return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result);
        reader.onerror = () => reject(reader.error);
        reader.readAsArrayBuffer(chunk);
      });
    }

    async function getFileFromIndexedDB(fileId) {
      console.log(`📂 Retrieving file: ${fileId}`);

      try {
        const db = await initFileDB();

        return await new Promise((resolve, reject) => {
          const transaction = db.transaction([FILE_STORE_NAME], 'readonly');
          const store = transaction.objectStore(FILE_STORE_NAME);
          const request = store.get(fileId);

          request.onsuccess = () => {
            if (request.result) {
              console.log(`✅ File retrieved: ${request.result.name}`);
              resolve(request.result);
            } else {
              console.warn(`⚠️ File not found: ${fileId}`);
              resolve(null);
            }
          };

          request.onerror = () => {
            console.error('❌ Failed to retrieve file:', request.error);
            reject(request.error);
          };
        });

      } catch (error) {
        console.error('❌ Error retrieving file:', error);
        throw error;
      }
    }

    async function deleteRoomFiles(roomId) {
      console.log('🗑️ ========================================');
      console.log('🗑️ DELETING ALL FILES FOR ROOM');
      console.log('🗑️ ========================================');
      console.log(`   Room ID: ${roomId}`);

      try {
        const db = await initFileDB();

        return await new Promise((resolve, reject) => {
          const transaction = db.transaction([FILE_STORE_NAME], 'readwrite');
          const store = transaction.objectStore(FILE_STORE_NAME);
          const index = store.index('roomId');
          const request = index.openCursor(IDBKeyRange.only(roomId));

          let deletedCount = 0;

          request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
              console.log(`   🗑️ Deleting file: ${cursor.value.name} (${cursor.value.id})`);
              cursor.delete();
              deletedCount++;
              cursor.continue();
            } else {
              console.log(`✅ Deleted ${deletedCount} file(s) for room ${roomId}`);
              console.log('🗑️ ========================================\n');
              resolve(deletedCount);
            }
          };

          request.onerror = () => {
            console.error('❌ Failed to delete room files:', request.error);
            console.log('🗑️ ========================================\n');
            reject(request.error);
          };
        });

      } catch (error) {
        console.error('❌ Error deleting room files:', error);
        console.log('🗑️ ========================================\n');
        throw error;
      }
    }

    async function deleteFileFromIndexedDB(fileId) {
      console.log(`🗑️ Deleting file: ${fileId}`);

      try {
        const db = await initFileDB();

        await new Promise((resolve, reject) => {
          const transaction = db.transaction([FILE_STORE_NAME], 'readwrite');
          const store = transaction.objectStore(FILE_STORE_NAME);
          const request = store.delete(fileId);

          request.onsuccess = () => {
            console.log(`✅ File deleted: ${fileId}`);
            resolve();
          };

          request.onerror = () => {
            console.error('❌ Failed to delete file:', request.error);
            reject(request.error);
          };
        });

      } catch (error) {
        console.error('❌ Error deleting file:', error);
        throw error;
      }
    }

    async function getRoomFileCount(roomId) {
      try {
        const db = await initFileDB();

        return await new Promise((resolve, reject) => {
          const transaction = db.transaction([FILE_STORE_NAME], 'readonly');
          const store = transaction.objectStore(FILE_STORE_NAME);
          const index = store.index('roomId');
          const request = index.count(IDBKeyRange.only(roomId));

          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });

      } catch (error) {
        console.error('❌ Error counting room files:', error);
        return 0;
      }
    }

    // ============================================
    // FILE VALIDATION
    // ============================================
    const SUPPORTED_FILE_TYPES = {
      'image/jpeg': { ext: ['.jpg', '.jpeg'], maxSize: 100 * 1024 * 1024 },
      'image/png': { ext: ['.png'], maxSize: 100 * 1024 * 1024 },
      'image/gif': { ext: ['.gif'], maxSize: 100 * 1024 * 1024 },
      'image/webp': { ext: ['.webp'], maxSize: 100 * 1024 * 1024 },
      'image/bmp': { ext: ['.bmp'], maxSize: 100 * 1024 * 1024 },
      'image/tiff': { ext: ['.tiff', '.tif'], maxSize: 100 * 1024 * 1024 },
      'image/svg+xml': { ext: ['.svg'], maxSize: 10 * 1024 * 1024 },

      'video/mp4': { ext: ['.mp4'], maxSize: 100 * 1024 * 1024 },
      'video/webm': { ext: ['.webm'], maxSize: 100 * 1024 * 1024 },
      'video/quicktime': { ext: ['.mov'], maxSize: 100 * 1024 * 1024 },
      'video/ogg': { ext: ['.ogv', '.ogg'], maxSize: 100 * 1024 * 1024 },
      'video/x-msvideo': { ext: ['.avi'], maxSize: 100 * 1024 * 1024 },
      'video/x-matroska': { ext: ['.mkv'], maxSize: 100 * 1024 * 1024 },
      'video/mpeg': { ext: ['.mpeg', '.mpg'], maxSize: 100 * 1024 * 1024 },

      'application/pdf': { ext: ['.pdf'], maxSize: 100 * 1024 * 1024 },
      'application/msword': { ext: ['.doc'], maxSize: 100 * 1024 * 1024 },
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': { ext: ['.docx'], maxSize: 100 * 1024 * 1024 },
      'application/vnd.ms-excel': { ext: ['.xls'], maxSize: 100 * 1024 * 1024 },
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': { ext: ['.xlsx'], maxSize: 100 * 1024 * 1024 },
      'application/vnd.ms-powerpoint': { ext: ['.ppt'], maxSize: 100 * 1024 * 1024 },
      'application/vnd.openxmlformats-officedocument.presentationml.presentation': { ext: ['.pptx'], maxSize: 100 * 1024 * 1024 },
      'text/plain': { ext: ['.txt'], maxSize: 100 * 1024 * 1024 },
      'application/rtf': { ext: ['.rtf'], maxSize: 100 * 1024 * 1024 },
      'application/zip': { ext: ['.zip'], maxSize: 100 * 1024 * 1024 },
      'application/x-zip-compressed': { ext: ['.zip'], maxSize: 100 * 1024 * 1024 }
    };

    const ABSOLUTE_MAX_FILE_SIZE = 100 * 1024 * 1024;

    function validateFile(file) {
      console.log('🔍 ========================================');
      console.log('🔍 FILE VALIDATION');
      console.log('🔍 ========================================');
      console.log(`   File: ${file.name}`);
      console.log(`   Type: ${file.type}`);
      console.log(`   Size: ${(file.size / 1024 / 1024).toFixed(2)} MB`);

      if (file.size > ABSOLUTE_MAX_FILE_SIZE) {
        const sizeMB = (file.size / 1024 / 1024).toFixed(2);
        const maxMB = (ABSOLUTE_MAX_FILE_SIZE / 1024 / 1024).toFixed(0);
        console.error(`❌ File exceeds absolute limit: ${sizeMB}MB > ${maxMB}MB`);
        console.log('🔍 ========================================\n');
        return {
          valid: false,
          error: `File too large (${sizeMB}MB). Maximum file size is ${maxMB}MB.`
        };
      }

      const typeConfig = SUPPORTED_FILE_TYPES[file.type];
      if (!typeConfig) {
        const mime = (file.type || '').toLowerCase();
        if (mime.startsWith('image/') || mime.startsWith('video/') ||
          mime === 'application/pdf' ||
          mime.startsWith('application/vnd.') || mime.startsWith('application/msword') ||
          mime === 'text/plain' || mime === 'application/rtf') {
          console.log(`🔍 Allowing generic type: ${file.type}`);
          return { valid: true, error: null };
        }
        console.warn(`⚠️ Unknown MIME type (${file.type || 'none'}) - allowing with fallback handling`);
        console.log('🔍 ========================================\n');
        return { valid: true, error: null };
      }

      const fileName = file.name.toLowerCase();
      const hasValidExtension = typeConfig.ext.some(ext => fileName.endsWith(ext));
      if (!hasValidExtension) {
        console.error(`❌ File extension doesn't match type: ${file.name}`);
        console.log('🔍 ========================================\n');
        return {
          valid: false,
          error: `File extension doesn't match its type. Expected: ${typeConfig.ext.join(', ')}`
        };
      }

      if (file.size > typeConfig.maxSize) {
        const sizeMB = (file.size / 1024 / 1024).toFixed(2);
        const maxMB = (typeConfig.maxSize / 1024 / 1024).toFixed(0);
        console.error(`❌ File exceeds type limit: ${sizeMB}MB > ${maxMB}MB`);
        console.log('🔍 ========================================\n');
        return {
          valid: false,
          error: `${file.type} files must be under ${maxMB}MB. Your file is ${sizeMB}MB.`
        };
      }

      console.log('✅ File validation passed');
      console.log('🔍 ========================================\n');
      return { valid: true, error: null };
    }

    // ============================================
    // CHUNKED FILE TRANSMISSION CONFIGURATION
    // ============================================
    const CHUNK_SIZE = 524288;
    const CHUNK_TIMEOUT = 20000;
    const MAX_RETRIES = 3;

    async function openImageViewer(fileId, fileName, fallbackUrl = null) {
      console.log('🖼️ ========================================');
      console.log('🖼️ OPENING IMAGE VIEWER');
      console.log('🖼️ ========================================');
      console.log(`   FileID: ${fileId}`);
      console.log(`   Filename: ${fileName}`);

      const overlay = document.getElementById('imageViewerOverlay');
      const image = document.getElementById('imageViewerImage');
      const filenameEl = document.getElementById('imageViewerFilename');
      const sizeEl = document.getElementById('imageViewerSize');

      if (!overlay || !image) {
        console.error('❌ Image viewer elements not found');
        console.error('   overlay:', !!overlay);
        console.error('   image:', !!image);
        return;
      }

      try {
        const fileData = await getFileFromIndexedDB(fileId);

        if (!fileData || !fileData.blob) {
          if (!fallbackUrl) {
            console.error('❌ File not found in IndexedDB');
            toast('Image not found', 'error');
            return;
          }

          image.src = fallbackUrl;
          image.alt = fileName || 'Image attachment';
          delete image.dataset.blobUrl;
          if (filenameEl) filenameEl.textContent = fileName || 'Image';
          if (sizeEl) sizeEl.textContent = 'Loaded from secure storage';
        } else {
          console.log(`✅ File retrieved: ${fileData.name} (${(fileData.size / 1024).toFixed(2)} KB)`);

          const blobUrl = URL.createObjectURL(fileData.blob);

          image.src = blobUrl;
          image.alt = fileData.name;
          image.dataset.blobUrl = blobUrl;

          if (filenameEl) filenameEl.textContent = fileData.name;
          if (sizeEl) sizeEl.textContent = `${formatFileSize(fileData.size)} • Click image to zoom`;
        }

        isImageZoomed = false;
        image.classList.remove('zoomed');

        overlay.style.display = 'flex';
        overlay.classList.add('active');

        void overlay.offsetHeight;

        console.log('✅ Image viewer opened');
        console.log('   Display:', overlay.style.display);
        console.log('   Has active class:', overlay.classList.contains('active'));
        console.log('🖼️ ========================================\n');

      } catch (error) {
        console.error('❌ Error opening image viewer:', error);
        toast('Failed to open image', 'error');
      }
    }

    function closeImageViewer() {
      console.log('🖼️ Closing image viewer');

      const overlay = document.getElementById('imageViewerOverlay');
      const image = document.getElementById('imageViewerImage');

      if (overlay) {
        overlay.classList.remove('active');
        setTimeout(() => {
          overlay.style.display = 'none';
        }, 200);
      }

      if (image && image.dataset.blobUrl) {
        URL.revokeObjectURL(image.dataset.blobUrl);
        delete image.dataset.blobUrl;
        console.log('🧹 Blob URL revoked');
      }

      isImageZoomed = false;
      if (image) {
        image.classList.remove('zoomed');
      }

      console.log('✅ Image viewer closed');
    }

    function toggleImageZoom() {
      const image = document.getElementById('imageViewerImage');
      if (!image) return;

      isImageZoomed = !isImageZoomed;

      if (isImageZoomed) {
        image.classList.add('zoomed');
        console.log('🔍 Image zoomed in');
      } else {
        image.classList.remove('zoomed');
        console.log('🔍 Image zoomed out');
      }
    }

    function base64ToBlob(base64, contentType) {
      if (!base64 || typeof base64 !== 'string') {
        console.error('❌ Cannot convert to blob: base64 data is missing or not a string', { type: typeof base64 });
        return null;
      }

      try {
        let cleanBase64 = base64.includes(',') ? base64.split(',')[1] : base64;

        cleanBase64 = cleanBase64.replace(/-/g, '+').replace(/_/g, '/');

        cleanBase64 = cleanBase64.replace(/[^A-Za-z0-9+/]/g, "");

        while (cleanBase64.length % 4 !== 0) {
          cleanBase64 += '=';
        }

        const byteCharacters = atob(cleanBase64);
        const byteNumbers = new Array(byteCharacters.length);
        for (let i = 0; i < byteCharacters.length; i++) {
          byteNumbers[i] = byteCharacters.charCodeAt(i);
        }
        const byteArray = new Uint8Array(byteNumbers);
        return new Blob([byteArray], { type: contentType });
      } catch (error) {
        console.error('❌ base64ToBlob conversion failed:', error);
        if (base64 && typeof base64 === 'string') {
          console.log('   Data sample (first 50 chars):', base64.substring(0, 50));
          console.log('   Data length:', base64.length);
        }
        return null;
      }
    }

    async function chunkFile(file) {
      console.log('✂️ ========================================');
      console.log('✂️ CHUNKING FILE FOR TRANSMISSION (BINARY)');
      console.log('✂️ ========================================');
      console.log(`   File: ${file.name}`);
      console.log(`   Total size: ${(file.size / 1024 / 1024).toFixed(2)} MB`);
      console.log(`   Chunk size: ${(CHUNK_SIZE / 1024).toFixed(0)} KB`);

      const chunks = [];
      const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
      console.log(`   Total chunks: ${totalChunks}`);

      for (let i = 0; i < totalChunks; i++) {
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const blob = file.slice(start, end);

        const arrayBuffer = await new Promise((resolve, reject) => {
          const reader = new FileReader();
          reader.onload = () => resolve(reader.result);
          reader.onerror = reject;
          reader.readAsArrayBuffer(blob);
        });

        chunks.push({
          index: i,
          data: arrayBuffer,
          size: blob.size
        });

        if ((i + 1) % 10 === 0 || i === totalChunks - 1) {
          const progress = ((i + 1) / totalChunks * 100).toFixed(1);
          console.log(`   Chunking progress: ${progress}% (${i + 1}/${totalChunks})`);
        }
      }

      console.log(`✅ File chunked: ${chunks.length} binary chunks ready`);
      console.log('✂️ ========================================\n');

      return chunks;
    }

    async function sendFileInChunks(fileId, fileName, fileType, fileSize, fileBlob, roomId) {
      console.log('📤 ========================================');
      console.log('📤 PIPELINED BINARY TRANSMISSION START');
      console.log('📤 ========================================');
      console.log(`   FileID: ${fileId}`);
      console.log(`   Size: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

      const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);
      const WINDOW_SIZE = 24;
      let nextChunkIndex = 0;
      let acknowledgedCount = 0;
      let failed = false;
      let lastProgressAt = Date.now();
      const inFlight = new Set();
      const acknowledgedChunks = new Set();

      return new Promise((resolve, reject) => {
        const fail = (error) => {
          if (failed) return;
          failed = true;
          socketInstance.off('file_chunk_ack', ackHandler);
          clearInterval(stallWatcher);
          reject(error);
        };

        const ackHandler = (ack) => {
          if (!ack || ack.fileId !== fileId || failed) return;
          if (acknowledgedChunks.has(ack.chunkIndex)) return;
          acknowledgedChunks.add(ack.chunkIndex);
          inFlight.delete(ack.chunkIndex);
          acknowledgedCount++;
          lastProgressAt = Date.now();

          const progress = Math.round((acknowledgedCount / totalChunks) * 100);
          const progressText = document.getElementById('uploadProgressText');
          if (progressText && fileName === (pendingAttachment?.name || progressText.dataset.fileName)) {
            progressText.textContent = `Uploading: ${progress}%`;
          }

          if (acknowledgedCount >= totalChunks) {
            socketInstance.off('file_chunk_ack', ackHandler);
            clearInterval(stallWatcher);
            console.log('✅ Pipelined transmission complete');
            resolve();
            return;
          }

          void fillPipeline();
        };

        const sendChunk = async (index) => {
          if (failed) return;
          inFlight.add(index);
          const start = index * CHUNK_SIZE;
          const end = Math.min(start + CHUNK_SIZE, fileSize);
          const chunkBlob = fileBlob.slice(start, end);
          const arrayBuffer = await chunkBlob.arrayBuffer();
          if (failed) {
            inFlight.delete(index);
            return;
          }
          if (index === 0 || index === totalChunks - 1 || index % 8 === 0) {
            console.log(`📤 Sending chunk ${index + 1}/${totalChunks}`);
          }

          socketInstance.emit('file_chunk', {
            fileId,
            fileName,
            fileType,
            fileSize,
            roomId,
            chunkIndex: index,
            totalChunks,
            chunkData: arrayBuffer,
            chunkSize: chunkBlob.size
          });
        };

        const fillPipeline = async () => {
          while (!failed && inFlight.size < WINDOW_SIZE && nextChunkIndex < totalChunks) {
            const index = nextChunkIndex++;
            sendChunk(index).catch((err) => {
              console.error('❌ Failed to read/send chunk:', err);
              fail(new Error('Chunk transmission failed'));
            });
          }
        };

        const stallWatcher = setInterval(() => {
          if (failed || acknowledgedCount >= totalChunks) return;
          if (Date.now() - lastProgressAt > CHUNK_TIMEOUT) {
            console.error('❌ Transmission stalled (timeout)');
            fail(new Error('Transmission stalled'));
          }
        }, 1000);

        socketInstance.on('file_chunk_ack', ackHandler);
        void fillPipeline();
      });
    }

    class ChunkedFileReceiver {
      constructor() {
        this.activeTransfers = new Map();
      }

      async handleChunk(data) {
        const { fileId, fileName, fileType, fileSize, roomId, chunkIndex, totalChunks, chunkData, chunkSize } = data;

        if (chunkIndex === 0 || chunkIndex === totalChunks - 1 || chunkIndex % 8 === 0) {
          console.log(`📥 Received chunk ${chunkIndex + 1}/${totalChunks} for ${fileName}`);
        }

        if (!this.activeTransfers.has(fileId)) {
          this.activeTransfers.set(fileId, {
            chunks: new Array(totalChunks),
            metadata: { fileName, fileType, fileSize, roomId, totalChunks },
            receivedCount: 0
          });
        }

        const transfer = this.activeTransfers.get(fileId);
        if (transfer.chunks[chunkIndex] !== undefined) {
          return;
        }

        transfer.chunks[chunkIndex] = new Uint8Array(chunkData);
        transfer.receivedCount++;

        const progress = Math.round((transfer.receivedCount / totalChunks) * 100);
        const progressText = document.getElementById('uploadProgressText');
        if (progressText && fileName === (pendingAttachment?.name || progressText.dataset.fileName)) {
          progressText.textContent = `Receiving: ${progress}%`;
        }

        if (transfer.receivedCount === totalChunks) {
          await this.reconstructFile(fileId, transfer);
        }
      }

      async reconstructFile(fileId, transfer) {
        const { chunks, metadata } = transfer;
        const { fileName, fileType, roomId } = metadata;

        try {
          const totalBytes = chunks.reduce((acc, chunk) => acc + chunk.length, 0);
          const combinedArray = new Uint8Array(totalBytes);
          let offset = 0;

          for (let i = 0; i < chunks.length; i++) {
            if (!chunks[i]) throw new Error(`Missing chunk at index ${i}`);
            combinedArray.set(chunks[i], offset);
            offset += chunks[i].length;
          }

          const blob = new Blob([combinedArray], { type: fileType });
          const db = await initFileDB();

          await new Promise((resolve, reject) => {
            const transaction = db.transaction([FILE_STORE_NAME], 'readwrite');
            const store = transaction.objectStore(FILE_STORE_NAME);

            const fileRecord = {
              id: fileId,
              roomId: roomId,
              messageId: null,
              name: fileName,
              type: fileType,
              size: blob.size,
              blob: blob,
              timestamp: Date.now()
            };

            const request = store.put(fileRecord);
            request.onsuccess = () => resolve();
            request.onerror = () => reject(request.error);
          });

          this.activeTransfers.delete(fileId);

          const imgElements = document.querySelectorAll(`img[data-file-id="${fileId}"]`);
          if (imgElements.length > 0) {
            imgElements.forEach(img => loadImageAttachment(fileId, img));
          }

          toast(`Received: ${fileName}`, 'success');
        } catch (error) {
          console.error('❌ File reconstruction failed:', error);
          this.activeTransfers.delete(fileId);
          toast(`Failed to receive: ${fileName}`, 'error');
          throw error;
        }
      }

      cleanup(fileId) {
        if (this.activeTransfers.has(fileId)) {
          this.activeTransfers.delete(fileId);
        }
      }
    }

    const fileReceiver = new ChunkedFileReceiver();

    function waitForAuthentication() {
      if (isSocketAuthenticated) {
        console.log(`✅ Already authenticated, proceeding immediately`);
        return Promise.resolve();
      }

      if (!authenticationPromise) {
        console.log(`⏳ Creating authentication promise...`);
        authenticationPromise = new Promise((resolve) => {
          const checkAuth = () => {
            if (isSocketAuthenticated) {
              console.log(`✅ Authentication promise resolved`);
              resolve();
            } else {
              setTimeout(checkAuth, 50);
            }
          };
          checkAuth();
        });
      }

      return authenticationPromise;
    }

    function enableCallButtons() {
      console.log(`🎚️ Enabling call buttons`);

      if (!isSocketAuthenticated || !hasJoinedRoom || !roomData?.roomId || isInitiatingCall) {
        console.log(`   ⏳ Call buttons stay disabled until chat room is ready`);
        disableCallButtons(false);
        return;
      }

      if (audioCallBtn) {
        audioCallBtn.disabled = false;
        audioCallBtn.classList.remove('opacity-50', 'cursor-not-allowed', 'animate-pulse', 'call-btn-loading', 'calling');
        audioCallBtn.classList.add('call-ready');
        audioCallBtn.removeAttribute('aria-disabled');
        audioCallBtn.title = 'Start Audio Call';
        console.log(`   ✅ Audio button enabled`);
      }

      if (videoCallBtn) {
        videoCallBtn.disabled = false;
        videoCallBtn.classList.remove('opacity-50', 'cursor-not-allowed', 'animate-pulse', 'call-btn-loading', 'calling');
        videoCallBtn.classList.add('call-ready');
        videoCallBtn.removeAttribute('aria-disabled');
        videoCallBtn.title = 'Start Video Call';
        console.log(`   ✅ Video button enabled`);
      }
    }

    function disableCallButtons(showLoading = false) {
      console.log(`🔒 Disabling call buttons (loading: ${showLoading})`);

      if (audioCallBtn) {
        audioCallBtn.disabled = true;
        audioCallBtn.classList.remove('call-ready');
        audioCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
        audioCallBtn.setAttribute('aria-disabled', 'true');
        if (showLoading) {
          audioCallBtn.classList.add('animate-pulse');
          audioCallBtn.title = 'Connecting...';
        } else {
          audioCallBtn.title = 'Authenticating...';
        }
        console.log(`   🔒 Audio button disabled`);
      }

      if (videoCallBtn) {
        videoCallBtn.disabled = true;
        videoCallBtn.classList.remove('call-ready');
        videoCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
        videoCallBtn.setAttribute('aria-disabled', 'true');
        if (showLoading) {
          videoCallBtn.classList.add('animate-pulse');
          videoCallBtn.title = 'Connecting...';
        } else {
          videoCallBtn.title = 'Authenticating...';
        }
        console.log(`   🔒 Video button disabled`);
      }
    }

    function formatFileSize(bytes) {
      if (bytes === 0) return '0 Bytes';
      const k = 1024;
      const sizes = ['Bytes', 'KB', 'MB', 'GB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
    }

    function getFileTypeCategory(mimeType) {
      if (mimeType.startsWith('image/')) return 'image';
      if (mimeType.startsWith('video/')) return 'video';
      if (mimeType === 'application/pdf') return 'pdf';
      return 'document';
    }

    async function uploadAttachmentToServer(file, roomId) {
      if (!file) throw new Error('No file provided');
      if (!roomId) throw new Error('roomId is required');

      const firebaseUser = firebase.auth().currentUser;
      if (!firebaseUser) throw new Error('Not authenticated');

      const token = await firebaseUser.getIdToken();
      const formData = new FormData();
      formData.append('file', file);
      formData.append('roomId', roomId);

      const response = await fetch(`${window.location.origin}/api/chat/attachments`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`
        },
        body: formData
      });

      let payload = null;
      try {
        payload = await response.json();
      } catch {
        payload = null;
      }

      if (!response.ok || !payload?.attachment) {
        const errorMessage = payload?.error || payload?.message || `Upload failed (${response.status})`;
        throw new Error(errorMessage);
      }

      return payload.attachment;
    }

    function createImagePreviewUrl(blob) {
      if (blob.type.startsWith('image/')) {
        return URL.createObjectURL(blob);
      }
      return null;
    }

    async function showAttachmentPreview(fileId, fileName, fileType, fileSize, blob) {
      console.log('🖼️ ========================================');
      console.log('🖼️ SHOWING ATTACHMENT PREVIEW');
      console.log('🖼️ ========================================');
      console.log(`   File: ${fileName}`);
      console.log(`   Type: ${fileType}`);
      console.log(`   Size: ${formatFileSize(fileSize)}`);

      const previewContainer = document.getElementById('attachmentPreview');
      const iconWrapper = document.getElementById('attachmentIconWrapper');
      const nameElement = document.getElementById('attachmentName');
      const sizeElement = document.getElementById('attachmentSize');

      if (!previewContainer || !iconWrapper || !nameElement || !sizeElement) {
        console.error('❌ Preview elements not found');
        return;
      }

      iconWrapper.innerHTML = '';
      iconWrapper.className = 'attachment-icon-wrapper';

      const fileCategory = getFileTypeCategory(fileType);
      let previewUrl = null;

      if (fileCategory === 'image') {
        previewUrl = createImagePreviewUrl(blob);
        if (previewUrl) {
          iconWrapper.classList.add('image-preview');
          const img = document.createElement('img');
          img.src = previewUrl;
          img.alt = fileName;
          iconWrapper.appendChild(img);
          console.log('✅ Image preview created');
        }
      } else if (fileCategory === 'pdf') {
        iconWrapper.classList.add('pdf-preview');
        iconWrapper.innerHTML = '<span class="material-symbols-outlined">picture_as_pdf</span>';
        console.log('✅ PDF icon displayed');
      } else if (fileCategory === 'video') {
        iconWrapper.classList.add('video-preview');
        iconWrapper.innerHTML = '<span class="material-symbols-outlined">videocam</span>';
        console.log('✅ Video icon displayed');
      } else {
        iconWrapper.classList.add('pdf-preview');
        iconWrapper.innerHTML = '<span class="material-symbols-outlined">description</span>';
        console.log('✅ Document icon displayed');
      }

      nameElement.textContent = fileName;
      sizeElement.textContent = formatFileSize(fileSize);

      pendingAttachment = {
        fileId,
        name: fileName,
        type: fileType,
        size: fileSize,
        previewUrl
      };

      previewContainer.classList.remove('hidden');

      console.log('✅ Attachment preview displayed');
      console.log('🖼️ ========================================\n');
    }

    function hideAttachmentPreview() {
      console.log('🧹 Hiding attachment preview');

      const previewContainer = document.getElementById('attachmentPreview');
      if (previewContainer) {
        previewContainer.classList.add('hidden');
      }

      if (pendingAttachment?.previewUrl) {
        URL.revokeObjectURL(pendingAttachment.previewUrl);
        console.log('✅ Blob URL revoked');
      }

      pendingAttachment = null;
      console.log('✅ Attachment preview cleaned up');
    }

    function checkForCachedCall(options = {}) {
      const logMiss = options.logMiss === true;

      const cachedCallStr = localStorage.getItem(CACHED_CALL_KEY);

      if (!cachedCallStr) {
        if (logMiss) console.log('ℹ️ No cached call found');
        return;
      }

      console.log('🔍 Checking for cached incoming call...');

      try {
        const cachedCall = JSON.parse(cachedCallStr);
        const cacheAge = Date.now() - cachedCall.timestamp;

        console.log(`📞 Found cached call from ${cachedCall.callerUsername}`);
        console.log(`   Age: ${(cacheAge / 1000).toFixed(1)}s`);
        console.log(`   Call ID: ${cachedCall.callId}`);

        if (cacheAge > CALL_CACHE_TIMEOUT) {
          console.log(`⏰ Cached call expired (${(cacheAge / 1000).toFixed(1)}s > ${CALL_CACHE_TIMEOUT / 1000}s)`);
          localStorage.removeItem(CACHED_CALL_KEY);
          return;
        }

        if (socketInstance && socketInstance.connected) {
          console.log(`📡 Validating cached call ${cachedCall.callId} with server...`);

          socketInstance.emit('validate_cached_call', {
            callId: cachedCall.callId,
            roomId: roomData.roomId
          });
        } else {
          console.warn('⚠️ Socket not connected, will retry validation');
        }

      } catch (e) {
        console.error('❌ Failed to parse cached call:', e);
        localStorage.removeItem(CACHED_CALL_KEY);
      }
    }

    function showCachedCallModal(callData) {
      if (activeCallConnectionState === 'initializing' || activeCallConnectionState === 'connecting' || activeCallConnectionState === 'connected' || isInitiatingCall) {
        console.log('📞 Skipping cached call modal - user already in/initiating call');
        return;
      }

      console.log('📞 ========================================');
      console.log('📞 SHOWING CACHED INCOMING CALL');
      console.log('📞 ========================================');
      console.log(`   Caller: ${callData.callerUsername}`);
      console.log(`   Call ID: ${callData.callId}`);
      console.log(`   Type: ${callData.callType}`);

      pendingCallData = {
        callId: callData.callId,
        callerUsername: callData.callerUsername,
        callerPfp: callData.callerPfp,
        callType: callData.callType,
        fromCache: true
      };

      const callerName = document.getElementById('callerName');
      const callType = document.getElementById('callType');
      const callerAvatar = document.getElementById('callerAvatar');

      if (callerName) callerName.textContent = `${callData.callerUsername} is calling`;
      if (callType) callType.textContent = callData.callType === 'video' ? 'Video Call' : 'Audio Call';
      if (callerAvatar) {
        callerAvatar.innerHTML = '';
        const pfp = createProfilePictureElement(callData.callerPfp, callData.callerUsername, 'w-full h-full');
        callerAvatar.appendChild(pfp);
      }

      incomingCallModal?.classList.remove('hidden');

      console.log('✅ Cached call modal displayed');
      console.log('📞 ========================================\n');
    }

    function startCachedCallMonitoring() {
      console.log('🔄 Starting cached call monitoring...');

      checkForCachedCall({ logMiss: true });

      cachedCallCheckInterval = setInterval(() => {
        checkForCachedCall();
      }, 2000);

      console.log('✅ Cached call monitoring started (checks every 2s)');
    }

    function stopCachedCallMonitoring() {
      if (cachedCallCheckInterval) {
        clearInterval(cachedCallCheckInterval);
        cachedCallCheckInterval = null;
        console.log('🛑 Cached call monitoring stopped');
      }
    }

    function cleanupParentCallPage(targetUrl = null) {
      console.log('🧹 ========================================');
      console.log('🧹 CLEANING UP PARENT CALL PAGE');
      console.log('🧹 ========================================');

      const finalUrl = targetUrl || '/mood.html';

      try {
        let hasActiveCallCached = false;
        const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
        if (activeCallStr) {
          try {
            const parsed = JSON.parse(activeCallStr);
            hasActiveCallCached = !!(parsed?.callId && parsed?.roomId);
          } catch { }
        }
        const backgroundCallMode = sessionStorage.getItem('backgroundCallMode') === 'true';
        if (isSocialClubMode && finalUrl === '/mood.html' && (navigatingToCall || isInBackgroundCall || backgroundCallMode || hasActiveCallCached)) {
          console.warn('🛡️ Social Club: Suppressing forced navigation to /mood.html during call flow');
          console.log('🧹 ========================================\n');
          return;
        }
      } catch { }

      try {
        localStorage.removeItem('discovery_state');
        localStorage.removeItem('discovery_state_timestamp');
        console.log('🧹 [Cleanup] Matchmaking state cleared');
      } catch (e) { }

      if (window.parent && window.parent !== window) {
        console.log('📱 We are in iframe - accessing parent window');
        try {
          const parentFloatingBtn = window.parent.document.getElementById('floatingReturnToCall');
          if (parentFloatingBtn) {
            parentFloatingBtn.classList.add('hidden');
            console.log('✅ Parent floating button hidden');
          }

          console.log(`🔄 Navigating parent window to ${finalUrl}`);
          window.parent.location.href = finalUrl;

          console.log('✅ Parent window navigation initiated');
        } catch (e) {
          console.error('❌ Cannot access parent window:', e);
          window.location.href = finalUrl;
        }
      } else {
        console.log(`ℹ️ Not in iframe - navigating directly to ${finalUrl}`);
        window.location.href = finalUrl;
      }

      console.log('🧹 ========================================\n');
    }

    function hideCallPageFloatingButton() {
      console.log('🎈 Attempting to hide call.html floating button');

      if (window.parent && window.parent !== window) {
        console.log('📱 We are in iframe - accessing parent window');
        try {
          const parentFloatingBtn = window.parent.document.getElementById('floatingReturnToCall');
          if (parentFloatingBtn) {
            parentFloatingBtn.classList.add('hidden');
            console.log('✅ Parent floating button hidden');
          } else {
            console.warn('⚠️ Parent floating button not found');
          }
        } catch (e) {
          console.error('❌ Cannot access parent window (CORS?):', e);
        }
      } else {
        console.log('ℹ️ Not in iframe - checking local DOM');
        const localFloatingBtn = document.getElementById('floatingReturnToCall');
        if (localFloatingBtn) {
          localFloatingBtn.classList.add('hidden');
          console.log('✅ Local floating button hidden');
        }
      }
    }

    function returnToActiveCall() {
      console.log('🔙 ========================================');
      console.log('🔙 RETURNING TO ACTIVE CALL FROM CHAT');
      console.log('🔙 ========================================');

      const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
      if (!activeCallStr) {
        console.error('❌ No active call data found');
        toast('No active call found', 'error');
        return;
      }

      try {
        const activeCallData = JSON.parse(activeCallStr);
        console.log(`   callId: ${activeCallData.callId}`);
        console.log(`   callType: ${activeCallData.callType}`);
        console.log(`   backgroundMode: ${activeCallData.backgroundMode}`);

        navigatingToCall = true;

        sessionStorage.removeItem('returningFromCall');
        sessionStorage.removeItem('backgroundCallMode');

        sessionStorage.setItem('returningToBackgroundCall', 'true');

        saveChatState(roomData.roomId, messagesCache);

        if (floatingCallSocket) {
          console.log('🔌 Disconnecting floating socket (will reconnect on call page)');
          floatingCallSocket.disconnect();
          floatingCallSocket = null;
        }

        console.log('✅ Navigating back to call page');
        window.location.href = '/call.html';

      } catch (e) {
        console.error('❌ Failed to parse active call data:', e);
        toast('Failed to return to call', 'error');
      }

      console.log('🔙 ========================================\n');
    }

    function checkForBackgroundCall() {
      const hasBackgroundCall = sessionStorage.getItem('hasBackgroundCall') === 'true';
      const activeCallStr = localStorage.getItem('activeCall');

      console.log('🔍 Checking for background call...');
      console.log(`   hasBackgroundCall flag: ${hasBackgroundCall}`);
      console.log(`   activeCall in storage: ${!!activeCallStr}`);

      if (hasBackgroundCall && activeCallStr) {
        console.log('✅ Background call detected - showing return button');

        const returnBtn = document.getElementById('floatingReturnToCall');
        if (returnBtn) {
          returnBtn.classList.remove('hidden');

          returnBtn.onclick = () => {
            console.log('📞 Return to call clicked');
            navigatingToCall = true;
            sessionStorage.removeItem('hasBackgroundCall');
            window.location.href = '/call.html';
          };

          console.log('✅ Return to call button activated');
        }
      } else {
        console.log('ℹ️ No background call detected');
      }
    }

    function saveChatState(roomId, messages) {
      try {
        console.log('💾 ========================================');
        console.log('💾 SAVING CHAT STATE');
        console.log('💾 ========================================');
        console.log(`   Room: ${roomId}`);
        console.log(`   Messages: ${messages?.length || 0}`);

        if (!roomId) {
          console.error('❌ Cannot save state: roomId missing');
          return;
        }

        if (!Array.isArray(messages)) {
          console.error('❌ Cannot save state: messages not an array');
          return;
        }

        const validMessages = messages.filter(msg => {
          const isValid = msg.userId && msg.timestamp && (msg.message || msg.attachment);
          if (!isValid) {
            console.warn(`⚠️ Skipping invalid message in cache save:`, msg);
          }
          return isValid;
        });

        if (validMessages.length < messages.length) {
          console.warn(`⚠️ Filtered out ${messages.length - validMessages.length} invalid messages`);
        }

        let callButtonState = 'none';
        const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);

        if (!activeCallStr) {
          callButtonState = 'none';
        } else {
          try {
            const activeCall = JSON.parse(activeCallStr);
            callButtonState = activeCall?.roomId === roomId ? 'back' : 'none';
          } catch {
            callButtonState = 'none';
          }
        }

        const state = {
          roomId,
          messages: validMessages,
          timestamp: Date.now(),
          callButtonState,
          activeCallInRoom: null,
          version: 2
        };

        try {
          localStorage.setItem(CHAT_STATE_KEY, JSON.stringify(state));
          localStorage.setItem(CHAT_TIMESTAMP_KEY, Date.now().toString());

          console.log('✅ Chat state saved successfully:');
          console.log(`   Valid messages: ${validMessages.length}`);
          console.log(`   Button state: ${callButtonState}`);
          console.log('💾 ========================================\n');

        } catch (storageError) {
          if (storageError.name === 'QuotaExceededError') {
            console.error('❌ LocalStorage quota exceeded - attempting cleanup');

            const trimmedMessages = validMessages.slice(-50);
            const trimmedState = { ...state, messages: trimmedMessages };

            try {
              localStorage.setItem(CHAT_STATE_KEY, JSON.stringify(trimmedState));
              console.warn(`⚠️ Saved trimmed state (${trimmedMessages.length} messages)`);
            } catch (retryError) {
              console.error('❌ Failed to save even trimmed state:', retryError);
              localStorage.removeItem(CHAT_STATE_KEY);
              localStorage.removeItem(CHAT_MESSAGES_KEY);
            }
          } else {
            throw storageError;
          }
        }

      } catch (e) {
        console.error('❌ ========================================');
        console.error('❌ FAILED TO SAVE CHAT STATE');
        console.error('❌ ========================================');
        console.error('   Error:', e.message);
        console.error('   Stack:', e.stack);
        console.error('❌ ========================================\n');
      }
    }

    function loadChatState() {
      try {
        const stateStr = localStorage.getItem(CHAT_STATE_KEY);
        if (!stateStr) return null;

        const state = JSON.parse(stateStr);
        console.log('✅ Chat state loaded:', state.messages?.length || 0, 'messages');
        return state;
      } catch (e) {
        console.warn('⚠️ Failed to load chat state:', e);
        return null;
      }
    }

    async function clearChatState(force = false) {
      console.log('🧹 ========================================');
      console.log('🧹 CLEARING CHAT STATE');
      console.log('🧹 ========================================');
      console.log(`   Force: ${force}`);

      const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);

      if (!force && activeCallStr) {
        console.log('⚠️ Active call exists - preserving chat state');
        console.log('🧹 ========================================\n');
        return;
      }

      if (roomData?.roomId) {
        try {
          console.log(`🗑️ Deleting all files for room: ${roomData.roomId}`);
          const deletedCount = await deleteRoomFiles(roomData.roomId);
          console.log(`✅ Deleted ${deletedCount} file(s) from IndexedDB`);
        } catch (error) {
          console.error('❌ Failed to delete room files:', error);
        }
      }

      localStorage.removeItem(CHAT_STATE_KEY);
      localStorage.removeItem(CHAT_MESSAGES_KEY);
      localStorage.removeItem(CHAT_TIMESTAMP_KEY);

      console.log('✅ Chat state cleared');
      console.log('🧹 ========================================\n');
    }

    function clearAllChatData() {
      localStorage.removeItem(CHAT_STATE_KEY);
      localStorage.removeItem(CHAT_MESSAGES_KEY);
      localStorage.removeItem(CHAT_TIMESTAMP_KEY);
      console.log('🧹 All chat state cleared on entry');
    }

    function escapeHtml(text) {
      if (_Utils && typeof _Utils.escapeHtml === 'function') {
        return _Utils.escapeHtml(text);
      }
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    function formatTimestamp(timestamp) {
      if (_Utils && typeof _Utils.formatDate === 'function') {
        return _Utils.formatDate(timestamp);
      }
      return new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }

    function debounce(func, wait) {
      let timeout;
      return function executedFunction(...args) {
        const later = () => {
          clearTimeout(timeout);
          func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
      };
    }

    function setInitialCallButtonState() {
      console.log('🔍 Checking for active call in localStorage...');

      const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);

      const clearStaleActiveCall = (reason) => {
        try {
          console.warn(`📞 Clearing stale activeCall (${reason || 'unknown'})`);
          localStorage.removeItem(ACTIVE_CALL_KEY);
        } catch { }
        resetHeaderCallButtons(reason || 'stale activeCall');
      };

      if (activeCallStr) {
        try {
          const activeCallData = JSON.parse(activeCallStr);

          const storedAt = typeof activeCallData?.storedAt === 'number' ? activeCallData.storedAt : 0;
          const ageMs = storedAt ? (Date.now() - storedAt) : Number.POSITIVE_INFINITY;
          const MAX_STALE_MS = 2 * 60 * 1000;

          if (!activeCallData?.callId || !activeCallData?.roomId) {
            clearStaleActiveCall('invalid payload');
            return false;
          }

          if (!storedAt || ageMs > MAX_STALE_MS) {
            const state = activeCallData.connectionState || 'connecting';
            const isActiveState = (state === 'initializing' || state === 'connecting' || state === 'connected');
            if (isActiveState && !storedAt) {
              try {
                activeCallData.storedAt = Date.now();
                localStorage.setItem(ACTIVE_CALL_KEY, JSON.stringify(activeCallData));
              } catch { }
            } else {
              clearStaleActiveCall(`stale (${Math.round(ageMs / 1000)}s)`);
              return false;
            }
          }

          const activeRoomId = getActiveChatRoomId();
          if (activeRoomId && activeCallData.roomId !== activeRoomId) {
            clearStaleActiveCall(`room mismatch (${activeCallData.roomId} !== ${activeRoomId})`);
            return false;
          }

          if (!activeRoomId && !shouldPreservePendingActiveCall()) {
            clearStaleActiveCall('chat room not resolved yet');
            return false;
          }

          if (activeCallData.roomId === getActiveChatRoomId()) {
            console.log(`📞 Active call found: ${activeCallData.callId} in room ${activeCallData.roomId}`);
            console.log(`✅ User is IN the call - will show "Back to Call" button`);

            if (audioCallBtn) {
              audioCallBtn.classList.add('hidden');
            }
            if (videoCallBtn) {
              videoCallBtn.classList.add('hidden');
            }

            if (joinCallBtn) {
              joinCallBtn.classList.remove('hidden');
              joinCallBtn.classList.add('flex');
              joinCallBtn.disabled = false;

              const btnIcon = joinCallBtn.querySelector('.material-symbols-outlined');
              const btnTextLong = joinCallBtn.querySelector('span:not(.material-symbols-outlined).hidden.sm\\:inline');
              const btnTextShort = joinCallBtn.querySelector('span:not(.material-symbols-outlined).sm\\:hidden');

              if (btnIcon) btnIcon.textContent = 'call';
              if (btnTextLong) btnTextLong.textContent = 'Back to Call';
              if (btnTextShort) btnTextShort.textContent = 'Back';

              joinCallBtn.classList.remove('bg-green-600', 'hover:bg-green-700');
              joinCallBtn.classList.add('bg-primary', 'hover:bg-primary/90');

              console.log('✅ "Back to Call" button configured');
            }

            return true;
          } else {
            console.log(`❌ Room mismatch: ${activeCallData.roomId} !== ${roomData?.roomId}`);
          }
        } catch (e) {
          console.error('❌ Failed to parse activeCall:', e);
          clearStaleActiveCall('parse error');
        }
      }

      if (roomData?.activeCall && roomData.activeCall.isActive && roomData.activeCall.participantCount > 0) {
        console.log('📞 Active call detected in room data - showing "Join Call" button');
        updateCallButtonState(true, roomData.activeCall);
        return true;
      }

      console.log('ℹ️ No active call - showing audio/video buttons');
      if (audioCallBtn) {
        audioCallBtn.classList.remove('hidden');
        audioCallBtn.disabled = false;
      }
      if (videoCallBtn) {
        videoCallBtn.classList.remove('hidden');
        videoCallBtn.disabled = false;
      }
      if (joinCallBtn) {
        joinCallBtn.classList.add('hidden');
        joinCallBtn.classList.remove('flex');
      }

      return false;
    }

    function updateCallButtonState(isActive, callData = null) {
      console.log('🔄 Updating call button state:', { isActive, participantCount: callData?.participantCount });

      if (audioCallBtn) {
        audioCallBtn.classList.remove('call-btn-loading', 'animate-pulse', 'calling');
        audioCallBtn.disabled = false;
      }
      if (videoCallBtn) {
        videoCallBtn.classList.remove('call-btn-loading', 'animate-pulse', 'calling');
        videoCallBtn.disabled = false;
      }
      if (joinCallBtn) {
        joinCallBtn.classList.remove('join-btn-loading');
      }

      const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
      let userInCall = false;
      if (activeCallStr) {
        try {
          const activeCallParsed = JSON.parse(activeCallStr);
          userInCall = !!(
            activeCallParsed?.callId &&
            activeCallParsed?.roomId &&
            activeCallParsed.roomId === roomData?.roomId
          );
        } catch { }
      }

      if (userInCall) {
        console.log('📞 User is IN call - showing "Back to Call" button');

        if (audioCallBtn) {
          audioCallBtn.classList.add('hidden');
          audioCallBtn.classList.remove('call-ready');
          audioCallBtn.setAttribute('aria-disabled', 'true');
          audioCallBtn.disabled = true;
        }
        if (videoCallBtn) {
          videoCallBtn.classList.add('hidden');
          videoCallBtn.classList.remove('call-ready');
          videoCallBtn.setAttribute('aria-disabled', 'true');
          videoCallBtn.disabled = true;
        }

        if (joinCallBtn) {
          joinCallBtn.classList.remove('hidden');
          joinCallBtn.classList.add('flex');
          joinCallBtn.disabled = false;

          const btnIcon = joinCallBtn.querySelector('.material-symbols-outlined');
          const btnTextLong = joinCallBtn.querySelector('span:not(.material-symbols-outlined).hidden.sm\\:inline');
          const btnTextShort = joinCallBtn.querySelector('span:not(.material-symbols-outlined).sm\\:hidden');

          if (btnIcon) btnIcon.textContent = 'call';
          if (btnTextLong) btnTextLong.textContent = 'Back to Call';
          if (btnTextShort) btnTextShort.textContent = 'Back';

          joinCallBtn.classList.remove('bg-green-600', 'hover:bg-green-700');
          joinCallBtn.classList.add('bg-primary', 'hover:bg-primary/90');

          console.log('✅ "Back to Call" button displayed');
        }
      } else if (isActive && callData) {
        console.log('📞 Active call in room but user NOT in it - showing "Join Call" button');

        if (callData && (!activeCallInRoom || activeCallInRoom.callId !== callData.callId)) {
          console.log('💾 Synchronizing activeCallInRoom inside updateCallButtonState');
          activeCallInRoom = {
            callId: callData.callId,
            callType: callData.callType,
            participantCount: typeof callData.participantCount === 'number' ? callData.participantCount : 0
          };
        }

        if (audioCallBtn) {
          audioCallBtn.classList.add('hidden');
          audioCallBtn.classList.remove('call-ready');
          audioCallBtn.setAttribute('aria-disabled', 'true');
          audioCallBtn.disabled = true;
        }
        if (videoCallBtn) {
          videoCallBtn.classList.add('hidden');
          videoCallBtn.classList.remove('call-ready');
          videoCallBtn.setAttribute('aria-disabled', 'true');
          videoCallBtn.disabled = true;
        }

        if (joinCallBtn) {
          joinCallBtn.classList.remove('hidden');
          joinCallBtn.classList.add('flex');
          joinCallBtn.disabled = false;

          const btnIcon = joinCallBtn.querySelector('.material-symbols-outlined');
          const btnTextLong = joinCallBtn.querySelector('span:not(.material-symbols-outlined).hidden.sm\\:inline');
          const btnTextShort = joinCallBtn.querySelector('span:not(.material-symbols-outlined).sm\\:hidden');

          if (btnIcon) btnIcon.textContent = 'login';
          if (btnTextLong) btnTextLong.textContent = 'Join Call';
          if (btnTextShort) btnTextShort.textContent = 'Join';

          joinCallBtn.classList.remove('bg-primary', 'hover:bg-primary/90');
          joinCallBtn.classList.add('bg-green-600', 'hover:bg-green-700');

          console.log(`✅ "Join Call" button displayed (${callData.participantCount} participants)`);
        }
      } else {
        console.log('📞 No active call - showing audio/video buttons');

        if (audioCallBtn) {
          audioCallBtn.classList.remove('hidden');
        }
        if (videoCallBtn) {
          videoCallBtn.classList.remove('hidden');
        }

        if (joinCallBtn) {
          joinCallBtn.classList.remove('flex');
          joinCallBtn.classList.add('hidden');
          joinCallBtn.disabled = true;
        }

        updateCallButtonStates();
        console.log('✅ Audio/Video buttons displayed');
      }
    }

    // ============================================
    // UI FUNCTIONS
    // ============================================

    function openSidebar() {
      if (!sidebar || !sidebarOverlay) return;
      sidebar.classList.add('show');
      sidebarOverlay.classList.add('active');
      document.body.style.overflow = 'hidden';
    }

    function closeSidebar() {
      if (!sidebar || !sidebarOverlay) return;
      sidebar.classList.remove('show');
      sidebarOverlay.classList.remove('active');
      document.body.style.overflow = '';
    }

    function toggleSidebar() {
      if (!sidebar) return;
      sidebar.classList.contains('show') ? closeSidebar() : openSidebar();
    }

    function setupUserCardClick() {
      document.querySelectorAll('.user-card').forEach(card => {
        card.addEventListener('click', () => {
          const userId = card.dataset.userId;
          if (userId && userId !== currentUser?.userId) toast(`User: ${card.dataset.username || userId}`, 'info');
        });
      });
    }

    function createProfilePictureElement(pfpData, username, className) {
      const el = document.createElement('div');
      el.className = className || 'w-8 h-8 rounded-full overflow-hidden flex-shrink-0';
      el.style.cssText = 'border-radius:9999px;overflow:hidden;display:flex;align-items:center;justify-content:center;flex-shrink:0;';
      if (pfpData && typeof pfpData === 'string' && pfpData.startsWith('http')) {
        const img = document.createElement('img');
        img.src = pfpData;
        img.alt = username || 'User';
        img.style.cssText = 'width:100%;height:100%;object-fit:cover;';
        el.appendChild(img);
      } else {
        const fallback = document.createElement('div');
        fallback.textContent = (username || '?')[0].toUpperCase();
        fallback.style.cssText = 'width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-weight:800;color:#16172b;background:linear-gradient(135deg,#a78bfa 0%,#6366f1 100%);font-size:14px;';
        el.appendChild(fallback);
      }
      return el;
    }

    function loadImageAttachment(fileId, imgElement) {
      if (!imgElement || !fileId) return;
      getFileFromIndexedDB(fileId).then(fileData => {
        if (fileData && fileData.blob) {
          const url = URL.createObjectURL(fileData.blob);
          imgElement.src = url;
          imgElement.dataset.blobUrl = url;
        }
      }).catch(() => {});
    }

    function reportChatPresenceContext(context, options = {}) {
      if (!socketInstance?.connected) return;
      safeSocketEmit('presence_context', { context, roomId: currentRoomId, ...options });
    }

    // ── Event Listeners Setup ──

    function setupEventListeners() {
      if (sidebarToggle) sidebarToggle.addEventListener('click', toggleSidebar);
      if (sidebarClose) sidebarClose.addEventListener('click', closeSidebar);
      if (sidebarOverlay) sidebarOverlay.addEventListener('click', closeSidebar);

      if (cancelReplyBtn) cancelReplyBtn.addEventListener('click', () => {
        replyingTo = null;
        document.getElementById('replyPreview')?.classList.add('hidden');
      });

      const cancelAttachmentBtn = document.getElementById('cancelAttachmentBtn');
      if (cancelAttachmentBtn) cancelAttachmentBtn.addEventListener('click', hideAttachmentPreview);

      if (leaveBtn) leaveBtn.addEventListener('click', () => {
        if (confirm('Leave this chat room?')) {
          if (socketInstance?.connected) socketInstance.emit('leave_room');
          window.location.href = '/mood.html';
        }
      });

      const imageViewerOverlay = document.getElementById('imageViewerOverlay');
      if (imageViewerOverlay) {
        imageViewerOverlay.addEventListener('click', (e) => {
          if (e.target === imageViewerOverlay) closeImageViewer();
        });
        const imageViewerImage = document.getElementById('imageViewerImage');
        if (imageViewerImage) imageViewerImage.addEventListener('click', toggleImageZoom);
      }

      if (messageForm) messageForm.addEventListener('submit', handleMessageSubmit);

      const fileInput = document.getElementById('fileInput');
      const attachmentBtn = document.getElementById('attachmentBtn');
      if (attachmentBtn && fileInput) {
        attachmentBtn.addEventListener('click', () => fileInput.click());
        fileInput.addEventListener('change', handleFileSelect);
      }

      if (acceptCallBtn) acceptCallBtn.addEventListener('click', acceptIncomingCall);
      if (declineCallBtn) declineCallBtn.addEventListener('click', declineIncomingCall);
      if (joinCallBtn) joinCallBtn.addEventListener('click', joinActiveCall);
      if (audioCallBtn) audioCallBtn.addEventListener('click', () => initiateCall('audio'));
      if (videoCallBtn) videoCallBtn.addEventListener('click', () => initiateCall('video'));

      if (messageInput) messageInput.addEventListener('input', handleTyping);
      if (messageInput) messageInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          messageForm?.requestSubmit();
        }
      });

      setupUserCardClick();
    }

    // ── Socket Event Handlers ──

    function setupSocketHandlers() {
      if (!socketInstance) return;

      socketInstance.on('chat_message', (data) => {
        if (data.roomId !== currentRoomId) return;
        messagesCache.push(data);
        renderMessage(data);
        if (messagesList) messagesList.scrollTop = messagesList.scrollHeight;
      });

      socketInstance.on('user_joined', (data) => {
        if (!shouldShowLifecycleMessage('join', data.userId)) return;
        addLifecycleMessage(`${data.username} joined`);
        loadOnlineUsers();
      });

      socketInstance.on('user_left', (data) => {
        if (!shouldShowLifecycleMessage('leave', data.userId)) return;
        addLifecycleMessage(`${data.username} left`);
        loadOnlineUsers();
      });

      socketInstance.on('room_users', (data) => {
        userToSocketId = new Map();
        if (Array.isArray(data.users)) {
          data.users.forEach(u => { if (u.socketId) userToSocketId.set(u.userId, u.socketId); });
        }
        renderUsersList(data.users || []);
      });

      socketInstance.on('user_typing_event', (data) => {
        if (data.userId === currentUser?.userId) return;
        if (data.isTyping) {
          typingUsers.set(data.userId, data.username || 'Someone');
        } else {
          typingUsers.delete(data.userId);
        }
        updateTypingUI();
      });

      socketInstance.on('join_matchmaking_timeout', () => {
        toast('No match found. Try again.', 'error');
        hideChatLoadingOverlay();
      });

      socketInstance.on('error', (data) => {
        toast(data?.message || 'An error occurred', 'error');
      });
    }

    // ── Message Rendering ──

    function renderMessage(data) {
      if (!messagesList) return;
      const isCurrentUser = data.userId === currentUser?.userId;
      const div = document.createElement('div');
      div.className = 'message-item flex flex-col ' + (isCurrentUser ? 'items-end' : 'items-start');
      div.dataset.messageId = data.messageId || '';

      const bubble = document.createElement('div');
      bubble.className = 'max-w-[80%] rounded-2xl px-4 py-2.5 ' +
        (isCurrentUser
          ? 'bg-[rgba(139,92,246,0.2)] rounded-br-md'
          : 'bg-[rgba(255,255,255,0.06)] rounded-bl-md');

      if (!isCurrentUser) {
        const nameEl = document.createElement('div');
        nameEl.className = 'text-xs font-semibold text-[var(--accent-purple-light)] mb-1';
        nameEl.textContent = data.username || 'Unknown';
        bubble.appendChild(nameEl);
      }

      if (data.message) {
        const msgEl = document.createElement('div');
        msgEl.className = 'text-sm break-words leading-relaxed';
        msgEl.textContent = data.message;
        bubble.appendChild(msgEl);
      }

      if (data.attachment) {
        const attEl = document.createElement('div');
        attEl.className = 'mt-2';
        if (data.attachment.type?.startsWith('image/')) {
          const img = document.createElement('img');
          img.className = 'max-w-[200px] max-h-[200px] rounded-xl cursor-pointer';
          img.dataset.fileId = data.attachment.fileId;
          img.src = data.attachment.url || '';
          img.alt = data.attachment.name || 'Image';
          img.addEventListener('click', () => {
            if (data.attachment.fileId) openImageViewer(data.attachment.fileId, data.attachment.name, data.attachment.url);
          });
          attEl.appendChild(img);
          if (data.attachment.fileId) loadImageAttachment(data.attachment.fileId, img);
        } else {
          const link = document.createElement('a');
          link.href = '#';
          link.className = 'flex items-center gap-2 text-xs text-[var(--accent-purple-light)]';
          link.textContent = '📎 ' + (data.attachment.name || 'File');
          link.addEventListener('click', (e) => {
            e.preventDefault();
            if (data.attachment.fileId) openImageViewer(data.attachment.fileId, data.attachment.name);
          });
          attEl.appendChild(link);
        }
        bubble.appendChild(attEl);
      }

      const time = document.createElement('div');
      time.className = 'text-[10px] text-[var(--text-muted)] mt-1';
      time.textContent = formatTimestamp(data.timestamp);
      bubble.appendChild(time);

      div.appendChild(bubble);
      messagesList.appendChild(div);
    }

    function addLifecycleMessage(text) {
      if (!messagesList) return;
      const div = document.createElement('div');
      div.className = 'text-center text-xs text-[var(--text-muted)] py-1';
      div.textContent = text;
      messagesList.appendChild(div);
    }

    function renderUsersList(users) {
      if (!usersList) return;
      usersList.innerHTML = '';
      if (onlineCount) onlineCount.textContent = users.length;
      users.forEach(user => {
        const card = document.createElement('div');
        card.className = 'user-card flex items-center gap-3 p-2 rounded-xl hover:bg-[rgba(255,255,255,0.04)] cursor-pointer transition-colors';
        card.dataset.userId = user.userId;
        card.dataset.username = user.username || 'User';
        const avatar = createProfilePictureElement(user.pfp, user.username, 'w-9 h-9');
        const name = document.createElement('span');
        name.className = 'text-sm font-semibold';
        name.textContent = user.username || 'Anonymous';
        card.appendChild(avatar);
        card.appendChild(name);
        usersList.appendChild(card);
      });
      setupUserCardClick();
    }

    function loadOnlineUsers() {
      if (socketInstance?.connected && currentRoomId) {
        socketInstance.emit('get_room_users', { roomId: currentRoomId });
      }
    }

    // ── Message Sending ──

    async function handleMessageSubmit(e) {
      e.preventDefault();
      if (!messageInput || !socketInstance?.connected || !roomData?.roomId) return;
      const text = messageInput.value.trim();
      if (!text && !pendingAttachment) return;

      if (pendingAttachment) {
        try {
          const attachment = pendingAttachment;
          const fileInput = document.getElementById('fileInput');
          const file = fileInput?.files?.[0];
          if (file) {
            const fileId = attachment.fileId;
            const blob = new Blob([await file.arrayBuffer()], { type: file.type });
            await storeFileToIndexedDB(file, roomData.roomId, null, fileId);
            await sendFileInChunks(fileId, file.name, file.type, file.size, blob, roomData.roomId);
          }
          const messageData = {
            message: text || null,
            attachment: { fileId: attachment.fileId, name: attachment.name, type: attachment.type },
            roomId: roomData.roomId,
            timestamp: Date.now(),
            userId: currentUser?.userId,
            username: currentUser?.username
          };
          socketInstance.emit('chat_message', messageData);
          renderMessage(messageData);
          hideAttachmentPreview();
        } catch (err) {
          toast('Failed to send attachment', 'error');
        }
        messageInput.value = '';
        return;
      }

      const messageData = {
        message: text,
        roomId: roomData.roomId,
        timestamp: Date.now(),
        userId: currentUser?.userId,
        username: currentUser?.username
      };
      socketInstance.emit('chat_message', messageData);
      renderMessage(messageData);
      messageInput.value = '';
      stopTyping();
      if (messagesList) messagesList.scrollTop = messagesList.scrollHeight;
    }

    async function handleFileSelect(e) {
      const file = e.target.files?.[0];
      if (!file) return;
      const validation = validateFile(file);
      if (!validation.valid) { toast(validation.error, 'error'); return; }
      try {
        const fileId = `file_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const blob = new Blob([await file.arrayBuffer()], { type: file.type });
        await storeFileToIndexedDB(file, roomData?.roomId || 'pending', null, fileId);
        showAttachmentPreview(fileId, file.name, file.type, file.size, blob);
      } catch (err) {
        toast('Failed to process file', 'error');
      }
      e.target.value = '';
    }

    // ── Call Functions ──

    function initiateCall(type) {
      if (!socketInstance?.connected || !roomData?.roomId || isInitiatingCall) return;
      isInitiatingCall = true;
      if (audioCallBtn) { audioCallBtn.disabled = true; audioCallBtn.classList.add('calling'); }
      if (videoCallBtn) { videoCallBtn.disabled = true; videoCallBtn.classList.add('calling'); }

      saveChatState(roomData.roomId, messagesCache);
      sessionStorage.setItem('returningFromCall', 'true');

      socketInstance.emit('initiate_call', { roomId: roomData.roomId, callType: type });
    }

    function acceptIncomingCall() {
      if (!pendingCallData || !socketInstance?.connected) return;
      navigatingToCall = true;
      sessionStorage.removeItem('returningFromCall');
      sessionStorage.setItem('returningFromCall', 'true');
      saveChatState(roomData?.roomId, messagesCache);
      socketInstance.emit('accept_call', { callId: pendingCallData.callId, roomId: roomData?.roomId });
      window.location.href = '/call.html';
    }

    function declineIncomingCall() {
      if (!pendingCallData || !socketInstance?.connected) return;
      socketInstance.emit('decline_call', { callId: pendingCallData.callId, roomId: roomData?.roomId });
      pendingCallData = null;
      incomingCallModal?.classList.add('hidden');
    }

    function joinActiveCall() {
      if (!roomData?.activeCall || !socketInstance?.connected) return;
      navigatingToCall = true;
      sessionStorage.removeItem('returningFromCall');
      sessionStorage.setItem('returningFromCall', 'true');
      saveChatState(roomData.roomId, messagesCache);
      window.location.href = '/call.html';
    }

    // ── Back Button Handler ──

    function setupBackButtonHandler() {
      let backPressedOnce = false;
      window.addEventListener('popstate', () => {
        if (backPressedOnce) {
          if (socketInstance?.connected) socketInstance.emit('leave_room');
          window.location.href = '/mood.html';
          return;
        }
        backPressedOnce = true;
        toast('Press back again to leave', 'info');
        setTimeout(() => { backPressedOnce = false; }, 2000);
      });
      history.pushState(null, '', window.location.href);
    }

    // ── Chat Initialization ──

    function initChat() {
      console.log('🚀 Initializing chat...');
      setupSocketHandlers();
      setupEventListeners();
      setupBackButtonHandler();
      setInitialCallButtonState();
      startCachedCallMonitoring();
      checkForBackgroundCall();
      loadOnlineUsers();
      const savedState = loadChatState();
      if (savedState?.messages?.length) {
        savedState.messages.forEach(m => renderMessage(m));
        setTimeout(() => { if (messagesList) messagesList.scrollTop = messagesList.scrollHeight; }, 100);
      }
      hideChatLoadingOverlay();
      console.log('✅ Chat initialized');
    }

    // ── Start ──
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', initChat);
    } else {
      initChat();
    }

  } catch (e) {
    console.error('❌ Chat app failed:', e);
  }
})();

