    if (!firebase.apps.length) {
      firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__);
    }
    firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL);

    // ============================================
    // PRESENCE & HEARTBEAT
    // ============================================
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

      // Send initial heartbeat immediately
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
      }, 10000); // 10s interval
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
          // Handle 'info' type by mapping to 'success' if no info function exists
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
      const CALL_CACHE_TIMEOUT = 60000; // 60 seconds max cache time
      let cachedCallCheckInterval = null;

      // ============================================
      // CALL STATE MONITORING
      // ============================================
      let activeCallConnectionState = null; // null | 'initializing' | 'connecting' | 'connected'

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

          // Production hardening: don't trust very old or missing timestamps.
          const storedAt = typeof callData?.storedAt === 'number' ? callData.storedAt : 0;
          const ageMs = storedAt ? (Date.now() - storedAt) : Number.POSITIVE_INFINITY;
          const MAX_STALE_MS = 2 * 60 * 1000; // 2 minutes
          if (!storedAt || ageMs > MAX_STALE_MS) {
            // If the call is actively initializing/connecting/connected, prefer self-repair
            // over clearing (missing storedAt is a known transient during fast call start).
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

          // Sync local state
          if (activeCallConnectionState !== newState) {
            console.log(`📞 Active call state changed: ${activeCallConnectionState} → ${newState}`);
            activeCallConnectionState = newState;

            // CRITICAL: Hide incoming call modal if user is now in an active call state
            if (activeCallConnectionState === 'initializing' || activeCallConnectionState === 'connecting' || activeCallConnectionState === 'connected') {
              const modal = document.getElementById('incomingCallModal');
              if (modal && !modal.classList.contains('hidden')) {
                console.log('🧹 Hiding incoming call modal - user is now in an active/initiating callState');
                modal.classList.add('hidden');
                pendingCallData = null;
              }
            }

            updateCallButtonStates();

            // CRITICAL: Only update room-scoped UI if the call belongs to this room
            const callInCurrentRoom = callData.roomId === getActiveChatRoomId();

            if (callInCurrentRoom) {
              // Synchronize global activeCallInRoom variable
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
              // Call in storage is for another room - ignore for main header buttons
              // (will be shown in floating popup if hasBackgroundCall is true)
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
          // Call is connecting - disable call buttons
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
          // Call is connected - disable call buttons
          console.log('✅ Call connected - disabling initiate buttons');
          audioCallBtn.disabled = true;
          videoCallBtn.disabled = true;
          audioCallBtn.classList.remove('call-ready');
          videoCallBtn.classList.remove('call-ready');
          audioCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
          videoCallBtn.classList.add('opacity-50', 'cursor-not-allowed');
          audioCallBtn.title = 'Call in progress';
          videoCallBtn.title = 'Call in progress';

          // CRITICAL: Automatically show call iframe when connected
          const callIframeContainer = document.getElementById('callIframeContainer');
          if (callIframeContainer && callIframeContainer.classList.contains('hidden')) {
            console.log('🎬 Call connected - automatically showing call UI');
            callIframeContainer.classList.remove('hidden');
          }

        } else {
          if (isSocketAuthenticated && hasJoinedRoom && roomData?.roomId && !isInitiatingCall) {
            // No active call - enable call buttons
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

      // Listen for call state changes from call.html
      window.addEventListener('storage', (e) => {
        if (e.key === 'callStateChanged' || e.key === 'activeCall') {
          console.log('🔄 Storage event detected - updating call state');
          updateActiveCallState();
        }
      });

      // ============================================
      // TYPING INDICATOR STATE & FUNCTIONS
      // ============================================
      const typingUsers = new Map(); // userId -> username
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
        typingTimeout = setTimeout(stopTyping, 2000); // 2 seconds of silence = stop
      };

      // Listen for messages from call iframe
      window.addEventListener('message', (event) => {
        const action = event.data?.action || event.data?.type; // Support both for compatibility

        if (action === 'hideCall' || action === 'REQUEST_SHOW_CHAT' || action === 'FORCE_HIDE_CALL') {
          console.log(`📨 Message from call iframe: ${action}`);
          const callIframeContainer = document.getElementById('callIframeContainer');
          if (callIframeContainer) {
            callIframeContainer.classList.add('hidden');
            console.log('✅ Call iframe hidden - chat visible');
          }

          if (action === 'FORCE_HIDE_CALL') {
            // Reset active call state
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

      // Poll for call state (fallback for same-window updates)
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
      let serverClockOffset = 0; // Milliseconds to add to client time to get server time
      let serverExpiresAt = null; // Legacy (rooms no longer expire)
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
              <style>@keyframes chat-loading-bar{from{transform:translateX(-20%);}to{transform:translateX(150%);}}</style>
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
      let emojiData = null; // Cached emoji data from API
      let emojiCache = null; // Parsed and optimized emoji cache
      let currentEmojiCategory = 'all'; // Currently selected category
      let emojiSearchTerm = ''; // Current search query
      let isEmojiPickerOpen = false; // Picker open state

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

      /**
       * Fetch all emojis from EmojiHub API
       * Implements caching to avoid repeated API calls
       */

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


      async function fetchEmojis() {
        console.log('😀 ========================================');
        console.log('😀 FETCHING EMOJIS FROM API');
        console.log('😀 ========================================');

        // Return cached data if available
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

      // Emoji picker overlay click - close picker
      const emojiPickerOverlay = document.getElementById('emojiPickerOverlay');
      if (emojiPickerOverlay) {
        emojiPickerOverlay.addEventListener('click', (e) => {
          // Only close if clicking the overlay itself, not the sheet
          if (e.target === emojiPickerOverlay) {
            closeEmojiPicker();
          }
        }, { passive: true });
        console.log('✅ Emoji overlay listener attached');
      }

      // Emoji search input - filter emojis
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

      // ESC key - close emoji picker
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && isEmojiPickerOpen) {
          closeEmojiPicker();
        }
      });




      /**
       * Process emoji data into optimized cache structure
       * Groups by category and creates search index
       */
      function processEmojiData(rawData) {
        console.log('⚙️ Processing emoji data for optimized access...');

        const categories = {};
        const searchIndex = [];

        rawData.forEach((emoji, index) => {
          // Parse HTML code to actual emoji character
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

          // Group by category
          if (!categories[emoji.category]) {
            categories[emoji.category] = [];
          }
          categories[emoji.category].push(processed);

          // Add to search index
          searchIndex.push(processed);
        });

        console.log(`✅ Processed ${searchIndex.length} emojis into ${Object.keys(categories).length} categories`);

        return { categories, searchIndex, all: searchIndex };
      }

      /**
       * Filter emojis based on search term and category
       */
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

      /**
       * Render category tabs
       */
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

        // Add click handlers
        container.querySelectorAll('.emoji-category-tab').forEach(btn => {
          btn.addEventListener('click', () => {
            currentEmojiCategory = btn.dataset.category;
            renderEmojiCategories();
            renderEmojiGrid();
          }, { passive: true });
        });
      }

      /**
       * Render emoji grid with current filters
       * Uses virtual scrolling concept by limiting initial render
       */
      function renderEmojiGrid() {
        const container = document.getElementById('emojiGrid');
        if (!container) return;

        const filteredEmojis = filterEmojis(emojiSearchTerm, currentEmojiCategory);

        console.log(`🎨 Rendering ${filteredEmojis.length} emojis (category: ${currentEmojiCategory}, search: "${emojiSearchTerm}")`);

        if (filteredEmojis.length === 0) {
          container.innerHTML = '<div class="emoji-empty">No emojis found</div>';
          return;
        }

        // Render first 200 emojis immediately for performance
        // Load more on scroll if needed (simplified approach)
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

        // Add click handlers
        container.querySelectorAll('.emoji-item').forEach(item => {
          item.addEventListener('click', () => {
            insertEmojiIntoInput(item.dataset.emoji);
          }, { passive: true });
        });
      }

      /**
       * Insert emoji into message input at cursor position
       * CRITICAL: Maintains cursor position and existing text
       */
      function insertEmojiIntoInput(emoji) {
        if (!messageInput) {
          console.error('❌ Message input not found');
          return;
        }

        console.log(`😀 Inserting emoji: ${emoji}`);

        // Get current cursor position
        const start = messageInput.selectionStart;
        const end = messageInput.selectionEnd;
        const currentValue = messageInput.value;

        // Insert emoji at cursor position
        const newValue = currentValue.substring(0, start) + emoji + currentValue.substring(end);
        messageInput.value = newValue;

        // Restore cursor position (after the inserted emoji)
        const newCursorPos = start + emoji.length;
        messageInput.setSelectionRange(newCursorPos, newCursorPos);

        // Focus input
        messageInput.focus();

        console.log(`✅ Emoji inserted at position ${start}, cursor now at ${newCursorPos}`);

        // Close picker after selection
        closeEmojiPicker();
      }

      /**
       * Open emoji picker bottom sheet
       */
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

        // Show loading state
        const grid = document.getElementById('emojiGrid');
        if (grid) {
          grid.innerHTML = `
      <div class="emoji-loading">
        <div class="emoji-loading-spinner"></div>
        <div>Loading emojis...</div>
      </div>
    `;
        }

        // Show overlay with animation
        overlay.classList.add('active');
        console.log('✅ Overlay shown');

        try {
          // Fetch emojis if not cached
          if (!emojiCache) {
            console.log('📡 Fetching emoji data...');
            const rawData = await fetchEmojis();
            emojiCache = processEmojiData(rawData);
            console.log('✅ Emoji cache ready');
          }

          // Render UI
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

      /**
       * Close emoji picker bottom sheet
       */
      function closeEmojiPicker() {
        console.log('😀 Closing emoji picker');

        const overlay = document.getElementById('emojiPickerOverlay');
        if (overlay) {
          overlay.classList.remove('active');
        }

        isEmojiPickerOpen = false;

        // Reset search
        emojiSearchTerm = '';
        const searchInput = document.getElementById('emojiSearchInput');
        if (searchInput) {
          searchInput.value = '';
        }

        // Reset to "all" category
        currentEmojiCategory = 'all';

        console.log('✅ Emoji picker closed');
      }

      /**
       * Debounce function for search input
       */
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
          disableCallButtons(true); // true = show loading state
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

      /**
       * Initialize IndexedDB for file storage
       * Returns promise that resolves to DB instance
       */
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

            // Create object store for file attachments
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

      /**
       * Store file in IndexedDB without keeping in RAM
       * Uses chunked reading for large files
       */
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

          // Use custom ID if provided, otherwise generate one
          const fileId = customFileId || `file_${roomId}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

          // Read file in chunks to avoid memory issues
          const chunks = [];
          let offset = 0;

          while (offset < file.size) {
            const chunk = file.slice(offset, offset + CHUNK_SIZE);
            const arrayBuffer = await readChunkAsArrayBuffer(chunk);
            chunks.push(arrayBuffer);
            offset += CHUNK_SIZE;

            // Log progress for large files
            if (file.size > 5 * 1024 * 1024) {
              const progress = ((offset / file.size) * 100).toFixed(1);
              console.log(`   Progress: ${progress}%`);
            }
          }

          // Combine chunks into single Blob (IndexedDB can store Blobs efficiently)
          const blob = new Blob(chunks, { type: file.type });

          // Clear chunks array to free memory
          chunks.length = 0;

          // Store file metadata and blob
          const fileRecord = {
            id: fileId,
            roomId: roomId,
            messageId: messageId,
            name: file.name,
            type: file.type,
            size: file.size,
            blob: blob, // IndexedDB stores this on disk, not in RAM
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

      /**
       * Read file chunk as ArrayBuffer without blocking main thread
       */
      function readChunkAsArrayBuffer(chunk) {
        return new Promise((resolve, reject) => {
          const reader = new FileReader();
          reader.onload = () => resolve(reader.result);
          reader.onerror = () => reject(reader.error);
          reader.readAsArrayBuffer(chunk);
        });
      }

      /**
       * Retrieve file from IndexedDB
       */
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

      /**
       * CRITICAL: Delete ALL files associated with a room
       * Called when room is destroyed, expired, or left
       */
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

      /**
       * Delete specific file by ID
       */
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

      /**
       * Get count of files for a room (for debugging/monitoring)
       */
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
        // Images - 100MB max
        'image/jpeg': { ext: ['.jpg', '.jpeg'], maxSize: 100 * 1024 * 1024 },
        'image/png': { ext: ['.png'], maxSize: 100 * 1024 * 1024 },
        'image/gif': { ext: ['.gif'], maxSize: 100 * 1024 * 1024 },
        'image/webp': { ext: ['.webp'], maxSize: 100 * 1024 * 1024 },
        'image/bmp': { ext: ['.bmp'], maxSize: 100 * 1024 * 1024 },
        'image/tiff': { ext: ['.tiff', '.tif'], maxSize: 100 * 1024 * 1024 },
        'image/svg+xml': { ext: ['.svg'], maxSize: 10 * 1024 * 1024 },

        // Videos - 100MB max
        'video/mp4': { ext: ['.mp4'], maxSize: 100 * 1024 * 1024 },
        'video/webm': { ext: ['.webm'], maxSize: 100 * 1024 * 1024 },
        'video/quicktime': { ext: ['.mov'], maxSize: 100 * 1024 * 1024 },
        'video/ogg': { ext: ['.ogv', '.ogg'], maxSize: 100 * 1024 * 1024 },
        'video/x-msvideo': { ext: ['.avi'], maxSize: 100 * 1024 * 1024 },
        'video/x-matroska': { ext: ['.mkv'], maxSize: 100 * 1024 * 1024 },
        'video/mpeg': { ext: ['.mpeg', '.mpg'], maxSize: 100 * 1024 * 1024 },

        // Documents - 100MB max
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

      /**
       * Validate file before processing
       * Returns { valid: boolean, error: string|null }
       */
      function validateFile(file) {
        console.log('🔍 ========================================');
        console.log('🔍 FILE VALIDATION');
        console.log('🔍 ========================================');
        console.log(`   File: ${file.name}`);
        console.log(`   Type: ${file.type}`);
        console.log(`   Size: ${(file.size / 1024 / 1024).toFixed(2)} MB`);

        // CRITICAL: Absolute size check first
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

        // Check file type - allow image/*, video/*, PDF, and common document types
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

        // Check file extension matches MIME type
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

        // Check type-specific size limit
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
      const CHUNK_SIZE = 524288; // 512KB chunks (fewer packets, faster global transfers)
      const CHUNK_TIMEOUT = 20000; // 20s stall watchdog
      const MAX_RETRIES = 3; // Retry failed chunks up to 3 times

      /**
       * Chunk file into manageable pieces for transmission
       * Returns array of { index, data: base64, size }
       */




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
          // Get file from IndexedDB
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

            // Create blob URL for display
            const blobUrl = URL.createObjectURL(fileData.blob);

            // Set image source
            image.src = blobUrl;
            image.alt = fileData.name;
            image.dataset.blobUrl = blobUrl;

            // Update info
            if (filenameEl) filenameEl.textContent = fileData.name;
            if (sizeEl) sizeEl.textContent = `${formatFileSize(fileData.size)} • Click image to zoom`;
          }

          // Reset zoom state
          isImageZoomed = false;
          image.classList.remove('zoomed');

          // CRITICAL FIX: Force display and ensure it's visible
          overlay.style.display = 'flex';
          overlay.classList.add('active');

          // Force a reflow to ensure the display change is applied
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

      /**
       * Close image viewer and cleanup
       */
      function closeImageViewer() {
        console.log('🖼️ Closing image viewer');

        const overlay = document.getElementById('imageViewerOverlay');
        const image = document.getElementById('imageViewerImage');

        if (overlay) {
          overlay.classList.remove('active');
          // Use setTimeout to allow animation to complete before hiding
          setTimeout(() => {
            overlay.style.display = 'none';
          }, 200); // Match the transition duration in CSS
        }

        // Cleanup blob URL to prevent memory leak
        if (image && image.dataset.blobUrl) {
          URL.revokeObjectURL(image.dataset.blobUrl);
          delete image.dataset.blobUrl;
          console.log('🧹 Blob URL revoked');
        }

        // Reset zoom
        isImageZoomed = false;
        if (image) {
          image.classList.remove('zoomed');
        }

        console.log('✅ Image viewer closed');
      }

      /**
       * Toggle zoom on image
       */
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

      /**
       * Robust helper to convert base64 string to Blob
       * Handles Data URL stripping, whitespace removal, and padding correction
       */
      function base64ToBlob(base64, contentType) {
        if (!base64 || typeof base64 !== 'string') {
          console.error('❌ Cannot convert to blob: base64 data is missing or not a string', { type: typeof base64 });
          return null;
        }

        try {
          // 1. Strip Data URL prefix if present
          let cleanBase64 = base64.includes(',') ? base64.split(',')[1] : base64;

          // 2. Handle URL-safe base64 (replace - with + and _ with /)
          cleanBase64 = cleanBase64.replace(/-/g, '+').replace(/_/g, '/');

          // 3. Remove any whitespace, newlines or non-base64 characters
          // CRITICAL: We remove ALL '=' first, because chunked joining can have '=' in the middle
          // Valid base64 chars: A-Z, a-z, 0-9, +, /
          cleanBase64 = cleanBase64.replace(/[^A-Za-z0-9+/]/g, "");

          // 4. Fix padding ONLY at the end
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

          // Convert chunk to ArrayBuffer (Binary)
          const arrayBuffer = await new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result);
            reader.onerror = reject;
            reader.readAsArrayBuffer(blob);
          });

          chunks.push({
            index: i,
            data: arrayBuffer, // Store as ArrayBuffer
            size: blob.size
          });

          // Log progress every 10 chunks
          if ((i + 1) % 10 === 0 || i === totalChunks - 1) {
            const progress = ((i + 1) / totalChunks * 100).toFixed(1);
            console.log(`   Chunking progress: ${progress}% (${i + 1}/${totalChunks})`);
          }
        }

        console.log(`✅ File chunked: ${chunks.length} binary chunks ready`);
        console.log('✂️ ========================================\n');

        return chunks;
      }

      /**
       * Send file using a pipelined chunk protocol with lazy chunk reads.
       * This starts transmitting immediately instead of pre-building all chunks in memory.
       */
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

      // Initialize global receiver
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



      /**
       * Format bytes to human readable size
       */
      function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
      }

      /**
       * Get file type category for icon display
       */
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

      /**
       * Create Blob URL for image preview (must be revoked after use)
       */
      function createImagePreviewUrl(blob) {
        if (blob.type.startsWith('image/')) {
          return URL.createObjectURL(blob);
        }
        return null;
      }

      /**
       * Show attachment preview in UI
       */
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

        // Clear previous preview
        iconWrapper.innerHTML = '';
        iconWrapper.className = 'attachment-icon-wrapper';

        const fileCategory = getFileTypeCategory(fileType);
        let previewUrl = null;

        // Generate appropriate preview based on file type
        if (fileCategory === 'image') {
          // Show image thumbnail
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
          // Show PDF icon
          iconWrapper.classList.add('pdf-preview');
          iconWrapper.innerHTML = '<span class="material-symbols-outlined">picture_as_pdf</span>';
          console.log('✅ PDF icon displayed');
        } else if (fileCategory === 'video') {
          // Show video icon
          iconWrapper.classList.add('video-preview');
          iconWrapper.innerHTML = '<span class="material-symbols-outlined">videocam</span>';
          console.log('✅ Video icon displayed');
        } else {
          // Show generic document icon
          iconWrapper.classList.add('pdf-preview');
          iconWrapper.innerHTML = '<span class="material-symbols-outlined">description</span>';
          console.log('✅ Document icon displayed');
        }

        // Update file info
        nameElement.textContent = fileName;
        sizeElement.textContent = formatFileSize(fileSize);

        // Store attachment data
        pendingAttachment = {
          fileId,
          name: fileName,
          type: fileType,
          size: fileSize,
          previewUrl // Store for cleanup
        };

        // Show preview
        previewContainer.classList.remove('hidden');

        console.log('✅ Attachment preview displayed');
        console.log('🖼️ ========================================\n');
      }

      /**
       * Hide and cleanup attachment preview
       */
      function hideAttachmentPreview() {
        console.log('🧹 Hiding attachment preview');

        const previewContainer = document.getElementById('attachmentPreview');
        if (previewContainer) {
          previewContainer.classList.add('hidden');
        }

        // Revoke blob URL to prevent memory leak
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

          // Check if cache is expired
          if (cacheAge > CALL_CACHE_TIMEOUT) {
            console.log(`⏰ Cached call expired (${(cacheAge / 1000).toFixed(1)}s > ${CALL_CACHE_TIMEOUT / 1000}s)`);
            localStorage.removeItem(CACHED_CALL_KEY);
            return;
          }

          // Check if call is still valid on server
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
        // GUARD: Don't show if already in/initiating a call
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

        // Set pending call data
        pendingCallData = {
          callId: callData.callId,
          callerUsername: callData.callerUsername,
          callerPfp: callData.callerPfp,
          callType: callData.callType,
          fromCache: true
        };

        // Update modal UI
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

        // Show modal immediately (no delay for cached calls)
        incomingCallModal?.classList.remove('hidden');

        console.log('✅ Cached call modal displayed');
        console.log('📞 ========================================\n');
      }

      function startCachedCallMonitoring() {
        console.log('🔄 Starting cached call monitoring...');

        // Check immediately on load
        checkForCachedCall({ logMiss: true });

        // Check every 2 seconds for cached calls
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


      // Function to cleanup parent call.html page
      function cleanupParentCallPage(targetUrl = null) {
        console.log('🧹 ========================================');
        console.log('🧹 CLEANING UP PARENT CALL PAGE');
        console.log('🧹 ========================================');

        const finalUrl = targetUrl || '/mood.html';

        // In Social Club mode, never force the user out to /mood.html during call initiation/participation.
        // Doing so triggers a page unload -> presence leave beacon -> server destroys the room.
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

        // ✅ CRITICAL: Clear any persistent matchmaking state from discovery.html
        // This prevents automatic re-match if the user lands on discovery.html via back-button
        try {
          localStorage.removeItem('discovery_state');
          localStorage.removeItem('discovery_state_timestamp');
          console.log('🧹 [Cleanup] Matchmaking state cleared');
        } catch (e) { }

        // Check if we're in an iframe (chat overlay on call.html)
        if (window.parent && window.parent !== window) {
          console.log('📱 We are in iframe - accessing parent window');
          try {
            // Hide the floating button
            const parentFloatingBtn = window.parent.document.getElementById('floatingReturnToCall');
            if (parentFloatingBtn) {
              parentFloatingBtn.classList.add('hidden');
              console.log('✅ Parent floating button hidden');
            }

            // Navigate parent window
            console.log(`🔄 Navigating parent window to ${finalUrl}`);
            window.parent.location.href = finalUrl;

            console.log('✅ Parent window navigation initiated');
          } catch (e) {
            console.error('❌ Cannot access parent window:', e);
            // Fallback: just navigate ourselves
            window.location.href = finalUrl;
          }
        } else {
          console.log(`ℹ️ Not in iframe - navigating directly to ${finalUrl}`);
          window.location.href = finalUrl;
        }

        console.log('🧹 ========================================\n');
      }


      // Function to hide floating button in parent call.html
      function hideCallPageFloatingButton() {
        console.log('🎈 Attempting to hide call.html floating button');

        // Check if we're in an iframe (chat overlay)
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
          // If not in iframe, might be standalone chat page
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

          // Mark that we're returning to call (not starting new call)
          navigatingToCall = true;

          // CRITICAL: Remove background mode flag so call page knows we're coming back
          sessionStorage.removeItem('returningFromCall');
          sessionStorage.removeItem('backgroundCallMode');

          // Mark that we're returning to existing call
          sessionStorage.setItem('returningToBackgroundCall', 'true');

          // Save current chat state
          saveChatState(roomData.roomId, messagesCache);

          // Disconnect floating socket before navigating
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
              navigatingToCall = true; // Set flag for auto-leave detection
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

          // CRITICAL FIX: Validate input data
          if (!roomId) {
            console.error('❌ Cannot save state: roomId missing');
            return;
          }

          if (!Array.isArray(messages)) {
            console.error('❌ Cannot save state: messages not an array');
            return;
          }

          // Validate messages have required fields
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

          // Determine call button state
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
            messages: validMessages, // Use validated messages
            timestamp: Date.now(),
            callButtonState,
            activeCallInRoom: null,
            version: 2 // Add version for future migrations
          };

          // CRITICAL FIX: Use try-catch for localStorage quota errors
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

              // Emergency cleanup: keep only last 50 messages
              const trimmedMessages = validMessages.slice(-50);
              const trimmedState = { ...state, messages: trimmedMessages };

              try {
                localStorage.setItem(CHAT_STATE_KEY, JSON.stringify(trimmedState));
                console.warn(`⚠️ Saved trimmed state (${trimmedMessages.length} messages)`);
              } catch (retryError) {
                console.error('❌ Failed to save even trimmed state:', retryError);
                // Clear old state to free space
                localStorage.removeItem(CHAT_STATE_KEY);
                localStorage.removeItem(CHAT_MESSAGES_KEY);
              }
            } else {
              throw storageError; // Re-throw non-quota errors
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

      // Replace the existing clearChatState function with this version:
      async function clearChatState(force = false) {
        console.log('🧹 ========================================');
        console.log('🧹 CLEARING CHAT STATE');
        console.log('🧹 ========================================');
        console.log(`   Force: ${force}`);

        // Only clear if room is actually expiring/ended, or if forced
        const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);

        if (!force && activeCallStr) {
          console.log('⚠️ Active call exists - preserving chat state');
          console.log('🧹 ========================================\n');
          return;
        }

        // CRITICAL: Delete all files for this room from IndexedDB
        if (roomData?.roomId) {
          try {
            console.log(`🗑️ Deleting all files for room: ${roomData.roomId}`);
            const deletedCount = await deleteRoomFiles(roomData.roomId);
            console.log(`✅ Deleted ${deletedCount} file(s) from IndexedDB`);
          } catch (error) {
            console.error('❌ Failed to delete room files:', error);
            // Continue with other cleanup even if file deletion fails
          }
        }

        // Clear localStorage
        localStorage.removeItem(CHAT_STATE_KEY);
        localStorage.removeItem(CHAT_MESSAGES_KEY);
        localStorage.removeItem(CHAT_TIMESTAMP_KEY);

        console.log('✅ Chat state cleared');
        console.log('🧹 ========================================\n');
      }

      function clearAllChatData() {
        // CRITICAL: Clear ALL chat-related data on entry
        localStorage.removeItem(CHAT_STATE_KEY);
        localStorage.removeItem(CHAT_MESSAGES_KEY);
        localStorage.removeItem(CHAT_TIMESTAMP_KEY);
        // Don't clear currentRoom or activeCall - those are needed
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

            // Production hardening: don't trust very old or missing timestamps.
            const storedAt = typeof activeCallData?.storedAt === 'number' ? activeCallData.storedAt : 0;
            const ageMs = storedAt ? (Date.now() - storedAt) : Number.POSITIVE_INFINITY;
            const MAX_STALE_MS = 2 * 60 * 1000; // 2 minutes

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

              // Hide audio/video buttons
              if (audioCallBtn) {
                audioCallBtn.classList.add('hidden');
              }
              if (videoCallBtn) {
                videoCallBtn.classList.add('hidden');
              }

              // Show join button as "Back to Call" button
              if (joinCallBtn) {
                joinCallBtn.classList.remove('hidden');
                joinCallBtn.classList.add('flex');
                joinCallBtn.disabled = false;

                // Update button text and icon
                const btnIcon = joinCallBtn.querySelector('.material-symbols-outlined');
                const btnTextLong = joinCallBtn.querySelector('span:not(.material-symbols-outlined).hidden.sm\\:inline');
                const btnTextShort = joinCallBtn.querySelector('span:not(.material-symbols-outlined).sm\\:hidden');

                if (btnIcon) btnIcon.textContent = 'call';
                if (btnTextLong) btnTextLong.textContent = 'Back to Call';
                if (btnTextShort) btnTextShort.textContent = 'Back';

                // Change color to green
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



        // ✅ NEW: Check for active call in room data (from matchmaking/restoration)
        if (roomData?.activeCall && roomData.activeCall.isActive && roomData.activeCall.participantCount > 0) {
          console.log('📞 Active call detected in room data - showing "Join Call" button');
          updateCallButtonState(true, roomData.activeCall);
          return true;
        }

        // No active call - show normal buttons
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

        // Remove loading states and re-enable buttons
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

        // Check if user is already in the call
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

          // Hide audio/video buttons
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

          // Show "Back to Call" button
          if (joinCallBtn) {
            joinCallBtn.classList.remove('hidden');
            joinCallBtn.classList.add('flex');
            joinCallBtn.disabled = false;

            // Update button text and icon
            const btnIcon = joinCallBtn.querySelector('.material-symbols-outlined');
            const btnTextLong = joinCallBtn.querySelector('span:not(.material-symbols-outlined).hidden.sm\\:inline');
            const btnTextShort = joinCallBtn.querySelector('span:not(.material-symbols-outlined).sm\\:hidden');

            if (btnIcon) btnIcon.textContent = 'call';
            if (btnTextLong) btnTextLong.textContent = 'Back to Call';
            if (btnTextShort) btnTextShort.textContent = 'Back';

            // Change to primary color
            joinCallBtn.classList.remove('bg-green-600', 'hover:bg-green-700');
            joinCallBtn.classList.add('bg-primary', 'hover:bg-primary/90');

            console.log('✅ "Back to Call" button displayed');
          }
        } else if (isActive && callData) {
          console.log('📞 Active call in room but user NOT in it - showing "Join Call" button');

          // CRITICAL: Synchronize global activeCallInRoom variable if provided
          if (callData && (!activeCallInRoom || activeCallInRoom.callId !== callData.callId)) {
            console.log('💾 Synchronizing activeCallInRoom inside updateCallButtonState');
            activeCallInRoom = {
              callId: callData.callId,
              callType: callData.callType,
              participantCount: typeof callData.participantCount === 'number' ? callData.participantCount : 0
            };
          }

          // Hide audio/video buttons
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

          // Show "Join Call" button
          if (joinCallBtn) {
            joinCallBtn.classList.remove('hidden');
            joinCallBtn.classList.add('flex');
            joinCallBtn.disabled = false;

            // Update button text and icon for join
            const btnIcon = joinCallBtn.querySelector('.material-symbols-outlined');
            const btnTextLong = joinCallBtn.querySelector('span:not(.material-symbols-outlined).hidden.sm\\:inline');
            const btnTextShort = joinCallBtn.querySelector('span:not(.material-symbols-outlined).sm\\:hidden');

            if (btnIcon) btnIcon.textContent = 'login';
            if (btnTextLong) btnTextLong.textContent = 'Join Call';
            if (btnTextShort) btnTextShort.textContent = 'Join';

            // Green color for join
            joinCallBtn.classList.remove('bg-primary', 'hover:bg-primary/90');
            joinCallBtn.classList.add('bg-green-600', 'hover:bg-green-700');

            console.log(`✅ "Join Call" button displayed (${callData.participantCount} participants)`);
          }
        } else {
          console.log('📞 No active call - showing audio/video buttons');

          // Show audio/video buttons
          if (audioCallBtn) {
            audioCallBtn.classList.remove('hidden');
          }
          if (videoCallBtn) {
            videoCallBtn.classList.remove('hidden');
          }

          // Hide join button
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
        if (window.innerWidth <= 768) {
          document.querySelectorAll('.user-card').forEach(card => {
            card.addEventListener('click', closeSidebar, { passive: true });
          });
        }
      }


      function createProfilePictureElement(pfpUrl, username, size = 'w-10 h-10') {
        const initial = username ? username.charAt(0).toUpperCase() : 'U';
        const container = document.createElement('div');
        container.className = `${size} rounded-full overflow-hidden bg-slate-100 border-2 border-white shadow-sm flex-shrink-0`;

        if (pfpUrl && pfpUrl !== 'https://ui-avatars.com/api/?name=User&background=367d7d&color=ffffff&size=200') {
          const img = document.createElement('img');
          img.src = pfpUrl;
          img.alt = username;
          img.className = 'h-full w-full object-cover';
          img.loading = 'lazy';
          img.onerror = function () {
            this.onerror = null;
            container.innerHTML = `<div class="h-full w-full bg-primary text-white flex items-center justify-center font-bold text-sm">${initial}</div>`;
          };
          container.appendChild(img);
        } else {
          container.innerHTML = `<div class="h-full w-full bg-primary text-white flex items-center justify-center font-bold text-sm">${initial}</div>`;
        }

        return container;
      }

      function showReplyPreview(messageData) {
        replyingTo = messageData;
        const replyPreview = document.getElementById('replyPreview');
        const replyToUsername = document.getElementById('replyToUsername');
        const replyToMessage = document.getElementById('replyToMessage');

        if (replyPreview && replyToUsername && replyToMessage) {
          replyToUsername.textContent = `Replying to ${messageData.username}`;
          replyToMessage.textContent = messageData.message;
          replyPreview.classList.remove('hidden');

          if (messageInput) messageInput.focus();
        }
      }

      function hideReplyPreview() {
        replyingTo = null;
        const replyPreview = document.getElementById('replyPreview');
        if (replyPreview) replyPreview.classList.add('hidden');
      }

      function createMessageElement(data, isCurrentUser) {
        const messageItem = document.createElement('div');
        messageItem.className = `message-item flex gap-2 items-start ${isCurrentUser ? 'flex-row-reverse' : ''} animate-fade-in-up`;
        messageItem.dataset.messageId = data.messageId || `msg-${Date.now()}-${Math.random()}`;

        const pfpContainer = createProfilePictureElement(data.pfpUrl, data.username, 'w-8 h-8 sm:w-10 sm:h-10');
        const messageContent = document.createElement('div');
        messageContent.className = `flex flex-col ${isCurrentUser ? 'items-end' : 'items-start'} flex-1 min-w-0`;

        const headerDiv = document.createElement('div');
        headerDiv.className = `flex items-center gap-1.5 sm:gap-2 mb-1 px-1 ${isCurrentUser ? 'flex-row-reverse' : ''}`;

        const usernameSpan = document.createElement('span');
        usernameSpan.className = 'text-xs sm:text-sm font-semibold text-gray-200 truncate';
        usernameSpan.textContent = data.username || 'User';

        const timestampSpan = document.createElement('span');
        timestampSpan.className = 'text-[10px] sm:text-xs text-gray-500 whitespace-nowrap';
        timestampSpan.textContent = data.timestamp ? formatTimestamp(data.timestamp) : 'Just now';

        headerDiv.appendChild(usernameSpan);
        headerDiv.appendChild(timestampSpan);
        messageContent.appendChild(headerDiv);

        const bubbleWrapper = document.createElement('div');
        bubbleWrapper.className = `flex items-center gap-1`;

        const messageBubble = document.createElement('div');
        messageBubble.className = `px-3 sm:px-4 py-2 rounded-2xl ${isCurrentUser ? 'bg-[#33bfcc] text-white' : 'bg-[#2a2d33] text-gray-200'} break-words text-sm sm:text-base max-w-full`;

        if (data.replyTo) {
          const quotedDiv = document.createElement('div');
          quotedDiv.className = `quoted-message ${isCurrentUser ? '' : 'quoted-message-light'}`;

          const quotedUsername = document.createElement('div');
          quotedUsername.className = `text-xs font-semibold mb-1 ${isCurrentUser ? 'text-white/80' : 'text-[#33bfcc]'}`;
          quotedUsername.textContent = data.replyTo.username;

          const quotedText = document.createElement('div');
          quotedText.className = `text-xs ${isCurrentUser ? 'text-white/70' : 'text-gray-400'} break-words`;
          quotedText.textContent = data.replyTo.message;

          quotedDiv.appendChild(quotedUsername);
          quotedDiv.appendChild(quotedText);
          messageBubble.appendChild(quotedDiv);
        }

        // Add message text if present
        if (data.message) {
          const messageText = document.createElement('div');
          messageText.className = 'text-sm sm:text-base break-words';
          messageText.innerHTML = escapeHtml(data.message);
          messageBubble.appendChild(messageText);
        }

        // Add attachment if present
        if (data.attachment) {
          console.log(`📎 Rendering attachment in message: ${data.attachment.name} (${data.attachment.type})`);

          const attachmentDiv = document.createElement('div');
          attachmentDiv.className = 'message-attachment';

          const fileCategory = getFileTypeCategory(data.attachment.type);
          console.log(`   Category: ${fileCategory}`);

          if (fileCategory === 'image') {
            // Show image preview (will be loaded from IndexedDB)
            console.log(`   Creating image preview element`);
            attachmentDiv.innerHTML = `
      <img src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='300' height='200'%3E%3Crect width='300' height='200' fill='%23161b22'/%3E%3Ctext x='50%25' y='50%25' dominant-baseline='middle' text-anchor='middle' fill='%238b949e' font-family='sans-serif' font-size='14'%3ELoading image...%3C/text%3E%3C/svg%3E" 
           alt="${escapeHtml(data.attachment.name)}" 
           data-file-id="${data.attachment.fileId}"
           loading="lazy"
           style="max-width: 100%; border-radius: 8px;">
    `;

            // Load actual image from IndexedDB
            const imgElement = attachmentDiv.querySelector('img');
            loadImageAttachment(
              data.attachment.fileId,
              imgElement,
              data.attachment.url || data.attachment.publicUrl || data.attachment.apiUrl || null,
              data.attachment.name
            );

          } else if (fileCategory === 'video') {
            const persistentUrl = data.attachment.url || data.attachment.publicUrl || data.attachment.apiUrl || null;
            if (persistentUrl) {
              console.log(`   Creating streaming video preview element`);

              const wrapper = document.createElement('div');
              wrapper.className = 'message-attachment-file video';
              wrapper.dataset.fileId = data.attachment.fileId;
              wrapper.style.display = 'flex';
              wrapper.style.flexDirection = 'column';
              wrapper.style.gap = '8px';
              wrapper.style.cursor = 'default';

              const videoEl = document.createElement('video');
              videoEl.src = persistentUrl;
              videoEl.controls = true;
              videoEl.preload = 'metadata';
              videoEl.playsInline = true;
              videoEl.setAttribute('playsinline', '');
              videoEl.style.width = '100%';
              videoEl.style.maxHeight = '260px';
              videoEl.style.borderRadius = '8px';
              videoEl.style.background = '#000';

              const info = document.createElement('div');
              info.className = 'message-attachment-info';
              info.innerHTML = `
                <div class="message-attachment-name">${escapeHtml(data.attachment.name)}</div>
                <div class="message-attachment-size">${formatFileSize(data.attachment.size)}</div>
                <div style="font-size: 0.7rem; opacity: 0.6; margin-top: 2px;">Streaming enabled (progressive loading)</div>
              `;

              const openBtn = document.createElement('button');
              openBtn.type = 'button';
              openBtn.className = 'text-xs text-[#a855f7] font-semibold text-left';
              openBtn.textContent = 'Open in full player';
              openBtn.addEventListener('click', () => {
                downloadAttachment(
                  data.attachment.fileId,
                  data.attachment.name,
                  persistentUrl,
                  data.attachment.type
                );
              }, { passive: true });

              videoEl.onerror = () => {
                videoEl.remove();
                openBtn.click();
              };

              wrapper.appendChild(videoEl);
              wrapper.appendChild(info);
              wrapper.appendChild(openBtn);
              attachmentDiv.appendChild(wrapper);
            } else {
              // Legacy fallback when URL is unavailable
              console.log(`   Creating video file fallback element`);
              attachmentDiv.innerHTML = `
      <div class="message-attachment-file video" 
           data-file-id="${data.attachment.fileId}"
           style="cursor: pointer;">
        <span class="material-symbols-outlined" style="font-size: 28px; color: #a855f7;">play_circle</span>
        <div class="message-attachment-info">
          <div class="message-attachment-name">${escapeHtml(data.attachment.name)}</div>
          <div class="message-attachment-size">${formatFileSize(data.attachment.size)}</div>
          <div style="font-size: 0.7rem; opacity: 0.6; margin-top: 2px;">Click to open in video player</div>
        </div>
      </div>
    `;

              const fileElement = attachmentDiv.querySelector('.message-attachment-file');
              if (fileElement) {
                fileElement.addEventListener('click', () => {
                  downloadAttachment(data.attachment.fileId, data.attachment.name, null, data.attachment.type);
                }, { passive: true });
              }
            }

          } else if (fileCategory === 'pdf') {
            // Show PDF icon with viewer option
            console.log(`   Creating PDF file element`);
            attachmentDiv.innerHTML = `
      <div class="message-attachment-file pdf" 
           data-file-id="${data.attachment.fileId}"
           style="cursor: pointer;">
        <span class="material-symbols-outlined" style="font-size: 28px; color: #ef4444;">picture_as_pdf</span>
        <div class="message-attachment-info">
          <div class="message-attachment-name">${escapeHtml(data.attachment.name)}</div>
          <div class="message-attachment-size">${formatFileSize(data.attachment.size)}</div>
          <div style="font-size: 0.7rem; opacity: 0.6; margin-top: 2px;">Click to open in PDF reader</div>
        </div>
        <span class="material-symbols-outlined" style="font-size: 18px; opacity: 0.5;">download</span>
      </div>
    `;

            // Add click handler for download
            const fileElement = attachmentDiv.querySelector('.message-attachment-file');
            if (fileElement) {
              fileElement.addEventListener('click', () => {
                console.log(`📄 Opening PDF: ${data.attachment.name}`);
                downloadAttachment(
                  data.attachment.fileId,
                  data.attachment.name,
                  data.attachment.url || data.attachment.publicUrl || data.attachment.apiUrl || null,
                  data.attachment.type
                );
              }, { passive: true });
            }


          } else {
            // Generic document
            console.log(`   Creating generic document element`);
            attachmentDiv.innerHTML = `
      <div class="message-attachment-file document" 
           data-file-id="${data.attachment.fileId}"
           style="cursor: pointer;">
        <span class="material-symbols-outlined" style="font-size: 28px;">description</span>
        <div class="message-attachment-info">
          <div class="message-attachment-name">${escapeHtml(data.attachment.name)}</div>
          <div class="message-attachment-size">${formatFileSize(data.attachment.size)}</div>
          <div style="font-size: 0.7rem; opacity: 0.6; margin-top: 2px;">Click to download</div>
        </div>
        <span class="material-symbols-outlined" style="font-size: 18px; opacity: 0.5;">download</span>
      </div>
    `;

            // Add click handler for download
            const fileElement = attachmentDiv.querySelector('.message-attachment-file');
            if (fileElement) {
              fileElement.addEventListener('click', () => {
                console.log(`📁 Downloading document: ${data.attachment.name}`);
                downloadAttachment(
                  data.attachment.fileId,
                  data.attachment.name,
                  data.attachment.url || data.attachment.publicUrl || data.attachment.apiUrl || null,
                  data.attachment.type
                );
              }, { passive: true });
            }
          }


          messageBubble.appendChild(attachmentDiv);
          console.log(`✅ Attachment element added to message bubble`);
        }

        const replyBtn = document.createElement('button');
        replyBtn.className = 'reply-btn p-1 sm:p-1.5 hover:bg-gray-800 rounded-full transition-colors flex-shrink-0';
        replyBtn.title = 'Reply';
        replyBtn.innerHTML = '<span class="material-symbols-outlined text-gray-500 text-base sm:text-lg">reply</span>';
        replyBtn.addEventListener('click', () => {
          showReplyPreview({
            messageId: messageItem.dataset.messageId,
            username: data.username,
            message: data.message || `[${data.attachment?.name}]`,
            userId: data.userId
          });
        }, { passive: true });

        if (isCurrentUser) {
          bubbleWrapper.appendChild(replyBtn);
          bubbleWrapper.appendChild(messageBubble);
        } else {
          bubbleWrapper.appendChild(messageBubble);
          bubbleWrapper.appendChild(replyBtn);
        }

        messageContent.appendChild(bubbleWrapper);
        messageItem.appendChild(pfpContainer);
        messageItem.appendChild(messageContent);

        return messageItem;
      }


      /**
       * Load image attachment from IndexedDB and display
       * Now includes retry mechanism and peer request fallback
       */
      async function loadImageAttachment(fileId, imgElement, remoteUrl = null, remoteName = null) {
        if (!imgElement) {
          console.error('❌ Image element not provided for fileId:', fileId);
          return;
        }

        console.log(`🖼️ Loading image attachment: ${fileId}`);

        if (remoteUrl) {
          imgElement.src = remoteUrl;
          imgElement.alt = remoteName || 'Image attachment';
          imgElement.style.cursor = 'pointer';
          imgElement.referrerPolicy = 'no-referrer';
          imgElement.loading = 'lazy';

          const openRemote = async (e) => {
            e.preventDefault();
            e.stopPropagation();
            await openImageViewer(fileId, remoteName || 'Image', remoteUrl);
          };

          if (imgElement._clickHandler) {
            imgElement.removeEventListener('click', imgElement._clickHandler);
          }
          imgElement._clickHandler = openRemote;
          imgElement.addEventListener('click', openRemote, { passive: false });
          return;
        }

        try {
          let fileData = await getFileFromIndexedDB(fileId);

          if (!fileData || !fileData.blob) {
            console.log(`⏳ File not found, waiting 2s for peer transfer...`);

            await new Promise(resolve => setTimeout(resolve, 2000));

            fileData = await getFileFromIndexedDB(fileId);

            if (!fileData || !fileData.blob) {
              console.error(`❌ File ${fileId} still not found after retry`);

              imgElement.alt = 'Loading...';
              imgElement.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="300" height="200"%3E%3Crect width="300" height="200" fill="%23161b22"/%3E%3Ctext x="50%25" y="50%25" dominant-baseline="middle" text-anchor="middle" fill="%238b949e" font-family="sans-serif" font-size="14"%3ELoading image...%3C/text%3E%3C/svg%3E';

              return;
            }
          }

          console.log(`✅ File data retrieved: ${fileData.name} (${(fileData.size / 1024).toFixed(2)} KB)`);

          const blobUrl = URL.createObjectURL(fileData.blob);
          console.log(`🔗 Created blob URL for display`);

          imgElement.src = blobUrl;
          imgElement.alt = fileData.name;
          imgElement.dataset.blobUrl = blobUrl;
          imgElement.dataset.fileName = fileData.name; // Store filename for viewer
          imgElement.style.cursor = 'pointer';

          imgElement.onload = () => {
            console.log(`✅ Image loaded successfully: ${fileData.name}`);
          };

          imgElement.onerror = () => {
            console.error(`❌ Failed to load image: ${fileData.name}`);
            URL.revokeObjectURL(blobUrl);
            imgElement.alt = 'Failed to load image';
          };

          // CRITICAL FIX: Add click handler directly to existing element
          // Remove any existing click handlers first
          const newClickHandler = async (e) => {
            e.preventDefault();
            e.stopPropagation();
            console.log(`🔍 Opening image in viewer: ${fileData.name}`);
            await openImageViewer(fileId, fileData.name, remoteUrl);
          };

          // Store handler reference to prevent duplicates
          if (imgElement._clickHandler) {
            imgElement.removeEventListener('click', imgElement._clickHandler);
          }

          imgElement._clickHandler = newClickHandler;
          imgElement.addEventListener('click', newClickHandler, { passive: false });

          console.log(`✅ Click handler attached to image`);

        } catch (error) {
          console.error('❌ Error loading image attachment:', error);
          imgElement.alt = 'Failed to load image';
          imgElement.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="300" height="200"%3E%3Crect width="300" height="200" fill="%23161b22"/%3E%3Ctext x="50%25" y="50%25" dominant-baseline="middle" text-anchor="middle" fill="%23ef4444" font-family="sans-serif" font-size="14"%3EError loading image%3C/text%3E%3C/svg%3E';
        }
      }

      /**
       * Download attachment file
       */
      async function downloadAttachment(fileId, fileName, attachmentUrl = null, mimeType = '') {
        console.log('📥 ========================================');
        console.log('📥 DOWNLOAD ATTACHMENT REQUESTED');
        console.log('📥 ========================================');
        console.log(`   FileID: ${fileId}`);
        console.log(`   Filename: ${fileName}`);

        if (attachmentUrl) {
          try {
            const isInlineFriendly = mimeType.startsWith('video/')
              || mimeType.startsWith('image/')
              || mimeType === 'application/pdf';

            const anchor = document.createElement('a');
            anchor.href = attachmentUrl;
            anchor.target = '_blank';
            anchor.rel = 'noopener noreferrer';
            if (!isInlineFriendly) {
              anchor.download = fileName || 'attachment';
            }
            anchor.style.display = 'none';
            document.body.appendChild(anchor);
            anchor.click();
            document.body.removeChild(anchor);

            console.log(`✅ Opened attachment via persistent URL`);
            console.log('📥 ========================================\n');
            return;
          } catch (error) {
            console.error('❌ Failed opening persistent URL, falling back to IndexedDB:', error);
          }
        }

        try {
          const fileData = await getFileFromIndexedDB(fileId);

          if (!fileData || !fileData.blob) {
            console.error('❌ File not found in IndexedDB');
            toast('File not found', 'error');
            console.log('📥 ========================================\n');
            return;
          }

          console.log(`✅ File retrieved from IndexedDB`);
          console.log(`   Size: ${(fileData.size / 1024).toFixed(2)} KB`);
          console.log(`   Type: ${fileData.type}`);

          // Create download link
          const blobUrl = URL.createObjectURL(fileData.blob);
          const a = document.createElement('a');
          a.href = blobUrl;
          a.download = fileName;
          a.style.display = 'none';
          document.body.appendChild(a);

          console.log('🔽 Triggering download...');
          a.click();

          // Cleanup
          document.body.removeChild(a);
          setTimeout(() => {
            URL.revokeObjectURL(blobUrl);
            console.log('🧹 Download blob URL revoked');
          }, 1000);

          toast(`Downloading ${fileName}`, 'success');
          console.log(`✅ Download initiated successfully`);
          console.log('📥 ========================================\n');

        } catch (error) {
          console.error('❌ Download error:', error);
          console.error('   Stack:', error.stack);
          toast('Download failed', 'error');
          console.log('📥 ========================================\n');
        }
      }

      async function reportChatPresenceContext(reason = 'chat_lifecycle', options = {}) {
        const roomId = options.roomId || roomData?.roomId || currentRoomId || null;
        const payload = {
          location: 'chat',
          path: window.location.pathname,
          roomId: roomId || null,
          source: options.source || 'chat_html',
          reason
        };

        try {
          if (MoodApp?.Presence?.reportContext) {
            MoodApp.Presence.reportContext(reason, {
              ...payload,
              allowRedirect: options.allowRedirect !== false,
              keepalive: options.keepalive !== false
            }).catch(() => { });
          }
        } catch { }

        if (socketInstance?.connected) {
          safeSocketEmitWithAck('page_context', payload, (response) => {
            if (!response) return;
            if (response.redirectTo && response.redirectTo !== window.location.pathname) {
              const allowRedirect = options.allowRedirect !== false;
              if (isSocialClubMode || !allowRedirect) return;
              cleanupParentCallPage('/mood.html');
              window.location.href = '/mood.html';
            }
          });
        }
      }

      async function validateChatPresenceState(trigger = 'manual_validation') {
        if (!socketInstance?.connected || !roomData?.roomId) return;
        if (!hasJoinedRoom) return;

        safeSocketEmitWithAck('validate_presence_state', {
          location: 'chat',
          roomId: roomData.roomId,
          path: window.location.pathname,
          trigger
        }, async (response) => {
          if (!response) return;
          if (response.valid) {
            presenceInvalidCount = 0;
            lastPresenceInvalidAt = 0;
            if ((trigger.includes('pageshow') || trigger.includes('visibility')) && socketInstance?.connected) {
              safeSocketEmit('request_room_sync', { roomId: response.roomId || roomData.roomId });
            }
            return;
          }

          if (response.reason === 'NOT_IN_ROOM') {
            if (socketInstance?.connected && roomData?.roomId) {
              pendingInitialRoomSync = true;
              safeSocketEmit('join_room', { roomId: roomData.roomId });
            }
            return;
          }

          // Recovery-first strategy: transient socket races and server lag should not force-leave users.
          // Only redirect after repeated failures, and never during call flow.
          const now = Date.now();
          if (!lastPresenceInvalidAt || (now - lastPresenceInvalidAt) > 15000) {
            presenceInvalidCount = 0;
          }
          lastPresenceInvalidAt = now;
          presenceInvalidCount++;

          let hasActiveCallCached = false;
          try {
            const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
            if (activeCallStr) {
              try {
                const parsed = JSON.parse(activeCallStr);
                hasActiveCallCached = !!(parsed?.callId && parsed?.roomId);
              } catch { }
            }
          } catch { }
          const backgroundCallMode = sessionStorage.getItem('backgroundCallMode') === 'true';
          const shouldSuppressRedirect = isSocialClubMode || navigatingToCall || isInBackgroundCall || backgroundCallMode || hasActiveCallCached;

          console.warn('⚠️ Presence validation failed:', response.reason, 'count=', presenceInvalidCount, 'suppressRedirect=', shouldSuppressRedirect);

          // Always attempt a re-join + sync first.
          try {
            if (socketInstance?.connected && roomData?.roomId) {
              pendingInitialRoomSync = true;
              safeSocketEmit('join_room', { roomId: roomData.roomId });
              safeSocketEmit('request_room_sync', { roomId: roomData.roomId });
            }
          } catch { }

          if (shouldSuppressRedirect) return;

          if (presenceInvalidCount < 2) {
            return;
          }

          console.warn('⚠️ Presence validation repeatedly failed. Exiting to mood.');
          try {
            await exitRoomSequence('/mood.html', { reason: 'presence_invalid' });
          } catch {
            try {
              await clearChatState(true);
            } catch { }
            cleanupParentCallPage('/mood.html');
          }
        });
      }

      // ============================================
      // CONFIRMATION DIALOG
      // ============================================

      function showConfirmationDialog() {
        if (isConfirmationDialogOpen) {
          return Promise.resolve(false);
        }

        return new Promise((resolve) => {
          const overlay = document.getElementById('confirmationOverlay');
          if (!overlay) return resolve(false);
          isConfirmationDialogOpen = true;
          confirmationDialogResolve = resolve;

          const closeDialog = (decision) => {
            isConfirmationDialogOpen = false;
            const resolver = confirmationDialogResolve;
            confirmationDialogResolve = null;
            overlay.style.display = 'none';
            overlay.innerHTML = '';
            resolver?.(decision);
          };

          const dialogHTML = `
        <div class="confirmation-overlay" style="position: fixed; inset: 0; background: rgba(0, 0, 0, 0.85); backdrop-filter: blur(8px); display: flex; align-items: center; justify-content: center; z-index: 9999;">
          <div class="confirmation-dialog" style="background: linear-gradient(135deg, #0d1117 0%, #161b22 100%); border: 1px solid #30363d; border-radius: 16px; padding: 2rem; max-width: 400px; width: 90%; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5), 0 0 1px rgba(6, 182, 212, 0.3);">
            <div style="text-align: center; margin-bottom: 1.5rem;">
              <div style="width: 64px; height: 64px; margin: 0 auto 1rem; background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, rgba(220, 38, 38, 0.15) 100%); border-radius: 50%; display: flex; align-items: center; justify-content: center; border: 1px solid rgba(239, 68, 68, 0.3);">
                <span class="material-symbols-outlined" style="font-size: 32px; color: #ef4444;">warning</span>
              </div>
              <h3 style="font-size: 1.35rem; font-weight: 700; margin-bottom: 0.75rem; color: #f0f6fc;">Leave Chat Room?</h3>
              <p style="color: #8b949e; font-size: 0.95rem; line-height: 1.6;">
                You're currently in an active chat room. If you leave, you'll be disconnected from the conversation.
              </p>
            </div>
            <div style="display: flex; gap: 0.75rem; justify-content: center;">
              <button class="btn-stay" id="btnStay" style="flex: 1; padding: 0.875rem 1.5rem; border-radius: 10px; font-weight: 600; font-size: 0.95rem; cursor: pointer; transition: all 0.2s; background-color: #1c2128; color: #c9d1d9; border: 1px solid #30363d;">Stay Here</button>
              <button class="btn-leave" id="btnLeave" style="flex: 1; padding: 0.875rem 1.5rem; border-radius: 10px; font-weight: 600; font-size: 0.95rem; cursor: pointer; transition: all 0.2s; background: linear-gradient(135deg, #dc2626 0%, #ef4444 100%); color: white; border: 1px solid #f87171; box-shadow: 0 0 20px rgba(239, 68, 68, 0.3);">Leave</button>
            </div>
          </div>
        </div>
      `;

          overlay.innerHTML = dialogHTML;
          overlay.style.display = 'flex';

          const btnStay = document.getElementById('btnStay');
          const btnLeave = document.getElementById('btnLeave');

          // Add hover effects for Stay button
          if (btnStay) {
            btnStay.addEventListener('mouseenter', () => {
              btnStay.style.backgroundColor = '#21262d';
              btnStay.style.borderColor = '#444c56';
            });
            btnStay.addEventListener('mouseleave', () => {
              btnStay.style.backgroundColor = '#1c2128';
              btnStay.style.borderColor = '#30363d';
            });
            btnStay.addEventListener('click', () => {
              closeDialog(false);
            }, { once: true });
          }

          // Add hover effects and complete click handler for Leave button
          if (btnLeave) {
            btnLeave.addEventListener('mouseenter', () => {
              btnLeave.style.background = 'linear-gradient(135deg, #ef4444 0%, #f87171 100%)';
              btnLeave.style.boxShadow = '0 0 25px rgba(239, 68, 68, 0.5)';
            });
            btnLeave.addEventListener('mouseleave', () => {
              btnLeave.style.background = 'linear-gradient(135deg, #dc2626 0%, #ef4444 100%)';
              btnLeave.style.boxShadow = '0 0 20px rgba(239, 68, 68, 0.3)';
            });
            btnLeave.addEventListener('click', async () => {
              console.log('✅ User confirmed leaving via dialog - starting cleanup');
              closeDialog(true);

              // CRITICAL: Clean up background call FIRST
              const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);

              if (activeCallStr) {
                console.log('📞 Active call detected - cleaning up');

                try {
                  const callData = JSON.parse(activeCallStr);

                  // 1. Emit leave_call to server
                  if (socketInstance && socketInstance.connected) {
                    console.log('📤 Emitting leave_call to server...');
                    socketInstance.emit('leave_call', { callId: callData.callId });

                    // Wait for server to process
                    await new Promise(resolve => setTimeout(resolve, 500));
                    console.log('✅ Leave_call emitted and processed');
                  }

                  // 2. Clear all call-related storage
                  localStorage.removeItem(ACTIVE_CALL_KEY);
                  sessionStorage.removeItem('hasBackgroundCall');
                  sessionStorage.removeItem('returningFromCall');
                  sessionStorage.removeItem('backgroundCallMode');
                  sessionStorage.removeItem('returningToBackgroundCall');

                  console.log('✅ Background call destroyed');

                } catch (e) {
                  console.error('❌ Failed during call cleanup:', e);
                }
              }

              exitRoomSequence().catch(console.error);
            }, { once: true });
          }
        });
      }

      // ============================================
      // BACK BUTTON HANDLER
      // ============================================
      async function exitRoomSequence(targetUrl = null, options = {}) {
        if (leaveSequenceInProgress) {
          console.log('⚠️ Leave sequence already in progress, ignoring duplicate trigger');
          return;
        }
        leaveSequenceInProgress = true;
        const reason = options?.reason || 'manual';
        try {
          sessionStorage.setItem('vibe_intentional_chat_leave_until', String(Date.now() + 15000));
        } catch { }
        console.log('🚪 ========================================');
        console.log('🚪 STARTING EXIT ROOM SEQUENCE (Server-Authoritative)');
        console.log('🚪 ========================================');
        try {
          // 1. Stop all monitoring and heartbeat
          stopCachedCallMonitoring();
          stopHeartbeat(); // ✅ Stop heartbeat IMMEDIATELY
          if (timerInterval) clearInterval(timerInterval);

          // 2. Logic to handle active call if user is leaving.
          // Only tear down the call on explicit/manual exits; avoid destructive cleanup for automated recovery flows.
          const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
          const shouldTearDownCall = (reason === 'manual' || reason === 'location_change' || reason === 'api_beacon');
          if (activeCallStr && shouldTearDownCall) {
            console.log('📞 Active call detected - ensuring server cleanup...');
            try {
              const callData = JSON.parse(activeCallStr);
              if (socketInstance && socketInstance.connected) {
                socketInstance.emit('leave_call', { callId: callData.callId });
              }
            } catch (e) { }

            localStorage.removeItem(ACTIVE_CALL_KEY);
            sessionStorage.removeItem('hasBackgroundCall');
          }

          // 3. Trigger server-side leave sequence (socket first, API fallback).
          if (socketInstance && socketInstance.connected) {
            console.log('📤 Emitting leave_room via socket...');
            // Wrap in promise to allow sequence to continue after acknowledgment
            const emitLeaveOnce = () => new Promise((resolve) => {
              const timeout = setTimeout(() => {
                console.warn('⚠️ leave_room acknowledgment timed out');
                resolve({ ok: false });
              }, 8000);
              socketInstance.emit('leave_room', { roomId: roomData?.roomId || currentRoomId || null }, (response) => {
                clearTimeout(timeout);
                console.log('✅ leave_room acknowledgment received:', response);
                resolve({ ok: true, response });
              });
            });

            const r1 = await emitLeaveOnce();
            if (!r1.ok) {
              console.warn('⚠️ Retrying leave_room once...');
              await emitLeaveOnce();
            }
          } else {
            const token = sessionStorage.getItem('vibe_auth_token');
            if (token && roomData?.roomId) {
              console.log('📡 Socket offline - using Beacon/Fetch API leave fallback...');
              fetch('/api/leave-room', {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json',
                  'Authorization': `Bearer ${token}`
                },
                body: JSON.stringify({ roomId: roomData.roomId }),
                keepalive: true
              }).catch(e => console.error('Exit sequence beacon failed:', e));
            }
          }

          // 4. Clear chat state
          await clearChatState(true);
          try {
            localStorage.removeItem('currentRoom');
          } catch { }
          currentRoomId = null;

          // 5. Navigate
          console.log('✅ Exit sequence complete');
          cleanupParentCallPage(targetUrl);
          console.log('🚪 ========================================\n');
        } finally {
          leaveSequenceInProgress = false;
        }
      }

      function setupBackButtonHandler() {
        const TRAP_STATE = { page: 'chat', trap: true };

        const ensureTrapState = () => {
          // Only push if we're not already in the trap state
          if (!history.state || !history.state.trap) {
            console.log('🔒 Injecting back button trap state');
            history.pushState(TRAP_STATE, '', window.location.href);
          }
        };

        // Initialize on load - push twice to create a deeper stack
        // This makes it harder for the browser to skip the trap
        ensureTrapState();
        setTimeout(ensureTrapState, 100);

        // Re-initialize on page show (handles BFCache and Refresh)
        window.addEventListener('pageshow', (event) => {
          // event.persisted check is optional but good for debugging BFCache
          console.log('📱 Page show event (persisted: ' + event.persisted + ') - verifying trap state');
          setTimeout(ensureTrapState, 0); // specialized timeout ensures it runs after browser restoration
          reconnectSocketIfNeeded(event.persisted ? 'pageshow_bfcache' : 'pageshow');
          reportChatPresenceContext('pageshow', {
            source: event.persisted ? 'chat_pageshow_bfcache' : 'chat_pageshow',
            allowRedirect: !isSocialClubMode
          }).catch(() => { });
          validateChatPresenceState(event.persisted ? 'pageshow_bfcache' : 'pageshow').catch(() => { });
        });

        // Handle the Back Button (popstate)
        window.addEventListener('popstate', async (event) => {
          if (backButtonHandled) return;

          console.log('⬅️ Back button pressed - Trapped!');

          // Immediately push the state back to keep the user here while the modal shows
          // This effectively "cancels" the back navigation from the browser's perspective
          history.pushState(TRAP_STATE, '', window.location.href);

          if (isConfirmationDialogOpen) {
            console.log('⚠️ Confirmation already open - second back triggers immediate leave');
            backButtonHandled = true;
            const leaveButton = document.getElementById('btnLeave');
            if (leaveButton) {
              leaveButton.click();
            } else {
              try {
                confirmationDialogResolve?.(true);
              } catch { }
              const overlay = document.getElementById('confirmationOverlay');
              if (overlay) {
                overlay.style.display = 'none';
                overlay.innerHTML = '';
              }
              isConfirmationDialogOpen = false;
              confirmationDialogResolve = null;
              await exitRoomSequence('/mood.html');
            }
            return;
          }

          // We use a small delay to ensure the UI has processed the pushState
          // before showing the heavy modal
          setTimeout(async () => {
            const shouldLeave = await showConfirmationDialog();

            if (shouldLeave) {
              console.log('✅ User confirmed leaving');
              backButtonHandled = true;

              // ✅ Explicitly redirect with reason 'manual'
              await exitRoomSequence('/mood.html');
            } else {
              console.log('❌ User staying in chat');
              // State was already pushed back, so the UI stays on chat.html
            }
          }, 10);
        });

        // Add pagehide listener for mobile browsers (iOS Safari reliability)
        window.addEventListener('pagehide', (event) => {
          if (!navigatingToCall && !backButtonHandled && !isInBackgroundCall) {
            // Treat sudden pagehide as a "might be leaving" event
            // But don't force a full leave sequence here to allow for task switching
            console.log('📱 Page hidden - backgrounding state preserved');
            reportChatPresenceContext('pagehide', {
              keepalive: true,
              allowRedirect: false,
              source: 'chat_pagehide'
            }).catch(() => { });
          }
        });
      }

      const updateTimer = () => {
        return;
      };


      // ============================================
      // SOCKET EVENT HANDLERS
      // ============================================

      function setupSocketHandlers() {
        if (!socketInstance) return;


        // ============================================
        // CHUNKED FILE TRANSMISSION HANDLERS
        // ============================================

        socketInstance.on('file_chunk', async (data) => {
          try {
            await fileReceiver.handleChunk(data);
          } catch (error) {
            console.error('❌ Error handling chunk:', error);
          }
        });

        socketInstance.on('file_transmission_complete', ({ fileId, fileName }) => {
          console.log(`✅ File transmission complete: ${fileName} (${fileId})`);
          toast(`Received: ${fileName}`, 'success');
        });

        socketInstance.on('file_transmission_failed', ({ fileId, fileName, reason }) => {
          console.error(`❌ File transmission failed: ${fileName}`);
          console.error(`   Reason: ${reason}`);

          fileReceiver.cleanup(fileId);
          toast(`Failed to receive: ${fileName}`, 'error');
        });


        socketInstance.on('cached_call_valid', (data) => {
          console.log('✅ Cached call validated by server:', data.callId);
          console.log(`   Showing modal for call from ${data.callerUsername}`);

          showCachedCallModal(data);
        });

        // Typing indicator events
        socketInstance.on('user_typing', ({ userId, username }) => {
          typingUsers.set(userId, username);
          updateTypingUI();
        });

        socketInstance.on('user_stop_typing', ({ userId }) => {
          typingUsers.delete(userId);
          updateTypingUI();
        });

        // Add event listener to message input
        const messageInput = document.getElementById('messageInput');
        if (messageInput) {
          messageInput.addEventListener('input', handleTyping);
        }

        socketInstance.on('cached_call_invalid', (data) => {
          console.log('❌ Cached call invalid:', data.callId);
          localStorage.removeItem(CACHED_CALL_KEY);
          console.log('🗑️ Cleared invalid cached call');
        });

        socketInstance.on('connect', () => {
          console.log('🔌 ========================================');
          console.log('🔌 SOCKET CONNECTED');
          console.log('🔌 ========================================');
          console.log(`   Socket ID: ${socketInstance.id}`);
          console.log(`   User: ${currentUser?.username || 'unknown'}`);
          socketReconnectFailures = 0;

          // CRITICAL: Reset auth state on new connection
          setAuthenticationState(false);

          firebase.auth().currentUser?.getIdToken().then(idToken => {
            console.log('📤 Sending authentication to server...');
            const sessionId = getScopedSocketSessionId('chat');
            const tabId = window.tabManager?.tabId || null;
            socketInstance.emit('authenticate', {
              token: idToken,
              userId: currentUser.userId,
              sessionId,
              tabId
            });
            loadSocketEmitQueue();
            flushSocketEmitQueue();
            reportChatPresenceContext('socket_connect', {
              allowRedirect: false,
              keepalive: true,
              source: 'chat_socket_connect'
            }).catch(() => { });
          }).catch(err => {
            console.error('❌ Failed to get token:', err);
            toast('Authentication failed. Please refresh.', 'error');
          });

          console.log('🔌 ========================================\n');
        });

        socketInstance.on('connect_error', (error) => {
          console.error('❌ Socket connection error:', error.message);
          setAuthenticationState(false);
          toast('Connection error. Retrying...', 'warning');
          socketReconnectFailures += 1;
          if (socketReconnectFailures >= 6) {
            triggerSocketRecoveryReload('chat_connect_error_threshold');
          }
        });

        socketInstance.on('disconnect', (reason) => {
          console.log('🔌 Socket disconnected:', reason);
          setAuthenticationState(false);

          if (reason === 'io server disconnect') {
            // Server disconnected us, need to reconnect manually
            socketInstance.connect();
          }
        });

        socketInstance.on('session_replaced', (data) => {
          console.warn('⚠️ Session replaced by newer socket:', data);
          setAuthenticationState(false);
          try { stopHeartbeat(); } catch { }

          // In Social Club (and during call flows), do NOT redirect away.
          // Redirecting triggers unload -> leave beacon -> room destruction.
          // Instead: rotate socket session id and reconnect.
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
            const shouldSelfHeal = isSocialClubMode || navigatingToCall || isInBackgroundCall || backgroundCallMode || hasActiveCallCached;
            if (shouldSelfHeal) {
              console.warn('🛡️ Suppressing redirect on session_replaced; rotating session and reconnecting');
              toast('Connection refreshed', 'warning');

              // Rotate the *base* session id so subsequent getScopedSocketSessionId('chat') becomes unique.
              try {
                const newBase = `sess_${Date.now()}_${Math.random().toString(36).slice(2, 12)}`;
                sessionStorage.setItem('vibe_socket_session_id', newBase);
              } catch { }

              try { socketInstance.disconnect(); } catch { }
              try { socketInstance.connect(); } catch { }
              return;
            }
          } catch { }

          // Non-Social-Club fallback behavior
          try { socketInstance.disconnect(); } catch { }
          toast('This session was opened in another tab/device. Redirecting...', 'warning');
          setTimeout(() => {
            cleanupParentCallPage('/mood.html');
            window.location.href = '/mood.html';
          }, 1200);
        });

        socketInstance.on('server_ping', (payload = {}) => {
          if (!socketInstance?.connected) return;
          socketInstance.emit('client_pong', {
            roomId: roomData?.roomId || currentRoomId || null,
            location: 'chat',
            path: window.location.pathname,
            serverPingTs: payload.ts || null
          });
        });

        if (socketInstance.io) {
          socketInstance.io.on('reconnect_attempt', (attempt) => {
            console.warn(`🔄 [Socket] Reconnect attempt ${attempt}`);
          });
          socketInstance.io.on('reconnect_error', (error) => {
            console.error(`❌ [Socket] Reconnect error: ${error?.message || error}`);
          });
          socketInstance.io.on('reconnect_failed', () => {
            triggerSocketRecoveryReload('chat_reconnect_failed');
          });
        }

        socketInstance.on('authenticated', async () => {
          console.log('✅ ========================================');
          console.log('✅ SOCKET AUTHENTICATED');
          console.log('✅ ========================================');
          console.log(`   Socket ID: ${socketInstance.id}`);
          console.log(`   User: ${currentUser?.username} (${currentUser?.userId})`);

          // CRITICAL: Set authentication state FIRST
          setAuthenticationState(true);

          // ✅ START HEARTBEAT after authentication
          startHeartbeat();

          await reportChatPresenceContext('socket_authenticated', {
            allowRedirect: !isSocialClubMode,
            keepalive: true,
            source: 'chat_authenticated'
          });

          // ✅ Cache token for Beacon API (beforeunload)
          if (MoodApp.Auth) {
            MoodApp.Auth.getToken().then(token => {
              sessionStorage.setItem('vibe_auth_token', token);
            }).catch(() => { });
          }

          startCachedCallMonitoring();
          if (roomData && roomData.roomId) {
            pendingInitialRoomSync = true;
            safeSocketEmit('join_room', { roomId: roomData.roomId });
          } else if (isSocialClubMode) {
            safeSocketEmit('join_social_club', {});
          } else {
            if (!selectedMood) {
              toast('Please select a mood first', 'warning');
              setTimeout(() => window.location.href = '/mood.html', 800);
              return;
            }
            showChatLoadingOverlay('Creating your room…', 'You can invite someone anytime.');
            safeSocketEmit('join_matchmaking', { mood: selectedMood });
          }

          console.log('✅ ========================================\n');
        });

        socketInstance.on('match_found', async (data) => {
          try {
            if (!data || !data.roomId) return;

            hideChatLoadingOverlay();
            freshRoomCreationInProgress = false;

            roomData = {
              roomId: data.roomId,
              mood: data.mood,
              users: data.users,
              expiresAt: null
            };
            currentRoomId = data.roomId;
            localStorage.setItem('currentRoom', JSON.stringify(roomData));

            pendingInitialRoomSync = true;
            safeSocketEmit('join_room', { roomId: data.roomId });
          } catch (error) {
            console.error('❌ Failed to handle match_found:', error);
          }
        });


        socketInstance.on('call_created', (data) => {
          console.log('📞 ========================================');
          console.log('📞 CALL_CREATED EVENT RECEIVED');
          console.log('📞 ========================================');
          console.log(`   callId: ${data.callId}`);
          console.log(`   callType: ${data.callType}`);
          console.log(`   isInitiator: ${data.isInitiator}`);

          // Only handle if we're the initiator
          if (data.isInitiator) {
            console.log('✅ We initiated this call - loading in iframe');

            // Mark initiation complete so the button timeout doesn't revert UI after call_created.
            isInitiatingCall = false;
            try {
              if (callInitiationTimeoutId) clearTimeout(callInitiationTimeoutId);
            } catch { }
            callInitiationTimeoutId = null;

            // Clear loading states
            if (audioCallBtn) {
              audioCallBtn.classList.remove('call-btn-loading');
              audioCallBtn.disabled = false;
            }
            if (videoCallBtn) {
              videoCallBtn.classList.remove('call-btn-loading');
              videoCallBtn.disabled = false;
            }

            // Store call data
            localStorage.setItem(ACTIVE_CALL_KEY, JSON.stringify({
              callId: data.callId,
              roomId: roomData.roomId,
              callType: data.callType,
              users: data.participants || [],
              connectionState: 'initializing', // Set initial state
              storedAt: Date.now()
            }));

            console.log(`✅ Stored call data with initializing state`);

            // CRITICAL: Load call.html in iframe instead of navigating
            console.log('📱 Loading call.html in iframe (background mode)');

            // Create or show call iframe container
            let callIframeContainer = document.getElementById('callIframeContainer');
            if (!callIframeContainer) {
              callIframeContainer = document.createElement('div');
              callIframeContainer.id = 'callIframeContainer';
              callIframeContainer.className = 'fixed inset-0 z-[9999] bg-black'; // ✅ Visible immediately
              callIframeContainer.innerHTML = '<iframe id="callIframe" class="w-full h-full border-none"></iframe>';
              document.body.appendChild(callIframeContainer);
              console.log('✅ Created call iframe container (visible)');
            } else {
              callIframeContainer.classList.remove('hidden'); // ✅ Make visible if hidden
              console.log('✅ Showing existing call iframe container');
            }

            const callIframe = document.getElementById('callIframe');
            if (callIframe) {
              callIframe.src = '/call.html';
              console.log('✅ Loading call.html in iframe');
            }

            // Update UI state immediately
            activeCallConnectionState = 'initializing';
            updateCallButtonStates();

            console.log('✅ Call will initialize in background iframe');
            console.log('📞 ========================================\n');
          } else {
            console.log('ℹ️ Call created by someone else - staying in chat, showing JOIN button');
            console.log('📞 ========================================\n');
          }
        });


        socketInstance.on('room_joined', async (data) => {
          console.log('✅ ========================================');
          console.log('✅ ROOM_JOINED EVENT RECEIVED');
          console.log('✅ ========================================');
          console.log(`   roomId: ${data.roomId}`);
          freshRoomCreationInProgress = false;
          hasJoinedRoom = true;
          currentRoomId = data.roomId || currentRoomId;
          roomRecoveryInProgress = false;
          roomRecoveryAttempts = 0;
          reportChatPresenceContext('room_joined', {
            roomId: currentRoomId,
            allowRedirect: !isSocialClubMode,
            keepalive: true,
            source: 'chat_room_joined'
          }).catch(() => { });
          validateChatPresenceState('room_joined').catch(() => { });

          playJoinRoomSound(currentUser?.userId || 'self');

          if (pendingInitialRoomSync && roomData?.roomId) {
            pendingInitialRoomSync = false;
            safeSocketEmit('request_room_sync', { roomId: roomData.roomId });
          }

          // [Keep existing clock sync code...]
          const clientNow = Date.now();

          if (data.serverTime) {
            serverClockOffset = data.serverTime - clientNow;
            console.log(`   Server time: ${new Date(data.serverTime).toISOString()}`);
            console.log(`   Client time: ${new Date(clientNow).toISOString()}`);
            console.log(`   ⏰ CLOCK OFFSET: ${(serverClockOffset / 1000).toFixed(1)}s`);

            if (Math.abs(serverClockOffset) > CLOCK_SKEW_WARNING_MS) {
              console.warn(`⚠️ WARNING: Large clock skew detected (${(serverClockOffset / 1000).toFixed(1)}s)`);
            } else if (Math.abs(serverClockOffset) > CLOCK_SKEW_NOTICE_MS) {
              console.log(`ℹ️ Minor clock skew detected (${(serverClockOffset / 1000).toFixed(1)}s); timer adjusted`);
            }
          } else {
            console.warn('⚠️ No serverTime in room_joined, assuming no clock skew');
            serverClockOffset = 0;
          }

          serverExpiresAt = null;

          const cachedState = loadChatState();
          const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
          const isInIframe = window.parent && window.parent !== window;

          let shouldUseCache = false;

          if (activeCallStr && isInIframe) {
            try {
            } catch (e) {
              console.warn('Failed to check active call:', e);
            }
          }

          let mergedMessages = [];

          if (data.chatHistory?.length > 0) {
            console.log(`📜 Server provided ${data.chatHistory.length} messages`);

            mergedMessages = [...data.chatHistory];

            if (shouldUseCache && cachedState?.messages?.length > 0) {
              console.log(`📦 Merging ${cachedState.messages.length} cached messages with server data`);

              const serverMessageIds = new Set(
                data.chatHistory.map(msg => msg.messageId || `${msg.userId}-${msg.timestamp}`)
              );

              const uniqueCachedMessages = cachedState.messages.filter(msg => {
                const msgId = msg.messageId || `${msg.userId}-${msg.timestamp}`;
                return !serverMessageIds.has(msgId);
              });

              if (uniqueCachedMessages.length > 0) {
                console.log(`➕ Adding ${uniqueCachedMessages.length} unique cached messages`);
                mergedMessages = [...mergedMessages, ...uniqueCachedMessages];
                mergedMessages.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
              }
            }

            // ============================================
            // CRITICAL FIX: REQUEST MISSING FILE DATA
            // ============================================
            console.log('🔍 Checking for attachments that need data...');

            for (const msg of mergedMessages) {
              if (msg.attachment && msg.attachment.fileId) {
                if (msg.attachment.url || msg.attachment.publicUrl || msg.attachment.apiUrl) {
                  continue;
                }
                // Check if file exists in local IndexedDB
                try {
                  const localFile = await getFileFromIndexedDB(msg.attachment.fileId);

                  if (!localFile) {
                    console.log(`📂 File ${msg.attachment.fileId} not in local storage`);

                    // OPTIMIZATION: If data is already in message (assembled on server), store it immediately
                    if (msg.attachment.data) {
                      console.log(`📦 Found inlined data for ${msg.attachment.name} in history - storing to IndexedDB`);
                      try {
                        const blob = base64ToBlob(msg.attachment.data, msg.attachment.type);

                        if (blob) {
                          const file = new File([blob], msg.attachment.name, { type: msg.attachment.type });

                          // Use existing ID
                          await storeFileToIndexedDB(file, roomData.roomId, msg.messageId, msg.attachment.fileId);
                          console.log(`✅ Stored inlined file to IndexedDB: ${msg.attachment.fileId}`);

                          // Clear data from memory after storing to keep cache light
                          delete msg.attachment.data;
                        } else {
                          console.error('❌ Failed to process inlined attachment data: Conversion returned null');
                        }
                      } catch (e) {
                        console.error('❌ Exception processing inlined attachment data:', e);
                      }
                    } else if (msg.userId !== currentUser.userId) {
                      // Only request if sent by someone else and no data present
                      console.log(`📡 Requesting file data from peer for ${msg.attachment.name}`);

                      // Request file from sender via socket
                      socketInstance.emit('request_attachment_data', {
                        fileId: msg.attachment.fileId,
                        roomId: roomData.roomId
                      });
                    }
                  } else {
                    console.log(`✅ File ${msg.attachment.name} already in local storage`);
                  }
                } catch (e) {
                  console.error(`❌ Error checking file ${msg.attachment.fileId}:`, e);
                }
              }
            }

          } else if (shouldUseCache && cachedState?.messages?.length > 0) {
            console.log(`📦 No server history, using ${cachedState.messages.length} cached messages`);
            mergedMessages = [...cachedState.messages];
          } else {
            console.log('ℹ️ No server history and no cache - starting fresh');
          }

          const seenMessageKeys = new Set();
          const dedupedMessages = [];
          for (const msg of mergedMessages) {
            const key = msg?.messageId || `${msg?.userId || 'u'}-${msg?.timestamp || 0}-${String(msg?.message || '').slice(0, 64)}`;
            if (seenMessageKeys.has(key)) continue;
            seenMessageKeys.add(key);
            dedupedMessages.push(msg);
          }

          messagesCache = dedupedMessages;

          if (messagesList) {
            messagesList.innerHTML = '';
          }

          console.log(`🎨 Rendering ${messagesCache.length} total messages`);
          messagesCache.forEach(msg => {
            const isCurrentUser = msg.userId === currentUser.userId;
            const msgEl = createMessageElement(msg, isCurrentUser);
            messagesList?.appendChild(msgEl);
          });

          if (messagesList && messagesCache.length > 0) {
            messagesList.scrollTop = messagesList.scrollHeight;
            console.log('✅ Scrolled to latest message');
          }

          saveChatState(roomData.roomId, messagesCache);
          console.log(`💾 Saved ${messagesCache.length} messages to cache after merge`);

          // [Rest of room_joined handler...]
          if (shouldUseCache && cachedState) {
            console.log('🔄 Restoring call button state from cache:', cachedState.callButtonState);
            console.log('   Cached activeCallInRoom:', cachedState.activeCallInRoom);

            if (cachedState.activeCallInRoom) {
              activeCallInRoom = cachedState.activeCallInRoom;
              console.log('💾 Restored activeCallInRoom from cache:', activeCallInRoom);
            } else if (cachedState.callButtonState === 'back') {
              const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
              if (activeCallStr) {
                try {
                  const activeCallData = JSON.parse(activeCallStr);
                  activeCallInRoom = {
                    callId: activeCallData.callId,
                    callType: activeCallData.callType,
                    participantCount: 2
                  };
                  console.log('💾 Reconstructed activeCallInRoom from ACTIVE_CALL_KEY:', activeCallInRoom);
                } catch (e) {
                  console.error('Failed to reconstruct activeCallInRoom:', e);
                }
              }
            } else if (cachedState.callButtonState === 'join' && !activeCallInRoom) {
              console.warn('⚠️ Cache says "join" but no activeCallInRoom - will wait for server update');
            }

            if (cachedState.callButtonState === 'back') {
              updateCallButtonState(true, activeCallInRoom || { participantCount: 1 });
            } else if (cachedState.callButtonState === 'join' && activeCallInRoom) {
              updateCallButtonState(true, activeCallInRoom);
            } else {
              updateCallButtonState(false);
            }

            console.log('✅ Call button state restored instantly');
          }

          if (data.activeCall) {
            console.log('📞 Active call in room detected');

            activeCallInRoom = {
              callId: data.activeCall.callId,
              callType: data.activeCall.callType,
              participantCount: data.activeCall.participantCount
            };
            console.log('💾 Stored activeCallInRoom:', activeCallInRoom);

            // ✅ CRITICAL: Update button state immediately
            updateCallButtonState(true, data.activeCall);
          } else {
            console.log('📞 No active call in room (server authoritative)');
            activeCallInRoom = null;
            updateCallButtonState(false);
          }

          console.log('⏰ Starting timer updates with server-synced time...');
          updateTimer();

          console.log('✅ ========================================\n');
        });

        // ============================================
        // PEER-TO-PEER FILE TRANSFER HANDLERS
        // ============================================

        socketInstance.on('send_attachment_to_peer', async ({ fileId, requesterId, requesterSocketId }) => {
          console.log('📤 ========================================');
          console.log('📤 PEER REQUESTING OUR FILE');
          console.log('📤 ========================================');
          console.log(`   FileID: ${fileId}`);
          console.log(`   Requester: ${requesterId}`);

          try {
            // Get file from our IndexedDB
            const fileData = await getFileFromIndexedDB(fileId);

            if (!fileData || !fileData.blob) {
              console.error(`❌ File ${fileId} not found in our IndexedDB`);
              socketInstance.emit('attachment_data_response', {
                fileId,
                requesterId,
                requesterSocketId,
                data: null,
                metadata: null
              });
              return;
            }

            console.log(`✅ File found: ${fileData.name} (${(fileData.size / 1024).toFixed(2)} KB)`);

            // Convert blob to ArrayBuffer (Binary)
            const arrayBuffer = await new Promise((resolve, reject) => {
              const reader = new FileReader();
              reader.onload = () => resolve(reader.result);
              reader.onerror = reject;
              reader.readAsArrayBuffer(fileData.blob);
            });

            console.log(`✅ Converted to binary: ${(arrayBuffer.byteLength / 1024).toFixed(2)} KB`);

            // Send to requester via server relay
            socketInstance.emit('attachment_data_response', {
              fileId,
              requesterId,
              requesterSocketId,
              data: arrayBuffer, // Binary
              metadata: {
                name: fileData.name,
                type: fileData.type,
                size: fileData.size
              }
            });

            console.log(`📤 Binary file data sent to requester`);
            console.log('📤 ========================================\n');

          } catch (error) {
            console.error('❌ Error sending file to peer:', error);
            console.log('📤 ========================================\n');
          }
        });

        socketInstance.on('attachment_data_received', async ({ fileId, data, metadata }) => {
          console.log('📥 ========================================');
          console.log('📥 BINARY ATTACHMENT RECEIVED FROM PEER');
          console.log('📥 ========================================');
          console.log(`   FileID: ${fileId}`);
          console.log(`   File: ${metadata?.name}`);

          if (!data || !metadata) {
            console.error('❌ Invalid data received');
            console.log('📥 ========================================\n');
            return;
          }

          try {
            let blob;
            if (typeof data === 'string') {
              // Legacy/History Base64 data
              console.log('📦 Processing Base64 data (History/Legacy)');
              blob = base64ToBlob(data, metadata.type);
            } else {
              // High-speed Binary data
              console.log('⚡ Processing Binary data (Live P2P)');
              blob = new Blob([data], { type: metadata.type });
            }

            if (!blob) {
              throw new Error('Blob creation failed');
            }

            // Create File object
            const file = new File([blob], metadata.name, { type: metadata.type });

            // Store to our IndexedDB with SAME fileId
            await new Promise((resolve, reject) => {
              initFileDB().then(db => {
                const transaction = db.transaction([FILE_STORE_NAME], 'readwrite');
                const store = transaction.objectStore(FILE_STORE_NAME);

                const fileRecord = {
                  id: fileId, // Use same ID as sender
                  roomId: roomData.roomId,
                  messageId: null,
                  name: metadata.name,
                  type: metadata.type,
                  size: metadata.size,
                  blob: blob,
                  timestamp: Date.now()
                };

                const request = store.add(fileRecord);

                request.onsuccess = () => {
                  console.log(`✅ File stored to local IndexedDB: ${fileId}`);
                  resolve();
                };

                request.onerror = () => {
                  console.error('❌ Failed to store received file:', request.error);
                  reject(request.error);
                };
              }).catch(reject);
            });

            // Find and update the image element if it's already rendered
            const imgElements = document.querySelectorAll(`img[data-file-id="${fileId}"]`);

            if (imgElements.length > 0) {
              console.log(`🖼️ Found ${imgElements.length} image element(s) to update`);

              imgElements.forEach(imgElement => {
                loadImageAttachment(fileId, imgElement);
              });
            } else {
              console.log('ℹ️ No rendered elements found for this file (will load when message is rendered)');
            }

            console.log('✅ File successfully received and stored');
            console.log('📥 ========================================\n');

          } catch (error) {
            console.error('❌ Error processing received file:', error);
            console.log('📥 ========================================\n');
          }
        });

        socketInstance.on('attachment_data_unavailable', ({ fileId, reason }) => {
          console.warn('⚠️ ========================================');
          console.warn('⚠️ ATTACHMENT DATA UNAVAILABLE');
          console.warn('⚠️ ========================================');
          console.warn(`   FileID: ${fileId}`);
          console.warn(`   Reason: ${reason}`);

          // Update UI to show file unavailable
          const imgElements = document.querySelectorAll(`img[data-file-id="${fileId}"]`);

          imgElements.forEach(img => {
            img.alt = `File unavailable: ${reason}`;
            img.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="300" height="200"%3E%3Crect width="300" height="200" fill="%23161b22"/%3E%3Ctext x="50%25" y="45%25" dominant-baseline="middle" text-anchor="middle" fill="%23ef4444" font-family="sans-serif" font-size="14"%3EFile unavailable%3C/text%3E%3Ctext x="50%25" y="60%25" dominant-baseline="middle" text-anchor="middle" fill="%238b949e" font-family="sans-serif" font-size="11"%3E' + encodeURIComponent(reason) + '%3C/text%3E%3C/svg%3E';
          });

          console.warn('⚠️ ========================================\n');
        });

        socketInstance.on('room_sync_data', (data) => {
          console.log('⏰ ========================================');
          console.log('⏰ ROOM SYNC DATA RECEIVED');
          console.log('⏰ ========================================');
          currentRoomId = data.roomId || currentRoomId;
          roomRecoveryInProgress = false;
          roomRecoveryAttempts = 0;
          reportChatPresenceContext('room_sync_data', {
            roomId: currentRoomId,
            allowRedirect: !isSocialClubMode,
            keepalive: true,
            source: 'chat_room_sync'
          }).catch(() => { });

          const clientNow = Date.now();

          if (!data.serverTime) {
            console.warn('⚠️ No serverTime in sync data, assuming no clock skew');
            data.serverTime = clientNow;
          }

          // Calculate clock offset
          serverClockOffset = data.serverTime - clientNow;

          console.log(`   Server time: ${new Date(data.serverTime).toISOString()}`);
          console.log(`   Client time: ${new Date(clientNow).toISOString()}`);
          console.log(`   ⏰ CLOCK OFFSET: ${(serverClockOffset / 1000).toFixed(1)}s`);

          if (Math.abs(serverClockOffset) > CLOCK_SKEW_WARNING_MS) {
            console.warn(`⚠️ WARNING: Large clock skew detected (${(serverClockOffset / 1000).toFixed(1)}s)`);
            console.warn(`   Timer will be adjusted to compensate`);
          } else if (Math.abs(serverClockOffset) > CLOCK_SKEW_NOTICE_MS) {
            console.log(`ℹ️ Minor clock skew detected (${(serverClockOffset / 1000).toFixed(1)}s); timer adjusted`);
          }

          serverExpiresAt = null;

          console.log('⏰ ========================================\n');

          // Force timer update with new data
          updateTimer();
        });

        socketInstance.on('room_expiring_soon', () => { return; });

        socketInstance.on('error', (data) => {
          console.error('❌ ========================================');
          console.error('❌ SOCKET ERROR EVENT');
          console.error('❌ ========================================');
          console.error('   Error code:', data.code);
          console.error('   Error message:', data.message);
          console.error('   Timestamp:', new Date().toISOString());
          console.error('   isInitiatingCall:', isInitiatingCall);
          console.error('   isSocketAuthenticated:', isSocketAuthenticated);

          // CRITICAL: Always clear initiating state on any error
          const wasInitiating = isInitiatingCall;
          isInitiatingCall = false;

          // CRITICAL: Handle authentication errors with auto-recovery
          if (data.message === 'Not authenticated') {
            console.error('❌ NOT AUTHENTICATED ERROR RECEIVED');
            console.error('   This indicates a race condition - call initiated before auth completed');
            console.error('   Current auth state:', isSocketAuthenticated);
            console.error('   Socket connected:', socketInstance?.connected);
            console.error('   User data exists:', !!currentUser);
            console.error('   Was initiating call:', wasInitiating);

            // Clear call loading states immediately
            if (audioCallBtn) {
              audioCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = audioCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete audioCallBtn.dataset.timeoutId;
                console.error('   ⏰ Cleared audio button timeout:', timeoutId);
              }
            }
            if (videoCallBtn) {
              videoCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = videoCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete videoCallBtn.dataset.timeoutId;
                console.error('   ⏰ Cleared video button timeout:', timeoutId);
              }
            }

            console.error('   🔒 Call loading states cleared');

            // Force re-authentication
            setAuthenticationState(false);

            // Attempt to recover
            if (socketInstance && socketInstance.connected && currentUser) {
              console.log('🔄 Attempting authentication recovery...');

              firebase.auth().currentUser?.getIdToken(true).then(idToken => {
                console.log('📤 Re-sending authentication with fresh token...');
                const sessionId = getScopedSocketSessionId('chat');
                const tabId = window.tabManager?.tabId || null;
                socketInstance.emit('authenticate', {
                  token: idToken,
                  userId: currentUser.userId,
                  sessionId,
                  tabId
                });

                toast('Reconnecting...', 'info');

                // Set recovery timeout
                const recoveryTimeout = setTimeout(() => {
                  if (!isSocketAuthenticated) {
                    console.error('❌ Recovery timeout - authentication failed after 10s');
                    toast('Authentication failed. Please refresh the page.', 'error');
                  } else {
                    console.log('✅ Authentication recovered successfully');

                    // If user was trying to initiate a call, show helpful message
                    if (wasInitiating) {
                      toast('Connected! You can now start a call.', 'success');
                    }
                  }
                }, 10000);

                console.log('⏰ Recovery timeout set:', recoveryTimeout);

              }).catch(err => {
                console.error('❌ Recovery authentication failed:', err);
                toast('Authentication failed. Please refresh the page.', 'error');
              });
            } else {
              console.error('❌ Cannot recover - missing socket or user data');
              console.error('   Socket exists:', !!socketInstance);
              console.error('   Socket connected:', socketInstance?.connected);
              console.error('   Current user:', !!currentUser);
              toast('Connection lost. Please refresh the page.', 'error');
            }

            console.error('❌ ========================================\n');
            return;
          }

          // CRITICAL FIX: Handle CALL_ALREADY_ACTIVE error - auto-join instead
          if (data.code === 'CALL_ALREADY_ACTIVE') {
            console.log('🔄 ========================================');
            console.log('🔄 CALL_ALREADY_ACTIVE ERROR HANDLING');
            console.log('🔄 ========================================');
            console.log(`   Existing callId: ${data.callId}`);
            console.log(`   callType: ${data.callType}`);
            console.log(`   participantCount: ${data.participantCount}`);
            console.log(`   Was initiating: ${wasInitiating}`);

            // Clear loading states from call initiation
            if (audioCallBtn) {
              audioCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = audioCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete audioCallBtn.dataset.timeoutId;
                console.log('   ⏰ Cleared audio button timeout');
              }
            }
            if (videoCallBtn) {
              videoCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = videoCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete videoCallBtn.dataset.timeoutId;
                console.log('   ⏰ Cleared video button timeout');
              }
            }

            // Re-enable buttons if authenticated
            if (isSocketAuthenticated) {
              console.log('   ✅ Re-enabling call buttons (authenticated)');
              enableCallButtons();
            } else {
              console.log('   ⏳ Keeping buttons disabled (not authenticated)');
            }

            // Store the active call info
            activeCallInRoom = {
              callId: data.callId,
              callType: data.callType || 'audio',
              participantCount: data.participantCount || 1
            };
            console.log('💾 Stored activeCallInRoom:', activeCallInRoom);

            // Update UI to show join button
            updateCallButtonState(true, activeCallInRoom);

            // Show friendly message
            toast('A call is already active. Click "Join Call" to join.', 'success');
            console.log('🔄 ========================================\n');
            return;
          }

          // Handle NOT_IN_ROOM as recoverable first (re-sync + rejoin).
          if (data.code === 'NOT_IN_ROOM') {
            if (!isSocialClubMode && selectedMood) {
              clearStaleRoomAndCreateFresh('socket_error_not_in_room');
              console.error('❌ ========================================\n');
              return;
            }
            console.warn('⚠️ ROOM MEMBERSHIP DESYNC DETECTED');
            console.warn('   Attempting room recovery before exiting');

            if (audioCallBtn) {
              audioCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = audioCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete audioCallBtn.dataset.timeoutId;
              }
            }
            if (videoCallBtn) {
              videoCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = videoCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete videoCallBtn.dataset.timeoutId;
              }
            }

            const now = Date.now();
            if (now - lastRoomRecoveryAttemptAt > 15000) {
              roomRecoveryAttempts = 0;
            }

            if (!roomRecoveryInProgress && socketInstance?.connected && roomData?.roomId && roomRecoveryAttempts < 3) {
              roomRecoveryInProgress = true;
              roomRecoveryAttempts++;
              lastRoomRecoveryAttemptAt = now;

              toast('Connection hiccup detected. Resyncing room...', 'warning');
              safeSocketEmit('join_room', { roomId: roomData.roomId });
              pendingInitialRoomSync = true;

              setTimeout(() => {
                roomRecoveryInProgress = false;
              }, 2000);

              console.warn(`🔄 Recovery attempt ${roomRecoveryAttempts}/3 initiated`);
              console.error('❌ ========================================\n');
              return;
            }

            console.error('🗑️ Room recovery exhausted; using unified exit sequence');
            exitRoomSequence().catch(console.error);
            console.error('❌ ========================================\n');
            return;
          }

          // Handle hard room-not-found as terminal.
          if (data.code === 'ROOM_NOT_FOUND') {
            console.error('❌ ROOM ERROR:', data.code);
            console.error('   Message:', data.message);
            console.error('   Current room:', roomData?.roomId);
            console.error('   Was initiating:', wasInitiating);

            try {
              localStorage.removeItem('currentRoom');
            } catch { }
            roomData = null;
            currentRoomId = null;
            hasJoinedRoom = false;
            pendingInitialRoomSync = false;

            if (!isSocialClubMode && selectedMood) {
              clearStaleRoomAndCreateFresh('socket_error_room_not_found');
              console.error('❌ ========================================\n');
              return;
            }

            toast('Room no longer available', 'error');

            if (isSocialClubMode && !roomData?.roomId) {
              console.warn('🎭 Social Club: no active room yet; retrying join_social_club');
              safeSocketEmit('join_social_club', {});
              console.error('❌ ========================================\n');
              return;
            }

            if (audioCallBtn) {
              audioCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = audioCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete audioCallBtn.dataset.timeoutId;
              }
            }
            if (videoCallBtn) {
              videoCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = videoCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete videoCallBtn.dataset.timeoutId;
              }
            }

            console.error('🗑️ Using unified exit sequence due to room error');
            exitRoomSequence().catch(console.error);
            console.error('❌ ========================================\n');
            return;
          }

          if (data.code === 'ROOM_EXPIRED') {
            console.error('💥 ROOM EXPIRED ERROR');
            console.error('   Message:', data.message);
            console.error('   Room:', roomData?.roomId);
            console.error('   Was initiating:', wasInitiating);

            if (!isSocialClubMode && selectedMood) {
              clearStaleRoomAndCreateFresh('socket_error_room_expired');
              console.error('❌ ========================================\n');
              return;
            }

            if (timerInterval) {
              clearInterval(timerInterval);
              timerInterval = null;
            }

            toast('Room has expired', 'warning');

            // Clear loading states
            if (audioCallBtn) {
              audioCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = audioCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete audioCallBtn.dataset.timeoutId;
              }
            }
            if (videoCallBtn) {
              videoCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = videoCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete videoCallBtn.dataset.timeoutId;
              }
            }

            // Clear all storage
            console.error('🗑️ Using unified exit sequence due to room expiry error');
            exitRoomSequence().catch(console.error);

            console.error('❌ ========================================\n');
            return;
          }

          // Handle call-related errors
          if (data.code === 'CALL_NOT_FOUND' || data.code === 'CALL_ENDED') {
            console.error('❌ CALL ERROR:', data.code);
            console.error('   Message:', data.message);
            console.error('   Was initiating:', wasInitiating);

            try { localStorage.removeItem(ACTIVE_CALL_KEY); } catch { }
            try { sessionStorage.removeItem('hasBackgroundCall'); } catch { }
            activeCallConnectionState = null;

            // Clear loading states
            if (audioCallBtn) {
              audioCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = audioCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete audioCallBtn.dataset.timeoutId;
              }
            }
            if (videoCallBtn) {
              videoCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
              const timeoutId = videoCallBtn.dataset.timeoutId;
              if (timeoutId) {
                clearTimeout(parseInt(timeoutId));
                delete videoCallBtn.dataset.timeoutId;
              }
            }
            if (joinCallBtn) {
              joinCallBtn.classList.remove('join-btn-loading');
              joinCallBtn.disabled = false;
            }

            // Re-enable buttons if authenticated
            if (isSocketAuthenticated) {
              console.log('   ✅ Re-enabling call buttons');
              enableCallButtons();
            }

            // Clear active call state
            activeCallInRoom = null;
            updateCallButtonState(false);

            toast(data.message || 'Call is no longer available', 'warning');
            console.error('❌ ========================================\n');
            return;
          }

          // Handle join call failures
          if (data.code === 'JOIN_FAILED' || data.code === 'PARTICIPANT_RESOLUTION_FAILED' || data.code === 'STATE_MISMATCH') {
            console.error('❌ JOIN CALL ERROR:', data.code);
            console.error('   Message:', data.message);

            // Clear join button loading state
            if (joinCallBtn) {
              joinCallBtn.classList.remove('join-btn-loading');
              joinCallBtn.disabled = false;
            }
            if (acceptCallBtn) {
              acceptCallBtn.classList.remove('join-btn-loading');
              acceptCallBtn.disabled = false;
            }

            toast(data.message || 'Failed to join call. Please try again.', 'error');
            console.error('❌ ========================================\n');
            return;
          }

          // Generic error handling
          console.error('❌ GENERIC ERROR');
          console.error('   Code:', data.code || 'UNKNOWN');
          console.error('   Message:', data.message || 'An error occurred');
          console.error('   Was initiating:', wasInitiating);

          // Clear any loading states on generic errors
          if (audioCallBtn) {
            audioCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
            const timeoutId = audioCallBtn.dataset.timeoutId;
            if (timeoutId) {
              clearTimeout(parseInt(timeoutId));
              delete audioCallBtn.dataset.timeoutId;
            }
          }
          if (videoCallBtn) {
            videoCallBtn.classList.remove('call-btn-loading', 'calling', 'animate-pulse');
            const timeoutId = videoCallBtn.dataset.timeoutId;
            if (timeoutId) {
              clearTimeout(parseInt(timeoutId));
              delete videoCallBtn.dataset.timeoutId;
            }
          }
          if (joinCallBtn) {
            joinCallBtn.classList.remove('join-btn-loading');
            joinCallBtn.disabled = false;
          }

          // Re-enable buttons if authenticated
          if (isSocketAuthenticated) {
            console.log('   ✅ Re-enabling call buttons');
            enableCallButtons();
          } else {
            console.log('   ⏳ Not authenticated - buttons will enable after auth completes');
          }

          toast(data.message || 'An error occurred', 'error');
          console.error('❌ ========================================\n');
        });

        socketInstance.on('chat_message', async (data) => {
          console.log('💬 ========================================');
          console.log('💬 INCOMING CHAT MESSAGE FROM SERVER');
          console.log('💬 ========================================');
          console.log(`   From: ${data.username} (${data.userId})`);
          console.log(`   MessageID: ${data.messageId}`);
          console.log(`   Has attachment: ${!!data.attachment}`);
          console.log(`   Attachment chunked: ${data.attachment?.chunked || false}`);

          const isOwnMessage = data.userId === currentUser.userId;
          console.log(`   Is own message: ${isOwnMessage}`);

          // ============================================
          // DEDUPLICATION - Check server messages only
          // ============================================
          if (isOwnMessage) {
            const optimisticIndex = messagesCache.findIndex(m =>
              m._localOnly &&
              m.userId === data.userId &&
              Math.abs((m.timestamp || 0) - (data.timestamp || 0)) < 2000
            );

            if (optimisticIndex !== -1) {
              console.log('🔄 Replacing optimistic message with server version');

              const optimisticMsg = messagesCache[optimisticIndex];
              messagesCache.splice(optimisticIndex, 1);

              const msgElements = messagesList?.querySelectorAll('.message-item');
              if (msgElements) {
                msgElements.forEach(el => {
                  if (el.dataset.messageId === optimisticMsg.messageId) {
                    el.remove();
                    console.log('✅ Removed optimistic message from DOM');
                  }
                });
              }
            }
          }

          const isDuplicate = messagesCache.some(cached => {
            if (cached._localOnly) return false;

            if (cached.messageId && data.messageId && cached.messageId === data.messageId) {
              return true;
            }

            if (cached.userId === data.userId &&
              Math.abs((cached.timestamp || 0) - (data.timestamp || 0)) < 100) {
              return true;
            }

            return false;
          });

          if (isDuplicate) {
            console.warn(`⚠️ DUPLICATE MESSAGE DETECTED - SKIPPING`);
            console.log('💬 ========================================\n');
            return;
          }

          console.log(`✅ New message validated - will render`);

          // ============================================
          // ATTACHMENT HANDLING - CHUNKED OR LEGACY
          // ============================================
          if (data.attachment) {
            console.log('📎 Message contains attachment');
            console.log(`   Chunked: ${data.attachment.chunked || false}`);

            if (data.attachment.url || data.attachment.publicUrl || data.attachment.apiUrl) {
              console.log('🌐 Persistent attachment URL detected');
              delete data.attachment.data;
            } else if (data.attachment.chunked) {
              // CHUNKED: File will arrive via file_chunk events
              console.log('📦 Chunked attachment - file will be received separately');
              console.log(`   FileID: ${data.attachment.fileId}`);
              console.log(`   Total chunks: ${data.attachment.totalChunks}`);

              // Message will be rendered with placeholder
              // File will load when chunks complete

            } else if (data.userId !== currentUser.userId && data.attachment.data) {
              // LEGACY: Single base64 payload (for backward compatibility)
              console.log('📦 Legacy attachment format detected');

              try {
                const blob = base64ToBlob(data.attachment.data, data.attachment.type);

                if (blob) {
                  const file = new File([blob], data.attachment.name, { type: data.attachment.type });
                  const storedFileId = await storeFileToIndexedDB(file, roomData.roomId, data.messageId);

                  data.attachment.fileId = storedFileId;
                  delete data.attachment.data;
                  console.log(`✅ Legacy attachment stored: ${storedFileId}`);
                } else {
                  console.error('❌ Failed to process legacy attachment: Conversion returned null');
                }

              } catch (error) {
                console.error('❌ Failed to store legacy attachment:', error);
              }
            } else if (data.userId === currentUser.userId) {
              console.log('ℹ️ Own message - file already in IndexedDB');
              delete data.attachment.data;
            }
          }

          // ============================================
          // RENDER MESSAGE
          // ============================================
          console.log(`🎨 Rendering message`);
          const msgEl = createMessageElement(data, isOwnMessage);

          if (messagesList) {
            messagesList.appendChild(msgEl);
            messagesList.scrollTop = messagesList.scrollHeight;
            console.log(`✅ Message rendered in UI`);
          }

          // ============================================
          // UPDATE CACHE AND SAVE
          // ============================================
          messagesCache.push(data);
          console.log(`➕ Added to cache (new size: ${messagesCache.length})`);

          saveChatState(roomData.roomId, messagesCache);
          console.log(`💾 Chat state saved`);

          console.log('💬 ========================================\n');
        });

        socketInstance.on('user_left', (data) => {
          const sequenceId = Math.random().toString(36).substring(2, 6).toUpperCase();
          console.log(`👤 [${sequenceId}] User left: ${data.username} (${data.userId}). Remaining: ${data.remainingUsers}`);

          // 1. Show message
          if (shouldShowLifecycleMessage('left', data.userId)) {
            const msgEl = document.createElement('div');
            msgEl.className = 'text-center text-xs sm:text-sm text-gray-400 my-2';
            msgEl.textContent = `${data.username || 'A user'} left the room`;
            messagesList?.appendChild(msgEl);
            if (messagesList) messagesList.scrollTop = messagesList.scrollHeight;
          } else {
            console.log(`ℹ️ [${sequenceId}] Suppressed duplicate left message for ${data.userId}`);
          }

          // 2. Authoritative User List Update (mirroring user_joined)
          if (data.users && Array.isArray(data.users)) {
            console.log(`👥 [${sequenceId}] Updating sidebar with ${data.users.length} users`);
            renderUserList(data.users);
          } else {
            // Fallback for older server versions/single element remove
            const userEl = document.getElementById(`user-${data.userId}`);
            if (userEl) {
              userEl.style.opacity = '0';
              setTimeout(() => userEl.remove(), 300);
            }
          }

          // 3. Update online count UI
          if (onlineCount && data.remainingUsers !== undefined) {
            onlineCount.textContent = `${data.remainingUsers} Online`;
          }

          // 4. Room Destruction Logic
          // Rooms should persist as long as at least 1 user remains.
          // Only trigger exit if the room is destroyed (empty) or if this event indicates the current user is no longer in the room.
          const roomDestroyed = data && data.destroyed === true;
          const remainingUsers = typeof data?.remainingUsers === 'number' ? data.remainingUsers : null;
          const leftUserId = data?.userId || null;
          const currentUserId = currentUser?.userId || null;
          const selfLeft = !!(leftUserId && currentUserId && leftUserId === currentUserId);

          // If we are transitioning into a call (or already have an active call cached), do not
          // run the destructive room exit sequence on a transient self-left/disconnect.
          // The socket/session may be temporarily replaced/reconnected while call UI initializes.
          let hasActiveCallCached = false;
          try {
            const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
            if (activeCallStr) {
              const parsed = JSON.parse(activeCallStr);
              hasActiveCallCached = !!(parsed?.callId && parsed?.roomId);
            }
          } catch { }

          if (selfLeft && (navigatingToCall || isInBackgroundCall || hasActiveCallCached)) {
            console.warn(`🛡️ [${sequenceId}] Suppressing exit sequence on self-left during call flow`);
            return;
          }

          if (!isSocialClubMode && (roomDestroyed || selfLeft)) {
            console.log(`💥 [${sequenceId}] Room state changed: Destroyed=${data.destroyed}, Remaining=${data.remainingUsers}`);
            console.log(`🚪 [${sequenceId}] Triggering room exit sequence`);
            toast('Room closing...', 'warning');
            exitRoomSequence().catch(console.error);
          }
        });

        const JOIN_ROOM_SOUND_URL = '/join_room_sound.mp3';
        console.log(`🔊 [JoinSound] Initializing join-room sound: ${JOIN_ROOM_SOUND_URL}`);

        const joinRoomSound = new Audio(JOIN_ROOM_SOUND_URL);
        joinRoomSound.preload = 'auto';
        joinRoomSound.crossOrigin = 'anonymous';

        const ENABLE_JOIN_SOUND_DIAGNOSTICS = false;
        if (ENABLE_JOIN_SOUND_DIAGNOSTICS) {
          (async () => {
            try {
              console.log('🔊 [JoinSound] Checking sound asset availability...');
            let res = null;
            try {
              res = await fetch(JOIN_ROOM_SOUND_URL, { method: 'HEAD', cache: 'no-store' });
            } catch (e) {
              console.warn('🔊 [JoinSound] HEAD check failed (will try GET):', e);
            }

            if (!res) {
              try {
                res = await fetch(JOIN_ROOM_SOUND_URL, { method: 'GET', cache: 'no-store' });
              } catch (e) {
                console.error('🔊 [JoinSound] GET check failed:', e);
              }
            }

            if (res) {
              console.log(`🔊 [JoinSound] Asset check response: status=${res.status} ok=${res.ok} type=${res.type}`);
              try {
                console.log(`🔊 [JoinSound] Final URL: ${res.url || JOIN_ROOM_SOUND_URL} (redirected=${!!res.redirected})`);
              } catch { }
              try {
                const ct = res.headers?.get?.('content-type');
                if (ct) console.log(`🔊 [JoinSound] content-type=${ct}`);
              } catch { }

              try {
                const cl = res.headers?.get?.('content-length');
                if (cl) console.log(`🔊 [JoinSound] content-length=${cl}`);
              } catch { }

              // Sample the first bytes using a small Range GET.
              // This avoids the "HEAD has no body" issue and helps detect invalid audio payloads.
              try {
                let sampleRes = null;
                try {
                  sampleRes = await fetch(JOIN_ROOM_SOUND_URL, {
                    method: 'GET',
                    cache: 'no-store',
                    headers: {
                      Range: 'bytes=0-1023'
                    }
                  });
                } catch (e) {
                  console.warn('🔊 [JoinSound] Range GET sample failed:', e);
                }

                if (!sampleRes) {
                  console.warn('🔊 [JoinSound] No sample response available for header check');
                  return;
                }

                console.log(`🔊 [JoinSound] Sample response: status=${sampleRes.status} ok=${sampleRes.ok} type=${sampleRes.type}`);
                try {
                  const acceptRanges = sampleRes.headers?.get?.('accept-ranges');
                  if (acceptRanges) console.log(`🔊 [JoinSound] accept-ranges=${acceptRanges}`);
                  const contentRange = sampleRes.headers?.get?.('content-range');
                  if (contentRange) console.log(`🔊 [JoinSound] content-range=${contentRange}`);
                } catch { }

                const buf = await sampleRes.arrayBuffer();
                const bytes = new Uint8Array(buf.slice(0, 24));

                if (!bytes.length) {
                  console.warn('🔊 [JoinSound] Sample returned 0 bytes; cannot detect header');
                  return;
                }

                let ascii = '';
                for (const b of bytes) {
                  ascii += (b >= 32 && b <= 126) ? String.fromCharCode(b) : '.';
                }

                const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join(' ');
                console.log(`🔊 [JoinSound] First bytes (ascii): ${ascii}`);
                console.log(`🔊 [JoinSound] First bytes (hex): ${hex}`);

                // Helpful hint: MP3 often starts with "ID3" or 0xFF 0xFB (frame sync)
                if (ascii.startsWith('<!DOCTYPE') || ascii.startsWith('<html') || ascii.includes('<html')) {
                  console.warn('🔊 [JoinSound] WARNING: Response looks like HTML (likely an error page), not an MP3');
                } else if (ascii.startsWith('ID3')) {
                  console.log('🔊 [JoinSound] Detected ID3 tag header (looks like a real MP3 file)');
                } else if (bytes.length >= 2 && bytes[0] === 0xff && (bytes[1] & 0xe0) === 0xe0) {
                  console.log('🔊 [JoinSound] Detected MP3 frame sync (looks like a real MP3 stream)');
                } else {
                  console.warn('🔊 [JoinSound] Header does not look like common MP3 signatures; decoding may fail');
                }
              } catch (e) {
                console.warn('🔊 [JoinSound] Failed to sample first bytes:', e);
              }
            } else {
              console.warn('🔊 [JoinSound] Asset check produced no response object');
            }
            } catch (e) {
              console.error('🔊 [JoinSound] Unexpected error during asset check:', e);
            }
          })();
        }

        try {
          if (ENABLE_JOIN_SOUND_DIAGNOSTICS) {
            joinRoomSound.addEventListener('loadeddata', () => {
              console.log('🔊 [JoinSound] loadeddata fired (some data available)');
            });
            joinRoomSound.addEventListener('canplaythrough', () => {
              console.log('🔊 [JoinSound] canplaythrough fired (likely buffered enough)');
            });
          }
          joinRoomSound.addEventListener('error', () => {
            const mediaErr = joinRoomSound.error;
            console.error('🔊 [JoinSound] Audio element error event fired');
            if (mediaErr) {
              console.error('🔊 [JoinSound] MediaError:', {
                code: mediaErr.code,
                message: mediaErr.message
              });
            }
            console.error('🔊 [JoinSound] Audio element src:', joinRoomSound.currentSrc || joinRoomSound.src);

            try {
              if (typeof playJoinBeep === 'function') {
                console.warn('🔊 [JoinSound] Falling back to WebAudio beep due to audio element error');
                playJoinBeep();
              }
            } catch { }
          });
        } catch (e) {
          console.warn('🔊 [JoinSound] Failed to attach audio event listeners:', e);
        }

        const joinSoundRecentlyPlayedForUser = new Map();
        const JOIN_SOUND_DEDUPE_WINDOW_MS = 3000;

        let joinBeepAudioContext = null;
        let joinBeepUnlocked = false;

        function getJoinBeepContext() {
          try {
            if (joinBeepAudioContext) return joinBeepAudioContext;
            const Ctx = window.AudioContext || window.webkitAudioContext;
            if (!Ctx) return null;
            joinBeepAudioContext = new Ctx();
            return joinBeepAudioContext;
          } catch {
            return null;
          }
        }

        function playJoinBeep() {
          try {
            const ctx = getJoinBeepContext();
            if (!ctx) return;

            // If locked, the gesture unlock handler will handle it.
            if (ctx.state === 'suspended') {
              void ctx.resume().catch(() => { });
            }

            const osc = ctx.createOscillator();
            const gain = ctx.createGain();

            osc.type = 'sine';
            osc.frequency.value = 880;
            gain.gain.value = 0.0001;

            osc.connect(gain);
            gain.connect(ctx.destination);

            const now = ctx.currentTime;
            gain.gain.setValueAtTime(0.0001, now);
            gain.gain.exponentialRampToValueAtTime(0.12, now + 0.01);
            gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.12);

            osc.start(now);
            osc.stop(now + 0.13);
          } catch { }
        }

        function playJoinRoomSound(sourceUserId = 'unknown') {
          try {
            const now = Date.now();
            const lastPlayedAt = joinSoundRecentlyPlayedForUser.get(sourceUserId) || 0;
            if (now - lastPlayedAt <= JOIN_SOUND_DEDUPE_WINDOW_MS) return;
            joinSoundRecentlyPlayedForUser.set(sourceUserId, now);

            joinRoomSound.currentTime = 0;
            const p = joinRoomSound.play();
            if (p && typeof p.catch === 'function') {
              p.catch((err) => {
                console.warn('🔊 [JoinSound] play() rejected:', err);
                // Common cause: autoplay policy. This will work after the unlock click/touch happens.

                try {
                  if (joinBeepUnlocked) {
                    console.warn('🔊 [JoinSound] Falling back to WebAudio beep due to play() rejection');
                    playJoinBeep();
                  }
                } catch { }
              });
            }
          } catch { }
        }

        const unlockJoinSoundOnce = () => {
          try {
            console.log('🔊 [JoinSound] User gesture detected; attempting to unlock audio...');
            const prevMuted = joinRoomSound.muted;
            joinRoomSound.muted = true;

            try {
              const ctx = getJoinBeepContext();
              if (ctx && ctx.state === 'suspended') {
                void ctx.resume().catch(() => { });
              }
              joinBeepUnlocked = true;
            } catch { }

            const p = joinRoomSound.play();
            if (p && typeof p.then === 'function') {
              p.then(() => {
                joinRoomSound.pause();
                joinRoomSound.currentTime = 0;
                joinRoomSound.muted = prevMuted;
                console.log('🔊 [JoinSound] Audio unlock succeeded');
              }).catch(() => {
                joinRoomSound.muted = prevMuted;
                console.warn('🔊 [JoinSound] Audio unlock failed (play rejected)');
              });
            } else {
              joinRoomSound.pause();
              joinRoomSound.currentTime = 0;
              joinRoomSound.muted = prevMuted;
              console.log('🔊 [JoinSound] Audio unlock attempted (non-promise play())');
            }
          } catch (e) {
            console.warn('🔊 [JoinSound] Audio unlock threw:', e);
          }
        };

        document.addEventListener('click', unlockJoinSoundOnce, { once: true, passive: true });
        document.addEventListener('touchstart', unlockJoinSoundOnce, { once: true, passive: true });

        socketInstance.on('user_joined', (data) => {
          console.log(`👤 User joined: ${data.username} (${data.userId})`);

          // Display join message in chat (skip if it's the current user)
          if (data.userId !== currentUser?.userId && shouldShowLifecycleMessage('joined', data.userId)) {
            playJoinRoomSound(data.userId);

            const msgEl = document.createElement('div');
            msgEl.className = 'text-center text-xs sm:text-sm text-gray-400 my-2';
            msgEl.textContent = `${data.username || 'A user'} joined the room`;
            messagesList?.appendChild(msgEl);

            // Scroll to show new message
            if (messagesList) {
              messagesList.scrollTop = messagesList.scrollHeight;
            }
          } else if (data.userId !== currentUser?.userId) {
            console.log(`ℹ️ Suppressed duplicate joined message for ${data.userId}`);
          }

          // Update online count
          if (onlineCount && data.onlineCount !== undefined) {
            onlineCount.textContent = `${data.onlineCount} Online`;
          }

          // Update user list if provided
          if (data.users && Array.isArray(data.users)) {
            console.log(`👥 Receiving full user list: ${data.users.length} users`);
            renderUserList(data.users);
          } else {
            console.warn('⚠️ user_joined event received without user list');
            // Fallback: append single user? (Wait, server sends full list)
          }
        });

        socketInstance.on('room_expired', () => { return; });

        socketInstance.on('left_room', (data) => {
          console.log('👋 Received left_room confirmation from server');
          toast('Left room', 'success');
          currentRoomId = null;

          if (data?.forceRedirect) {
            clearChatState(true).catch(() => { });
            cleanupParentCallPage('/mood.html');
            return;
          }

          // CRITICAL: Don't clear storage here - already done in leave button handler
          // Just disconnect socket
          if (socketInstance) {
            socketInstance.disconnect();
          }

          console.log('✅ Socket disconnected after leaving room');
        });

        socketInstance.on('force_navigation', (data) => {
          const target = data?.to || '/mood.html';
          const safeTarget = target === '/discovery.html' ? '/mood.html' : target;
          console.warn(`🚦 Server-authoritative navigation requested: ${safeTarget} (${data?.reason || 'unspecified'})`);
          if (isSocialClubMode) return;
          clearChatState(true).catch(() => { });
          cleanupParentCallPage(safeTarget);
        });

        socketInstance.on('incoming_call', (data) => {
          // GUARD: Don't show if already in/initiating a call
          if (activeCallConnectionState === 'initializing' || activeCallConnectionState === 'connecting' || activeCallConnectionState === 'connected' || isInitiatingCall) {
            console.log('📞 Skipping incoming call modal - user already in/initiating call');
            return;
          }

          console.log('📞 ========================================');
          console.log('📞 INCOMING_CALL EVENT RECEIVED');
          console.log('📞 ========================================');
          console.log(`   Caller: ${data.callerUsername}`);
          console.log(`   Call ID: ${data.callId}`);
          console.log(`   Type: ${data.callType}`);

          // CRITICAL: Cache the call immediately
          const cachedCall = {
            callId: data.callId,
            callerUsername: data.callerUsername,
            callerPfp: data.callerPfp,
            callType: data.callType,
            roomId: data.roomId,
            timestamp: Date.now()
          };

          try {
            localStorage.setItem(CACHED_CALL_KEY, JSON.stringify(cachedCall));
            console.log('💾 Call cached to localStorage');
          } catch (e) {
            console.error('❌ Failed to cache call:', e);
          }

          // Set pending call data
          pendingCallData = data;

          // Update modal UI
          const callerName = document.getElementById('callerName');
          const callType = document.getElementById('callType');
          const callerAvatar = document.getElementById('callerAvatar');

          if (callerName) callerName.textContent = `${data.callerUsername} is calling`;
          if (callType) callType.textContent = data.callType === 'video' ? 'Video Call' : 'Audio Call';
          if (callerAvatar) {
            callerAvatar.innerHTML = '';
            const pfp = createProfilePictureElement(data.callerPfp, data.callerUsername, 'w-full h-full');
            callerAvatar.appendChild(pfp);
          }

          // Show modal after 2 second delay
          setTimeout(() => {
            // Re-check guard conditions before showing
            const isBusy = activeCallConnectionState === 'initializing' || activeCallConnectionState === 'connecting' || activeCallConnectionState === 'connected' || isInitiatingCall;
            if (pendingCallData && pendingCallData.callId === data.callId && !isBusy) {
              incomingCallModal?.classList.remove('hidden');
              console.log(`✅ Showing incoming call modal after 2s delay`);
            } else {
              console.log(`📞 Call already handled or user became busy, skipping modal display`);
            }
          }, 2000);

          console.log('📞 ========================================\n');
        });

        socketInstance.on('call_accepted', (data) => {
          console.log('✅ Call accepted:', data);

          // Clear loading states
          isInitiatingCall = false;
          if (audioCallBtn) {
            audioCallBtn.classList.remove('call-btn-loading');
            audioCallBtn.classList.remove('calling');
            audioCallBtn.disabled = false;
          }
          if (videoCallBtn) {
            videoCallBtn.classList.remove('call-btn-loading');
            videoCallBtn.classList.remove('calling');
            videoCallBtn.disabled = false;
          }

          navigatingToCall = true;

          saveChatState(roomData.roomId, messagesCache);
          sessionStorage.setItem('returningFromCall', 'true');

          localStorage.setItem(ACTIVE_CALL_KEY, JSON.stringify({
            callId: data.callId,
            roomId: roomData.roomId,
            callType: data.callType,
            users: data.users
          }));

          console.log('🚀 Navigating to call');
          window.location.href = '/call.html';
        });

        socketInstance.on('call_declined', (data) => {
          console.log('❌ Call was declined');

          // Clear loading states
          toast('Call was declined', 'warning');
          isInitiatingCall = false;
          if (audioCallBtn) {
            audioCallBtn.classList.remove('call-btn-loading');
            audioCallBtn.classList.remove('calling');
            audioCallBtn.disabled = false;
          }
          if (videoCallBtn) {
            videoCallBtn.classList.remove('call-btn-loading');
            videoCallBtn.classList.remove('calling');
            videoCallBtn.disabled = false;
          }
        });

        socketInstance.on('call_state_update', (data) => {
          console.log('📞 Call state update:', data);

          if (data.isActive && data.participantCount > 0) {
            // CRITICAL FIX: Store call data so JOIN button can access it
            activeCallInRoom = {
              callId: data.callId,
              callType: data.callType,
              participantCount: data.participantCount
            };
            console.log(`📞 Stored active call: ${data.callId} with ${data.participantCount} participants`);

            updateCallButtonState(true, data);
          } else {
            // CRITICAL FIX: Clear call data when call ends
            activeCallInRoom = null;
            console.log('📞 Cleared active call - no participants');

            updateCallButtonState(false);
          }
        });


        socketInstance.on('call_ended_notification', (data) => {
          console.log('📞 Call ended:', data.callId);
          updateCallButtonState(false);
        });

        socketInstance.on('join_existing_call_success', (data) => {
          console.log('✅ ========================================');
          console.log('✅ JOIN_EXISTING_CALL_SUCCESS');
          console.log('✅ ========================================');
          console.log(`   callId: ${data.callId}`);
          console.log(`   callType: ${data.callType}`);
          console.log(`   roomId: ${data.roomId}`);

          // Remove loading states
          if (joinCallBtn) {
            joinCallBtn.classList.remove('join-btn-loading');
            joinCallBtn.disabled = false;
          }
          if (acceptCallBtn) {
            acceptCallBtn.disabled = false;
          }

          // ✅ CRITICAL FIX: Show call iframe IMMEDIATELY on successful join
          console.log('📱 Showing call iframe for joined call');
          let callIframeContainer = document.getElementById('callIframeContainer');

          if (!callIframeContainer) {
            console.log('📱 Call iframe container missing - creating it');
            callIframeContainer = document.createElement('div');
            callIframeContainer.id = 'callIframeContainer';
            callIframeContainer.className = 'fixed inset-0 z-[9999] bg-black'; // Visible immediately
            callIframeContainer.innerHTML = '<iframe id="callIframe" src="/call.html" class="w-full h-full border-none"></iframe>';
            document.body.appendChild(callIframeContainer);
            console.log('✅ Call iframe container created and call.html loaded');
          } else {
            callIframeContainer.classList.remove('hidden');
            const iframe = document.getElementById('callIframe');
            if (iframe) iframe.src = '/call.html'; // Reload to pick up new callId
            console.log('✅ Call iframe shown');
          }

          activeCallConnectionState = 'connecting';
          updateCallButtonStates();

        });
      }

      // ============================================
      // EVENT LISTENERS
      // ============================================

      function setupEventListeners() {
        // Sidebar
        sidebarToggle?.addEventListener('click', toggleSidebar, { passive: true });
        sidebarClose?.addEventListener('click', closeSidebar, { passive: true });
        sidebarOverlay?.addEventListener('click', closeSidebar, { passive: true });

        // Reply
        cancelReplyBtn?.addEventListener('click', hideReplyPreview, { passive: true });

        // Image viewer controls
        const imageViewerClose = document.getElementById('imageViewerClose');
        const imageViewerOverlay = document.getElementById('imageViewerOverlay');
        const imageViewerImage = document.getElementById('imageViewerImage');

        if (imageViewerClose) {
          imageViewerClose.addEventListener('click', closeImageViewer, { passive: true });
          console.log('✅ Image viewer close button listener attached');
        }

        if (imageViewerOverlay) {
          imageViewerOverlay.addEventListener('click', (e) => {
            // Close if clicking overlay background (not the image)
            if (e.target === imageViewerOverlay || e.target.id === 'imageViewerContainer') {
              closeImageViewer();
            }
          }, { passive: true });
          console.log('✅ Image viewer overlay listener attached');
        }

        if (imageViewerImage) {
          imageViewerImage.addEventListener('click', toggleImageZoom, { passive: true });
          console.log('✅ Image viewer zoom listener attached');
        }

        // ESC key to close image viewer
        document.addEventListener('keydown', (e) => {
          if (e.key === 'Escape') {
            const imageViewerOverlay = document.getElementById('imageViewerOverlay');
            if (imageViewerOverlay && imageViewerOverlay.classList.contains('active')) {
              closeImageViewer();
            }
          }
        });



        const attachmentBtn = document.querySelector('.attachment-btn');
        if (attachmentBtn) {
          attachmentBtn.addEventListener('click', async () => {
            console.log('📎 ========================================');
            console.log('📎 ATTACHMENT BUTTON CLICKED');
            console.log('📎 ========================================');

            // Create file input dynamically (ensures fresh state)
            const fileInput = document.createElement('input');
            fileInput.type = 'file';
            fileInput.accept = '*/*';
            fileInput.style.display = 'none';

            fileInput.addEventListener('change', async (e) => {
              const file = e.target.files?.[0];
              if (!file) {
                console.log('ℹ️ No file selected');
                console.log('📎 ========================================\n');
                return;
              }

              console.log(`📄 File selected: ${file.name}`);
              console.log(`   Size: ${(file.size / 1024 / 1024).toFixed(2)} MB`);
              console.log(`   Type: ${file.type}`);

              // Validate file
              const validation = validateFile(file);
              if (!validation.valid) {
                toast(validation.error, 'error');
                console.log('📎 ========================================\n');
                return;
              }

              try {
                // Show loading indicator
                attachmentBtn.disabled = true;
                attachmentBtn.classList.add('opacity-50', 'cursor-not-allowed');
                const originalIcon = attachmentBtn.innerHTML;
                attachmentBtn.innerHTML = '<span class="material-symbols-outlined text-xl animate-spin">progress_activity</span>';

                // Store file to IndexedDB (disk-backed, not RAM)
                const fileId = await storeFileToIndexedDB(file, roomData.roomId, null);

                console.log(`✅ File stored successfully: ${fileId}`);

                // Get file back from IndexedDB to show preview
                const storedFile = await getFileFromIndexedDB(fileId);
                if (storedFile) {
                  await showAttachmentPreview(
                    fileId,
                    storedFile.name,
                    storedFile.type,
                    storedFile.size,
                    storedFile.blob
                  );
                  toast(`File attached`, 'success');
                }

                // Restore button state
                attachmentBtn.disabled = false;
                attachmentBtn.classList.remove('opacity-50', 'cursor-not-allowed');
                attachmentBtn.innerHTML = originalIcon;

              } catch (error) {
                console.error('❌ Failed to store file:', error);
                toast('Failed to attach file. Please try again.', 'error');

                // Restore button state
                attachmentBtn.disabled = false;
                attachmentBtn.classList.remove('opacity-50', 'cursor-not-allowed');
                attachmentBtn.innerHTML = originalIcon;
              }

              console.log('📎 ========================================\n');

              // Clean up file input
              document.body.removeChild(fileInput);
            }, { once: true });

            // Trigger file picker
            document.body.appendChild(fileInput);
            fileInput.click();

          }, { passive: true });
        }


        // Remove attachment button
        const removeAttachmentBtn = document.getElementById('removeAttachment');
        if (removeAttachmentBtn) {
          removeAttachmentBtn.addEventListener('click', async () => {
            console.log('🗑️ Remove attachment clicked');

            if (pendingAttachment) {
              try {
                // Delete from IndexedDB
                await deleteFileFromIndexedDB(pendingAttachment.fileId);
                console.log(`✅ Deleted file: ${pendingAttachment.fileId}`);

                // Hide preview
                hideAttachmentPreview();

                toast('Attachment removed', 'success');
              } catch (error) {
                console.error('❌ Failed to remove attachment:', error);
                toast('Failed to remove attachment', 'error');
              }
            }
          }, { passive: true });
        }


        // ============================================
        // AUTO-LEAVE & NAVIGATION DETECTION
        // ============================================

        // 1. Link Click Interceptor
        document.addEventListener('click', async (e) => {
          const link = e.target.closest('a');
          if (!link) return;

          const h = link.getAttribute('href');
          if (!h || h.startsWith('#') || h.startsWith('javascript:')) return;

          // Normalize path
          const url = new URL(link.href, window.location.origin);
          const isCallPage = url.pathname === '/call.html' || url.pathname === 'call.html' || url.pathname.endsWith('/call.html');

          if (isCallPage) {
            console.log('🏁 Navigating to call page - suppressing auto-leave');
            navigatingToCall = true;
          } else if (url.origin === window.location.origin) {
            // Internal navigation to other pages
            console.log(`🚪 Navigating to ${url.pathname} - triggering leave sequence`);
            e.preventDefault();
            await exitRoomSequence(url.href);
          }
        });

        // 2. Page Unload (Refresh/Close/Address Bar)
        window.addEventListener('beforeunload', (event) => {
          const hasBackgroundCall = sessionStorage.getItem('hasBackgroundCall') === 'true';

          if (navigatingToCall || hasBackgroundCall || isInBackgroundCall) {
            console.log('ℹ️ Unloading but navigating to call or in background');
            return;
          }

          // CRITICAL: DO NOT send leave_room on beforeunload.
          // This allows the room to persist during refreshes/temporary disconnects.
          // The server-side heartbeat timeout (35s) will clean up orphaned rooms.

          console.log('🚪 Page unloading (refresh/close) - relying on server-side timeout for cleanup');

          try {
            if (roomData?.roomId) {
              saveChatState(roomData.roomId, messagesCache);
              localStorage.setItem('currentRoom', JSON.stringify(roomData));
              sessionStorage.setItem('vibe_chat_reload_restore_until', String(Date.now() + 60000));
            }
          } catch { }

          // We can still stop the local heartbeat to be clean
          stopHeartbeat();
          stopTyping();
        });

        // 3. Visibility Change (Detect minimizing app/switching tabs on mobile)
        document.addEventListener('visibilitychange', () => {
          if (document.visibilityState === 'hidden') {
            lastVisibilityHiddenAt = Date.now();
            try {
              if (roomData?.roomId) saveChatState(roomData.roomId, messagesCache);
            } catch { }
            const hasBackgroundCall = sessionStorage.getItem('hasBackgroundCall') === 'true';
            if (!navigatingToCall && !hasBackgroundCall && !isInBackgroundCall) {
              console.log('📱 Tab hidden - user might be leaving chat');
              reportChatPresenceContext('visibility_hidden', {
                allowRedirect: false,
                keepalive: true,
                source: 'chat_visibility_hidden'
              }).catch(() => { });
            }
          } else {
            const hiddenDuration = lastVisibilityHiddenAt ? (Date.now() - lastVisibilityHiddenAt) : 0;
            reconnectSocketIfNeeded(hiddenDuration > 60000 ? 'visibility_resume_long_idle' : 'visibility_resume');
            reportChatPresenceContext('visibility_visible', {
              allowRedirect: !isSocialClubMode,
              keepalive: true,
              source: 'chat_visibility_visible'
            }).catch(() => { });
            validateChatPresenceState('visibility_visible').catch(() => { });
          }
        });

        window.addEventListener('online', () => {
          reconnectSocketIfNeeded('network_online');
        });

        window.addEventListener('offline', () => {
          console.warn('📴 Network offline detected');
        });


        if (audioCallBtn) {
          audioCallBtn.addEventListener('click', async () => {
            console.log('📞 ========================================');
            console.log('📞 AUDIO CALL BUTTON CLICKED');
            console.log('📞 ========================================');
            console.log(`   Authentication state: ${isSocketAuthenticated}`);
            console.log(`   Socket connected: ${socketInstance?.connected}`);
            console.log(`   Already initiating: ${isInitiatingCall}`);

            // CRITICAL: Block if already initiating
            if (isInitiatingCall) {
              console.log('⚠️ Call already being initiated, ignoring click');
              console.log('📞 ========================================\n');
              return;
            }

            // CRITICAL: Wait for authentication if not ready
            if (!isSocketAuthenticated) {
              console.log('⏳ Not authenticated yet - waiting...');
              audioCallBtn.disabled = true;
              audioCallBtn.classList.add('animate-pulse');

              try {
                await waitForAuthentication();
                console.log('✅ Authentication complete, proceeding with call');
              } catch (e) {
                console.error('❌ Authentication wait failed:', e);
                toast('Connection failed. Please refresh.', 'error');
                audioCallBtn.disabled = false;
                audioCallBtn.classList.remove('animate-pulse');
                console.log('📞 ========================================\n');
                return;
              }
            }

            // CRITICAL: Verify socket still connected
            if (!socketInstance || !socketInstance.connected) {
              console.error('❌ Socket disconnected during call initiation');
              toast('Connection lost. Please refresh.', 'error');
              console.log('📞 ========================================\n');
              return;
            }

            if (!hasJoinedRoom || !roomData?.roomId) {
              console.warn('⚠️ Chat room is not ready for call initiation yet');
              toast('Chat is still connecting. Please try again in a moment.', 'warning');
              audioCallBtn.disabled = true;
              audioCallBtn.classList.remove('animate-pulse');
              updateCallButtonStates();
              console.log('📞 ========================================\n');
              return;
            }

            console.log('✅ All checks passed - initiating audio call');
            isInitiatingCall = true;

            try {
              if (callInitiationTimeoutId) clearTimeout(callInitiationTimeoutId);
            } catch { }
            callInitiationTimeoutId = null;

            // Add loading state
            audioCallBtn.classList.add('call-btn-loading', 'animate-pulse');
            audioCallBtn.disabled = true;
            if (videoCallBtn) {
              videoCallBtn.classList.add('calling');
              videoCallBtn.disabled = true;
            }

            socketInstance.emit('initiate_call', {
              roomId: roomData.roomId,
              callType: 'audio'
            });

            console.log('📤 Audio call initiation sent to server');
            toast('Calling...', 'success');

            // Timeout with recovery (iframe + auth + signaling can easily take >2s)
            callInitiationTimeoutId = setTimeout(() => {
              if (!isInitiatingCall) return;
              let hasActiveCall = false;
              try { hasActiveCall = !!localStorage.getItem(ACTIVE_CALL_KEY); } catch { }
              if (hasActiveCall) return;

              console.log('⏰ Call initiation timeout (12s)');
              isInitiatingCall = false;
              callInitiationTimeoutId = null;
              audioCallBtn.classList.remove('call-btn-loading', 'animate-pulse');
              if (videoCallBtn) {
                videoCallBtn.classList.remove('calling');
              }

              // CRITICAL: Re-enable buttons after timeout
              if (isSocketAuthenticated) {
                enableCallButtons();
              }

              toast('Call timeout - no response', 'warning');
            }, 12000);

            console.log('📞 ========================================\n');

          }, { passive: true });
        }

        if (videoCallBtn) {
          videoCallBtn.addEventListener('click', async () => {
            console.log('📞 ========================================');
            console.log('📞 VIDEO CALL BUTTON CLICKED');
            console.log('📞 ========================================');
            console.log(`   Authentication state: ${isSocketAuthenticated}`);
            console.log(`   Socket connected: ${socketInstance?.connected}`);
            console.log(`   Already initiating: ${isInitiatingCall}`);

            // CRITICAL: Block if already initiating
            if (isInitiatingCall) {
              console.log('⚠️ Call already being initiated, ignoring click');
              console.log('📞 ========================================\n');
              return;
            }

            // CRITICAL: Wait for authentication if not ready
            if (!isSocketAuthenticated) {
              console.log('⏳ Not authenticated yet - waiting...');
              videoCallBtn.disabled = true;
              videoCallBtn.classList.add('animate-pulse');

              try {
                await waitForAuthentication();
                console.log('✅ Authentication complete, proceeding with call');
              } catch (e) {
                console.error('❌ Authentication wait failed:', e);
                toast('Connection failed. Please refresh.', 'error');
                videoCallBtn.disabled = false;
                videoCallBtn.classList.remove('animate-pulse');
                console.log('📞 ========================================\n');
                return;
              }
            }

            // CRITICAL: Verify socket still connected
            if (!socketInstance || !socketInstance.connected) {
              console.error('❌ Socket disconnected during call initiation');
              toast('Connection lost. Please refresh.', 'error');
              console.log('📞 ========================================\n');
              return;
            }

            if (!hasJoinedRoom || !roomData?.roomId) {
              console.warn('⚠️ Chat room is not ready for call initiation yet');
              toast('Chat is still connecting. Please try again in a moment.', 'warning');
              videoCallBtn.disabled = true;
              videoCallBtn.classList.remove('animate-pulse');
              updateCallButtonStates();
              console.log('📞 ========================================\n');
              return;
            }

            console.log('✅ All checks passed - initiating video call');
            isInitiatingCall = true;

            try {
              if (callInitiationTimeoutId) clearTimeout(callInitiationTimeoutId);
            } catch { }
            callInitiationTimeoutId = null;

            // Add loading state
            videoCallBtn.classList.add('call-btn-loading', 'animate-pulse');
            videoCallBtn.disabled = true;
            if (audioCallBtn) {
              audioCallBtn.classList.add('calling');
              audioCallBtn.disabled = true;
            }

            socketInstance.emit('initiate_call', {
              roomId: roomData.roomId,
              callType: 'video'
            });

            console.log('📤 Video call initiation sent to server');
            toast('Calling...', 'success');

            // Timeout with recovery (iframe + auth + signaling can easily take >2s)
            callInitiationTimeoutId = setTimeout(() => {
              if (!isInitiatingCall) return;
              let hasActiveCall = false;
              try { hasActiveCall = !!localStorage.getItem(ACTIVE_CALL_KEY); } catch { }
              if (hasActiveCall) return;

              console.log('⏰ Call initiation timeout (12s)');
              isInitiatingCall = false;
              callInitiationTimeoutId = null;
              videoCallBtn.classList.remove('call-btn-loading', 'animate-pulse');
              if (audioCallBtn) {
                audioCallBtn.classList.remove('calling');
              }

              // CRITICAL: Re-enable buttons after timeout
              if (isSocketAuthenticated) {
                enableCallButtons();
              }

              toast('Call timeout - no response', 'warning');
            }, 12000);

            console.log('📞 ========================================\n');

          }, { passive: true });
        }



        if (joinCallBtn) {
          let joinClickInFlight = false;
          let lastJoinClickAt = 0;

          joinCallBtn.addEventListener('click', () => {
            console.log('🔘 ========================================');
            console.log('🔘 JOIN CALL BUTTON CLICKED');
            console.log('🔘 ========================================');

            const now = Date.now();
            if (joinClickInFlight) {
              console.warn('⚠️ Join call click ignored: already in-flight');
              return;
            }
            if (now - lastJoinClickAt < 800) {
              console.warn('⚠️ Join call click throttled');
              return;
            }
            lastJoinClickAt = now;
            joinClickInFlight = true;

            const resetJoinBtnState = () => {
              joinClickInFlight = false;
              try { joinCallBtn.classList.remove('join-btn-loading'); } catch { }
              try { joinCallBtn.disabled = false; } catch { }
            };

            // Check if this is "Back to Call" or "Join Call"
            const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
            let userInCall = false;
            if (activeCallStr) {
              try {
                const parsed = JSON.parse(activeCallStr);
                userInCall = parsed?.roomId === roomData?.roomId && !!parsed?.callId;
              } catch {
                try { localStorage.removeItem(ACTIVE_CALL_KEY); } catch { }
                userInCall = false;
              }
            }

            console.log(`   activeCallStr exists: ${!!activeCallStr}`);
            console.log(`   userInCall: ${userInCall}`);
            console.log(`   activeCallInRoom exists: ${!!activeCallInRoom}`);

            if (userInCall) {
              // User is already in call - show call iframe or navigate
              console.log('📞 BACK TO CALL MODE');

              // CRITICAL: Force show local iframe first
              let callIframeContainer = document.getElementById('callIframeContainer');

              if (!callIframeContainer) {
                // Create iframe container if it doesn't exist
                console.log('📱 Call iframe container missing - creating it');
                callIframeContainer = document.createElement('div');
                callIframeContainer.id = 'callIframeContainer';
                callIframeContainer.className = 'fixed inset-0 z-[9999] bg-black';
                callIframeContainer.innerHTML = '<iframe id="callIframe" src="/call.html" class="w-full h-full border-none"></iframe>';
                document.body.appendChild(callIframeContainer);
                console.log('✅ Call iframe container created and call.html loaded');
              } else {
                // Iframe exists, just show it
                callIframeContainer.classList.remove('hidden');
                console.log('✅ Call iframe shown');
              }

              // Fallback for iframe mode (if applicable)
              if (window.parent && window.parent !== window) {
                console.log('📱 In iframe - sending showCall to parent (redundant but safe)');
                window.parent.postMessage({ action: 'REQUEST_SHOW_CALL' }, '*');
              }

              // CRITICAL: Force UI update to show we are in call
              activeCallConnectionState = 'connected';
              updateCallButtonStates();

              // Back-to-call is synchronous UI show; allow next click.
              resetJoinBtnState();

            } else {
              if (!activeCallInRoom) {
                console.error('❌ Cannot join call: activeCallInRoom is null');
                toast('Failed to join call data. Please refresh.', 'error');
                resetJoinBtnState();
                return;
              }

              joinCallBtn.classList.add('join-btn-loading');
              joinCallBtn.disabled = true;
              console.log('⏳ Join button set to loading state');

              // CRITICAL FIX: Store call data BEFORE emitting join_existing_call
              const callDataToStore = {
                callId: activeCallInRoom.callId,
                roomId: roomData.roomId,
                callType: activeCallInRoom.callType,
                storedAt: Date.now(),
                users: []
              };

              localStorage.setItem(ACTIVE_CALL_KEY, JSON.stringify(callDataToStore));
              console.log('💾 Stored call data to localStorage (pre-join):');
              console.log('   ', JSON.stringify(callDataToStore, null, 2));

              const eventData = {
                callId: activeCallInRoom.callId,
                roomId: roomData.roomId
              };

              console.log('📤 Emitting join_existing_call with data:');
              console.log('   ', JSON.stringify(eventData, null, 2));

              if (!socketInstance || !socketInstance.connected) {
                console.error('❌ Socket not connected!');
                toast('Connection lost. Please refresh the page.', 'error');
                resetJoinBtnState();
                console.log('🔘 ========================================\n');
                return;
              }

              socketInstance.emit('join_existing_call', eventData);

              toast('Joining call...', 'success');
              console.log('✅ Event emitted successfully');
              console.log('🔘 ========================================\n');

              // If we never navigate / never get server response, don't keep the button locked forever.
              setTimeout(() => {
                if (joinClickInFlight) {
                  console.warn('⏱️ Join click watchdog releasing button (no navigation yet)');
                  resetJoinBtnState();
                }
              }, 6000);
            }
          }, { passive: true });
        }

        if (acceptCallBtn) {
          acceptCallBtn.addEventListener('click', () => {
            if (pendingCallData) {
              const callToAccept = pendingCallData;
              pendingCallData = null;

              console.log('✅ ========================================');
              console.log('✅ USER ACCEPTED INCOMING CALL');
              console.log('✅ ========================================');
              console.log(`   callId: ${callToAccept.callId}`);

              // Clear cached call
              localStorage.removeItem(CACHED_CALL_KEY);

              // Hide modal
              incomingCallModal?.classList.add('hidden');

              // Mark navigating
              navigatingToCall = true;

              // Add loading state
              acceptCallBtn.classList.add('join-btn-loading');
              acceptCallBtn.disabled = true;

              // Save chat state
              saveChatState(roomData.roomId, messagesCache);
              sessionStorage.setItem('returningFromCall', 'true');

              // ✅ CRITICAL FIX: Store call data BEFORE emitting join_existing_call
              localStorage.setItem(ACTIVE_CALL_KEY, JSON.stringify({
                callId: callToAccept.callId,
                roomId: roomData.roomId,
                callType: callToAccept.callType,
                storedAt: Date.now(),
                users: []
              }));
              console.log('💾 Stored call data to localStorage (pre-join)');

              console.log('📤 Emitting join_existing_call to server');

              // Use join_existing_call to ensure server adds us to participants list first
              socketInstance?.emit('join_existing_call', {
                callId: callToAccept.callId,
                roomId: roomData.roomId
              });

              console.log('✅ Join request sent - will navigate on success');
              console.log('✅ ========================================\n');

              // Don't navigate here - wait for join_existing_call_success event
            }
          }, { passive: true });
        }

        if (declineCallBtn) {
          declineCallBtn.addEventListener('click', () => {
            if (pendingCallData) {
              const callToDecline = pendingCallData;
              pendingCallData = null;

              console.log('❌ ========================================');
              console.log('❌ USER DECLINED INCOMING CALL');
              console.log('❌ ========================================');
              console.log(`   callId: ${callToDecline.callId}`);
              console.log(`   Call remains active - user can still join via JOIN button`);

              // Clear cached call
              localStorage.removeItem(CACHED_CALL_KEY);
              console.log('🗑️ Cleared cached call after decline');

              // Hide modal
              incomingCallModal?.classList.add('hidden');

              console.log('✅ Modal dismissed - call still active in room');
              console.log('❌ ========================================\n');
            }
          }, { passive: true });
        }

        if (messageForm) {
          messageForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const message = messageInput?.value.trim();

            // Allow sending if there's a message OR an attachment
            if (!message && !pendingAttachment) {
              console.log('⚠️ No message or attachment to send');
              return;
            }

            console.log('📤 ========================================');
            console.log('📤 SENDING MESSAGE TO SERVER');
            console.log('📤 ========================================');
            console.log(`   Message: ${message || '[attachment only]'}`);
            console.log(`   Has attachment: ${!!pendingAttachment}`);

            // ============================================
            // PREPARE SERVER MESSAGE DATA
            // ============================================
            const serverMessageData = {
              roomId: roomData.roomId,
              message: message || ''
            };

            // Add reply data for server
            if (replyingTo) {
              serverMessageData.replyTo = {
                messageId: replyingTo.messageId,
                username: replyingTo.username,
                message: replyingTo.message,
                userId: replyingTo.userId
              };
              hideReplyPreview();
            }

            // ============================================
            // HANDLE ATTACHMENT - UPLOAD FIRST, THEN BROADCAST METADATA
            // ============================================
            if (pendingAttachment) {
              try {
                console.log('📎 ========================================');
                console.log('📎 PREPARING ATTACHMENT UPLOAD');
                console.log('📎 ========================================');
                console.log(`   FileID: ${pendingAttachment.fileId}`);
                console.log(`   File: ${pendingAttachment.name}`);
                console.log(`   Size: ${(pendingAttachment.size / 1024 / 1024).toFixed(2)} MB`);

                // Show upload progress UI
                const progressDiv = document.createElement('div');
                progressDiv.id = 'uploadProgress';
                progressDiv.className = 'fixed top-4 right-4 bg-[#161b22] border border-[#30363d] rounded-lg px-4 py-3 shadow-lg z-50';
                progressDiv.innerHTML = `
          <div class="flex items-center gap-3">
            <div class="animate-spin">
              <span class="material-symbols-outlined text-[#06b6d4]">progress_activity</span>
            </div>
            <div>
              <div class="text-sm font-semibold text-gray-200">${pendingAttachment.name}</div>
              <div id="uploadProgressText" class="text-xs text-gray-400">Preparing...</div>
            </div>
          </div>
        `;
                document.body.appendChild(progressDiv);

                // Get file from IndexedDB
                const fileData = await getFileFromIndexedDB(pendingAttachment.fileId);

                if (!fileData || !fileData.blob) {
                  throw new Error('Failed to retrieve file from storage');
                }

                console.log(`✅ File retrieved from IndexedDB`);

                // Upload payload
                const file = new File([fileData.blob], fileData.name, { type: fileData.type });
                const progressText = document.getElementById('uploadProgressText');
                if (progressText) progressText.textContent = 'Uploading to secure storage...';

                const uploadedAttachment = await uploadAttachmentToServer(file, roomData.roomId);
                if (!uploadedAttachment?.fileId || !uploadedAttachment?.url) {
                  throw new Error('Server returned invalid attachment metadata');
                }

                // Keep local cache aligned with server fileId for fast fallback previews.
                if (uploadedAttachment.fileId !== pendingAttachment.fileId) {
                  await storeFileToIndexedDB(file, roomData.roomId, null, uploadedAttachment.fileId).catch(() => { });
                  await deleteFileFromIndexedDB(pendingAttachment.fileId).catch(() => { });
                }

                serverMessageData.attachment = uploadedAttachment;
                if (progressText) progressText.textContent = 'Broadcasting message...';

                // IMMEDIATE: Hide preview for responsive UX
                hideAttachmentPreview();

                console.log('📤 Emitting message metadata to server...');
                safeSocketEmit('chat_message', serverMessageData);

                console.log('✅ Attachment uploaded and message broadcasted');

                // Remove progress UI
                progressDiv.remove();

                toast(`File uploaded: ${uploadedAttachment.name || file.name}`, 'success');

                console.log('📎 ========================================\\n');

              } catch (error) {
                console.error('❌ Attachment transmission failed:', error);

                // Remove progress UI
                const progressDiv = document.getElementById('uploadProgress');
                if (progressDiv) progressDiv.remove();

                toast('Failed to send attachment. Please try again.', 'error');
                console.log('📎 ========================================\n');
                return; // Don't clear input on failure
              }
            } else {
              // No attachment - send message normally
              console.log('📤 Emitting text-only message...');
              safeSocketEmit('chat_message', serverMessageData);
              console.log('✅ Message sent to server');
            }

            console.log('📤 ========================================\n');

            // ============================================
            // CLEAR INPUT AND ATTACHMENTS
            // ============================================
            if (messageInput) {
              messageInput.value = '';
              messageInput.style.height = 'auto';
            }

            // Clear typing state immediately on send
            stopTyping();
            if (typingTimeout) clearTimeout(typingTimeout);

            // Note: Attachment preview is now hidden immediately at start of send
            // (moved earlier in the flow for instant feedback)
          });
        }

        leaveBtn?.addEventListener('click', async () => {
          console.log('🚪 Leave button clicked - initiating exit sequence');
          await exitRoomSequence();
        });

      }

      // ============================================
      // INITIALIZATION
      // ============================================


      function renderUserList(users) {
        const usersListEl = document.getElementById('usersList');
        if (!usersListEl) return;

        // Clear current list to prevent duplicates (since we get full list)
        usersListEl.innerHTML = '';

        users.forEach(user => {
          const isMe = currentUser && user.userId === currentUser.userId;

          const userCard = document.createElement('div');
          userCard.id = `user-${user.userId}`;
          userCard.className = 'user-card flex items-center gap-3 p-2 rounded-lg bg-[#2a2d33] border border-gray-700 transition-all duration-300 animate-fade-in';

          // Profile Picture
          const pfpContainer = createProfilePictureElement(user.pfpUrl, user.username, 'w-8 h-8');

          // Username
          const infoContainer = document.createElement('div');
          infoContainer.className = 'min-w-0 flex-1';

          const nameEl = document.createElement('div');
          nameEl.className = 'text-sm font-medium text-gray-200 truncate';
          nameEl.textContent = (user.username || 'User') + (isMe ? ' (You)' : '');

          infoContainer.appendChild(nameEl);

          userCard.appendChild(pfpContainer);
          userCard.appendChild(infoContainer);

          usersListEl.appendChild(userCard);
        });
      }

      try {
        await initFileDB();
        console.log('✅ File storage initialized');

        // Clean up any orphaned files from previous sessions
        if (roomData?.roomId) {
          const fileCount = await getRoomFileCount(roomData.roomId);
          if (fileCount > 0) {
            console.log(`ℹ️ Found ${fileCount} existing file(s) for this room`);
          }
        }
      } catch (error) {
        console.error('⚠️ File storage initialization failed:', error);
        console.error('   File attachments will be disabled');
        // Disable attachment button if IndexedDB fails
        const attachBtn = document.querySelector('.attachment-btn');
        if (attachBtn) {
          attachBtn.disabled = true;
          attachBtn.classList.add('opacity-50', 'cursor-not-allowed');
          attachBtn.title = 'File attachments unavailable';
        }
      }

      try {
        // Check if we're returning from a call OR if we're in an iframe (chat overlay)
        const returningFromCall = sessionStorage.getItem('returningFromCall') === 'true';
        const isBackgroundCall = sessionStorage.getItem('backgroundCallMode') === 'true';
        const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
        const isInIframe = window.parent && window.parent !== window;
        const cachedRoomStr = localStorage.getItem('currentRoom');
        const cachedChatStateStr = localStorage.getItem(CHAT_STATE_KEY);

        // CRITICAL: If there's an active call in this room, we're likely returning from it
        let shouldPreserveState = returningFromCall || isBackgroundCall || !!cachedRoomStr || !!cachedChatStateStr;

        if (!shouldPreserveState && activeCallStr && isInIframe) {
          try {
            const activeCall = JSON.parse(activeCallStr);
            const roomStr = localStorage.getItem('currentRoom');
            if (roomStr) {
              const room = JSON.parse(roomStr);
              if (activeCall.roomId === room.roomId) {
                console.log('🔍 Active call detected in iframe - treating as return from call');
                shouldPreserveState = true;
              }
            }
          } catch (e) {
            console.warn('Failed to parse call/room data:', e);
          }
        }


        console.log('🔍 Page load context:', {
          returningFromCall,
          isBackgroundCall,
          isInIframe,
          shouldPreserveState,
          hasActiveCall: !!activeCallStr
        });

        if (!shouldPreserveState) {
          // Only clear if this is a fresh entry to chat (not returning from call)
          console.log('🧹 Fresh chat entry - clearing old state');
          clearAllChatData();
        } else {
          console.log('🔄 Returning from call - preserving chat state');
          // Don't clear - we want to restore the previous state
        }

        // Auth check (allow anonymous)
        await _Auth.requireAuth();
      } catch (err) {
        console.error('❌ Auth failed:', err);
        toast('Failed to sign in', 'error');
        return;
      }


      // CRITICAL: Initialize floating call popup elements early
      console.log('🎈 Pre-initializing floating call popup');

      // Check if returning from call
      const returningFromCall = sessionStorage.getItem('returningFromCall');
      if (returningFromCall === 'true') {
        console.log('🔄 Returning from call - popup will be shown after room_joined');
        console.log('🔍 Active call check:', !!localStorage.getItem(ACTIVE_CALL_KEY));
      }

      const roomDataStr = localStorage.getItem('currentRoom');
      if (!roomDataStr) {
        if (!isSocialClubMode) {
          console.warn('ℹ️ No currentRoom in localStorage - will create room immediately');
        }
      }

      try {
        if (isSocialClubMode && roomDataStr) {
          roomData = JSON.parse(roomDataStr);
          currentRoomId = roomData?.roomId || null;
          console.log('🎭 Social Club reload/reconnect - restoring cached currentRoom before server sync');
          console.log(`   roomId: ${currentRoomId || 'unknown'}`);
        } else if (isSocialClubMode) {
          roomData = null;
          currentRoomId = null;
          console.log('🎭 Social Club mode - ignoring cached currentRoom, will join via join_social_club');
        } else if (roomDataStr) {
          roomData = JSON.parse(roomDataStr);
          currentRoomId = roomData?.roomId || null;
          console.log('📦 Loaded room data from localStorage:');
          console.log(`   roomId: ${roomData.roomId}`);
        } else {
          roomData = null;
          currentRoomId = null;
        }

        // CRITICAL: Don't trust localStorage expiresAt - wait for server
        if (roomData && roomData.expiresAt) {
          console.log(`   LocalStorage expiresAt: ${new Date(roomData.expiresAt).toISOString()}`);
          console.log(`   ⚠️ Will be replaced with server time after connection`);
        } else {
          console.log(`   No expiresAt in localStorage - will get from server`);
        }

        if (roomData && !roomData.roomId) {
          throw new Error('Invalid room data: missing roomId');
        }
        checkForBackgroundCall();
      } catch (err) {
        console.error('❌ Invalid room data:', err);
        toast('Invalid room data', 'error');
        clearChatState(true);
        setTimeout(() => window.location.href = '/mood.html', 1500);
        return;
      }

      // Get Firebase user (anonymous allowed)
      const firebaseUser = firebase.auth().currentUser;
      if (!firebaseUser) {
        toast('Sign-in required', 'error');
        return;
      }

      // Ensure a server-side guest profile exists for anonymous users (and upgrades guest_* usernames)
      let ensuredGuestUsername = null;
      try {
        const token = await firebaseUser.getIdToken();
        const moodForGuest = isSocialClubMode ? 'social_club' : (selectedMood || 'social_club');
        const ensureRes = await fetch('/api/users/ensure-guest', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ mood: moodForGuest })
        });
        if (ensureRes.ok) {
          const ensurePayload = await ensureRes.json().catch(() => null);
          ensuredGuestUsername = (ensurePayload?.user?.username || '').trim() || null;
          try {
            if (ensuredGuestUsername) localStorage.setItem('guest_username', ensuredGuestUsername);
          } catch { }
        }
      } catch (e) {
        console.warn('⚠️ ensure-guest failed (continuing):', e?.message || e);
      }

      // Get current user data
      try {
        let userData = await _API.get('/api/users/me');
        const boring = typeof userData?.username === 'string' && /^guest_/i.test(userData.username.trim());
        if (boring && ensuredGuestUsername) {
          userData = {
            ...userData,
            username: ensuredGuestUsername
          };
        } else if (boring && !ensuredGuestUsername) {
          try {
            userData = await _API.get('/api/users/me');
          } catch { }
        }

        currentUser = {
          userId: userData._id,
          username: userData.username,
          pfpUrl: userData.pfpUrl
        };
      } catch (err) {
        console.error('❌ Failed to get user data:', err);
        toast('Failed to load user data', 'error');
        return;
      }


      // CRITICAL: Start with buttons DISABLED until authenticated
      console.log('🎚️ Initializing call buttons (disabled until authenticated)');
      disableCallButtons(false); // false = don't show loading, just disabled

      const cachedState = loadChatState();

      if (cachedState && roomData && cachedState.roomId === roomData.roomId) {
        console.log('🎨 Restoring UI from cache IMMEDIATELY on page load');
        console.log(`   Cached button state: ${cachedState.callButtonState}`);
        console.log(`   Cached activeCallInRoom:`, cachedState.activeCallInRoom);

        // Restore activeCallInRoom immediately
        if (cachedState.activeCallInRoom) {
          activeCallInRoom = cachedState.activeCallInRoom;
          console.log('💾 Pre-loaded activeCallInRoom from cache');
        } else if (cachedState.callButtonState === 'back') {
          // Reconstruct from ACTIVE_CALL_KEY
          const activeCallStr = localStorage.getItem(ACTIVE_CALL_KEY);
          if (activeCallStr) {
            try {
              const activeCallData = JSON.parse(activeCallStr);
              activeCallInRoom = {
                callId: activeCallData.callId,
                callType: activeCallData.callType,
                participantCount: 2
              };
              console.log('💾 Pre-loaded activeCallInRoom from ACTIVE_CALL_KEY');
            } catch (e) {
              console.error('Failed to reconstruct activeCallInRoom:', e);
            }
          }
        }

        // Set button state immediately from cache (but keep disabled until auth)
        if (cachedState.callButtonState === 'back') {
          updateCallButtonState(true, activeCallInRoom || { participantCount: 1 });
          console.log('✅ "Back to Call" button shown immediately from cache (disabled until auth)');
        } else if (cachedState.callButtonState === 'join' && activeCallInRoom) {
          updateCallButtonState(true, activeCallInRoom);
          console.log('✅ "Join Call" button shown immediately from cache (disabled until auth)');
        } else {
          setInitialCallButtonState();
        }
      } else {
        // No cache - use normal initial state (disabled)
        setInitialCallButtonState();
      }
      // Render initial users
      const initialUsers = (roomData && Array.isArray(roomData.users)) ? roomData.users : [];
      if (usersList && initialUsers.length) {
        initialUsers.forEach(user => {
          const userCard = document.createElement('div');
          userCard.className = 'user-card flex items-center gap-2 sm:gap-3 p-2 sm:p-3 rounded-lg hover:bg-gray-50 transition-all cursor-pointer';

          const pfpElement = createProfilePictureElement(user.pfpUrl, user.username, 'w-10 h-10 sm:w-12 sm:h-12');

          const userInfo = document.createElement('div');
          userInfo.className = 'flex-1 min-w-0';

          const nameSpan = document.createElement('div');
          nameSpan.className = 'text-sm font-semibold truncate';
          nameSpan.textContent = user.username || 'User';

          const statusSpan = document.createElement('div');
          statusSpan.className = 'text-xs text-gray-500 flex items-center gap-1';
          statusSpan.innerHTML = '<span class="flex h-2 w-2 rounded-full bg-green-500 flex-shrink-0"></span> Online';

          userInfo.appendChild(nameSpan);
          userInfo.appendChild(statusSpan);

          userCard.appendChild(pfpElement);
          userCard.appendChild(userInfo);
          usersList.appendChild(userCard);
        });

        setupUserCardClick();

        if (onlineCount) {
          onlineCount.textContent = `${initialUsers.length} Online`;
        }
      }

      if (timerInterval) clearInterval(timerInterval);

      // Initialize socket
      try {
        const idToken = await firebaseUser.getIdToken();
        const socketUrl = window.location.origin;
        const sessionId = getScopedSocketSessionId('chat');
        const tabId = window.tabManager?.tabId || null;

        socketInstance = io(socketUrl, {
          transports: ['websocket', 'polling'],
          reconnection: true,
          reconnectionDelay: 500,
          reconnectionDelayMax: 10000,
          randomizationFactor: 0.5,
          reconnectionAttempts: 1000,
          auth: { token: idToken, sessionId, tabId },
          timeout: 15000,
          closeOnBeforeunload: false
        });

        setupSocketHandlers();
      } catch (err) {
        console.error('❌ Socket initialization failed:', err);
        toast('Connection failed', 'error');
      }

      // Setup all event listeners
      setupEventListeners();

      // Setup back button handler
      setupBackButtonHandler();

      console.log('✅ Chat initialization complete');

    } catch (err) {
      console.error('❌ Chat bootstrap failed:', err);
      try { toast('Failed to initialize chat', 'error'); } catch { }
    }

    })();

