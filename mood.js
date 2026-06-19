// ───────────────────────────────
// 1. Console Silencing
// ───────────────────────────────
(function () {
  try {
    window.__VIBE_SILENCE_CONSOLE__ = false;
  } catch { }
})();

// ───────────────────────────────
// 2. Contact Support Modal
// ───────────────────────────────
(function () {
  const btn = document.getElementById('contactSupportBtn');
  const modal = document.getElementById('contactSupportModal');
  const copyBtn = document.getElementById('contactSupportCopyBtn');
  const emailEl = document.getElementById('contactSupportEmail');
  const hint = document.getElementById('contactSupportCopyHint');
  if (!btn || !modal || !copyBtn || !emailEl) return;

  const open = () => {
    modal.classList.remove('hidden');
    modal.style.display = 'flex';
  };
  const close = () => {
    modal.classList.add('hidden');
    modal.style.display = 'none';
    if (hint) hint.classList.add('hidden');
  };

  btn.addEventListener('click', open);
  modal.addEventListener('click', (e) => {
    const t = e.target;
    if (t && t.closest && t.closest('[data-close="1"]')) close();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') close();
  });

  copyBtn.addEventListener('click', async () => {
    const text = emailEl.textContent || 'contact@vibegra.com';
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        ta.remove();
      }
      if (hint) {
        hint.classList.remove('hidden');
        setTimeout(() => hint.classList.add('hidden'), 1200);
      }
    } catch { }
  });
})();

// ───────────────────────────────
// 3. Firebase Initialization + StateManager
// ───────────────────────────────
(function () {
  if (window.StateManager) {
    const state = window.StateManager.getState();
    if (state.room && window.StateManager.isRoomExpired()) {
      window.StateManager.clearRoom();
    }
    window.StateManager.setPage('mood');
  }

  try {
    if (!firebase.apps?.length) {
      firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__);
    }
    firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL);
  } catch (err) {
    console.error('Firebase init error', err);
  }
})();

// ───────────────────────────────
// 4. VibePWA Init
// ───────────────────────────────
(function () {
  try { window.VibePWA && window.VibePWA.init && window.VibePWA.init(); } catch { }
})();

// ───────────────────────────────
// 5. Social Club Mount (Mood Page)
// ───────────────────────────────
(function () {
  const mount = () => {
    try {
      const el = document.getElementById('socialClubMountMood');
      if (el && window.SocialClub && typeof window.SocialClub.mount === 'function') {
        window.SocialClub.mount(el);
      }
    } catch { }
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mount);
  } else {
    mount();
  }
})();

// ───────────────────────────────
// 6. Social Club Owner Toggle (Mood Page)
// ───────────────────────────────
(function () {
  const root = document.getElementById('socialClubOwnerToggleMood');
  const checkbox = document.getElementById('socialClubOwnerToggleCheckboxMood');
  const stateEl = document.getElementById('socialClubOwnerToggleStateMood');
  const hintEl = document.getElementById('socialClubOwnerToggleHintMood');
  if (!root || !checkbox || !stateEl) return;

  const OWNER_EMAIL = 'samirahmed1887@gmail.com';

  async function authedHeaders() {
    const u = firebase?.auth?.().currentUser;
    if (!u) return null;
    const token = await u.getIdToken();
    return {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    };
  }

  async function isOwner() {
    try {
      const headers = await authedHeaders();
      if (!headers) return false;
      const res = await fetch('/api/users/me', { headers, credentials: 'same-origin' });
      if (!res.ok) return false;
      const me = await res.json();
      const email = String(me?.email || '').trim().toLowerCase();
      return email === OWNER_EMAIL;
    } catch {
      return false;
    }
  }

  async function refresh() {
    const headers = await authedHeaders();
    if (!headers) return;
    const res = await fetch('/api/admin/social_club/event-owner', { headers, credentials: 'same-origin' });
    if (!res.ok) throw new Error('Failed to fetch event state');
    const payload = await res.json();
    const open = !!payload?.event?.isEventOpen;
    checkbox.checked = open;
    stateEl.textContent = open ? 'ON' : 'OFF';
  }

  async function setOpen(nextOpen) {
    const headers = await authedHeaders();
    if (!headers) throw new Error('Not authenticated');
    const res = await fetch('/api/admin/social_club/event-owner', {
      method: 'POST',
      headers,
      credentials: 'same-origin',
      body: JSON.stringify({ isEventOpen: !!nextOpen })
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(t || 'Failed to update event');
    }
    await refresh();
  }

  checkbox.addEventListener('change', async () => {
    const desired = checkbox.checked;
    checkbox.disabled = true;
    try {
      await setOpen(desired);
    } catch (e) {
      checkbox.checked = !desired;
      if (hintEl) hintEl.textContent = e?.message ? String(e.message) : 'Failed to update';
    } finally {
      checkbox.disabled = false;
    }
  });

  async function boot() {
    const ok = await isOwner();
    if (!ok) return;
    root.classList.remove('hidden');
    try {
      await refresh();
    } catch (e) {
      if (hintEl) hintEl.textContent = e?.message ? String(e.message) : 'Failed to load';
    }
  }

  try {
    firebase?.auth?.().onAuthStateChanged(() => { boot(); });
  } catch {
    boot();
  }
})();

// ───────────────────────────────
// 7. Page-specific Main App Script
// ───────────────────────────────
(async function () {
  const MoodApp = window.MoodApp || {};
  const _Auth = MoodApp.Auth;
  const _API = MoodApp.API;
  const _Toast = MoodApp.Toast;
  const _Loading = MoodApp.Loading;
  const _PageTransition = MoodApp.PageTransition;
  const _Utils = MoodApp.Utils;
  const _Session = MoodApp.Session;

  const toast = (m, t = 'success') => {
    if (_Toast && typeof _Toast[t] === 'function') return _Toast[t](m);
    console[t === 'error' ? 'error' : 'log'](m);
  };
  const showLoading = (m) => {
    try {
      if (_Loading && typeof _Loading.show === 'function') _Loading.show(m);
    } catch { }
    let el = document.getElementById('moodLoadingPopup');
    if (!el) {
      el = document.createElement('div');
      el.id = 'moodLoadingPopup';
      el.className = 'fixed inset-0 z-[9999] hidden items-center justify-center';
      el.innerHTML = `
        <div class="absolute inset-0" style="background: rgba(0,0,0,0.6); backdrop-filter: blur(4px);"></div>
        <div style="position: relative; width: 92vw; max-width: 380px; border-radius: 16px; background: rgba(15, 17, 21, 0.96); border: 1px solid rgba(255,255,255,0.10); box-shadow: 0 20px 60px rgba(0,0,0,0.5); padding: 18px;">
          <div style="display:flex; align-items:center; gap:12px;">
            <div class="loading-spinner" style="width: 36px; height: 36px;"></div>
            <div>
              <div style="font-size:14px; font-weight:900;">Please wait</div>
              <div id="moodLoadingText" style="margin-top:2px; font-size:12px; color: var(--text-secondary);">Loading…</div>
            </div>
          </div>
        </div>
      `;
      document.body.appendChild(el);
    }
    const t = el.querySelector('#moodLoadingText');
    if (t) t.textContent = m || 'Loading…';
    el.classList.remove('hidden');
    el.classList.add('flex');
  };
  const hideLoading = () => {
    try {
      if (_Loading && typeof _Loading.hide === 'function') _Loading.hide();
    } catch { }
    const el = document.getElementById('moodLoadingPopup');
    if (!el) return;
    el.classList.add('hidden');
    el.classList.remove('flex');
  };

  const moodsGrid = document.getElementById('moodsGrid');
  const noteText = document.getElementById('noteText');
  const logMoodBtn = document.getElementById('logMoodBtn');
  const enterChatBtn = document.getElementById('enterChatBtn');
  const notesList = document.getElementById('notesList');
  const loadingEl = document.getElementById('loading');
  const loadMoreBtn = document.getElementById('loadMoreBtn');
  const logoutBtn = document.getElementById('logoutBtn');

  const moods = {
    happy: '😊', sad: '😭', angry: '😠', lonely: '😔',
    calm: '😌', excited: '🤩', tired: '😴', stressed: '😣', confused: '😕'
  };

  let selectedMood = null;
  let page = 0;
  let socketInstance = null;
  let currentUser = null;
  let socketConnectFailures = 0;

  const profileIconBtn = document.getElementById('profileIconBtn');
  const profileIconFallback = document.getElementById('profileIconFallback');
  const profileIconImg = document.getElementById('profileIconImg');
  const profileMenu = document.getElementById('profileMenu');
  const menuChangePfp = document.getElementById('menuChangePfp');
  const menuChangeUsername = document.getElementById('menuChangeUsername');

  function isDefaultAvatarUrl(url) {
    if (!url || typeof url !== 'string') return true;
    const u = url.trim();
    if (!u || u === 'null') return true;
    if (/ui-avatars\.com\/api\/.+name=User/i.test(u)) return true;
    return false;
  }

  function setProfileIcon(pfpUrl, username) {
    if (!profileIconFallback || !profileIconImg) return;
    const name = typeof username === 'string' ? username.trim() : '';
    const initial = (name ? name.charAt(0) : 'U').toUpperCase();
    profileIconFallback.textContent = initial;

    if (pfpUrl && !isDefaultAvatarUrl(pfpUrl)) {
      profileIconImg.src = pfpUrl;
      profileIconImg.classList.remove('hidden');
      profileIconFallback.classList.add('hidden');
      profileIconImg.onerror = () => {
        profileIconImg.classList.add('hidden');
        profileIconFallback.classList.remove('hidden');
        profileIconImg.onerror = null;
      };
    } else {
      profileIconImg.classList.add('hidden');
      profileIconFallback.classList.remove('hidden');
    }
  }

  const profileSettingsModal = document.getElementById('profileSettingsModal');
  const profileModalAvatarFallback = document.getElementById('profileModalAvatarFallback');
  const profileModalAvatarImg = document.getElementById('profileModalAvatarImg');
  const profileModalFileInput = document.getElementById('profileModalFileInput');
  const profileModalUsername = document.getElementById('profileModalUsername');
  const profileModalUsernameHint = document.getElementById('profileModalUsernameHint');
  const profileModalUsernameStatus = document.getElementById('profileModalUsernameStatus');
  const profileModalCancelBtn = document.getElementById('profileModalCancelBtn');
  const profileModalSaveBtn = document.getElementById('profileModalSaveBtn');
  const profileModalError = document.getElementById('profileModalError');

  let profileModalSelectedFile = null;
  let profileModalInitialUser = null;

  function setProfileModalError(msg) {
    if (!profileModalError) return;
    if (!msg) {
      profileModalError.classList.add('hidden');
      profileModalError.textContent = '';
      return;
    }
    profileModalError.textContent = msg;
    profileModalError.classList.remove('hidden');
  }

  function isValidUsername(u) {
    if (!u || typeof u !== 'string') return false;
    const trimmed = u.trim();
    return /^[a-zA-Z0-9_-]{3,20}$/.test(trimmed);
  }

  function openProfileSettingsModal(focus = 'username') {
    if (!profileSettingsModal) return;
    setProfileModalError('');
    profileModalSelectedFile = null;

    if (!currentUser) {
      showLoading('Loading profile...');
      ensureCurrentUserLoaded().finally(() => {
        hideLoading();
        openProfileSettingsModal(focus);
      });
      return;
    }

    const name = (currentUser && currentUser.username) ? String(currentUser.username).trim() : '';
    const initial = (name ? name.charAt(0) : 'U').toUpperCase();
    if (profileModalAvatarFallback) profileModalAvatarFallback.textContent = initial;

    const pfpUrl = currentUser && currentUser.pfpUrl ? String(currentUser.pfpUrl) : '';
    if (profileModalAvatarImg && pfpUrl && !isDefaultAvatarUrl(pfpUrl)) {
      profileModalAvatarImg.src = pfpUrl;
      profileModalAvatarImg.classList.remove('hidden');
      if (profileModalAvatarFallback) profileModalAvatarFallback.classList.add('hidden');
      profileModalAvatarImg.onerror = () => {
        profileModalAvatarImg.classList.add('hidden');
        if (profileModalAvatarFallback) profileModalAvatarFallback.classList.remove('hidden');
        profileModalAvatarImg.onerror = null;
      };
    } else {
      if (profileModalAvatarImg) profileModalAvatarImg.classList.add('hidden');
      if (profileModalAvatarFallback) profileModalAvatarFallback.classList.remove('hidden');
    }

    if (profileModalUsername) profileModalUsername.value = name || '';
    if (profileModalUsernameHint) profileModalUsernameHint.textContent = '3-20 chars: letters, numbers, _ and -';
    if (profileModalUsernameStatus) profileModalUsernameStatus.textContent = '';

    profileModalInitialUser = { username: name || '', pfpUrl: pfpUrl || '' };

    profileSettingsModal.classList.remove('hidden');
    profileSettingsModal.classList.add('flex');
    if (focus === 'username' && profileModalUsername) profileModalUsername.focus();
  }

  function closeProfileSettingsModal() {
    if (!profileSettingsModal) return;
    profileSettingsModal.classList.add('hidden');
    profileSettingsModal.classList.remove('flex');
    setProfileModalError('');
  }

  async function refreshCurrentUserAndIcon() {
    if (!_API || typeof _API.get !== 'function') return;
    const userData = await _API.get('/api/users/me');
    currentUser = {
      userId: userData._id,
      username: userData.username,
      pfpUrl: userData.pfpUrl,
      email: userData.email
    };
    try { setProfileIcon(currentUser.pfpUrl, currentUser.username); } catch { }
  }

  async function ensureCurrentUserLoaded() {
    if (currentUser && currentUser.userId) return currentUser;

    if (_API && typeof _API.get === 'function') {
      await refreshCurrentUserAndIcon();
      return currentUser;
    }

    try {
      const firebaseUser = firebase.auth().currentUser;
      if (!firebaseUser) return null;
      const token = await firebaseUser.getIdToken();
      const res = await fetch('/api/users/me', {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      if (!res.ok) return null;
      const userData = await res.json();
      currentUser = {
        userId: userData._id,
        username: userData.username,
        pfpUrl: userData.pfpUrl,
        email: userData.email
      };
      try { setProfileIcon(currentUser.pfpUrl, currentUser.username); } catch { }
      return currentUser;
    } catch {
      return null;
    }
  }

  function openProfileMenu() {
    if (!profileMenu || !profileIconBtn) return;
    profileMenu.classList.remove('hidden');
    profileIconBtn.setAttribute('aria-expanded', 'true');
  }

  function closeProfileMenu() {
    if (!profileMenu || !profileIconBtn) return;
    profileMenu.classList.add('hidden');
    profileIconBtn.setAttribute('aria-expanded', 'false');
  }

  function toggleProfileMenu() {
    if (!profileMenu) return;
    if (profileMenu.classList.contains('hidden')) openProfileMenu();
    else closeProfileMenu();
  }

  // ============================================
  // SOCKET.IO CONNECTION
  // ============================================

  function exitWholeWebsiteFromMood() {
    try { socketInstance?.disconnect?.(); } catch { }
    try {
      sessionStorage.removeItem('hasBackgroundCall');
      sessionStorage.removeItem('returningFromCall');
      sessionStorage.removeItem('backgroundCallMode');
      localStorage.removeItem('activeCall');
    } catch { }

    try { window.open('', '_self'); } catch { }
    try { window.close(); } catch { }

    setTimeout(() => {
      try {
        document.documentElement.innerHTML = '<head><title></title></head><body style="margin:0;background:#000;"></body>';
      } catch { }
      try {
        window.location.replace('about:blank');
      } catch { }
    }, 80);
  }

  function preventBackNavigation() {
    const EXIT_TRAP_STATE = { page: 'mood', exitTrap: true };

    try {
      history.replaceState({ page: 'mood', root: true }, '', location.href);
      history.pushState(EXIT_TRAP_STATE, '', location.href);
    } catch { }

    window.addEventListener('popstate', function (event) {
      event.preventDefault();
      exitWholeWebsiteFromMood();
    });
  }

  function waitForTabManager() {
    return new Promise((resolve, reject) => {
      if (window.tabManager && window.tabManager.tabId) {
        console.log('✅ [Auth] TabManager already ready:', window.tabManager.tabId);
        resolve(window.tabManager);
        return;
      }

      console.log('⏳ [Auth] Waiting for TabManager to initialize...');

      let attempts = 0;
      const maxAttempts = 100;

      const interval = setInterval(() => {
        attempts++;

        if (window.tabManager && window.tabManager.tabId) {
          clearInterval(interval);
          console.log('✅ [Auth] TabManager ready after waiting:', window.tabManager.tabId);
          resolve(window.tabManager);
        } else if (attempts >= maxAttempts) {
          clearInterval(interval);
          console.error('❌ [Auth] TabManager timeout after 10s');
          reject(new Error('TabManager initialization timeout'));
        }
      }, 100);
    });
  }

  function getStableSocketSessionId() {
    if (_Session && typeof _Session.getSocketSessionId === 'function') {
      return _Session.getSocketSessionId();
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

  function triggerSocketRecoveryReload(reason = 'socket_recovery') {
    try {
      const key = 'mood_socket_recovery_state';
      const now = Date.now();
      const state = JSON.parse(sessionStorage.getItem(key) || '{}');
      const recentWindowMs = 5 * 60 * 1000;
      const withinWindow = state.lastReloadAt && (now - state.lastReloadAt) < recentWindowMs;
      const count = withinWindow ? (state.count || 0) + 1 : 1;

      if (count > 2) {
        console.error(`❌ [Socket] Reload suppressed (loop guard): ${reason}`);
        return;
      }

      sessionStorage.setItem(key, JSON.stringify({ count, lastReloadAt: now, reason }));
    } catch { }

    window.location.reload();
  }

  async function authenticateSocketWithTabManagement(socket, userId) {
    try {
      console.log('🔐 [Auth] Starting socket authentication...');

      const tabManager = await waitForTabManager();
      const tabId = tabManager.tabId;
      const sessionId = getScopedSocketSessionId('mood');

      console.log('✅ [Auth] TabId retrieved:', tabId);

      const firebaseUser = firebase.auth().currentUser;
      if (!firebaseUser) {
        throw new Error('Not authenticated with Firebase');
      }

      const token = await firebaseUser.getIdToken();
      console.log('✅ [Auth] Firebase token retrieved');

      console.log('📤 [Auth] Sending authenticate event:', {
        userId,
        tabId,
        socketId: socket.id
      });

      socket.emit('authenticate', {
        token: token,
        userId: userId,
        tabId: tabId,
        sessionId
      });

      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Authentication timeout (10s)'));
        }, 10000);

        socket.once('authenticated', (data) => {
          clearTimeout(timeout);
          console.log('✅ [Auth] Socket authenticated successfully:', data);
          resolve(data);
        });

        socket.once('auth_error', (error) => {
          clearTimeout(timeout);
          console.error('❌ [Auth] Authentication failed:', error);

          if (error.code === 'DUPLICATE_TAB') {
            console.warn('⚠️ [Auth] Another tab is active');
          }

          reject(new Error(error.message || 'Authentication failed'));
        });
      });

    } catch (error) {
      console.error('❌ [Auth] Socket authentication error:', error);
      throw error;
    }
  }

  function setupSessionReplacementHandler(socket) {
    socket.on('session_replaced', (data) => {
      console.log('⚠️ [Session] This tab has been replaced by another:', data);
    });

    console.log('✅ [Session] Replacement handler registered');
  }

  console.log('✅ Socket authentication helpers loaded');

  async function initializeSocket() {
    try {
      const firebaseUser = firebase.auth().currentUser;
      if (!firebaseUser) {
        console.error('No Firebase user found');
        return null;
      }

      const idToken = await firebaseUser.getIdToken();

      if (!_API || typeof _API.get !== 'function') {
        console.error('API helper not available');
        return null;
      }

      const userData = await _API.get('/api/users/me');
      currentUser = {
        userId: userData._id,
        username: userData.username,
        pfpUrl: userData.pfpUrl,
        email: userData.email
      };

      try { setProfileIcon(currentUser.pfpUrl, currentUser.username); } catch { }

      console.log('✅ Current user loaded:', currentUser);

      const socketUrl = window.location.origin;
      const sessionId = getScopedSocketSessionId('mood');
      const tabId = window.tabManager?.tabId || null;
      const socket = io(socketUrl, {
        transports: ['websocket', 'polling'],
        reconnection: true,
        reconnectionAttempts: Infinity,
        reconnectionDelay: 1000,
        reconnectionDelayMax: 5000,
        timeout: 15000,
        auth: {
          token: idToken,
          sessionId,
          tabId
        }
      });

      socket.on('mood_counts_initial', (counts) => {
        console.log('📊 Received initial mood counts:', counts);
        Object.entries(counts).forEach(([mood, count]) => {
          updateMoodCount(mood, count);
        });
      });

      socket.on('mood_count_update', ({ mood, count }) => {
        console.log(`📊 Mood count update: ${mood} = ${count}`);
        updateMoodCount(mood, count);
      });

      socket.on('connect', async () => {
        console.log('🔌 Socket connected:', socket.id);
        socketConnectFailures = 0;

        try {
          await authenticateSocketWithTabManagement(socket, currentUser.userId);
          setupSessionReplacementHandler(socket);
          console.log('✅ Socket authenticated and ready');
        } catch (error) {
          console.error('❌ Authentication failed:', error);
          MoodApp.Toast.error('Connection failed. Please refresh the page.');
        }
      });

      socket.on('authenticated', (data) => {
        console.log('✅ Socket authenticated:', data);
      });

      socket.on('auth_error', (data) => {
        console.error('❌ Socket auth error:', data);
        toast('Authentication failed. Please refresh the page.', 'error');
      });

      socket.on('connect_error', (error) => {
        console.error('🔌 Socket connection error:', error);
        socketConnectFailures += 1;
        if (socketConnectFailures >= 6) {
          triggerSocketRecoveryReload('mood_connect_error_threshold');
        }
      });

      socket.on('disconnect', (reason) => {
        console.log('🔌 Socket disconnected:', reason);
        if (reason === 'io server disconnect') {
          socket.connect();
        }
      });

      socket.on('server_ping', (payload = {}) => {
        if (!socket.connected) return;
        socket.emit('client_pong', {
          roomId: null,
          location: 'mood',
          path: window.location.pathname,
          serverPingTs: payload.ts || null
        });
      });

      if (socket.io) {
        socket.io.on('reconnect_attempt', (attempt) => {
          console.warn(`🔄 [Socket] Reconnect attempt ${attempt}`);
        });
        socket.io.on('reconnect_failed', () => {
          triggerSocketRecoveryReload('mood_reconnect_failed');
        });
      }

      return socket;
    } catch (error) {
      console.error('Socket initialization error:', error);
      return null;
    }
  }

  // ============================================
  // MOOD SELECTION
  // ============================================
  function renderMoods() {
    if (!moodsGrid) return;
    moodsGrid.innerHTML = '';
    Object.entries(moods).forEach(([id, emoji]) => {
      const card = document.createElement('div');
      card.className = 'mood-card';
      card.setAttribute('data-mood', id);
      card.innerHTML = `
        <div class="emoji-wrapper">
          <span class="emoji-3d">${emoji}</span>
        </div>
        <div class="mood-label" style="margin-top: 8px;">${id}</div>
        <div class="mood-user-count" id="mood-count-${id}" style="font-size: 11px; color: rgba(255,255,255,0.45); margin-top: 4px;">
          <span>—</span>
        </div>
      `;
      card.addEventListener('click', () => selectMood(id, card));
      moodsGrid.appendChild(card);
    });
  }

  function selectMood(mood, el) {
    selectedMood = mood;
    document.querySelectorAll('.mood-card').forEach(b => {
      b.classList.remove('selected');
    });
    if (el) {
      el.classList.add('selected');
    }
    if (enterChatBtn) enterChatBtn.disabled = false;
    if (logMoodBtn) logMoodBtn.disabled = false;
  }

  // ============================================
  // ENTER CHAT (MATCHMAKING)
  // ============================================
  async function handleEnterChat() {
    if (!selectedMood) {
      toast('Please select a mood first', 'warning');
      return;
    }

    if (!socketInstance || !socketInstance.connected) {
      toast('Connecting to server...', 'warning');
      socketInstance = await initializeSocket();

      await new Promise((resolve) => {
        if (socketInstance && socketInstance.connected) {
          resolve();
        } else if (socketInstance) {
          socketInstance.once('connect', resolve);
          setTimeout(() => resolve(), 5000);
        } else {
          resolve();
        }
      });

      if (!socketInstance || !socketInstance.connected) {
        toast('Failed to connect to server. Please try again.', 'error');
        return;
      }
    }

    try {
      localStorage.setItem('selectedMood', selectedMood);
    } catch (e) {
      console.warn('LocalStorage not available:', e);
    }

    console.log('🎮 Entering chat with mood:', selectedMood);

    if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
      _PageTransition.navigateTo('/chat.html');
    } else {
      window.location.href = '/chat.html';
    }
  }

  // ============================================
  // LOG MOOD (NOTES)
  // ============================================
  async function handleLogMood() {
    const text = (noteText && noteText.value) ? noteText.value.trim() : '';
    if (!text) {
      toast('Please add a note', 'warning');
      return;
    }
    if (!selectedMood) {
      toast('Please select a mood', 'warning');
      return;
    }

    showLoading('Saving your mood...');
    try {
      if (!_API || typeof _API.post !== 'function') throw new Error('API helper missing');

      console.log(`💾 Logging mood: ${selectedMood} - "${text.substring(0, 50)}..."`);

      await _API.post('/api/notes', { text, mood: selectedMood });

      hideLoading();
      toast('Mood logged!', 'success');

      try {
        await refreshCurrentUserAndIcon();
      } catch { }

      if (noteText) noteText.value = '';

      console.log(`🔄 Refreshing notes list after new entry`);
      currentNotePage = 0;
      hasMoreNotes = true;
      loadNotes(0, false);

    } catch (err) {
      hideLoading();
      console.error('❌ Failed to log mood:', err);
      toast(err.message || 'Failed to log mood', 'error');
    }
  }

  // ============================================
  // NOTES DISPLAY
  // ============================================
  function renderNote(note) {
    console.log(`📝 Rendering note:`, {
      id: note._id,
      username: note.username,
      hasPfp: !!note.pfpUrl,
      mood: note.mood
    });

    const card = document.createElement('div');
    card.className = 'session-card';

    const rawUsername = typeof note?.username === 'string' ? note.username : '';
    const username = rawUsername.trim() || 'Anonymous';
    const initial = username.charAt(0).toUpperCase();
    const defaultAvatarRegex = /ui-avatars\.com\/api\/.+name=User/i;
    const hasPfp = typeof note?.pfpUrl === 'string'
      ? note.pfpUrl.trim().length > 0
      && note.pfpUrl !== 'null'
      && !defaultAvatarRegex.test(note.pfpUrl)
      : false;

    const createdAt = note?.createdAt ?
      (_Utils && typeof _Utils.formatDate === 'function' ?
        _Utils.formatDate(note.createdAt) :
        new Date(note.createdAt).toLocaleString()) : '';

    const moodLabel = note?.mood ?
      note.mood.charAt(0).toUpperCase() + note.mood.slice(1) :
      'Unknown';

    const textHtml = note?.text ?
      (_Utils && typeof _Utils.escapeHtml === 'function' ?
        _Utils.escapeHtml(note.text) :
        (() => { const d = document.createElement('div'); d.textContent = note.text; return d.innerHTML; })()) : '';

    const avatarHtml = hasPfp
      ? `<img src="${note.pfpUrl}" alt="${username}" class="session-emoji" style="border-radius: 50%; object-fit: cover; width: 40px; height: 40px;">`
      : `<div class="session-emoji" style="display:flex;align-items:center;justify-content:center;font-weight:700;">${initial}</div>`;

    console.log(`✅ Using ${hasPfp ? 'profile picture' : 'initial fallback'} for ${username}`);

    card.innerHTML = `
    <div class="session-header">
      ${avatarHtml}
      <div class="session-info">
        <div class="session-mood">${username} · ${moodLabel}</div>
        <div class="session-time">${createdAt}</div>
      </div>
    </div>
    <div class="session-text">${textHtml}</div>
  `;

    return card;
  }

  let isLoadingNotes = false;
  let hasMoreNotes = true;
  let currentNotePage = 0;

  async function loadNotes(pageNum = 0, append = false) {
    if (isLoadingNotes) {
      console.log(`⏳ Already loading notes, skipping duplicate request`);
      return;
    }

    if (!hasMoreNotes && append) {
      console.log(`✋ No more notes to load`);
      return;
    }

    isLoadingNotes = true;
    console.log(`📥 Loading notes: page=${pageNum}, append=${append}`);

    if (loadingEl) loadingEl.classList.remove('hidden');

    try {
      if (!_API || typeof _API.get !== 'function') {
        throw new Error('API.get missing');
      }

      const data = await _API.get(`/api/notes?page=${pageNum}&limit=10`);

      console.log(`✅ Received ${data.notes?.length || 0} notes from server`);
      console.log(`   Total in database: ${data.total}`);
      console.log(`   Has more: ${data.hasMore}`);

      if (!append && notesList) {
        notesList.innerHTML = '';
        console.log(`🗑️ Cleared notes list (fresh load)`);
      }

      const notesArray = Array.isArray(data.notes) ? data.notes : [];

      if (notesArray.length === 0) {
        console.log(`ℹ️ No notes returned for page ${pageNum}`);
        if (!append && notesList) {
          notesList.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--text-muted);">No mood entries yet. Log your first mood above!</div>';
        }
      } else {
        notesArray.forEach((n, idx) => {
          console.log(`  [${idx}] ${n.username}: ${n.mood}`);
          if (notesList) notesList.appendChild(renderNote(n));
        });
        console.log(`✅ Rendered ${notesArray.length} note cards`);
      }

      currentNotePage = pageNum;
      hasMoreNotes = data.hasMore;

      console.log(`📊 Pagination state: page=${currentNotePage}, hasMore=${hasMoreNotes}`);

    } catch (err) {
      console.error('❌ Failed to load notes:', err);
      toast(err.message || 'Failed to load notes', 'error');
    } finally {
      if (loadingEl) loadingEl.classList.add('hidden');
      isLoadingNotes = false;
      console.log(`🔓 Released loading lock`);
    }
  }

  function initInfiniteScroll() {
    const scrollContainer = window;
    let scrollTimeout;

    scrollContainer.addEventListener('scroll', () => {
      clearTimeout(scrollTimeout);
      scrollTimeout = setTimeout(() => {
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        const windowHeight = window.innerHeight;
        const documentHeight = document.documentElement.scrollHeight;

        const threshold = 200;
        const distanceFromBottom = documentHeight - (scrollTop + windowHeight);

        if (distanceFromBottom < threshold && hasMoreNotes && !isLoadingNotes) {
          console.log(`📜 Scroll threshold reached (${distanceFromBottom.toFixed(0)}px from bottom)`);
          console.log(`   Loading next page: ${currentNotePage + 1}`);
          loadNotes(currentNotePage + 1, true);
        }
      }, 100);
    });

    console.log(`✅ Infinite scroll initialized (threshold: 200px from bottom)`);
  }

  // ============================================
  // LOGOUT
  // ============================================
  function handleLogout() {
    try {
      if (socketInstance) {
        socketInstance.disconnect();
        socketInstance = null;
      }

      if (_Auth && typeof _Auth.clearAuth === 'function') {
        _Auth.clearAuth();
      } else {
        localStorage.removeItem('user');
      }

      firebase.auth().signOut();
    } catch (e) {
      console.error('Logout error', e);
    }

    if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
      _PageTransition.navigateTo('/login.html');
    } else {
      window.location.href = '/login.html';
    }
  }

  // ============================================
  // EVENT LISTENERS
  // ============================================
  function attachListeners() {
    if (logMoodBtn) logMoodBtn.addEventListener('click', handleLogMood);
    if (enterChatBtn) enterChatBtn.addEventListener('click', handleEnterChat);

    if (logoutBtn) logoutBtn.addEventListener('click', handleLogout);

    if (profileIconBtn) {
      profileIconBtn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        toggleProfileMenu();
      });
    }

    if (menuChangePfp) {
      menuChangePfp.addEventListener('click', () => {
        closeProfileMenu();
        openProfileSettingsModal('photo');
      });
    }

    if (menuChangeUsername) {
      menuChangeUsername.addEventListener('click', () => {
        closeProfileMenu();
        openProfileSettingsModal('username');
      });
    }

    if (profileSettingsModal) {
      profileSettingsModal.addEventListener('click', (e) => {
        const t = e.target;
        if (t && t.closest && t.closest('[data-close="1"]')) closeProfileSettingsModal();
      });
    }
    if (profileModalCancelBtn) profileModalCancelBtn.addEventListener('click', closeProfileSettingsModal);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closeProfileSettingsModal();
    });

    if (profileModalFileInput) {
      profileModalFileInput.addEventListener('change', (e) => {
        const file = e.target.files && e.target.files[0] ? e.target.files[0] : null;
        if (!file) return;
        if (file.size > 5 * 1024 * 1024) {
          setProfileModalError('File size must be under 5MB');
          return;
        }
        if (!file.type || !file.type.startsWith('image/')) {
          setProfileModalError('Please select an image file');
          return;
        }
        setProfileModalError('');
        profileModalSelectedFile = file;

        const reader = new FileReader();
        reader.onload = (ev) => {
          if (!profileModalAvatarImg) return;
          profileModalAvatarImg.src = ev.target.result;
          profileModalAvatarImg.classList.remove('hidden');
          if (profileModalAvatarFallback) profileModalAvatarFallback.classList.add('hidden');
        };
        reader.readAsDataURL(file);
      });
    }

    if (profileModalSaveBtn) {
      profileModalSaveBtn.addEventListener('click', async () => {
        setProfileModalError('');
        await ensureCurrentUserLoaded();
        if (!currentUser) {
          setProfileModalError('User not loaded. Please refresh.');
          return;
        }

        const desiredUsername = profileModalUsername ? String(profileModalUsername.value || '').trim().toLowerCase() : '';
        const usernameChanged = profileModalInitialUser && desiredUsername && desiredUsername !== (profileModalInitialUser.username || '').toLowerCase();
        const hasUsernameAttempt = !!desiredUsername;

        try {
          showLoading('Saving profile...');

          if (hasUsernameAttempt && !isValidUsername(desiredUsername)) {
            throw new Error('Username must be 3-20 characters: letters, numbers, underscores, hyphens');
          }

          if (usernameChanged) {
            const checkRes = await fetch('/api/check-username', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ username: desiredUsername })
            });
            const checkJson = await checkRes.json().catch(() => null);
            if (!checkRes.ok) {
              throw new Error((checkJson && checkJson.error) ? checkJson.error : 'Unable to check username');
            }
            if (!checkJson || !checkJson.available) {
              const suggestions = checkJson && Array.isArray(checkJson.suggestions) ? checkJson.suggestions : [];
              throw new Error(suggestions.length ? `Username taken. Try: ${suggestions.slice(0, 3).join(', ')}` : 'Username already taken');
            }

            if (profileModalUsernameStatus) profileModalUsernameStatus.textContent = 'Available';
            await _API.post('/api/users/profile', { username: desiredUsername });
          }

          if (profileModalSelectedFile) {
            await _API.uploadFile('/api/users/upload-pfp', profileModalSelectedFile);
          }

          await refreshCurrentUserAndIcon();
          hideLoading();
          toast('Profile updated', 'success');
          closeProfileSettingsModal();
        } catch (err) {
          hideLoading();
          const msg = err && err.message ? err.message : 'Failed to save profile';
          setProfileModalError(msg);
          toast(msg, 'error');
        }
      });
    }

    document.addEventListener('click', (e) => {
      if (!profileMenu || profileMenu.classList.contains('hidden')) return;
      const t = e.target;
      if (t && t.closest && (t.closest('#profileMenu') || t.closest('#profileIconBtn'))) return;
      closeProfileMenu();
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closeProfileMenu();
    });
    if (noteText) {
      noteText.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
          if (!logMoodBtn.disabled) handleLogMood();
        }
      });
    }

    initInfiniteScroll();
    console.log(`✅ Event listeners attached`);
  }

  function formatUserCount(count) {
    if (count >= 1000000) {
      return `${(count / 1000000).toFixed(1)}M users`;
    } else if (count >= 1000) {
      return `${(count / 1000).toFixed(1)}K users`;
    } else {
      return `${count} user${count !== 1 ? 's' : ''}`;
    }
  }

  function updateMoodCount(mood, count) {
    const countEl = document.getElementById(`mood-count-${mood}`);
    if (countEl) {
      countEl.innerHTML = `<span>${formatUserCount(count)}</span>`;
    }
  }

  function setupMoodCountListeners() {
    console.log('📊 Setting up mood count listeners...');
  }

  // ============================================
  // INITIALIZATION
  // ============================================
  async function initPage() {
    try {
      if (_Auth && typeof _Auth.requireAuth === 'function') {
        await _Auth.requireAuth();
      }
    } catch (err) {
      console.error('Error running requireAuth():', err);
      return;
    }

    preventBackNavigation();

    renderMoods();

    setupMoodCountListeners();

    console.log('🔌 Initializing socket connection...');
    socketInstance = await initializeSocket();

    attachListeners();
    loadNotes(0);
  }

  initPage();
})();
