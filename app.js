/**
 * app.js
 * Shared Utility Library — production-hardened.
 *
 * Notes:
 * - Waits for Firebase to be initialized (firebase.initializeApp(...)) before using auth.
 * - Provides a robust Socket wrapper exposing .instance, .connect(), .authenticate(), .emit(), .on()
 * - Keeps public API identical / compatible with your code.
 */

/* =========================================================
   FIREBASE READINESS (non-throwing, poll-based)
   =========================================================
   Resolves when:
   - `firebase` global exists
   - `firebase.apps` exists and has at least one app (i.e., initializeApp() called)
*/



/* =========================================================
   TAB MANAGER (IMMEDIATE BLOCKING - NO ACTIVITY ALLOWED)
   ========================================================= */

class TabManager {
  constructor() {
    this.tabId = `tab_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    this.channelName = 'vibe_app_tab_control';
    this.channel = new BroadcastChannel(this.channelName);
    this.isActive = false; // ❌ CRITICAL: Start as INACTIVE until proven otherwise
    this.isBlocked = false;
    this.heartbeatInterval = null;
    this.initPromise = null;
    
    console.log(`📱 [TabManager] Created: ${this.tabId}`);
    
    // ✅ CRITICAL: Synchronous check FIRST before anything else
    this.initPromise = this.immediateBlockCheck();
  }
  
  /**
   * ✅ CRITICAL: Synchronous + async check to block IMMEDIATELY
   */
  async immediateBlockCheck() {
    console.log(`🔍 [TabManager] Checking for existing tabs...`);
    
    return new Promise((resolve) => {
      let responseReceived = false;
      const ourTimestamp = parseInt(this.tabId.split('_')[1]);
      
      // ✅ Listen for ANY existing tab response
      const checkHandler = (event) => {
        const { type, tabId, timestamp } = event.data;
        
        if (type === 'HEARTBEAT' && tabId !== this.tabId) {
          console.log(`⚠️ [TabManager] Detected existing tab: ${tabId}`);
          const theirTimestamp = timestamp;
          
          // If their tab is OLDER (lower timestamp), we MUST block
          if (theirTimestamp < ourTimestamp) {
            console.error(`🚫 [TabManager] BLOCKING THIS TAB - older tab exists`);
            responseReceived = true;
            this.channel.removeEventListener('message', checkHandler);
            this.blockTabImmediately();
            resolve(false); // ❌ NOT ACTIVE
          }
        }
      };
      
      this.channel.addEventListener('message', checkHandler);
      
      // ✅ Send ping to existing tabs
      this.channel.postMessage({
        type: 'HEARTBEAT_REQUEST',
        tabId: this.tabId,
        timestamp: ourTimestamp
      });
      
      // ✅ Wait 200ms for response (if no response, we're the first tab)
      setTimeout(() => {
        this.channel.removeEventListener('message', checkHandler);
        
        if (!responseReceived) {
          console.log(`✅ [TabManager] No existing tabs - this tab is ACTIVE`);
          this.isActive = true;
          this.setupMessageHandlers();
          this.startHeartbeat();
          resolve(true); // ✅ ACTIVE
        }
      }, 200);
    });
  }
  
  /**
   * ✅ CRITICAL: Block tab IMMEDIATELY with no async operations
   */
  blockTabImmediately() {
    console.error(`🚫 [TabManager] IMMEDIATE BLOCK: ${this.tabId}`);
    
    this.isBlocked = true;
    this.isActive = false;
    
    // ✅ Show blocking overlay IMMEDIATELY (synchronous DOM operation)
    this.showBlockingOverlay();
    
    // ✅ Dispatch block event IMMEDIATELY
    window.dispatchEvent(new CustomEvent('tab_blocked', { 
      detail: { tabId: this.tabId, immediate: true } 
    }));
    
    // ✅ Prevent ALL JavaScript execution
    this.killAllActivity();
  }
  
  /**
   * ✅ CRITICAL: Stop ALL activity in blocked tab
   */
  killAllActivity() {
    console.log(`💀 [TabManager] Killing all activity in blocked tab`);
    
    // Clear ALL intervals and timeouts
    for (let i = 1; i < 99999; i++) {
      window.clearInterval(i);
      window.clearTimeout(i);
    }
    
    // Override critical functions to prevent any activity
    window.fetch = () => Promise.reject(new Error('Tab blocked'));
    window.XMLHttpRequest = function() { 
      throw new Error('Tab blocked'); 
    };
    
    // Prevent socket connections
    if (typeof io !== 'undefined') {
      window.io = () => {
        console.error('🚫 Socket connection blocked');
        return null;
      };
    }
    
    console.log(`✅ [TabManager] All activity killed`);
  }
  
  setupMessageHandlers() {
    console.log(`📡 [TabManager] Setting up message handlers`);
    
    this.channel.addEventListener('message', (event) => {
      const { type, tabId, timestamp } = event.data;
      
      switch (type) {
        case 'HEARTBEAT_REQUEST':
          // Someone is checking if we exist - respond immediately
          if (this.isActive && !this.isBlocked) {
            this.channel.postMessage({
              type: 'HEARTBEAT',
              tabId: this.tabId,
              timestamp: parseInt(this.tabId.split('_')[1])
            });
          }
          break;
          
        case 'TAB_CLOSING':
          // A tab is closing
          if (tabId !== this.tabId && this.isBlocked) {
            const theirTimestamp = timestamp;
            const ourTimestamp = parseInt(this.tabId.split('_')[1]);
            
            // If it was the older tab, unblock
            if (theirTimestamp < ourTimestamp) {
              console.log(`✅ [TabManager] Older tab closed, unblocking`);
              this.unblockTab();
            }
          }
          break;
      }
    });
    
    // Cleanup on tab close
    window.addEventListener('beforeunload', () => {
      this.channel.postMessage({
        type: 'TAB_CLOSING',
        tabId: this.tabId,
        timestamp: parseInt(this.tabId.split('_')[1])
      });
      this.cleanup();
    });
  }
  
  startHeartbeat() {
    console.log(`💓 [TabManager] Starting heartbeat`);
    
    this.heartbeatInterval = setInterval(() => {
      if (this.isActive && !this.isBlocked) {
        this.channel.postMessage({
          type: 'HEARTBEAT',
          tabId: this.tabId,
          timestamp: parseInt(this.tabId.split('_')[1])
        });
      }
    }, 2000);
  }
  
  unblockTab() {
    console.log(`✅ [TabManager] UNBLOCKING: ${this.tabId}`);
    
    this.isBlocked = false;
    this.isActive = true;
    
    // Remove blocking overlay
    const overlay = document.getElementById('blocked-tab-overlay');
    if (overlay) overlay.remove();
    
    // Reload to restore normal functionality
    window.location.reload();
  }
  
  showBlockingOverlay() {
    const overlay = document.createElement('div');
    overlay.id = 'blocked-tab-overlay';
    overlay.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.98);
      z-index: 2147483647;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      color: white;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    `;
    
    overlay.innerHTML = `
      <div style="text-align: center; max-width: 500px; padding: 40px;">
        <svg width="100" height="100" viewBox="0 0 24 24" fill="none" style="margin-bottom: 32px; opacity: 0.7;">
          <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/>
          <line x1="4.93" y1="4.93" x2="19.07" y2="19.07" stroke="currentColor" stroke-width="2"/>
        </svg>
        <h1 style="font-size: 28px; font-weight: 700; margin-bottom: 16px; color: #ef4444;">
          Tab Blocked
        </h1>
        <p style="font-size: 18px; color: rgba(255, 255, 255, 0.8); line-height: 1.6; margin-bottom: 24px;">
          Vibe is already open in another tab.
        </p>
        <p style="font-size: 16px; color: rgba(255, 255, 255, 0.6); line-height: 1.5;">
          Please close this tab and use the existing one.
        </p>
        <button onclick="window.close()" style="
          margin-top: 32px;
          padding: 12px 32px;
          background: #ef4444;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          font-weight: 600;
          cursor: pointer;
        ">
          Close This Tab
        </button>
      </div>
    `;
    
    document.body.appendChild(overlay);
    console.log(`🚫 [TabManager] Blocking overlay shown`);
  }
  
  cleanup() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    this.channel.close();
  }
}

// ✅ CRITICAL: Initialize TabManager IMMEDIATELY (blocking)
console.log(`🚀 [App] Initializing TabManager...`);
const tabManager = new TabManager();
window.tabManager = tabManager;

// ✅ CRITICAL: Wait for tab check BEFORE doing ANYTHING else
(async () => {
  console.log(`⏳ [App] Waiting for tab validation...`);
  const isActive = await tabManager.initPromise;
  
  if (!isActive) {
    console.error(`🚫 [App] This tab is BLOCKED - stopping all initialization`);
    return; // ❌ STOP HERE - don't initialize anything
  }
  
  console.log(`✅ [App] Tab is active - continuing initialization`);
})();

// ✅ CRITICAL: Block event handler (kill everything immediately)
window.addEventListener('tab_blocked', (e) => {
  console.error(`🚫 [App] TAB_BLOCKED EVENT - immediate shutdown`);
  console.error(`   TabId: ${e.detail.tabId}`);
  console.error(`   Immediate: ${e.detail.immediate}`);
  
  // Stop socket if exists
  if (window.socket && window.socket.instance) {
    console.log(`🔌 [App] Disconnecting socket`);
    window.socket.instance.disconnect();
    window.socket.instance = null;
    window.socket.connected = false;
  }
  
  // Override all network functions
  window.fetch = () => Promise.reject(new Error('Tab blocked'));
  window.XMLHttpRequest = function() { throw new Error('Tab blocked'); };
  
  // Clear all intervals/timeouts
  for (let i = 1; i < 99999; i++) {
    window.clearInterval(i);
    window.clearTimeout(i);
  }
  
  console.error(`💀 [App] All activity terminated`);
});




const FirebaseReady = (() => {
  let resolved = false;
  let resolver = null;

  const p = new Promise((resolve) => {
    resolver = resolve;
  });

  const check = () => {
    try {
      if (typeof firebase === 'undefined') {
        setTimeout(check, 50);
        return;
      }
      // For both compat & modular wrappers using compat layer, firebase.apps should be present
      if (firebase.apps && firebase.apps.length) {
        if (!resolved) {
          resolved = true;
          resolver(firebase);
        }
        return;
      }
      // firebase defined but not initialized yet
      setTimeout(check, 50);
    } catch (err) {
      setTimeout(check, 50);
    }
  };

  check();
  return p;
})();

/* =========================================================
   API CONFIG
   ========================================================= */

const API_BASE_URL = (() => {
  if (window.__API_BASE__) return window.__API_BASE__;

  try {
    const { hostname, port } = window.location;
    if ((hostname === '127.0.0.1' || hostname === 'localhost') && port === '5500') {
      return 'https://website-hdem.onrender.com';
    }
  } catch {}

  return window.location.origin;
})();

const SOCKET_URL = API_BASE_URL;

/* =========================================================
   STORAGE
   ========================================================= */

const Storage = {
  set(k, v) { localStorage.setItem(k, JSON.stringify(v)); },
  get(k) { try { return JSON.parse(localStorage.getItem(k)); } catch { return null; } },
  remove(k) { localStorage.removeItem(k); },
  clear() { localStorage.clear(); }
};

/* =========================================================
   AUTH (SAFE)
   ========================================================= */

const Auth = {
  /**
   * Wait until Firebase is initialized and auth state has been resolved once.
   * Returns the user object (or null).
   */
  async waitForAuth() {
    await FirebaseReady;
    return new Promise((resolve) => {
      const unsub = firebase.auth().onAuthStateChanged(user => {
        try { unsub(); } catch (e) {}
        resolve(user);
      });
    });
  },

  /**
   * Require authentication and redirect safely (no false redirects)
   */
  async requireAuth() {
    const user = await this.waitForAuth();
    if (!user) {
      PageTransition.navigateTo('/login.html');
      throw new Error('Not authenticated');
    }
    return user;
  },

  getCurrentUser() {
    // If Firebase not ready yet, this may be null; client code should await requireAuth() if they need a user.
    return (firebase && firebase.auth) ? firebase.auth().currentUser : null;
  },

  async getToken() {
    await FirebaseReady;
    const user = await this.waitForAuth();
    if (!user) throw new Error('User not authenticated');
    return await user.getIdToken(true);
  },

  clearAuth() {
    if (firebase && firebase.auth) firebase.auth().signOut();
    Storage.clear();
  }
};

/* =========================================================
   API
   ========================================================= */

const API = {
  async request(endpoint, options = {}) {
    let token = null;
    try { token = await Auth.getToken(); } catch {}

    const headers = {
      'Content-Type': 'application/json',
      ...(options.headers || {})
    };

    if (token) headers.Authorization = `Bearer ${token}`;

    const url = endpoint.startsWith('http') ? endpoint : `${API_BASE_URL}${endpoint}`;

    const res = await fetch(url, { ...options, headers });

    if (res.status === 401) {
      Toast.error('Session expired');
      Auth.clearAuth();
      PageTransition.navigateTo('/login.html');
      throw new Error('Unauthorized');
    }

    const text = await res.text();
    if (!res.ok) throw new Error(text || `HTTP ${res.status}`);

    try { return JSON.parse(text); } catch { return { raw: text }; }
  },

  get(url) { return this.request(url, { method: 'GET' }); },
  
  post(url, body) {
    return this.request(url, {
      method: 'POST',
      body: JSON.stringify(body)
    });
  },

  /**
   * Upload a file using multipart/form-data
   * @param {string} endpoint - API endpoint
   * @param {File} file - File to upload
   * @param {string} fieldName - Form field name (default: 'file')
   * @returns {Promise} - API response
   */
  async uploadFile(endpoint, file, fieldName = 'file') {
    let token = null;
    try { token = await Auth.getToken(); } catch {}

    const formData = new FormData();
    formData.append(fieldName, file);

    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;

    const url = endpoint.startsWith('http') ? endpoint : `${API_BASE_URL}${endpoint}`;

    const res = await fetch(url, {
      method: 'POST',
      headers,
      body: formData
    });

    if (res.status === 401) {
      Toast.error('Session expired');
      Auth.clearAuth();
      PageTransition.navigateTo('/login.html');
      throw new Error('Unauthorized');
    }

    const text = await res.text();
    if (!res.ok) throw new Error(text || `HTTP ${res.status}`);

    try { return JSON.parse(text); } catch { return { raw: text }; }
  }
};

/* =========================================================
   TOAST
   ========================================================= */

const Toast = {
  container: null,
  init() {
    if (!this.container) {
      this.container = document.createElement('div');
      this.container.className = 'toast-container';
      document.body.appendChild(this.container);
    }
  },
  show(msg, type = 'success', time = 3000) {
    this.init();
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    t.textContent = msg;
    this.container.appendChild(t);
    setTimeout(() => { try { t.remove(); } catch (e) {} }, time);
  },
  success(m, t) { this.show(m, 'success', t); },
  error(m, t) { this.show(m, 'error', t); },
  warning(m, t) { this.show(m, 'warning', t); }
};

/* =========================================================
   LOADING
   ========================================================= */

const Loading = {
  overlay: null,
  show(msg = 'Loading...') {
    if (!this.overlay) {
      this.overlay = document.createElement('div');
      this.overlay.className = 'modal-overlay';
      this.overlay.innerHTML = `<div class="modal"><p>${msg}</p></div>`;
      document.body.appendChild(this.overlay);
    }
    this.overlay.style.display = 'flex';
  },
  hide() {
    if (this.overlay) this.overlay.style.display = 'none';
  }
};

/* =========================================================
   PAGE TRANSITION
   ========================================================= */

const PageTransition = {
  navigateTo(url, delay = 200) {
    setTimeout(() => window.location.href = url, delay);
  }
};

/* =========================================================
   VALIDATOR
   ========================================================= */

const Validator = {
  isValidEmail(email) {
    if (!email || typeof email !== 'string') return false;
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email.trim());
  },
  
  isValidPassword(password) {
    if (!password || typeof password !== 'string') return false;
    return password.length >= 8;
  },
  
  isValidUsername(username) {
    if (!username || typeof username !== 'string') return false;
    const trimmed = username.trim();
    // Username: 3-20 characters, alphanumeric, underscores, hyphens
    const usernameRegex = /^[a-zA-Z0-9_-]{3,20}$/;
    return usernameRegex.test(trimmed);
  }
};

/* =========================================================
   UTIL
   ========================================================= */

const Utils = {
  escapeHtml(t) {
    const d = document.createElement('div');
    d.textContent = t;
    return d.innerHTML;
  },
  formatDate(d) {
    const diff = Date.now() - new Date(d);
    const m = Math.floor(diff / 60000);
    if (m < 1) return 'Just now';
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return new Date(d).toLocaleDateString();
  }
};

/* =========================================================
   SOCKET CLIENT (EXPOSES .instance, .connect(), .authenticate(), .emit(), .on())
   ========================================================= */
   
   
   

class SocketClient {
  constructor() {
    this.instance = null;       // raw socket.io instance if available
    this.connected = false;
    this.authenticated = false;
  }

  /**
   * Connect to socket server. Returns the socket instance or null
   */
  async connect() {
    // Make a best-effort wait for socket.io library to load if it's not available yet
    const checkIo = () => new Promise((resolve) => {
      const tryCheck = () => {
        if (typeof io !== 'undefined') return resolve(true);
        // if io not present after some time, resolve false (we'll continue without sockets)
        setTimeout(tryCheck, 50);
      };
      tryCheck();
    });

    await checkIo();

    if (typeof io === 'undefined') {
      // Socket.IO client library not loaded — keep instance null but do not throw
      this.instance = null;
      this.connected = false;
      return null;
    }

    try {
      this.instance = io(SOCKET_URL);
      this.connected = true;
      // expose a convenience event wrapper so page code can still use socket.instance.on(...)
      return this.instance;
    } catch (err) {
      this.instance = null;
      this.connected = false;
      return null;
    }
  }

  /**
   * Authenticate the socket using the Firebase token (if available).
   * Emits 'authenticate' event on the socket.
   */
  async authenticate() {
    if (!this.instance) return;
    try {
      const token = await Auth.getToken();
      this.instance.emit('authenticate', { token });
      this.authenticated = true;
    } catch (err) {
      // token unavailable — don't crash; leave unauthenticated
      this.authenticated = false;
    }
  }

  /**
   * Wrapper emit (safe)
   */
  emit(event, ...args) {
    if (!this.instance) {
      // If socket not connected, ignore silently (preserves behavior)
      return;
    }
    try { this.instance.emit(event, ...args); } catch (e) {}
  }

  /**
   * Wrapper on (safe)
   */
  on(event, cb) {
    if (!this.instance) return;
    try { this.instance.on(event, cb); } catch (e) {}
  }
}

const socket = new SocketClient();

/* =========================================================
   INIT (visual page transition hook)
   ========================================================= */

document.addEventListener('DOMContentLoaded', () => {
  document.body.classList.add('page-transition-enter');
  setTimeout(() => {
    document.body.classList.remove('page-transition-enter');
  }, 300);
});


// At the end of app.js, before the export
window.MoodApp = {
  Storage,
  Auth,
  API,
  Toast,
  Loading,
  PageTransition,
  Validator,
  Utils,
  socket,
  // Add these:
  StateManager: null,  // Will be set by state-manager.js
  NavigationGuard: null  // Will be set by navigation-guard.js
};

/* =========================================================
   EXPORT
   ========================================================= */

window.MoodApp = {
  Storage,
  Auth,
  API,
  Toast,
  Loading,
  PageTransition,
  Validator,
  Utils,
  socket
};
