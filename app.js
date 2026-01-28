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
   
   
/* =========================================================
   TAB MANAGER (NON-BLOCKING BACKGROUND CHECK)
   ========================================================= */
   
class TabManager {
  constructor() {
    this.tabId = `tab_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    this.channelName = 'vibe_app_tab_control';
    this.channel = new BroadcastChannel(this.channelName);
    this.isActive = true; // ✅ Always active by default
    this.heartbeatInterval = null;
    
    console.log(`📱 [TabManager] Initialized: ${this.tabId}`);
    
    this.setupMessageHandlers();
    this.checkForExistingTabs();
    this.startHeartbeat();
  }
  
  setupMessageHandlers() {
    this.channel.addEventListener('message', (event) => {
      const { type, tabId, timestamp } = event.data;
      
      switch (type) {
        case 'HEARTBEAT':
          // Another tab is alive
          if (tabId !== this.tabId) {
            const theirTimestamp = timestamp;
            const ourTimestamp = parseInt(this.tabId.split('_')[1]);
            
            // If their tab is OLDER, we should block ourselves
            if (theirTimestamp < ourTimestamp) {
              console.log(`⚠️ [TabManager] Older tab detected, blocking this tab`);
              this.blockTab();
            }
          }
          break;
          
        case 'TAB_CLOSING':
          // A tab is closing - if it was older and we're blocked, unblock
          if (tabId !== this.tabId) {
            const theirTimestamp = timestamp;
            const ourTimestamp = parseInt(this.tabId.split('_')[1]);
            
            if (theirTimestamp < ourTimestamp && !this.isActive) {
              console.log(`✅ [TabManager] Older tab closed, unblocking this tab`);
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
  
  checkForExistingTabs() {
    // Send a ping to see if any tabs respond
    this.channel.postMessage({
      type: 'HEARTBEAT',
      tabId: this.tabId,
      timestamp: parseInt(this.tabId.split('_')[1])
    });
  }
  
  startHeartbeat() {
    // Send heartbeat every 2 seconds
    this.heartbeatInterval = setInterval(() => {
      if (this.isActive) {
        this.channel.postMessage({
          type: 'HEARTBEAT',
          tabId: this.tabId,
          timestamp: parseInt(this.tabId.split('_')[1])
        });
      }
    }, 2000);
  }
  
  blockTab() {
    console.log(`🚫 [TabManager] BLOCKING this tab: ${this.tabId}`);
    
    this.isActive = false;
    
    // Stop heartbeat
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    
    // Notify application
    window.dispatchEvent(new CustomEvent('tab_blocked', { 
      detail: { tabId: this.tabId } 
    }));
    
    // Show blocking overlay
    this.showBlockingOverlay();
    
    // Disconnect socket
    if (window.socket && window.socket.instance && window.socket.instance.connected) {
      console.log(`🔌 [TabManager] Disconnecting socket from blocked tab`);
      window.socket.instance.disconnect();
    }
  }
  
  unblockTab() {
    console.log(`✅ [TabManager] UNBLOCKING this tab: ${this.tabId}`);
    
    this.isActive = true;
    
    // Remove blocking overlay
    const overlay = document.getElementById('blocked-tab-overlay');
    if (overlay) {
      overlay.remove();
    }
    
    // Restart heartbeat
    this.startHeartbeat();
    
    // Notify application
    window.dispatchEvent(new CustomEvent('tab_unblocked', { 
      detail: { tabId: this.tabId } 
    }));
    
    // Reload page to reconnect
    window.location.reload();
  }
  
  showBlockingOverlay() {
    // Remove existing overlay if any
    const existingOverlay = document.getElementById('blocked-tab-overlay');
    if (existingOverlay) {
      existingOverlay.remove();
    }
    
    const overlay = document.createElement('div');
    overlay.id = 'blocked-tab-overlay';
    overlay.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.98);
      z-index: 999999;
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
      </div>
    `;
    
    document.body.appendChild(overlay);
    
    console.log(`🚫 [TabManager] Blocking overlay displayed`);
  }
  
  cleanup() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    this.channel.close();
  }
}

// ✅ Initialize TabManager (runs in background, never blocks)
const tabManager = new TabManager();

// ✅ Handle tab blocking (stop all activity)
window.addEventListener('tab_blocked', () => {
  console.log(`🚫 [App] This tab is now BLOCKED`);
  
  // Stop ALL activity
  if (window.socket && window.socket.instance) {
    window.socket.instance.disconnect();
  }
  
  // Clear intervals/timeouts
  for (let i = 1; i < 99999; i++) {
    window.clearInterval(i);
    window.clearTimeout(i);
  }
});
   

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
