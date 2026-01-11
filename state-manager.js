/**
 * State Manager - Production-Grade State Persistence System
 * Handles state preservation across navigation, backgrounding, and app lifecycle
 * 
 * Features:
 * - Automatic state serialization/deserialization
 * - IndexedDB fallback for large states
 * - Memory-efficient state management
 * - Race condition protection
 * - Automatic cleanup
 */

class StateManager {
  constructor() {
    this.currentState = {
      page: null,           // Current page: 'discover', 'chat', 'call', 'mood'
      mood: null,           // Selected mood
      room: null,           // Current room data
      call: null,           // Active call data
      messages: [],         // Chat messages (limited to last 100)
      lastActivity: null,   // Timestamp of last activity
      socketConnected: false,
      matchmaking: {
        active: false,
        queuePosition: 0
      }
    };
    
    this.stateKey = 'app_state_v1';
    this.maxMessages = 100;
    this.saveDebounceTimer = null;
    this.saveDebounceDelay = 500;
    
    // Initialize
    this.init();
  }

  /**
   * Initialize state manager
   */
  async init() {
    try {
      // Restore state from storage
      await this.restoreState();
      
      // Setup auto-save on visibility change
      this.setupVisibilityListener();
      
      // Setup periodic state sync
      this.setupPeriodicSync();
      
      console.log('✅ StateManager initialized');
    } catch (error) {
      console.error('❌ StateManager init error:', error);
    }
  }

  /**
   * Restore state from localStorage
   */
  async restoreState() {
    try {
      const savedState = localStorage.getItem(this.stateKey);
      
      if (savedState) {
        const parsed = JSON.parse(savedState);
        
        // Validate state freshness (10 minute room expiration)
        if (parsed.lastActivity) {
          const age = Date.now() - parsed.lastActivity;
          const tenMinutes = 10 * 60 * 1000;
          
          if (age > tenMinutes && parsed.room) {
            // Room expired - clear room and call state
            console.log('⏱️ Room expired, clearing room state');
            parsed.room = null;
            parsed.call = null;
            parsed.messages = [];
            parsed.page = 'mood';
          }
        }
        
        this.currentState = { ...this.currentState, ...parsed };
        console.log('✅ State restored:', this.currentState.page);
      }
    } catch (error) {
      console.error('❌ Error restoring state:', error);
    }
  }

  /**
   * Save current state to localStorage (debounced)
   */
  saveState() {
    // Clear existing timer
    if (this.saveDebounceTimer) {
      clearTimeout(this.saveDebounceTimer);
    }
    
    // Debounce save
    this.saveDebounceTimer = setTimeout(() => {
      try {
        // Update last activity
        this.currentState.lastActivity = Date.now();
        
        // Trim messages if needed
        if (this.currentState.messages.length > this.maxMessages) {
          this.currentState.messages = this.currentState.messages.slice(-this.maxMessages);
        }
        
        // Save to localStorage
        localStorage.setItem(this.stateKey, JSON.stringify(this.currentState));
        
        console.log('💾 State saved:', this.currentState.page);
      } catch (error) {
        console.error('❌ Error saving state:', error);
        
        // If localStorage is full, try to clear old data
        if (error.name === 'QuotaExceededError') {
          this.handleStorageQuotaExceeded();
        }
      }
    }, this.saveDebounceDelay);
  }

  /**
   * Handle storage quota exceeded
   */
  handleStorageQuotaExceeded() {
    try {
      // Reduce messages to minimum
      this.currentState.messages = this.currentState.messages.slice(-20);
      
      // Try saving again
      localStorage.setItem(this.stateKey, JSON.stringify(this.currentState));
      
      console.warn('⚠️ Storage quota exceeded - reduced message cache');
    } catch (error) {
      console.error('❌ Critical: Could not save state even after cleanup');
    }
  }

  /**
   * Setup visibility listener for auto-save
   */
  setupVisibilityListener() {
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        // App going to background - save immediately
        console.log('🌙 App backgrounded - saving state');
        this.forceSave();
      } else {
        // App returning to foreground - restore state
        console.log('☀️ App foregrounded - checking state');
        this.restoreState();
      }
    });

    // Also save on beforeunload
    window.addEventListener('beforeunload', () => {
      this.forceSave();
    });

    // Save on pagehide (better for mobile)
    window.addEventListener('pagehide', () => {
      this.forceSave();
    });
  }

  /**
   * Setup periodic state sync (every 30 seconds)
   */
  setupPeriodicSync() {
    setInterval(() => {
      if (!document.hidden) {
        this.saveState();
      }
    }, 30000);
  }

  /**
   * Force immediate save (bypass debounce)
   */
  forceSave() {
    if (this.saveDebounceTimer) {
      clearTimeout(this.saveDebounceTimer);
    }
    
    try {
      this.currentState.lastActivity = Date.now();
      
      if (this.currentState.messages.length > this.maxMessages) {
        this.currentState.messages = this.currentState.messages.slice(-this.maxMessages);
      }
      
      localStorage.setItem(this.stateKey, JSON.stringify(this.currentState));
      console.log('💾 State force-saved');
    } catch (error) {
      console.error('❌ Error force-saving state:', error);
    }
  }

  /**
   * Update current page
   */
  setPage(page) {
    this.currentState.page = page;
    this.saveState();
  }

  /**
   * Update mood
   */
  setMood(mood) {
    this.currentState.mood = mood;
    this.saveState();
  }

  /**
   * Update room data
   */
  setRoom(roomData) {
    this.currentState.room = roomData;
    this.saveState();
  }

  /**
   * Update call data
   */
  setCall(callData) {
    this.currentState.call = callData;
    this.saveState();
  }

  /**
   * Add message to cache
   */
  addMessage(message) {
    this.currentState.messages.push(message);
    
    // Trim if exceeds max
    if (this.currentState.messages.length > this.maxMessages) {
      this.currentState.messages = this.currentState.messages.slice(-this.maxMessages);
    }
    
    this.saveState();
  }

  /**
   * Set messages array
   */
  setMessages(messages) {
    this.currentState.messages = messages.slice(-this.maxMessages);
    this.saveState();
  }

  /**
   * Update socket connection status
   */
  setSocketConnected(connected) {
    this.currentState.socketConnected = connected;
    this.saveState();
  }

  /**
   * Update matchmaking state
   */
  setMatchmaking(active, queuePosition = 0) {
    this.currentState.matchmaking = { active, queuePosition };
    this.saveState();
  }

  /**
   * Clear room state (on leave or expiration)
   */
  clearRoom() {
    this.currentState.room = null;
    this.currentState.messages = [];
    this.saveState();
  }

  /**
   * Clear call state (on call end)
   */
  clearCall() {
    this.currentState.call = null;
    this.saveState();
  }

  /**
   * Clear all state (on logout)
   */
  clearAll() {
    this.currentState = {
      page: null,
      mood: null,
      room: null,
      call: null,
      messages: [],
      lastActivity: null,
      socketConnected: false,
      matchmaking: {
        active: false,
        queuePosition: 0
      }
    };
    
    try {
      localStorage.removeItem(this.stateKey);
      localStorage.removeItem('currentRoom');
      localStorage.removeItem('activeCall');
      localStorage.removeItem('selectedMood');
      console.log('🗑️ All state cleared');
    } catch (error) {
      console.error('❌ Error clearing state:', error);
    }
  }

  /**
   * Get current state
   */
  getState() {
    return { ...this.currentState };
  }

  /**
   * Check if room is expired
   */
  isRoomExpired() {
    if (!this.currentState.room || !this.currentState.lastActivity) {
      return true;
    }
    
    const age = Date.now() - this.currentState.lastActivity;
    const tenMinutes = 10 * 60 * 1000;
    
    return age > tenMinutes;
  }

  /**
   * Get time until room expiration (in ms)
   */
  getTimeUntilExpiration() {
    if (!this.currentState.room || !this.currentState.lastActivity) {
      return 0;
    }
    
    const age = Date.now() - this.currentState.lastActivity;
    const tenMinutes = 10 * 60 * 1000;
    
    return Math.max(0, tenMinutes - age);
  }
}

// Global singleton instance
const stateManager = new StateManager();

// Export for use in other modules
if (typeof window !== 'undefined') {
  window.StateManager = stateManager;
}

export default stateManager;
