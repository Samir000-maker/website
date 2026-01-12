/**
 * Chat Page Integration
 * Integrates StateManager and NavigationGuard with message preservation
 * 
 * Features:
 * - Full message history preservation
 * - State restoration when returning from call
 * - Android back button handling
 * - 10-minute room expiration tracking
 * - Seamless call transitions
 */

// Load dependencies
const stateManagerScript = document.createElement('script');
stateManagerScript.src = '/state-manager.js';
document.head.appendChild(stateManagerScript);

const navGuardScript = document.createElement('script');
navGuardScript.src = '/navigation-guard.js';
document.head.appendChild(navGuardScript);

// Wait for scripts to load
stateManagerScript.onload = () => {
  navGuardScript.onload = () => {
    initializeChatPage();
  };
};

function initializeChatPage() {
  const StateManager = window.StateManager;
  const NavigationGuard = window.NavigationGuard;

  if (!StateManager || !NavigationGuard) {
    console.error('❌ Required managers not loaded');
    return;
  }

  // Set current page
  StateManager.setPage('chat');

  // Check if room is expired
  if (StateManager.isRoomExpired()) {
    console.log('⏱️ Room expired - redirecting to mood page');
    window.location.href = '/mood.html';
    return;
  }

  // Restore messages from state
  const state = StateManager.getState();
  if (state.messages && state.messages.length > 0) {
    restoreMessages(state.messages);
  }

  // Enable navigation guard
  NavigationGuard.enable('chat', () => {
    // On confirm leave
    console.log('👋 User confirmed leaving chat');
    
    // Leave room via socket
    if (window.socketInstance && window.socketInstance.connected) {
      window.socketInstance.emit('leave_room');
    }
    
    // Clear room state
    StateManager.clearRoom();
    
    // Navigate to mood page
    NavigationGuard.navigateAway('/mood.html');
  });

  // Intercept socket chat messages and save to state
  if (window.socketInstance) {
    const originalOnChatMessage = window.socketInstance._callbacks?.$chat_message;
    
    window.socketInstance.on('chat_message', (data) => {
      // Add to state manager
      StateManager.addMessage(data);
      
      // Call original handler if it exists
      if (originalOnChatMessage) {
        originalOnChatMessage.forEach(fn => fn(data));
      }
    });

    // Track room state
    window.socketInstance.on('room_joined', (data) => {
      StateManager.setRoom(data);
    });

    window.socketInstance.on('user_left', (data) => {
      // Update state if needed
      const state = StateManager.getState();
      if (state.room && data.remainingUsers !== undefined) {
        state.room.remainingUsers = data.remainingUsers;
        StateManager.setRoom(state.room);
      }
    });

    window.socketInstance.on('left_room', () => {
      StateManager.clearRoom();
    });
  }

  // Setup room expiration timer
  setupExpirationTimer();

  // Save state on visibility change
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      // Save current messages
      const messagesList = document.getElementById('messagesList');
      if (messagesList) {
        const messages = extractMessagesFromDOM();
        StateManager.setMessages(messages);
      }
      
      StateManager.forceSave();
    } else {
      // Check expiration on return
      if (StateManager.isRoomExpired()) {
        console.log('⏱️ Room expired while backgrounded');
        window.location.href = '/mood.html';
      }
    }
  });

  // Handle call navigation
  handleCallTransitions();

  console.log('✅ Chat page integrated with StateManager and NavigationGuard');
}

/**
 * Restore messages from state
 */
function restoreMessages(messages) {
  const messagesList = document.getElementById('messagesList');
  if (!messagesList) return;

  console.log(`📨 Restoring ${messages.length} messages from state`);

  messages.forEach(msgData => {
    // Check if message already exists
    const existingMsg = messagesList.querySelector(`[data-message-id="${msgData.messageId}"]`);
    if (existingMsg) return;

    // Create message element (reuse existing function)
    if (typeof window.createMessageElement === 'function') {
      const currentUser = window.currentUser || {};
      const isCurrentUser = msgData.userId === currentUser.userId;
      const msgEl = window.createMessageElement(msgData, isCurrentUser);
      messagesList.appendChild(msgEl);
    }
  });

  // Scroll to bottom
  messagesList.scrollTop = messagesList.scrollHeight;
}

/**
 * Extract messages from DOM
 */
function extractMessagesFromDOM() {
  const messagesList = document.getElementById('messagesList');
  if (!messagesList) return [];

  const messages = [];
  const messageElements = messagesList.querySelectorAll('.message-item');

  messageElements.forEach(el => {
    const messageId = el.dataset.messageId;
    const usernameEl = el.querySelector('.font-semibold');
    const messageTextEl = el.querySelector('.break-words');
    
    if (messageId && usernameEl && messageTextEl) {
      messages.push({
        messageId,
        username: usernameEl.textContent,
        message: messageTextEl.textContent,
        timestamp: Date.now()
      });
    }
  });

  return messages;
}

/**
 * Setup room expiration timer
 */
function setupExpirationTimer() {
  const StateManager = window.StateManager;
  
  // Check every 30 seconds
  const expirationCheck = setInterval(() => {
    if (StateManager.isRoomExpired()) {
      console.log('⏱️ Room expired - cleaning up');
      
      // Clear interval
      clearInterval(expirationCheck);
      
      // Show notification
      if (window.MoodApp && window.MoodApp.Toast) {
        window.MoodApp.Toast.warning('Room has expired');
      }
      
      // Clear state
      StateManager.clearRoom();
      
      // Redirect
      setTimeout(() => {
        window.location.href = '/mood.html';
      }, 2000);
    }
  }, 30000);

  // Clear on page unload
  window.addEventListener('beforeunload', () => {
    clearInterval(expirationCheck);
  });
}

/**
 * Handle call transitions
 */
function handleCallTransitions() {
  const StateManager = window.StateManager;

  // Before navigating to call, save full chat state
  const audioCallBtn = document.getElementById('audioCallBtn');
  const videoCallBtn = document.getElementById('videoCallBtn');

  if (audioCallBtn) {
    audioCallBtn.addEventListener('click', () => {
      console.log('📞 Navigating to call - saving chat state');
      
      // Save messages
      const messages = extractMessagesFromDOM();
      StateManager.setMessages(messages);
      
      // Force save
      StateManager.forceSave();
    });
  }

  if (videoCallBtn) {
    videoCallBtn.addEventListener('click', () => {
      console.log('📞 Navigating to call - saving chat state');
      
      // Save messages
      const messages = extractMessagesFromDOM();
      StateManager.setMessages(messages);
      
      // Force save
      StateManager.forceSave();
    });
  }

  // Listen for call acceptance
  if (window.socketInstance) {
    window.socketInstance.on('call_accepted', (data) => {
      console.log('✅ Call accepted - saving state before navigation');
      
      // Save call data
      StateManager.setCall(data);
      
      // Save messages
      const messages = extractMessagesFromDOM();
      StateManager.setMessages(messages);
      
      // Force save
      StateManager.forceSave();
    });
  }
}

// Alternative: If scripts are already loaded
if (window.StateManager && window.NavigationGuard) {
  initializeChatPage();
}
