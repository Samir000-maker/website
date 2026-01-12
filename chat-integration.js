/**
 * Chat Page - Production Integration
 * Fixes: Message loss, call transitions, background execution
 * 
 * Add this script INLINE in chat.html before </body>
 * Or load after state-manager-fixed.js and navigation-guard-fixed.js
 */

(function() {
  'use strict';

  console.log('🚀 Chat page integration loading...');

  // Wait for dependencies
  const waitForDependencies = () => {
    return new Promise((resolve) => {
      const check = setInterval(() => {
        if (window.StateManager && window.NavigationGuard) {
          clearInterval(check);
          resolve();
        }
      }, 100);
      
      // Timeout after 5 seconds
      setTimeout(() => {
        clearInterval(check);
        if (!window.StateManager || !window.NavigationGuard) {
          console.error('❌ Dependencies not loaded');
        }
        resolve();
      }, 5000);
    });
  };

  waitForDependencies().then(() => {
    initChatPage();
  });

  function initChatPage() {
    const StateManager = window.StateManager;
    const NavigationGuard = window.NavigationGuard;

    if (!StateManager || !NavigationGuard) {
      console.error('❌ Required managers missing');
      return;
    }

    // Set current page
    StateManager.setPage('chat');

    // Check room expiration
    if (StateManager.isRoomExpired()) {
      console.log('⏱️ Room expired');
      window.location.href = '/mood.html';
      return;
    }

    // Restore missed messages if any
    const state = StateManager.getState();
    if (state.messages && state.messages.length > 0) {
      console.log(`📨 Restoring ${state.messages.length} messages`);
      restoreMessages(state.messages);
    }

    // Enable navigation guard
    NavigationGuard.enable('chat', () => {
      console.log('👋 User leaving chat');
      
      // Leave room via socket
      if (window.socketInstance?.connected) {
        window.socketInstance.emit('leave_room');
      }
      
      // Clear room state
      StateManager.clearRoom();
      
      // Navigate to mood (NOT discovery!)
      NavigationGuard.navigateTo('/mood.html');
    });

    // Setup message interceptor
    setupMessageInterceptor();

    // Setup call button interceptors
    setupCallButtons();

    // Setup leave button
    setupLeaveButton();

    // Setup background message handling
    setupBackgroundHandling();

    // Setup room expiration tracking
    setupExpirationTracking();

    console.log('✅ Chat page integrated');
  }

  /**
   * Restore messages from state
   */
  function restoreMessages(messages) {
    const messagesList = document.getElementById('messagesList');
    if (!messagesList) return;

    messages.forEach(msg => {
      // Skip if already exists
      const existing = messagesList.querySelector(`[data-message-id="${msg.messageId}"]`);
      if (existing) return;

      // Create and append
      if (typeof window.createMessageElement === 'function') {
        const currentUser = window.currentUser || {};
        const isCurrentUser = msg.userId === currentUser.userId;
        const msgEl = window.createMessageElement(msg, isCurrentUser);
        messagesList.appendChild(msgEl);
      }
    });

    // Scroll to bottom
    messagesList.scrollTop = messagesList.scrollHeight;
  }

  /**
   * Display missed messages received while backgrounded
   */
  window.displayMissedMessages = function(missedMessages) {
    if (!missedMessages || missedMessages.length === 0) return;

    console.log(`📬 Displaying ${missedMessages.length} missed messages`);

    const messagesList = document.getElementById('messagesList');
    if (!messagesList) return;

    // Show notification
    const notification = document.createElement('div');
    notification.style.cssText = `
      position: sticky;
      top: 0;
      z-index: 100;
      background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%);
      color: white;
      padding: 12px;
      text-align: center;
      font-size: 14px;
      font-weight: 600;
      animation: slideDown 0.3s ease-out;
      margin-bottom: 16px;
      border-radius: 12px;
      box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3);
    `;
    notification.textContent = `📬 ${missedMessages.length} new message${missedMessages.length > 1 ? 's' : ''} while you were away`;
    
    messagesList.appendChild(notification);

    // Remove after 3 seconds
    setTimeout(() => {
      notification.style.animation = 'slideUp 0.3s ease-out';
      setTimeout(() => notification.remove(), 300);
    }, 3000);

    // Add messages
    missedMessages.forEach(msg => {
      if (typeof window.createMessageElement === 'function') {
        const currentUser = window.currentUser || {};
        const isCurrentUser = msg.userId === currentUser.userId;
        const msgEl = window.createMessageElement(msg, isCurrentUser);
        
        // Highlight as new
        msgEl.style.animation = 'highlightNew 1s ease-out';
        
        messagesList.appendChild(msgEl);
      }
    });

    // Scroll to bottom
    messagesList.scrollTop = messagesList.scrollHeight;
  };

  /**
   * Setup message interceptor
   */
  function setupMessageInterceptor() {
    if (!window.socketInstance) return;

    // Store original handler
    const originalHandlers = window.socketInstance._callbacks?.$chat_message || [];

    // Remove original handlers
    window.socketInstance.off('chat_message');

    // Add our interceptor
    window.socketInstance.on('chat_message', (data) => {
      // Add to state manager
      window.StateManager.addMessage(data);

      // If not backgrounded, display immediately
      if (!document.hidden) {
        // Call original handlers
        originalHandlers.forEach(handler => handler(data));
      }
    });

    console.log('✅ Message interceptor installed');
  }

  /**
   * Setup call button interceptors
   */
  function setupCallButtons() {
    const audioBtn = document.getElementById('audioCallBtn');
    const videoBtn = document.getElementById('videoCallBtn');

    if (audioBtn) {
      audioBtn.addEventListener('click', () => {
        console.log('📞 Saving state before audio call');
        saveStateBeforeCall();
      }, true); // Use capture phase
    }

    if (videoBtn) {
      videoBtn.addEventListener('click', () => {
        console.log('📞 Saving state before video call');
        saveStateBeforeCall();
      }, true); // Use capture phase
    }

    // Intercept call_accepted event
    if (window.socketInstance) {
      window.socketInstance.on('call_accepted', (data) => {
        console.log('✅ Call accepted - preserving chat state');
        saveStateBeforeCall();
        window.StateManager.setCall(data);
      });
    }
  }

  /**
   * Save state before navigating to call
   */
  function saveStateBeforeCall() {
    const messages = extractMessages();
    window.StateManager.state.messages = messages;
    window.StateManager.forceSave();
  }

  /**
   * Extract messages from DOM
   */
  function extractMessages() {
    const messagesList = document.getElementById('messagesList');
    if (!messagesList) return [];

    const messages = [];
    const messageElements = messagesList.querySelectorAll('.message-item');

    messageElements.forEach(el => {
      try {
        const messageId = el.dataset.messageId;
        const usernameEl = el.querySelector('.font-semibold');
        const textEl = el.querySelector('.break-words');

        if (messageId && usernameEl && textEl) {
          messages.push({
            messageId,
            username: usernameEl.textContent,
            message: textEl.textContent,
            timestamp: Date.now()
          });
        }
      } catch (err) {
        console.warn('Failed to extract message:', err);
      }
    });

    return messages;
  }

  /**
   * Setup leave button
   */
  function setupLeaveButton() {
    const leaveBtn = document.getElementById('leaveBtn');
    if (!leaveBtn) return;

    // Remove all existing listeners
    const newLeaveBtn = leaveBtn.cloneNode(true);
    leaveBtn.parentNode.replaceChild(newLeaveBtn, leaveBtn);

    // Add new listener that triggers back button
    newLeaveBtn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      
      console.log('🚪 Leave button clicked - showing confirmation');
      
      // Trigger back button (will show dialog)
      history.back();
    });

    console.log('✅ Leave button intercepted');
  }

  /**
   * Setup background handling
   */
  function setupBackgroundHandling() {
    // Notify socket of background state
    document.addEventListener('visibilitychange', () => {
      if (window.socketInstance?.connected) {
        window.socketInstance.emit('background_mode', {
          active: document.hidden
        });
      }

      // Save state when backgrounding
      if (document.hidden) {
        const messages = extractMessages();
        window.StateManager.state.messages = messages;
        window.StateManager.forceSave();
      }
    });
  }

  /**
   * Setup expiration tracking
   */
  function setupExpirationTracking() {
    // Check every 30 seconds
    const checkExpiration = setInterval(() => {
      if (window.StateManager.isRoomExpired()) {
        clearInterval(checkExpiration);
        
        if (window.MoodApp?.Toast) {
          window.MoodApp.Toast.warning('Room has expired');
        }
        
        window.StateManager.clearRoom();
        
        setTimeout(() => {
          window.location.href = '/mood.html';
        }, 2000);
      }
    }, 30000);

    // Listen for expiration events
    if (window.socketInstance) {
      window.socketInstance.on('room_expired', (data) => {
        console.log('⏱️ Room expired:', data.message);
        clearInterval(checkExpiration);
        
        if (window.MoodApp?.Toast) {
          window.MoodApp.Toast.warning(data.message);
        }
        
        window.StateManager.clearRoom();
        
        setTimeout(() => {
          window.location.href = '/mood.html';
        }, 2000);
      });

      window.socketInstance.on('room_expiring_soon', (data) => {
        if (window.MoodApp?.Toast) {
          window.MoodApp.Toast.warning('This conversation will end in 1 minute');
        }
      });
    }
  }

  // Add CSS animations
  const style = document.createElement('style');
  style.textContent = `
    @keyframes slideDown {
      from {
        transform: translateY(-100%);
        opacity: 0;
      }
      to {
        transform: translateY(0);
        opacity: 1;
      }
    }
    
    @keyframes slideUp {
      from {
        transform: translateY(0);
        opacity: 1;
      }
      to {
        transform: translateY(-100%);
        opacity: 0;
      }
    }
    
    @keyframes highlightNew {
      0%, 100% {
        background: transparent;
      }
      50% {
        background: rgba(59, 130, 246, 0.1);
      }
    }
  `;
  document.head.appendChild(style);

})();
