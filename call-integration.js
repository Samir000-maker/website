/**
 * Call Page Integration
 * Preserves state during calls and enables seamless return to chat
 * 
 * Features:
 * - Call state preservation
 * - Return to chat with full message history
 * - No navigation guard (calls can be left freely)
 * - Automatic state sync
 */

// Load state manager
const stateManagerScript = document.createElement('script');
stateManagerScript.src = '/state-manager.js';
document.head.appendChild(stateManagerScript);

stateManagerScript.onload = () => {
  initializeCallPage();
};

function initializeCallPage() {
  const StateManager = window.StateManager;

  if (!StateManager) {
    console.error('❌ StateManager not loaded');
    return;
  }

  // Set current page
  StateManager.setPage('call');

  // Verify call and room data
  const state = StateManager.getState();
  
  if (!state.call && !state.room) {
    console.warn('⚠️ No active call or room found in state');
    
    // Check localStorage fallback
    const callDataStr = localStorage.getItem('activeCall');
    const roomDataStr = localStorage.getItem('currentRoom');
    
    if (callDataStr) {
      try {
        const callData = JSON.parse(callDataStr);
        StateManager.setCall(callData);
      } catch (e) {
        console.error('❌ Invalid call data in localStorage');
      }
    }
    
    if (roomDataStr) {
      try {
        const roomData = JSON.parse(roomDataStr);
        StateManager.setRoom(roomData);
      } catch (e) {
        console.error('❌ Invalid room data in localStorage');
      }
    }
  }

  // Track call state changes
  if (window.socketInstance) {
    window.socketInstance.on('call_joined', (data) => {
      StateManager.setCall(data);
    });

    window.socketInstance.on('user_joined_call', (data) => {
      // Update call participants in state
      const state = StateManager.getState();
      if (state.call) {
        // This would be handled by call.html logic
        console.log('👤 User joined call:', data.user.username);
      }
    });

    window.socketInstance.on('user_left_call', (data) => {
      console.log('👤 User left call:', data.username);
    });

    window.socketInstance.on('call_ended', () => {
      console.log('📵 Call ended');
      StateManager.clearCall();
    });
  }

  // Override leave call button
  const leaveCallBtn = document.getElementById('leaveCallBtn');
  if (leaveCallBtn) {
    // Store original handler
    const originalLeaveHandler = leaveCallBtn.onclick;
    
    leaveCallBtn.onclick = (e) => {
      console.log('📵 Leaving call - preserving chat state');
      
      // Call original handler
      if (originalLeaveHandler) {
        originalLeaveHandler.call(leaveCallBtn, e);
      }
      
      // Clear call state but preserve room
      StateManager.clearCall();
      
      // Force save before navigation
      StateManager.forceSave();
      
      // Navigate back to chat with state preserved
      setTimeout(() => {
        window.location.href = '/chat.html';
      }, 100);
    };
  }

  // Save state periodically during call
  const callStateInterval = setInterval(() => {
    if (!document.hidden) {
      StateManager.forceSave();
    }
  }, 15000); // Every 15 seconds

  // Clean up interval on page unload
  window.addEventListener('beforeunload', () => {
    clearInterval(callStateInterval);
    StateManager.forceSave();
  });

  // Save state on visibility change
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      StateManager.forceSave();
    }
  });

  console.log('✅ Call page integrated with StateManager');
}

// Alternative: If script is already loaded
if (window.StateManager) {
  initializeCallPage();
}
