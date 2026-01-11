/**
 * Discovery Page Integration
 * Integrates StateManager and NavigationGuard
 * 
 * Features:
 * - State preservation on backgrounding
 * - Android back button handling with confirmation
 * - Automatic state restoration
 * - Matchmaking state tracking
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
    initializeDiscoveryPage();
  };
};

function initializeDiscoveryPage() {
  const StateManager = window.StateManager;
  const NavigationGuard = window.NavigationGuard;

  if (!StateManager || !NavigationGuard) {
    console.error('❌ Required managers not loaded');
    return;
  }

  // Set current page
  StateManager.setPage('discover');

  // Enable navigation guard
  NavigationGuard.enable('discover', () => {
    // On confirm leave
    console.log('👋 User confirmed leaving discovery');
    
    // Clear matchmaking state
    StateManager.setMatchmaking(false, 0);
    
    // Cancel matchmaking on socket
    if (window.socketInstance && window.socketInstance.connected) {
      window.socketInstance.emit('cancel_matchmaking');
    }
    
    // Navigate to mood page (NOT back to discovery)
    NavigationGuard.navigateAway('/mood.html');
  });

  // Override cancel button to use navigation guard
  const originalCancelBtn = document.getElementById('cancelBtn');
  if (originalCancelBtn) {
    originalCancelBtn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      
      // Show confirmation dialog via back button simulation
      history.back();
    });
  }

  // Track matchmaking state
  if (window.socketInstance) {
    window.socketInstance.on('queued', (data) => {
      StateManager.setMatchmaking(true, data.position || 0);
    });

    window.socketInstance.on('match_found', (data) => {
      StateManager.setMatchmaking(false, 0);
      StateManager.setRoom(data);
    });

    window.socketInstance.on('matchmaking_cancelled', () => {
      StateManager.setMatchmaking(false, 0);
    });
  }

  // Save state on visibility change
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      StateManager.forceSave();
    }
  });

  console.log('✅ Discovery page integrated with StateManager and NavigationGuard');
}

// Alternative: If scripts are already loaded
if (window.StateManager && window.NavigationGuard) {
  initializeDiscoveryPage();
}
