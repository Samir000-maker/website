/**
 * Navigation Guard - Android Back Button Handler with Elegant Dialogs
 * Provides smooth, professional confirmation dialogs when users attempt to leave
 * critical pages (discovery, chat)
 * 
 * Features:
 * - Modern, animated confirmation dialogs
 * - Android back button interception
 * - Accessibility support
 * - Responsive design
 * - No navigation artifacts
 */

class NavigationGuard {
  constructor() {
    this.isGuardActive = false;
    this.currentPage = null;
    this.onConfirmCallback = null;
    this.dialogElement = null;
    this.dialogOverlay = null;
    
    // Initialize
    this.init();
  }

  /**
   * Initialize navigation guard
   */
  init() {
    // Create dialog elements
    this.createDialogElements();
    
    // Setup history state
    this.setupHistoryManagement();
    
    console.log('✅ NavigationGuard initialized');
  }

  /**
   * Create dialog DOM elements
   */
  createDialogElements() {
    // Create overlay
    this.dialogOverlay = document.createElement('div');
    this.dialogOverlay.className = 'nav-guard-overlay';
    this.dialogOverlay.style.cssText = `
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.5);
      backdrop-filter: blur(4px);
      z-index: 9998;
      display: none;
      opacity: 0;
      transition: opacity 0.3s ease;
    `;

    // Create dialog
    this.dialogElement = document.createElement('div');
    this.dialogElement.className = 'nav-guard-dialog';
    this.dialogElement.style.cssText = `
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%) scale(0.9);
      background: white;
      border-radius: 20px;
      padding: 24px;
      max-width: 90%;
      width: 380px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      z-index: 9999;
      display: none;
      opacity: 0;
      transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
    `;

    this.dialogElement.innerHTML = `
      <div class="nav-guard-header" style="margin-bottom: 16px;">
        <div style="width: 56px; height: 56px; background: #FEF3C7; border-radius: 50%; display: flex; align-items: center; justify-center; margin: 0 auto 16px;">
          <span style="font-size: 28px;">⚠️</span>
        </div>
        <h3 style="font-size: 20px; font-weight: 700; color: #1F2937; text-align: center; margin: 0 0 8px 0;">
          Leave this conversation?
        </h3>
        <p style="font-size: 14px; color: #6B7280; text-align: center; margin: 0; line-height: 1.5;">
          You're about to leave this space. Your progress will be saved, but you'll need to start a new search.
        </p>
      </div>
      
      <div class="nav-guard-actions" style="display: flex; gap: 12px; margin-top: 24px;">
        <button id="navGuardCancel" style="
          flex: 1;
          padding: 14px 20px;
          background: #F3F4F6;
          border: none;
          border-radius: 12px;
          font-size: 15px;
          font-weight: 600;
          color: #374151;
          cursor: pointer;
          transition: all 0.2s;
        ">
          Stay
        </button>
        <button id="navGuardConfirm" style="
          flex: 1;
          padding: 14px 20px;
          background: #EF4444;
          border: none;
          border-radius: 12px;
          font-size: 15px;
          font-weight: 600;
          color: white;
          cursor: pointer;
          transition: all 0.2s;
        ">
          Leave
        </button>
      </div>
    `;

    // Add hover effects via JavaScript
    const cancelBtn = this.dialogElement.querySelector('#navGuardCancel');
    const confirmBtn = this.dialogElement.querySelector('#navGuardConfirm');

    cancelBtn.addEventListener('mouseenter', () => {
      cancelBtn.style.background = '#E5E7EB';
    });
    cancelBtn.addEventListener('mouseleave', () => {
      cancelBtn.style.background = '#F3F4F6';
    });

    confirmBtn.addEventListener('mouseenter', () => {
      confirmBtn.style.background = '#DC2626';
    });
    confirmBtn.addEventListener('mouseleave', () => {
      confirmBtn.style.background = '#EF4444';
    });

    // Add active states
    cancelBtn.addEventListener('mousedown', () => {
      cancelBtn.style.transform = 'scale(0.97)';
    });
    cancelBtn.addEventListener('mouseup', () => {
      cancelBtn.style.transform = 'scale(1)';
    });

    confirmBtn.addEventListener('mousedown', () => {
      confirmBtn.style.transform = 'scale(0.97)';
    });
    confirmBtn.addEventListener('mouseup', () => {
      confirmBtn.style.transform = 'scale(1)';
    });

    // Append to body
    document.body.appendChild(this.dialogOverlay);
    document.body.appendChild(this.dialogElement);

    // Setup event listeners
    this.setupDialogListeners();
  }

  /**
   * Setup dialog event listeners
   */
  setupDialogListeners() {
    const cancelBtn = this.dialogElement.querySelector('#navGuardCancel');
    const confirmBtn = this.dialogElement.querySelector('#navGuardConfirm');

    cancelBtn.addEventListener('click', () => {
      this.hideDialog();
      // Push state back to prevent navigation
      this.pushGuardState();
    });

    confirmBtn.addEventListener('click', () => {
      this.hideDialog();
      // Allow navigation
      if (this.onConfirmCallback) {
        this.onConfirmCallback();
      }
    });

    // Close on overlay click
    this.dialogOverlay.addEventListener('click', () => {
      this.hideDialog();
      this.pushGuardState();
    });

    // Handle escape key
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && this.dialogElement.style.display !== 'none') {
        this.hideDialog();
        this.pushGuardState();
      }
    });
  }

  /**
   * Setup history management for back button
   */
  setupHistoryManagement() {
    // Listen for popstate (back button)
    window.addEventListener('popstate', (event) => {
      if (this.isGuardActive) {
        // Prevent default back navigation
        event.preventDefault();
        
        // Show confirmation dialog
        this.showDialog();
      }
    });
  }

  /**
   * Enable guard for a specific page
   */
  enable(page, onConfirm) {
    this.isGuardActive = true;
    this.currentPage = page;
    this.onConfirmCallback = onConfirm;
    
    // Push initial guard state
    this.pushGuardState();
    
    console.log(`🛡️ Navigation guard enabled for: ${page}`);
  }

  /**
   * Disable guard
   */
  disable() {
    this.isGuardActive = false;
    this.currentPage = null;
    this.onConfirmCallback = null;
    
    console.log('🛡️ Navigation guard disabled');
  }

  /**
   * Push guard state to history
   */
  pushGuardState() {
    if (this.isGuardActive) {
      history.pushState({ guard: true }, '');
    }
  }

  /**
   * Show confirmation dialog
   */
  showDialog() {
    // Show overlay
    this.dialogOverlay.style.display = 'block';
    this.dialogElement.style.display = 'block';
    
    // Force reflow
    this.dialogOverlay.offsetHeight;
    this.dialogElement.offsetHeight;
    
    // Animate in
    requestAnimationFrame(() => {
      this.dialogOverlay.style.opacity = '1';
      this.dialogElement.style.opacity = '1';
      this.dialogElement.style.transform = 'translate(-50%, -50%) scale(1)';
    });

    // Prevent body scroll
    document.body.style.overflow = 'hidden';
  }

  /**
   * Hide confirmation dialog
   */
  hideDialog() {
    // Animate out
    this.dialogOverlay.style.opacity = '0';
    this.dialogElement.style.opacity = '0';
    this.dialogElement.style.transform = 'translate(-50%, -50%) scale(0.9)';
    
    // Hide after animation
    setTimeout(() => {
      this.dialogOverlay.style.display = 'none';
      this.dialogElement.style.display = 'none';
      
      // Restore body scroll
      document.body.style.overflow = '';
    }, 300);
  }

  /**
   * Navigate away (bypass guard)
   */
  navigateAway(url) {
    this.disable();
    
    // Use timeout to ensure state is cleared
    setTimeout(() => {
      window.location.href = url;
    }, 50);
  }
}

// Global singleton instance
const navigationGuard = new NavigationGuard();

// Export for use in other modules
if (typeof window !== 'undefined') {
  window.NavigationGuard = navigationGuard;
}

export default navigationGuard;
