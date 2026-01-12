/**
 * Production Navigation Guard
 * Handles Android back button with beautiful confirmation dialogs
 * Works reliably across all browsers and devices
 */

(function() {
  'use strict';

  class NavigationGuard {
    constructor() {
      this.enabled = false;
      this.currentPage = null;
      this.onLeaveCallback = null;
      this.dialog = null;
      this.overlay = null;
      this.guardState = { guard: true, timestamp: Date.now() };
      
      this.init();
    }

    init() {
      this.createDialog();
      this.setupBackButton();
      console.log('✅ NavigationGuard initialized (Production)');
    }

    /**
     * Create confirmation dialog
     */
    createDialog() {
      // Overlay
      this.overlay = document.createElement('div');
      this.overlay.style.cssText = `
        position: fixed;
        inset: 0;
        background: rgba(0, 0, 0, 0.6);
        backdrop-filter: blur(8px);
        z-index: 999998;
        display: none;
        opacity: 0;
        transition: opacity 0.3s cubic-bezier(0.4, 0, 0.2, 1);
      `;

      // Dialog
      this.dialog = document.createElement('div');
      this.dialog.style.cssText = `
        position: fixed;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%) scale(0.95);
        background: white;
        border-radius: 24px;
        padding: 32px 24px 24px;
        max-width: min(90vw, 400px);
        width: 100%;
        box-shadow: 0 24px 48px rgba(0, 0, 0, 0.3);
        z-index: 999999;
        display: none;
        opacity: 0;
        transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
      `;

      this.dialog.innerHTML = `
        <div style="text-align: center;">
          <div style="width: 72px; height: 72px; background: linear-gradient(135deg, #FEF3C7 0%, #FDE68A 100%); border-radius: 50%; display: flex; align-items: center; justify-center; margin: 0 auto 20px; box-shadow: 0 8px 16px rgba(251, 191, 36, 0.3);">
            <span style="font-size: 36px;">⚠️</span>
          </div>
          <h3 style="font-size: 22px; font-weight: 700; color: #111827; margin: 0 0 12px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
            Leave this space?
          </h3>
          <p style="font-size: 15px; color: #6B7280; margin: 0 0 28px 0; line-height: 1.6; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
            You're about to leave this conversation. Your messages will be saved, but you'll need to find a new match.
          </p>
          <div style="display: flex; gap: 12px;">
            <button id="navGuardStay" style="
              flex: 1;
              padding: 16px;
              background: #F3F4F6;
              border: none;
              border-radius: 14px;
              font-size: 16px;
              font-weight: 600;
              color: #374151;
              cursor: pointer;
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
              transition: all 0.2s;
              touch-action: manipulation;
              -webkit-tap-highlight-color: transparent;
            ">
              Stay
            </button>
            <button id="navGuardLeave" style="
              flex: 1;
              padding: 16px;
              background: linear-gradient(135deg, #EF4444 0%, #DC2626 100%);
              border: none;
              border-radius: 14px;
              font-size: 16px;
              font-weight: 600;
              color: white;
              cursor: pointer;
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
              transition: all 0.2s;
              box-shadow: 0 4px 12px rgba(239, 68, 68, 0.3);
              touch-action: manipulation;
              -webkit-tap-highlight-color: transparent;
            ">
              Leave
            </button>
          </div>
        </div>
      `;

      document.body.appendChild(this.overlay);
      document.body.appendChild(this.dialog);

      this.setupDialogEvents();
    }

    /**
     * Setup dialog button events
     */
    setupDialogEvents() {
      const stayBtn = document.getElementById('navGuardStay');
      const leaveBtn = document.getElementById('navGuardLeave');

      // Stay button
      stayBtn.addEventListener('click', () => {
        this.hideDialog();
        this.pushState();
      });

      // Leave button
      leaveBtn.addEventListener('click', () => {
        this.hideDialog();
        this.enabled = false;
        if (this.onLeaveCallback) {
          this.onLeaveCallback();
        }
      });

      // Hover effects
      stayBtn.addEventListener('mouseenter', () => {
        stayBtn.style.background = '#E5E7EB';
      });
      stayBtn.addEventListener('mouseleave', () => {
        stayBtn.style.background = '#F3F4F6';
      });

      leaveBtn.addEventListener('mouseenter', () => {
        leaveBtn.style.background = 'linear-gradient(135deg, #DC2626 0%, #B91C1C 100%)';
      });
      leaveBtn.addEventListener('mouseleave', () => {
        leaveBtn.style.background = 'linear-gradient(135deg, #EF4444 0%, #DC2626 100%)';
      });

      // Active states
      stayBtn.addEventListener('touchstart', () => {
        stayBtn.style.transform = 'scale(0.97)';
      });
      stayBtn.addEventListener('touchend', () => {
        stayBtn.style.transform = 'scale(1)';
      });

      leaveBtn.addEventListener('touchstart', () => {
        leaveBtn.style.transform = 'scale(0.97)';
      });
      leaveBtn.addEventListener('touchend', () => {
        leaveBtn.style.transform = 'scale(1)';
      });

      // Close on overlay click
      this.overlay.addEventListener('click', () => {
        this.hideDialog();
        this.pushState();
      });

      // Escape key
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && this.dialog.style.display !== 'none') {
          this.hideDialog();
          this.pushState();
        }
      });
    }

    /**
     * Setup back button handling
     */
    setupBackButton() {
      // Listen for popstate
      window.addEventListener('popstate', (event) => {
        if (this.enabled && event.state?.guard) {
          console.log('🛡️ Back button intercepted');
          event.preventDefault();
          event.stopPropagation();
          this.showDialog();
          return false;
        }
      });

      // Also intercept hashchange
      window.addEventListener('hashchange', (event) => {
        if (this.enabled) {
          event.preventDefault();
          this.pushState();
        }
      });
    }

    /**
     * Enable guard for a page
     */
    enable(page, onLeave) {
      this.enabled = true;
      this.currentPage = page;
      this.onLeaveCallback = onLeave;
      this.pushState();
      console.log(`🛡️ Guard enabled: ${page}`);
    }

    /**
     * Disable guard
     */
    disable() {
      this.enabled = false;
      this.currentPage = null;
      this.onLeaveCallback = null;
      console.log('🛡️ Guard disabled');
    }

    /**
     * Push guard state to history
     */
    pushState() {
      if (this.enabled) {
        this.guardState = { guard: true, timestamp: Date.now() };
        history.pushState(this.guardState, '', location.href);
      }
    }

    /**
     * Show dialog
     */
    showDialog() {
      // Show elements
      this.overlay.style.display = 'block';
      this.dialog.style.display = 'block';
      
      // Trigger reflow
      void this.overlay.offsetHeight;
      void this.dialog.offsetHeight;
      
      // Animate in
      requestAnimationFrame(() => {
        this.overlay.style.opacity = '1';
        this.dialog.style.opacity = '1';
        this.dialog.style.transform = 'translate(-50%, -50%) scale(1)';
      });

      // Prevent body scroll
      document.body.style.overflow = 'hidden';
      document.body.style.position = 'fixed';
      document.body.style.width = '100%';
    }

    /**
     * Hide dialog
     */
    hideDialog() {
      // Animate out
      this.overlay.style.opacity = '0';
      this.dialog.style.opacity = '0';
      this.dialog.style.transform = 'translate(-50%, -50%) scale(0.95)';
      
      // Hide after animation
      setTimeout(() => {
        this.overlay.style.display = 'none';
        this.dialog.style.display = 'none';
        
        // Restore body scroll
        document.body.style.overflow = '';
        document.body.style.position = '';
        document.body.style.width = '';
      }, 300);
    }

    /**
     * Navigate away (bypass guard)
     */
    navigateTo(url) {
      this.disable();
      setTimeout(() => {
        window.location.href = url;
      }, 100);
    }
  }

  // Create global instance
  window.NavigationGuard = new NavigationGuard();

})();
