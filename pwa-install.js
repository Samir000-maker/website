(function () {
  function ensureStyles() {
    if (document.getElementById('vibePwaInstallStyles')) return;
    const style = document.createElement('style');
    style.id = 'vibePwaInstallStyles';
    style.textContent = `
      [data-pwa-install="1"] {
        appearance: none;
        -webkit-appearance: none;
        border: 1px solid rgba(255,255,255,0.12);
        background: linear-gradient(135deg, rgba(255,255,255,0.10), rgba(255,255,255,0.04));
        color: rgba(255,255,255,0.92);
        border-radius: 9999px;
        padding: 10px 14px;
        font-weight: 700;
        font-size: 13px;
        line-height: 1;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        box-shadow: 0 10px 30px rgba(0,0,0,0.30);
        transition: transform 180ms ease, box-shadow 180ms ease, background 180ms ease, border-color 180ms ease;
        user-select: none;
      }
      [data-pwa-install="1"]:hover {
        transform: translateY(-1px);
        border-color: rgba(99,32,233,0.45);
        box-shadow: 0 16px 45px rgba(99,32,233,0.20), 0 12px 35px rgba(0,0,0,0.35);
      }
      [data-pwa-install="1"]:active {
        transform: translateY(0);
      }
      [data-pwa-install="1"]:focus-visible {
        outline: 2px solid rgba(99,32,233,0.7);
        outline-offset: 3px;
      }
      [data-pwa-install="1"]:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }
      [data-pwa-install="1"] .vibe-pwa-icon {
        width: 18px;
        height: 18px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: 8px;
        background: rgba(99,32,233,0.25);
        border: 1px solid rgba(99,32,233,0.35);
        box-shadow: 0 0 0 1px rgba(0,0,0,0.15) inset;
      }

      .pwa-install-modal {
        position: fixed;
        inset: 0;
        z-index: 1000;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .pwa-install-modal.hidden { display: none !important; }
      .pwa-install-modal__backdrop {
        position: absolute;
        inset: 0;
        background: rgba(0,0,0,0.65);
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
      }
      .pwa-install-modal__card {
        position: relative;
        width: min(90vw, 400px);
        border-radius: 20px;
        background: rgba(21, 22, 28, 0.97);
        border: 1px solid rgba(255,255,255,0.12);
        box-shadow: 0 30px 80px rgba(0,0,0,0.60);
        overflow: hidden;
        padding: 24px;
      }
      .pwa-install-modal__icon {
        width: 56px;
        height: 56px;
        margin: 0 auto 16px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 16px;
        background: linear-gradient(135deg, rgba(99,32,233,0.25), rgba(99,32,233,0.15));
        border: 1px solid rgba(99,32,233,0.35);
        box-shadow: 0 8px 24px rgba(99,32,233,0.20);
      }
      .pwa-install-modal__icon svg {
        width: 28px;
        height: 28px;
        color: rgba(255,255,255,0.92);
        animation: installPulse 2s ease-in-out infinite;
      }
      @keyframes installPulse {
        0%, 100% { transform: scale(1); opacity: 1; }
        50% { transform: scale(1.05); opacity: 0.85; }
      }
      .pwa-install-modal__title {
        font-weight: 800;
        font-size: 18px;
        letter-spacing: -0.01em;
        color: rgba(255,255,255,0.96);
        text-align: center;
        margin-bottom: 8px;
      }
      .pwa-install-modal__status {
        font-size: 14px;
        color: rgba(148,163,184,0.90);
        text-align: center;
        margin-bottom: 20px;
      }
      .pwa-install-modal__spinner {
        width: 40px;
        height: 40px;
        margin: 0 auto 16px;
        border: 3px solid rgba(99,32,233,0.2);
        border-top-color: #6320e9;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
      }
      @keyframes spin {
        to { transform: rotate(360deg); }
      }
      .pwa-install-modal__complete {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 16px;
      }
      .pwa-install-modal__complete-icon {
        width: 64px;
        height: 64px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 50%;
        background: rgba(34,197,94,0.15);
        border: 2px solid rgba(34,197,94,0.40);
        animation: scaleIn 0.4s ease;
      }
      @keyframes scaleIn {
        0% { transform: scale(0.8); opacity: 0; }
        100% { transform: scale(1); opacity: 1; }
      }
      .pwa-install-modal__complete-icon svg {
        width: 32px;
        height: 32px;
        color: rgba(34,197,94,0.95);
      }
      .pwa-install-modal__complete-text {
        font-weight: 700;
        font-size: 16px;
        color: rgba(255,255,255,0.94);
      }
      .pwa-install-modal__button {
        appearance: none;
        -webkit-appearance: none;
        border: 1px solid rgba(99,32,233,0.45);
        background: rgba(99,32,233,0.22);
        color: rgba(255,255,255,0.95);
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 800;
        font-size: 14px;
        cursor: pointer;
        transition: all 180ms ease;
        margin-top: 8px;
      }
      .pwa-install-modal__button:hover {
        background: rgba(99,32,233,0.30);
        border-color: rgba(99,32,233,0.55);
        transform: translateY(-1px);
      }
    `;
    document.head.appendChild(style);
  }

  function setHidden(el, hidden) {
    if (!el) return;
    if (hidden) {
      el.classList.add('hidden');
      el.setAttribute('aria-hidden', 'true');
    } else {
      el.classList.remove('hidden');
      el.setAttribute('aria-hidden', 'false');
    }
  }

  function ensureInstallModal() {
    const existing = document.getElementById('pwaInstallModal');
    if (existing) return existing;

    const modal = document.createElement('div');
    modal.id = 'pwaInstallModal';
    modal.className = 'pwa-install-modal hidden';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.innerHTML = `
      <div class="pwa-install-modal__backdrop"></div>
      <div class="pwa-install-modal__card">
        <div class="pwa-install-modal__content">
          <div class="pwa-install-modal__icon">
            <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 18h.01M8 21h8a2 2 0 002-2V5a2 2 0 00-2-2H8a2 2 0 00-2 2v14a2 2 0 002 2z" />
            </svg>
          </div>
          <div class="pwa-install-modal__title">Installing Vibegra</div>
          <div class="pwa-install-modal__status">Please wait...</div>
          <div class="pwa-install-modal__spinner"></div>
        </div>
      </div>
    `;

    document.body.appendChild(modal);
    return modal;
  }

  async function clearAllCaches() {
    try {
      // Clear all service worker caches
      if ('caches' in window) {
        const cacheNames = await caches.keys();
        await Promise.all(cacheNames.map(name => caches.delete(name)));
      }

      // Unregister all service workers
      if ('serviceWorker' in navigator) {
        const registrations = await navigator.serviceWorker.getRegistrations();
        await Promise.all(registrations.map(reg => reg.unregister()));
      }

      // Clear localStorage completely
      try {
        localStorage.clear();
      } catch (e) {}

      // Clear sessionStorage
      try {
        sessionStorage.clear();
      } catch (e) {}

      // Clear IndexedDB
      try {
        if (window.indexedDB) {
          const dbs = await indexedDB.databases();
          await Promise.all(dbs.map(db => {
            if (db.name) {
              return new Promise((resolve) => {
                const request = indexedDB.deleteDatabase(db.name);
                request.onsuccess = () => resolve();
                request.onerror = () => resolve();
              });
            }
          }));
        }
      } catch (e) {}

      // Clear cookies related to PWA
      try {
        document.cookie.split(";").forEach(c => {
          document.cookie = c.replace(/^ +/, "").replace(/=.*/, "=;expires=" + new Date().toUTCString() + ";path=/");
        });
      } catch (e) {}

    } catch (err) {
      console.log('Cache clearing:', err);
    }
  }

  async function showInstallProcess(deferredPrompt) {
    const modal = ensureInstallModal();
    const content = modal.querySelector('.pwa-install-modal__content');
    
    setHidden(modal, false);

    try {
      // Step 1: Clear all caches
      content.innerHTML = `
        <div class="pwa-install-modal__icon">
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
          </svg>
        </div>
        <div class="pwa-install-modal__title">Clearing Cache</div>
        <div class="pwa-install-modal__status">Removing old data...</div>
        <div class="pwa-install-modal__spinner"></div>
      `;
      
      await clearAllCaches();
      await new Promise(resolve => setTimeout(resolve, 800));

      // Step 2: Installing
      content.innerHTML = `
        <div class="pwa-install-modal__icon">
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
          </svg>
        </div>
        <div class="pwa-install-modal__title">Installing Vibegra</div>
        <div class="pwa-install-modal__status">Setting up your app...</div>
        <div class="pwa-install-modal__spinner"></div>
      `;

      // Trigger the install prompt
      if (deferredPrompt) {
        await deferredPrompt.prompt();
        const choiceResult = await deferredPrompt.userChoice;
        
        if (choiceResult.outcome === 'accepted') {
          await new Promise(resolve => setTimeout(resolve, 500));
          
          // Step 3: Success
          content.innerHTML = `
            <div class="pwa-install-modal__complete">
              <div class="pwa-install-modal__complete-icon">
                <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="3">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <div class="pwa-install-modal__complete-text">Installed Successfully!</div>
              <div class="pwa-install-modal__status">Launching Vibegra...</div>
            </div>
          `;

          // Auto-launch the PWA
          await new Promise(resolve => setTimeout(resolve, 1000));
          
          // Try to open the installed PWA
          try {
            // Get the app's start URL
            const startUrl = window.location.origin + '/';
            
            // Try to launch the installed app
            if (navigator.setAppBadge) {
              // PWA is installed, can use app APIs
              window.location.href = startUrl;
            } else {
              // Fallback: open in new window
              window.open(startUrl, '_blank');
            }
          } catch (e) {
            console.log('Launch:', e);
          }

          // Close modal after launching
          await new Promise(resolve => setTimeout(resolve, 500));
          setHidden(modal, true);
          
        } else {
          // User cancelled
          content.innerHTML = `
            <div class="pwa-install-modal__title">Installation Cancelled</div>
            <div class="pwa-install-modal__status">You can install anytime by clicking the button again.</div>
            <button type="button" class="pwa-install-modal__button" data-close="1">Close</button>
          `;
          
          const closeBtn = content.querySelector('[data-close="1"]');
          if (closeBtn) {
            closeBtn.addEventListener('click', () => setHidden(modal, true));
          }
        }
      } else {
        throw new Error('No install prompt available');
      }
      
    } catch (err) {
      console.error('Install error:', err);
      
      // Show error state
      content.innerHTML = `
        <div class="pwa-install-modal__title">Installation Not Available</div>
        <div class="pwa-install-modal__status">Your browser doesn't support PWA installation, or the app is already installed.</div>
        <button type="button" class="pwa-install-modal__button" data-close="1">Close</button>
      `;
      
      const closeBtn = content.querySelector('[data-close="1"]');
      if (closeBtn) {
        closeBtn.addEventListener('click', () => setHidden(modal, true));
      }
    }
  }

  function initInstallButtons() {
    const buttons = Array.from(document.querySelectorAll('[data-pwa-install="1"]'));
    if (!buttons.length) return;

    ensureStyles();

    let deferredPrompt = null;

    // Always show buttons
    buttons.forEach(btn => {
      btn.disabled = false;
      setHidden(btn, false);
    });

    // Capture the beforeinstallprompt event
    window.addEventListener('beforeinstallprompt', (e) => {
      e.preventDefault();
      deferredPrompt = e;
      console.log('PWA install prompt captured');
    });

    // Listen for successful install
    window.addEventListener('appinstalled', () => {
      console.log('PWA installed successfully');
      deferredPrompt = null;
    });

    // Handle button clicks
    buttons.forEach(btn => {
      btn.addEventListener('click', async () => {
        btn.disabled = true;
        
        // If we have a deferred prompt, use it
        if (deferredPrompt) {
          await showInstallProcess(deferredPrompt);
        } else {
          // No prompt available - clear caches and try to force it
          const modal = ensureInstallModal();
          const content = modal.querySelector('.pwa-install-modal__content');
          
          setHidden(modal, false);
          
          content.innerHTML = `
            <div class="pwa-install-modal__icon">
              <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                <path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </div>
            <div class="pwa-install-modal__title">Preparing Installation</div>
            <div class="pwa-install-modal__status">Clearing cache and refreshing...</div>
            <div class="pwa-install-modal__spinner"></div>
          `;
          
          // Clear everything
          await clearAllCaches();
          await new Promise(resolve => setTimeout(resolve, 1000));
          
          // Show refresh prompt
          content.innerHTML = `
            <div class="pwa-install-modal__title">Ready to Install</div>
            <div class="pwa-install-modal__status">Cache cleared! Please refresh the page to enable installation.</div>
            <button type="button" class="pwa-install-modal__button" onclick="window.location.reload()">Refresh Now</button>
          `;
        }
        
        btn.disabled = false;
      });
    });
  }

  function registerServiceWorker() {
    try {
      if (!('serviceWorker' in navigator)) return;
      
      window.addEventListener('load', async () => {
        try {
          // First unregister any existing service workers
          const registrations = await navigator.serviceWorker.getRegistrations();
          await Promise.all(registrations.map(reg => reg.unregister()));
          
          // Wait a bit
          await new Promise(resolve => setTimeout(resolve, 100));
          
          // Register fresh service worker
          const registration = await navigator.serviceWorker.register('/sw.js', {
            updateViaCache: 'none'
          });
          
          console.log('Service Worker registered:', registration);
          
          // Force update
          registration.update();
          
        } catch (err) {
          console.log('Service Worker registration failed:', err);
        }
      });
    } catch (err) {
      console.log('Service Worker not supported:', err);
    }
  }

  // Expose API
  window.VibePWA = window.VibePWA || {
    init: function () {
      registerServiceWorker();
      initInstallButtons();
    },
    clearCache: clearAllCaches
  };

  // Auto-init when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => window.VibePWA.init());
  } else {
    window.VibePWA.init();
  }
})();
