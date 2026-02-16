(function () {
  'use strict';

  // Prevent duplicate initialization
  if (window.__VibePWAInitialized) {
    console.log('✅ VibePWA already initialized, skipping duplicate');
    return;
  }
  window.__VibePWAInitialized = true;

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
        width: 100%;
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
      el.style.display = 'none';
      el.setAttribute('aria-hidden', 'true');
    } else {
      el.classList.remove('hidden');
      el.style.display = '';
      el.setAttribute('aria-hidden', 'false');
    }
  }

  function isRunningAsPWA() {
    const isStandalone = window.matchMedia('(display-mode: standalone)').matches;
    const isIOSStandalone = window.navigator.standalone === true;
    const isFullscreen = window.matchMedia('(display-mode: fullscreen)').matches;
    return isStandalone || isIOSStandalone || isFullscreen;
  }

  function ensureInstallModal() {
    let modal = document.getElementById('pwaInstallModal');
    if (modal) return modal;

    modal = document.createElement('div');
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

  async function showInstallProcess(deferredPrompt) {
    const modal = ensureInstallModal();
    const content = modal.querySelector('.pwa-install-modal__content');
    
    setHidden(modal, false);

    try {
      // Installing
      content.innerHTML = `
        <div class="pwa-install-modal__icon">
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
          </svg>
        </div>
        <div class="pwa-install-modal__title">Installing Vibegra</div>
        <div class="pwa-install-modal__status">Click "Install" in the browser prompt...</div>
        <div class="pwa-install-modal__spinner"></div>
      `;

      // Trigger the install prompt
      await deferredPrompt.prompt();
      const choiceResult = await deferredPrompt.userChoice;
      
      if (choiceResult.outcome === 'accepted') {
        await new Promise(resolve => setTimeout(resolve, 500));
        
        // Success
        content.innerHTML = `
          <div class="pwa-install-modal__complete">
            <div class="pwa-install-modal__complete-icon">
              <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="3">
                <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <div class="pwa-install-modal__complete-text">Installed Successfully!</div>
            <div class="pwa-install-modal__status">You can now use Vibegra as an app!</div>
          </div>
        `;

        // Close modal after 2 seconds
        await new Promise(resolve => setTimeout(resolve, 2000));
        setHidden(modal, true);
        
      } else {
        // User cancelled - just close the modal
        setHidden(modal, true);
      }
      
    } catch (err) {
      console.error('Install error:', err);
      // On error, just close the modal
      setHidden(modal, true);
    }
  }

  function initInstallButtons() {
    const buttons = Array.from(document.querySelectorAll('[data-pwa-install="1"]:not([data-pwa-initialized])'));
    if (!buttons.length) {
      console.log('✅ No new install buttons to initialize');
      return;
    }

    console.log(`🔧 Initializing ${buttons.length} PWA install button(s)`);

    ensureStyles();

    let deferredPrompt = null;

    // Hide buttons ONLY if running as PWA app
    if (isRunningAsPWA()) {
      buttons.forEach(btn => {
        setHidden(btn, true);
        btn.setAttribute('data-pwa-initialized', 'true');
      });
      console.log('🙈 Running as PWA - install buttons hidden');
      return;
    }

    // Show buttons when in browser
    buttons.forEach(btn => {
      btn.disabled = false;
      setHidden(btn, false);
      btn.setAttribute('data-pwa-initialized', 'true');
    });

    // Capture the beforeinstallprompt event
    window.addEventListener('beforeinstallprompt', (e) => {
      e.preventDefault();
      deferredPrompt = e;
      window.__pwaPromptCaptured = true;
      console.log('✅ PWA install prompt captured');
    });

    // Handle button clicks - IMMEDIATE TRIGGER
    buttons.forEach(btn => {
      btn.addEventListener('click', async () => {
        console.log('📱 Install button clicked - triggering installation...');
        
        if (deferredPrompt) {
          // We have a prompt - proceed with installation
          console.log('📱 Starting PWA installation...');
          await showInstallProcess(deferredPrompt);
        } else {
          // No prompt - silently do nothing or log
          console.log('⚠️ No install prompt available - browser may not support PWA installation');
        }
      });
    });

    console.log('✅ PWA install buttons initialized (unlimited mode)');
  }

  function registerServiceWorker() {
    try {
      if (!('serviceWorker' in navigator)) return;
      
      window.addEventListener('load', async () => {
        try {
          const registration = await navigator.serviceWorker.register('/sw.js', {
            updateViaCache: 'none'
          });
          
          console.log('✅ Service Worker registered:', registration.scope);
          
          // Force update
          registration.update();
          
        } catch (err) {
          console.log('❌ Service Worker registration failed:', err);
        }
      });
    } catch (err) {
      console.log('❌ Service Worker not supported:', err);
    }
  }

  // Expose API
  window.VibePWA = window.VibePWA || {
    init: function () {
      console.log('🚀 Initializing VibePWA (Unlimited Mode)...');
      registerServiceWorker();
      initInstallButtons();
    },
    // Manual trigger function
    triggerInstall: function() {
      const btn = document.querySelector('[data-pwa-install="1"]');
      if (btn) {
        btn.click();
      }
    }
  };

  // Auto-init when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      console.log('📄 DOM ready - initializing VibePWA');
      window.VibePWA.init();
    });
  } else {
    console.log('📄 DOM already ready - initializing VibePWA now');
    window.VibePWA.init();
  }
})();
