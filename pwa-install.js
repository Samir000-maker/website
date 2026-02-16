(function () {
  'use strict';

  // Prevent duplicate initialization
  if (window.__VibePWAInitialized) {
    console.log('✅ VibePWA already initialized, skipping duplicate');
    return;
  }
  window.__VibePWAInitialized = true;

  // Global state
  let deferredPrompt = null;
  let installAttempted = false;

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
        cursor: pointer;
      }
      .pwa-install-modal__card {
        position: relative;
        width: min(90vw, 420px);
        max-height: 85vh;
        overflow-y: auto;
        border-radius: 20px;
        background: rgba(21, 22, 28, 0.97);
        border: 1px solid rgba(255,255,255,0.12);
        box-shadow: 0 30px 80px rgba(0,0,0,0.60);
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
        line-height: 1.5;
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
      .pwa-install-modal__steps {
        text-align: left;
        margin: 20px 0;
      }
      .pwa-install-modal__step {
        display: flex;
        gap: 12px;
        margin-bottom: 16px;
        padding: 12px;
        background: rgba(255,255,255,0.03);
        border-radius: 10px;
        border: 1px solid rgba(255,255,255,0.06);
      }
      .pwa-install-modal__step-number {
        flex-shrink: 0;
        width: 28px;
        height: 28px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 50%;
        background: rgba(99,32,233,0.25);
        border: 1px solid rgba(99,32,233,0.35);
        font-weight: 800;
        font-size: 13px;
        color: rgba(255,255,255,0.95);
      }
      .pwa-install-modal__step-text {
        flex: 1;
        font-size: 14px;
        color: rgba(255,255,255,0.88);
        line-height: 1.6;
      }
      .pwa-install-modal__step-text strong {
        color: rgba(255,255,255,0.98);
        font-weight: 700;
      }
      .pwa-install-modal__browser-badge {
        display: inline-block;
        padding: 4px 10px;
        background: rgba(99,32,233,0.15);
        border: 1px solid rgba(99,32,233,0.25);
        border-radius: 6px;
        font-size: 13px;
        font-weight: 700;
        color: rgba(255,255,255,0.92);
        margin-bottom: 12px;
      }
      .pwa-diagnostic {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 10px;
        padding: 12px;
        margin: 16px 0;
        font-size: 13px;
      }
      .pwa-diagnostic__item {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 6px 0;
        color: rgba(255,255,255,0.85);
      }
      .pwa-diagnostic__icon {
        width: 16px;
        height: 16px;
        flex-shrink: 0;
      }
      .pwa-diagnostic__icon.success { color: #22c55e; }
      .pwa-diagnostic__icon.error { color: #ef4444; }
      .pwa-diagnostic__icon.warning { color: #f59e0b; }
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

  function detectBrowser() {
    const ua = navigator.userAgent;
    const isChrome = /Chrome/.test(ua) && /Google Inc/.test(navigator.vendor);
    const isEdge = /Edg/.test(ua);
    const isSafari = /Safari/.test(ua) && !/Chrome/.test(ua);
    const isFirefox = /Firefox/.test(ua);
    const isOpera = /OPR/.test(ua);
    const isIOS = /iPad|iPhone|iPod/.test(ua);
    const isAndroid = /Android/.test(ua);

    if (isIOS) return 'ios-safari';
    if (isAndroid && isChrome) return 'android-chrome';
    if (isEdge) return 'edge';
    if (isChrome) return 'chrome';
    if (isSafari) return 'safari';
    if (isFirefox) return 'firefox';
    if (isOpera) return 'opera';
    return 'unknown';
  }

  async function validatePWACriteria() {
    const results = {
      https: false,
      serviceWorker: false,
      manifest: false,
      icons: false,
      display: false,
      engagement: true, // Assume true since user is interacting
    };

    // Check HTTPS
    results.https = window.location.protocol === 'https:' || window.location.hostname === 'localhost';

    // Check Service Worker
    if ('serviceWorker' in navigator) {
      try {
        const registration = await navigator.serviceWorker.getRegistration();
        results.serviceWorker = !!registration;
      } catch (e) {
        results.serviceWorker = false;
      }
    }

    // Check Manifest
    try {
      const manifestLink = document.querySelector('link[rel="manifest"]');
      if (manifestLink) {
        const response = await fetch(manifestLink.href);
        if (response.ok) {
          const manifest = await response.json();
          results.manifest = !!(manifest.name || manifest.short_name);
          results.icons = !!(manifest.icons && manifest.icons.length > 0);
          results.display = !!manifest.display;
        }
      }
    } catch (e) {
      results.manifest = false;
    }

    return results;
  }

  function getManualInstallInstructions() {
    const browser = detectBrowser();
    
    const instructions = {
      'chrome': {
        name: 'Chrome',
        steps: [
          'Click the <strong>⋮</strong> menu (top right corner)',
          'Select <strong>"Install Vibegra"</strong>',
          'Click <strong>"Install"</strong>',
          'App opens in its own window! 🎉'
        ]
      },
      'edge': {
        name: 'Edge',
        steps: [
          'Click the <strong>⋮</strong> menu (top right)',
          'Go to <strong>"Apps"</strong> → <strong>"Install Vibegra"</strong>',
          'Click <strong>"Install"</strong>',
          'Find it in your Start menu! 🎉'
        ]
      },
      'android-chrome': {
        name: 'Chrome (Android)',
        steps: [
          'Tap <strong>⋮</strong> menu (top right)',
          'Tap <strong>"Add to Home screen"</strong>',
          'Tap <strong>"Add"</strong>',
          'Find Vibegra on your home screen! 🎉'
        ]
      },
      'ios-safari': {
        name: 'Safari (iOS)',
        steps: [
          'Tap <strong>Share</strong> button (bottom bar)',
          'Scroll down, tap <strong>"Add to Home Screen"</strong>',
          'Tap <strong>"Add"</strong> (top right)',
          'Find Vibegra on your home screen! 🎉'
        ]
      },
      'safari': {
        name: 'Safari',
        steps: [
          '⚠️ Limited PWA support on macOS Safari',
          'Best: Use <strong>Chrome</strong> or <strong>Edge</strong>',
          'Or use Safari on <strong>iPhone/iPad</strong>',
          'iOS: Share → Add to Home Screen'
        ]
      },
      'firefox': {
        name: 'Firefox',
        steps: [
          '⚠️ Limited PWA support on desktop',
          'Best: Use <strong>Chrome</strong> or <strong>Edge</strong>',
          'Firefox Android: Menu → Install',
          'Or try Chrome for full support'
        ]
      },
      'opera': {
        name: 'Opera',
        steps: [
          'Click <strong>Opera menu</strong>',
          'Select <strong>"Install Vibegra"</strong>',
          'Click <strong>"Install"</strong>',
          'App opens standalone! 🎉'
        ]
      },
      'unknown': {
        name: 'Your Browser',
        steps: [
          'Look for <strong>install icon</strong> in address bar',
          'Check browser <strong>menu</strong> for install option',
          'Best: Use <strong>Chrome</strong> or <strong>Edge</strong>',
          'Look for "Install app" or "Add to Home screen"'
        ]
      }
    };

    return instructions[browser] || instructions['unknown'];
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
        <div class="pwa-install-modal__content"></div>
      </div>
    `;

    document.body.appendChild(modal);
    
    // Close on backdrop click
    const backdrop = modal.querySelector('.pwa-install-modal__backdrop');
    backdrop.addEventListener('click', () => setHidden(modal, true));
    
    return modal;
  }

  async function showDiagnostics() {
    const modal = ensureInstallModal();
    const content = modal.querySelector('.pwa-install-modal__content');
    
    content.innerHTML = `
      <div class="pwa-install-modal__icon">
        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
        </svg>
      </div>
      <div class="pwa-install-modal__title">Checking Requirements...</div>
      <div class="pwa-install-modal__spinner"></div>
    `;
    
    setHidden(modal, false);
    
    const criteria = await validatePWACriteria();
    
    const icons = {
      success: '<svg class="pwa-diagnostic__icon success" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" /></svg>',
      error: '<svg class="pwa-diagnostic__icon error" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" /></svg>',
      warning: '<svg class="pwa-diagnostic__icon warning" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>'
    };
    
    const diagnosticHTML = `
      <div class="pwa-diagnostic">
        <div class="pwa-diagnostic__item">
          ${criteria.https ? icons.success : icons.error}
          <span>${criteria.https ? 'HTTPS ✓' : 'HTTPS Required'}</span>
        </div>
        <div class="pwa-diagnostic__item">
          ${criteria.serviceWorker ? icons.success : icons.error}
          <span>${criteria.serviceWorker ? 'Service Worker ✓' : 'Service Worker Missing'}</span>
        </div>
        <div class="pwa-diagnostic__item">
          ${criteria.manifest ? icons.success : icons.error}
          <span>${criteria.manifest ? 'Manifest ✓' : 'Manifest Missing'}</span>
        </div>
        <div class="pwa-diagnostic__item">
          ${criteria.icons ? icons.success : icons.warning}
          <span>${criteria.icons ? 'Icons ✓' : 'Icons Not Found'}</span>
        </div>
        <div class="pwa-diagnostic__item">
          ${criteria.display ? icons.success : icons.warning}
          <span>${criteria.display ? 'Display Mode ✓' : 'Display Mode Missing'}</span>
        </div>
      </div>
    `;
    
    const allPassed = criteria.https && criteria.serviceWorker && criteria.manifest;
    
    if (allPassed) {
      content.innerHTML = `
        <div class="pwa-install-modal__icon">
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        </div>
        <div class="pwa-install-modal__title">All Requirements Met!</div>
        <div class="pwa-install-modal__status">Your browser should show the install prompt soon. If not, use manual installation:</div>
        ${diagnosticHTML}
        <button type="button" class="pwa-install-modal__button" data-manual="1">Show Manual Installation</button>
      `;
    } else {
      content.innerHTML = `
        <div class="pwa-install-modal__icon">
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <div class="pwa-install-modal__title">Some Requirements Missing</div>
        <div class="pwa-install-modal__status">Automatic installation not available. Use manual installation instead:</div>
        ${diagnosticHTML}
        <button type="button" class="pwa-install-modal__button" data-manual="1">Show Manual Installation</button>
      `;
    }
    
    const manualBtn = content.querySelector('[data-manual="1"]');
    if (manualBtn) {
      manualBtn.addEventListener('click', showManualInstructions);
    }
  }

  function showManualInstructions() {
    const modal = ensureInstallModal();
    const content = modal.querySelector('.pwa-install-modal__content');
    const instructions = getManualInstallInstructions();
    
    const stepsHTML = instructions.steps
      .map((step, i) => `
        <div class="pwa-install-modal__step">
          <div class="pwa-install-modal__step-number">${i + 1}</div>
          <div class="pwa-install-modal__step-text">${step}</div>
        </div>
      `)
      .join('');
    
    content.innerHTML = `
      <div class="pwa-install-modal__icon">
        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      </div>
      <div class="pwa-install-modal__title">How to Install</div>
      <div class="pwa-install-modal__browser-badge">${instructions.name}</div>
      <div class="pwa-install-modal__status">Follow these steps:</div>
      <div class="pwa-install-modal__steps">
        ${stepsHTML}
      </div>
      <button type="button" class="pwa-install-modal__button" data-close="1">Got It!</button>
    `;
    
    const closeBtn = content.querySelector('[data-close="1"]');
    if (closeBtn) {
      closeBtn.addEventListener('click', () => setHidden(modal, true));
    }
    
    setHidden(modal, false);
  }

  async function showInstallProcess(prompt) {
    const modal = ensureInstallModal();
    const content = modal.querySelector('.pwa-install-modal__content');
    
    setHidden(modal, false);

    try {
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

      await prompt.prompt();
      const choiceResult = await prompt.userChoice;
      
      if (choiceResult.outcome === 'accepted') {
        await new Promise(resolve => setTimeout(resolve, 500));
        
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

        await new Promise(resolve => setTimeout(resolve, 2000));
        setHidden(modal, true);
        
      } else {
        showManualInstructions();
      }
      
    } catch (err) {
      console.error('Install error:', err);
      showManualInstructions();
    }
  }

  async function attemptInstallation() {
    if (installAttempted) {
      console.log('Installation already attempted this session');
      return;
    }
    
    installAttempted = true;

    if (deferredPrompt) {
      console.log('🎯 Triggering automatic installation with captured prompt');
      await showInstallProcess(deferredPrompt);
    } else {
      console.log('⚠️ No prompt captured - showing diagnostics and manual instructions');
      await showDiagnostics();
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

    // Handle button clicks
    buttons.forEach(btn => {
      btn.addEventListener('click', async () => {
        console.log('📱 Install button clicked');
        await attemptInstallation();
      });
    });

    console.log('✅ PWA install buttons initialized');
  }

  function setupPromptCapture() {
    // Capture as early as possible
    window.addEventListener('beforeinstallprompt', (e) => {
      e.preventDefault();
      deferredPrompt = e;
      window.__pwaPromptCaptured = true;
      console.log('✅ PWA install prompt captured and ready!');
      
      // Optionally trigger automatically after a delay
      // setTimeout(() => {
      //   if (!installAttempted && deferredPrompt) {
      //     console.log('Auto-triggering installation...');
      //     attemptInstallation();
      //   }
      // }, 3000);
    });

    // Monitor for successful installation
    window.addEventListener('appinstalled', () => {
      console.log('✅ PWA installed successfully!');
      deferredPrompt = null;
    });
  }

  function registerServiceWorker() {
    try {
      if (!('serviceWorker' in navigator)) {
        console.log('⚠️ Service Workers not supported');
        return;
      }
      
      window.addEventListener('load', async () => {
        try {
          const registration = await navigator.serviceWorker.register('/sw.js', {
            updateViaCache: 'none'
          });
          
          console.log('✅ Service Worker registered:', registration.scope);
          registration.update();
          
        } catch (err) {
          console.log('❌ Service Worker registration failed:', err);
        }
      });
    } catch (err) {
      console.log('❌ Service Worker error:', err);
    }
  }

  // Expose API
  window.VibePWA = window.VibePWA || {
    init: function () {
      console.log('🚀 Initializing Intelligent VibePWA System...');
      setupPromptCapture();
      registerServiceWorker();
      initInstallButtons();
    },
    triggerInstall: function() {
      attemptInstallation();
    },
    showInstructions: function() {
      showManualInstructions();
    },
    showDiagnostics: function() {
      showDiagnostics();
    },
    getPromptStatus: function() {
      return {
        captured: !!deferredPrompt,
        attempted: installAttempted,
        isPWA: isRunningAsPWA()
      };
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
