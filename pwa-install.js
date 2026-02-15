(function () {
  const LS_DISMISSED = 'vibe_pwa_install_dismissed_v1';
  const LS_INSTALLED = 'vibe_pwa_installed_v1';

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

      .pwa-ios-modal {
        position: fixed;
        inset: 0;
        z-index: 1000;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .pwa-ios-modal.hidden { display: none !important; }
      .pwa-ios-modal__backdrop {
        position: absolute;
        inset: 0;
        background: rgba(0,0,0,0.60);
        backdrop-filter: blur(6px);
        -webkit-backdrop-filter: blur(6px);
      }
      .pwa-ios-modal__card {
        position: relative;
        width: min(92vw, 440px);
        border-radius: 18px;
        background: rgba(21, 22, 28, 0.95);
        border: 1px solid rgba(255,255,255,0.10);
        box-shadow: 0 30px 80px rgba(0,0,0,0.55);
        overflow: hidden;
      }
      .pwa-ios-modal__header {
        padding: 16px 16px 10px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
      }
      .pwa-ios-modal__title {
        font-weight: 800;
        letter-spacing: -0.01em;
        color: rgba(255,255,255,0.96);
      }
      .pwa-ios-modal__close {
        appearance: none;
        -webkit-appearance: none;
        border: 1px solid rgba(255,255,255,0.10);
        background: rgba(255,255,255,0.06);
        color: rgba(255,255,255,0.90);
        width: 34px;
        height: 34px;
        border-radius: 12px;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        justify-content: center;
      }
      .pwa-ios-modal__body { padding: 0 16px 14px; }
      .pwa-ios-steps { display: grid; gap: 10px; margin-top: 8px; }
      .pwa-ios-step {
        display: grid;
        grid-template-columns: 26px 1fr;
        gap: 10px;
        align-items: start;
        padding: 12px;
        border-radius: 14px;
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.08);
        color: rgba(255,255,255,0.90);
        font-size: 14px;
        line-height: 1.4;
      }
      .pwa-ios-step__num {
        width: 26px;
        height: 26px;
        border-radius: 10px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: rgba(99,32,233,0.25);
        border: 1px solid rgba(99,32,233,0.35);
        font-weight: 800;
      }
      .pwa-ios-modal__hint {
        margin-top: 12px;
        font-size: 12px;
        color: rgba(148,163,184,0.85);
      }
      .pwa-ios-modal__footer {
        padding: 0 16px 16px;
        display: flex;
        justify-content: flex-end;
      }
      .pwa-ios-modal__primary {
        appearance: none;
        -webkit-appearance: none;
        border: 1px solid rgba(99,32,233,0.45);
        background: rgba(99,32,233,0.22);
        color: rgba(255,255,255,0.95);
        border-radius: 12px;
        padding: 10px 12px;
        font-weight: 800;
        cursor: pointer;
      }
    `;
    document.head.appendChild(style);
  }

  function isInIframe() {
    try {
      return window.self !== window.top;
    } catch {
      return true;
    }
  }

  function isStandalone() {
    return (
      window.matchMedia && window.matchMedia('(display-mode: standalone)').matches
    ) ||
      (window.matchMedia && window.matchMedia('(display-mode: fullscreen)').matches) ||
      (window.navigator && window.navigator.standalone === true);
  }

  function isIOS() {
    const ua = navigator.userAgent || '';
    const platform = navigator.platform || '';
    const maxTouchPoints = navigator.maxTouchPoints || 0;

    const iOSLike = /iPad|iPhone|iPod/.test(ua) ||
      (platform === 'MacIntel' && maxTouchPoints > 1);

    return iOSLike;
  }

  function shouldSuppressUI() {
    if (isInIframe()) return true;
    if (isStandalone()) return true;
    try {
      if (localStorage.getItem(LS_INSTALLED) === '1') return true;
      if (localStorage.getItem(LS_DISMISSED) === '1') return true;
    } catch { }
    return false;
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

  function ensureIOSModal() {
    const existing = document.getElementById('pwaIosInstallModal');
    if (existing) return existing;

    const modal = document.createElement('div');
    modal.id = 'pwaIosInstallModal';
    modal.className = 'pwa-ios-modal hidden';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.innerHTML = `
      <div class="pwa-ios-modal__backdrop" data-close="1"></div>
      <div class="pwa-ios-modal__card">
        <div class="pwa-ios-modal__header">
          <div class="pwa-ios-modal__title">Install vibegra</div>
          <button type="button" class="pwa-ios-modal__close" data-close="1" aria-label="Close">
            <span aria-hidden="true">×</span>
          </button>
        </div>
        <div class="pwa-ios-modal__body">
          <div class="pwa-ios-steps">
            <div class="pwa-ios-step">
              <div class="pwa-ios-step__num">1</div>
              <div class="pwa-ios-step__text">Tap the <strong>Share</strong> button in Safari</div>
            </div>
            <div class="pwa-ios-step">
              <div class="pwa-ios-step__num">2</div>
              <div class="pwa-ios-step__text">Select <strong>Add to Home Screen</strong></div>
            </div>
          </div>
          <div class="pwa-ios-modal__hint">This makes vibegra open fullscreen like a real app.</div>
        </div>
        <div class="pwa-ios-modal__footer">
          <button type="button" class="pwa-ios-modal__primary" data-close="1">Got it</button>
        </div>
      </div>
    `;

    document.body.appendChild(modal);

    modal.addEventListener('click', (e) => {
      const t = e.target;
      if (t && t.closest && t.closest('[data-close="1"]')) {
        setHidden(modal, true);
      }
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') setHidden(modal, true);
    });

    return modal;
  }

  function showIOSModal() {
    const modal = ensureIOSModal();
    setHidden(modal, false);
  }

  function markDismissed() {
    try { localStorage.setItem(LS_DISMISSED, '1'); } catch { }
  }

  function markInstalled() {
    try { localStorage.setItem(LS_INSTALLED, '1'); } catch { }
  }

  function initInstallButtons() {
    const buttons = Array.from(document.querySelectorAll('[data-pwa-install="1"]'));
    if (!buttons.length) return;

    ensureStyles();

    // Start hidden by default.
    buttons.forEach((b) => {
      setHidden(b, true);
      b.disabled = false;
    });

    if (shouldSuppressUI()) {
      buttons.forEach((b) => setHidden(b, true));
      return;
    }

    let deferredPrompt = null;
    let promptInFlight = false;

    function showButtons() {
      if (shouldSuppressUI()) return;
      buttons.forEach((b) => setHidden(b, false));
    }

    function hideButtonsPermanently() {
      buttons.forEach((b) => setHidden(b, true));
    }

    window.addEventListener('beforeinstallprompt', (e) => {
      // Chrome/Edge/Android/Desktop.
      e.preventDefault();
      deferredPrompt = e;
      showButtons();
    });

    window.addEventListener('appinstalled', () => {
      markInstalled();
      hideButtonsPermanently();
    });

    // iOS: no beforeinstallprompt.
    if (isIOS()) {
      // Only show when not standalone and not dismissed.
      if (!shouldSuppressUI()) {
        showButtons();
      }
    }

    buttons.forEach((btn) => {
      btn.addEventListener('click', async () => {
        if (shouldSuppressUI()) {
          hideButtonsPermanently();
          return;
        }

        if (isIOS()) {
          showIOSModal();
          // Don't mark dismissed unless user closes the modal (they might want later); but you asked to store dismissal.
          // We'll mark dismissal when they close via any close target.
          const modal = ensureIOSModal();
          const observer = new MutationObserver(() => {
            if (modal.classList.contains('hidden')) {
              observer.disconnect();
              markDismissed();
              hideButtonsPermanently();
            }
          });
          observer.observe(modal, { attributes: true, attributeFilter: ['class'] });
          return;
        }

        if (!deferredPrompt || promptInFlight) return;

        promptInFlight = true;
        try {
          deferredPrompt.prompt();
          const choice = await deferredPrompt.userChoice;
          if (choice && choice.outcome === 'accepted') {
            // appinstalled should also fire, but mark as installed defensively.
            markInstalled();
            hideButtonsPermanently();
          } else {
            markDismissed();
            hideButtonsPermanently();
          }
        } catch {
          markDismissed();
          hideButtonsPermanently();
        } finally {
          deferredPrompt = null;
          promptInFlight = false;
        }
      });
    });
  }

  function registerServiceWorker() {
    try {
      if (!('serviceWorker' in navigator)) return;
      if (isInIframe()) return;
      window.addEventListener('load', () => {
        navigator.serviceWorker.register('/sw.js').catch(() => { });
      });
    } catch { }
  }

  // Expose a tiny API if needed.
  window.VibePWA = window.VibePWA || {
    init: function () {
      registerServiceWorker();
      initInstallButtons();
    }
  };
})();
