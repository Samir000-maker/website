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
        transform: none !important;
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

      .pwa-download-modal {
        position: fixed;
        inset: 0;
        z-index: 1000;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .pwa-download-modal.hidden { display: none !important; }
      .pwa-download-modal__backdrop {
        position: absolute;
        inset: 0;
        background: rgba(0,0,0,0.65);
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
      }
      .pwa-download-modal__card {
        position: relative;
        width: min(90vw, 400px);
        border-radius: 20px;
        background: rgba(21, 22, 28, 0.97);
        border: 1px solid rgba(255,255,255,0.12);
        box-shadow: 0 30px 80px rgba(0,0,0,0.60);
        overflow: hidden;
        padding: 24px;
      }
      .pwa-download-modal__icon {
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
      .pwa-download-modal__icon svg {
        width: 28px;
        height: 28px;
        color: rgba(255,255,255,0.92);
        animation: downloadPulse 2s ease-in-out infinite;
      }
      @keyframes downloadPulse {
        0%, 100% { transform: scale(1); opacity: 1; }
        50% { transform: scale(1.05); opacity: 0.85; }
      }
      .pwa-download-modal__title {
        font-weight: 800;
        font-size: 18px;
        letter-spacing: -0.01em;
        color: rgba(255,255,255,0.96);
        text-align: center;
        margin-bottom: 8px;
      }
      .pwa-download-modal__status {
        font-size: 14px;
        color: rgba(148,163,184,0.90);
        text-align: center;
        margin-bottom: 20px;
      }
      .pwa-download-modal__progress-wrap {
        position: relative;
        width: 100%;
        height: 8px;
        border-radius: 9999px;
        background: rgba(255,255,255,0.08);
        overflow: hidden;
        margin-bottom: 12px;
      }
      .pwa-download-modal__progress-bar {
        position: absolute;
        left: 0;
        top: 0;
        bottom: 0;
        width: 0%;
        background: linear-gradient(90deg, #6320e9, #8b5cf6);
        box-shadow: 0 0 12px rgba(99,32,233,0.40);
        transition: width 0.3s ease;
        border-radius: 9999px;
      }
      .pwa-download-modal__percentage {
        font-weight: 800;
        font-size: 24px;
        color: rgba(255,255,255,0.96);
        text-align: center;
        margin-bottom: 8px;
        font-variant-numeric: tabular-nums;
      }
      .pwa-download-modal__complete {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 16px;
      }
      .pwa-download-modal__complete-icon {
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
      .pwa-download-modal__complete-icon svg {
        width: 32px;
        height: 32px;
        color: rgba(34,197,94,0.95);
      }
      .pwa-download-modal__complete-text {
        font-weight: 700;
        font-size: 16px;
        color: rgba(255,255,255,0.94);
      }
      .pwa-download-modal__button {
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
      .pwa-download-modal__button:hover {
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

  function ensureDownloadModal() {
    const existing = document.getElementById('pwaDownloadModal');
    if (existing) return existing;

    const modal = document.createElement('div');
    modal.id = 'pwaDownloadModal';
    modal.className = 'pwa-download-modal hidden';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.innerHTML = `
      <div class="pwa-download-modal__backdrop"></div>
      <div class="pwa-download-modal__card">
        <div class="pwa-download-modal__content">
          <div class="pwa-download-modal__icon">
            <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M9 19l3 3m0 0l3-3m-3 3V10" />
            </svg>
          </div>
          <div class="pwa-download-modal__title">Downloading vibegra</div>
          <div class="pwa-download-modal__status">Please wait while we prepare your download...</div>
          <div class="pwa-download-modal__percentage">0%</div>
          <div class="pwa-download-modal__progress-wrap">
            <div class="pwa-download-modal__progress-bar"></div>
          </div>
        </div>
      </div>
    `;

    document.body.appendChild(modal);
    return modal;
  }

  function clearBrowserCache() {
    try {
      // Clear service worker caches
      if ('serviceWorker' in navigator && 'caches' in window) {
        caches.keys().then(names => {
          names.forEach(name => {
            caches.delete(name);
          });
        });
        
        // Unregister all service workers to ensure clean state
        navigator.serviceWorker.getRegistrations().then(registrations => {
          registrations.forEach(registration => {
            registration.unregister();
          });
        });
      }
      
      // Clear localStorage
      try {
        if (window.localStorage) {
          // Only clear app-specific keys, not all localStorage
          const keysToRemove = [];
          for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key && (key.includes('vibegra') || key.includes('pwa') || key.includes('install'))) {
              keysToRemove.push(key);
            }
          }
          keysToRemove.forEach(key => localStorage.removeItem(key));
        }
      } catch (e) {
        console.log('localStorage clearing not supported');
      }
      
      // Clear sessionStorage
      try {
        if (window.sessionStorage) {
          const keysToRemove = [];
          for (let i = 0; i < sessionStorage.length; i++) {
            const key = sessionStorage.key(i);
            if (key && (key.includes('vibegra') || key.includes('pwa') || key.includes('install'))) {
              keysToRemove.push(key);
            }
          }
          keysToRemove.forEach(key => sessionStorage.removeItem(key));
        }
      } catch (e) {
        console.log('sessionStorage clearing not supported');
      }
      
      // Clear IndexedDB for app-specific databases
      try {
        if (window.indexedDB) {
          indexedDB.databases().then(databases => {
            databases.forEach(db => {
              if (db.name && (db.name.includes('vibegra') || db.name.includes('pwa'))) {
                indexedDB.deleteDatabase(db.name);
              }
            });
          }).catch(() => {});
        }
      } catch (e) {
        console.log('IndexedDB clearing not supported');
      }
      
      return true;
    } catch (e) {
      console.log('Cache clearing not supported:', e);
    }
    return false;
  }

  function autoLaunchApp(filePath) {
    try {
      // Detect file type from URL
      const fileExtension = filePath.split('.').pop().toLowerCase().split('?')[0];
      
      // Try to open the file/app automatically
      const link = document.createElement('a');
      link.href = filePath;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      
      // Set appropriate MIME types for different file types
      const mimeTypes = {
        'apk': 'application/vnd.android.package-archive',
        'exe': 'application/x-msdownload',
        'msi': 'application/x-msi',
        'dmg': 'application/x-apple-diskimage',
        'pkg': 'application/x-newton-compatible-pkg',
        'deb': 'application/x-debian-package',
        'rpm': 'application/x-rpm',
        'app': 'application/x-app',
        'ipa': 'application/x-ios-app',
        'zip': 'application/zip',
        'tar': 'application/x-tar',
        'gz': 'application/gzip'
      };
      
      if (mimeTypes[fileExtension]) {
        link.type = mimeTypes[fileExtension];
      }
      
      // Trigger click to open/launch
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      // For desktop apps, try additional launch methods
      if (['exe', 'msi', 'dmg', 'app', 'deb', 'rpm'].includes(fileExtension)) {
        // Set a small delay and try to open in system file manager
        setTimeout(() => {
          try {
            // Try to trigger system open dialog
            const openLink = document.createElement('a');
            openLink.href = filePath;
            openLink.download = '';
            document.body.appendChild(openLink);
            openLink.click();
            document.body.removeChild(openLink);
          } catch (e) {}
        }, 500);
      }
      
      return true;
    } catch (e) {
      console.log('Auto-launch not supported:', e);
      return false;
    }
  }

  function showDownloadProgress(downloadUrl = '/vibegra-app.zip') {
    const modal = ensureDownloadModal();
    const progressBar = modal.querySelector('.pwa-download-modal__progress-bar');
    const percentageText = modal.querySelector('.pwa-download-modal__percentage');
    const statusText = modal.querySelector('.pwa-download-modal__status');
    const content = modal.querySelector('.pwa-download-modal__content');

    setHidden(modal, false);

    // Clear cache before download
    statusText.textContent = 'Clearing cache and preparing download...';
    clearBrowserCache();

    // Add cache-busting timestamp to URL
    const cacheBuster = `?t=${Date.now()}&nocache=${Math.random().toString(36).substring(7)}`;
    const finalUrl = downloadUrl + (downloadUrl.includes('?') ? '&' : '') + cacheBuster.substring(1);

    // Simulate download with actual file fetch
    let progress = 0;
    let downloadedBlob = null;
    let downloadedUrl = null;
    
    // Try to fetch and download the actual file if it exists
    fetch(finalUrl, {
      cache: 'no-store', // Ensure fresh download, bypass cache
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      },
      mode: 'cors',
      credentials: 'omit'
    })
      .then(response => {
        if (!response.ok) throw new Error('File not found');
        
        statusText.textContent = 'Downloading...';
        
        const contentLength = response.headers.get('content-length');
        const total = parseInt(contentLength, 10);
        
        if (!contentLength || isNaN(total)) {
          // Fallback to simulated progress
          return simulateDownload();
        }
        
        let loaded = 0;
        const reader = response.body.getReader();
        const chunks = [];
        
        return new ReadableStream({
          start(controller) {
            function push() {
              reader.read().then(({ done, value }) => {
                if (done) {
                  controller.close();
                  return;
                }
                
                chunks.push(value);
                loaded += value.length;
                progress = Math.min(Math.round((loaded / total) * 100), 100);
                
                progressBar.style.width = progress + '%';
                percentageText.textContent = progress + '%';
                
                controller.enqueue(value);
                push();
              });
            }
            push();
          }
        });
      })
      .then(stream => stream ? new Response(stream) : null)
      .then(response => response ? response.blob() : simulateDownload())
      .then(blob => {
        if (blob) {
          downloadedBlob = blob;
          // Create download link
          const url = URL.createObjectURL(blob);
          downloadedUrl = url;
          
          const a = document.createElement('a');
          a.href = url;
          
          // Extract filename from URL or use default
          const filename = downloadUrl.split('/').pop() || 'vibegra-app.zip';
          a.download = filename;
          
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          
          // Don't revoke URL yet, we'll use it for auto-launch
          // URL.revokeObjectURL(url);
        }
        
        showComplete(downloadedUrl);
      })
      .catch(() => {
        // Fallback to simulated download
        statusText.textContent = 'Downloading...';
        simulateDownload().then(() => showComplete(null));
      });

    function simulateDownload() {
      return new Promise((resolve) => {
        const interval = setInterval(() => {
          progress += Math.random() * 15 + 5;
          if (progress >= 100) {
            progress = 100;
            clearInterval(interval);
            setTimeout(() => resolve(), 300);
          }
          
          progressBar.style.width = progress + '%';
          percentageText.textContent = Math.round(progress) + '%';
        }, 200);
      });
    }

    function showComplete(fileUrl) {
      // Attempt auto-launch
      let launchAttempted = false;
      if (fileUrl) {
        launchAttempted = autoLaunchApp(fileUrl);
      }
      
      content.innerHTML = `
        <div class="pwa-download-modal__complete">
          <div class="pwa-download-modal__complete-icon">
            <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="3">
              <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <div class="pwa-download-modal__complete-text">Download Complete!</div>
          ${launchAttempted ? '<div class="pwa-download-modal__status" style="margin-top: 8px;">Opening app automatically...</div>' : '<div class="pwa-download-modal__status" style="margin-top: 8px;">Please check your downloads folder.</div>'}
          <button type="button" class="pwa-download-modal__button" data-close="1">Close</button>
        </div>
      `;
      
      const closeBtn = content.querySelector('[data-close="1"]');
      if (closeBtn) {
        closeBtn.addEventListener('click', () => {
          // Clean up blob URL if it exists
          if (fileUrl) {
            try {
              URL.revokeObjectURL(fileUrl);
            } catch (e) {}
          }
          
          setHidden(modal, true);
          
          // Reset modal content after animation
          setTimeout(() => {
            content.innerHTML = `
              <div class="pwa-download-modal__icon">
                <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M9 19l3 3m0 0l3-3m-3 3V10" />
                </svg>
              </div>
              <div class="pwa-download-modal__title">Downloading vibegra</div>
              <div class="pwa-download-modal__status">Please wait while we prepare your download...</div>
              <div class="pwa-download-modal__percentage">0%</div>
              <div class="pwa-download-modal__progress-wrap">
                <div class="pwa-download-modal__progress-bar"></div>
              </div>
            `;
          }, 300);
        });
      }
      
      // Auto-close modal after 3 seconds if launch was successful
      if (launchAttempted) {
        setTimeout(() => {
          if (!modal.classList.contains('hidden')) {
            closeBtn.click();
          }
        }, 3000);
      }
    }
  }

  function initInstallButtons() {
    const buttons = Array.from(document.querySelectorAll('[data-pwa-install="1"]'));
    if (!buttons.length) return;

    ensureStyles();

    // Always show buttons, no hiding logic
    buttons.forEach((btn) => {
      btn.disabled = false;
      setHidden(btn, false);
      
      btn.addEventListener('click', () => {
        // Disable button during download
        btn.disabled = true;
        
        // Get download URL from data attribute or use default
        const downloadUrl = btn.getAttribute('data-download-url') || '/vibegra-app.zip';
        
        // Show download progress
        showDownloadProgress(downloadUrl);
        
        // Re-enable button after a delay
        setTimeout(() => {
          btn.disabled = false;
        }, 2000);
      });
    });
  }

  function registerServiceWorker() {
    try {
      if (!('serviceWorker' in navigator)) return;
      window.addEventListener('load', () => {
        // Clear old registrations first
        navigator.serviceWorker.getRegistrations().then(registrations => {
          // Unregister old service workers
          registrations.forEach(registration => registration.unregister());
          
          // Register fresh service worker after clearing
          setTimeout(() => {
            navigator.serviceWorker.register('/sw.js', {
              updateViaCache: 'none' // Don't cache the service worker file itself
            }).then(registration => {
              // Force update check
              registration.update();
            }).catch(() => {});
          }, 100);
        });
      });
    } catch {}
  }

  // Expose API
  window.VibePWA = window.VibePWA || {
    init: function () {
      registerServiceWorker();
      initInstallButtons();
    },
    download: function(url) {
      showDownloadProgress(url);
    }
  };

  // Auto-init when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => window.VibePWA.init());
  } else {
    window.VibePWA.init();
  }
})();
