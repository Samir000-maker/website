(function () {
  const SocialClub = (window.SocialClub = window.SocialClub || {});

  function getRootConfig() {
    return window.__VIBE_FIREBASE_CONFIG__ || {};
  }

  function getVapidKey() {
    return (window.__VIBE_FCM_VAPID_KEY__ || 'BL-9MFwZP_dnUxzFT-YHzQqVAFxykQDPtKNP9Y9pOfb7KNaLby0v2j3ykPuQCSM-2XGXooecNEp8pYrMIyKr1Ec').trim();
  }

  function qs(el, sel) {
    return el ? el.querySelector(sel) : null;
  }

  function createEl(html) {
    const t = document.createElement('template');
    t.innerHTML = html.trim();
    return t.content.firstElementChild;
  }

  function ensureStyles() {
    if (document.getElementById('socialClubStyles')) return;
    const style = document.createElement('style');
    style.id = 'socialClubStyles';
    style.textContent = `
      .social-club-wrap{width:100%;max-width:48rem;margin:0 auto;}
      .social-club-kicker{color:rgba(148,163,184,0.9);font-size:0.95rem;line-height:1.4;margin-bottom:12px;text-align:center;}
      .social-club-card{position:relative;overflow:hidden;border-radius:18px;padding:18px 18px 16px;border:1px solid rgba(255,255,255,0.12);background:linear-gradient(135deg,rgba(255,255,255,0.10),rgba(255,255,255,0.04));backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);box-shadow:0 18px 60px rgba(0,0,0,0.35);transition:transform 180ms ease,box-shadow 180ms ease,border-color 180ms ease;}
      .social-club-card:hover{transform:translateY(-2px);border-color:rgba(99,32,233,0.55);box-shadow:0 22px 70px rgba(99,32,233,0.18),0 18px 60px rgba(0,0,0,0.40);}
      .social-club-title{display:flex;align-items:center;justify-content:space-between;gap:12px;}
      .social-club-title h3{font-weight:900;font-size:1.2rem;letter-spacing:-0.01em;color:rgba(255,255,255,0.96);}
      .social-club-status{display:flex;align-items:center;gap:10px;justify-content:flex-end;}
      .social-club-dot{width:10px;height:10px;border-radius:9999px;background:rgba(148,163,184,0.45);box-shadow:none;}
      .social-club-dot.live{background:#22c55e;box-shadow:0 0 0 6px rgba(34,197,94,0.12);}
      .social-club-status-text{font-size:0.85rem;font-weight:800;color:rgba(226,232,240,0.88);white-space:nowrap;}
      .social-club-body{margin-top:12px;display:flex;flex-direction:column;gap:14px;}
      .social-club-desc{color:rgba(148,163,184,0.92);font-size:0.92rem;line-height:1.5;}
      .social-club-actions{display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:flex-end;}
      .social-club-btn{appearance:none;-webkit-appearance:none;border:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:10px;padding:11px 14px;border-radius:14px;font-weight:900;font-size:0.92rem;line-height:1;color:rgba(255,255,255,0.95);background:linear-gradient(135deg,rgba(99,32,233,0.95),rgba(45,212,191,0.60));box-shadow:0 14px 40px rgba(99,32,233,0.25);transition:transform 160ms ease,box-shadow 160ms ease,filter 160ms ease;}
      .social-club-btn:hover{transform:translateY(-1px);filter:saturate(1.1);box-shadow:0 18px 55px rgba(99,32,233,0.35);}
      .social-club-btn:disabled{opacity:0.55;cursor:not-allowed;transform:none;box-shadow:none;}
      .social-club-btn-icon{width:22px;height:22px;border-radius:10px;display:inline-flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.20);border:1px solid rgba(255,255,255,0.16);}
      .social-club-note{font-size:0.78rem;color:rgba(148,163,184,0.85);line-height:1.4;}
      @media (max-width: 640px){
        .social-club-card{padding:16px;}
        .social-club-actions{justify-content:stretch;}
        .social-club-btn{width:100%;}
        .social-club-kicker{text-align:left;}
      }
    `;
    document.head.appendChild(style);
  }

  function ensureToastHost() {
    if (document.getElementById('socialClubToastHost')) return;
    const el = document.createElement('div');
    el.id = 'socialClubToastHost';
    el.style.cssText = 'position:fixed;left:50%;transform:translateX(-50%);bottom:18px;z-index:99999;display:flex;flex-direction:column;gap:10px;max-width:min(92vw,520px);width:100%;pointer-events:none;';
    document.body.appendChild(el);
  }

  function toast(message) {
    try {
      if (window.MoodApp && window.MoodApp.Toast && typeof window.MoodApp.Toast.info === 'function') {
        window.MoodApp.Toast.info(message);
        return;
      }
    } catch { }

    ensureToastHost();
    const host = document.getElementById('socialClubToastHost');
    if (!host) return;

    const node = document.createElement('div');
    node.style.cssText = 'pointer-events:auto;background:rgba(15,17,21,0.96);border:1px solid rgba(255,255,255,0.12);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);border-radius:14px;padding:12px 14px;color:rgba(255,255,255,0.92);font-weight:800;font-size:13px;box-shadow:0 20px 60px rgba(0,0,0,0.50);';
    node.textContent = String(message || '');
    host.appendChild(node);
    setTimeout(() => node.remove(), 3800);
  }

  async function ensureFirebase() {
    if (typeof firebase === 'undefined') {
      throw new Error('Firebase not loaded');
    }

    if (!firebase.apps || !firebase.apps.length) {
      firebase.initializeApp(getRootConfig());
    }

    try {
      firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL);
    } catch { }
  }

  async function ensureSignedIn() {
    await ensureFirebase();
    let user = firebase.auth().currentUser;
    if (user) return user;
    const cred = await firebase.auth().signInAnonymously();
    user = cred && cred.user;
    if (!user) throw new Error('Sign-in failed');
    return user;
  }

  async function ensureMessaging() {
    await ensureFirebase();

    if (typeof firebase.messaging !== 'function') {
      throw new Error('Firebase Messaging not loaded');
    }

    const vapidKey = getVapidKey();
    if (!vapidKey) {
      throw new Error('Missing Web Push VAPID key');
    }

    const messaging = firebase.messaging();
    return { messaging, vapidKey };
  }

  async function ensureFcmToken() {
    const { messaging, vapidKey } = await ensureMessaging();

    if (!('serviceWorker' in navigator)) {
      throw new Error('Service workers are not supported in this browser');
    }

    let registration = null;
    try {
      registration = await navigator.serviceWorker.getRegistration('/');
    } catch { }

    if (!registration) {
      registration = await navigator.serviceWorker.register('/sw.js');
    }

    try {
      if (typeof messaging.useServiceWorker === 'function') {
        messaging.useServiceWorker(registration);
      }
    } catch { }

    let permission = Notification.permission;
    if (permission !== 'granted') {
      const ok = window.confirm(
        'Enable notifications so we can alert you when Social Club goes live.\n\nIf you don\'t allow notifications, you may miss the event.'
      );
      if (!ok) {
        throw new Error('Notifications permission not granted. Please allow notifications to join the waitlist.');
      }
    }

    permission = Notification.permission;
    if (permission === 'default') {
      permission = await Notification.requestPermission();
    }

    if (permission === 'denied') {
      try {
        window.open('chrome://settings/content/notifications');
      } catch { }
      throw new Error('Notifications are blocked in your browser settings. Click the lock icon in the address bar → Site settings → Notifications → Allow, then try again.');
    }

    if (permission !== 'granted') {
      throw new Error('Notifications permission not granted. Please allow notifications to join the waitlist.');
    }

    const token = await messaging.getToken({ vapidKey, serviceWorkerRegistration: registration });
    if (!token) {
      throw new Error('Unable to obtain notification token');
    }

    return token;
  }

  function wireForegroundMessages() {
    try {
      if (!firebase.apps || !firebase.apps.length) return;
      if (typeof firebase.messaging !== 'function') return;
      const messaging = firebase.messaging();
      if (wireForegroundMessages._wired) return;
      wireForegroundMessages._wired = true;
      messaging.onMessage((payload) => {
        const title = payload?.notification?.title || 'Notification';
        const body = payload?.notification?.body || '';
        try { console.log('📩 [SocialClub] FCM foreground message:', payload); } catch { }
        try { void title; void body; } catch { }
      });
    } catch { }
  }

  async function apiFetchJson(url, options = {}) {
    const res = await fetch(url, {
      credentials: 'same-origin',
      ...options
    });
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    if (!res.ok) {
      const msg = (json && (json.message || json.error)) ? (json.message || json.error) : (text || `HTTP ${res.status}`);
      const err = new Error(msg);
      err.status = res.status;
      err.payload = json;
      throw err;
    }
    return json;
  }

  async function getEventStatus() {
    return await apiFetchJson('/api/events/social_club');
  }

  async function joinWaitlist() {
    const user = await ensureSignedIn();
    const token = await user.getIdToken();

    let fcmToken = null;
    try {
      fcmToken = await ensureFcmToken();
    } catch (err) {
      throw err;
    }

    return await apiFetchJson('/api/events/social_club/waitlist', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`
      },
      body: JSON.stringify({ fcmToken })
    });
  }

  function setUiState(cardEl, state) {
    const root = cardEl ? cardEl.closest('[data-social-club-root]') : null;
    const kicker = qs(root, '.social-club-kicker');
    const dot = qs(cardEl, '[data-social-dot]');
    const statusText = qs(cardEl, '[data-social-status-text]');
    const btn = qs(cardEl, '[data-social-action]');

    const isOpen = !!state?.isEventOpen;

    try {
      console.log('🎭 [SocialClub] UI state update:', { isEventOpen: isOpen, updatedAt: state?.updatedAt || null });
    } catch { }

    if (kicker) {
      kicker.textContent = isOpen ? 'Event is ongoing' : 'Event will start soon';
    }

    if (dot) {
      dot.classList.toggle('live', isOpen);
    }

    if (statusText) {
      statusText.textContent = isOpen ? 'Event is ongoing' : 'Event will start soon';
    }

    if (btn) {
      btn.dataset.mode = isOpen ? 'enter' : 'waitlist';
      btn.disabled = false;
      btn.innerHTML = isOpen
        ? `<span class="social-club-btn-icon"><span class="material-symbols-outlined" style="font-size:18px;">login</span></span><span>Enter</span>`
        : `<span class="social-club-btn-icon"><span class="material-symbols-outlined" style="font-size:18px;">playlist_add</span></span><span>Join Waitlist</span>`;
    }
  }

  SocialClub.mount = function mount(targetEl, options = {}) {
    if (!targetEl) return;
    ensureStyles();

    const wrap = createEl(`
      <section class="social-club-wrap" data-social-club-root="1">
        <div class="social-club-kicker">Event will start soon</div>
        <div class="social-club-card">
          <div class="social-club-title">
            <h3>Social Club</h3>
            <div class="social-club-status">
              <span class="social-club-dot" data-social-dot="1"></span>
              <span class="social-club-status-text" data-social-status-text="1">Event will start soon</span>
            </div>
          </div>
          <div class="social-club-body">
            <div class="social-club-desc">A quick event matchmaking room. When live, you can enter instantly to meet new people.</div>
            <div class="social-club-actions">
              <button type="button" class="social-club-btn" data-social-action="1" disabled>
                <span class="social-club-btn-icon"><span class="material-symbols-outlined" style="font-size:18px;">playlist_add</span></span>
                <span>Join Waitlist</span>
              </button>
            </div>
            <div class="social-club-note">Notifications work best while your browser is running. Delivery can vary if the browser is fully closed.</div>
          </div>
        </div>
      </section>
    `);

    targetEl.appendChild(wrap);

    const btn = qs(wrap, '[data-social-action]');
    const card = qs(wrap, '.social-club-card');

    let pollTimer = null;
    let sse = null;

    async function refresh() {
      try {
        const status = await getEventStatus();
        try { console.log('🎭 [SocialClub] Poll status:', status); } catch { }
        setUiState(card, status);
        if (btn) btn.disabled = false;
      } catch (err) {
        try { console.warn('⚠️ [SocialClub] Poll failed:', err?.message || err); } catch { }
        if (btn) {
          btn.disabled = false;
        }
      }
    }

    function startRealtime() {
      try {
        if (typeof EventSource === 'undefined') return;
        if (sse) return;

        sse = new EventSource('/api/events/social_club/stream');
        sse.onopen = () => {
          try { console.log('📡 [SocialClub] SSE connected'); } catch { }
        };
        sse.onmessage = (ev) => {
          try {
            const data = ev?.data ? JSON.parse(ev.data) : null;
            if (!data || data.type !== 'social_club_state') return;
            const event = data.event || {};
            try { console.log('📡 [SocialClub] SSE state:', event); } catch { }
            setUiState(card, { isEventOpen: !!event.isEventOpen });
          } catch { }
        };
        sse.onerror = () => {
          try { console.warn('⚠️ [SocialClub] SSE error - reconnecting via poll'); } catch { }
          try {
            sse && sse.close && sse.close();
          } catch { }
          sse = null;
        };
      } catch {
        sse = null;
      }
    }

    async function handleAction() {
      if (!btn) return;
      const mode = btn.dataset.mode || 'waitlist';

      if (mode === 'enter') {
        window.location.href = '/chat.html?mode=social-club';
        return;
      }

      btn.disabled = true;

      try {
        wireForegroundMessages();
        await joinWaitlist();
        toast('You are on the waitlist. We’ll notify you when the event goes live.');
      } catch (err) {
        const raw = err && err.message ? err.message : 'Failed to join waitlist';
        const msg = String(raw || 'Failed to join waitlist');
        if (msg.toLowerCase().includes('blocked') || msg.toLowerCase().includes('denied')) {
          toast(msg);
          toast('Tip: Click the lock icon in the address bar → Site settings → Notifications → Allow. Then click Join Waitlist again.');
        } else {
          toast(msg);
        }
      } finally {
        btn.disabled = false;
      }
    }

    if (btn) {
      btn.addEventListener('click', handleAction);
    }

    refresh();

    startRealtime();

    const intervalMs = Math.max(4000, Number(options.pollIntervalMs || 8000));
    pollTimer = setInterval(refresh, intervalMs);

    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) refresh();
    });

    return {
      refresh,
      destroy: () => {
        if (pollTimer) clearInterval(pollTimer);
        try {
          if (sse) sse.close();
        } catch { }
        wrap.remove();
      }
    };
  };
})();
