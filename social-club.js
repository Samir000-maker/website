(function () {
  const SocialClub = (window.SocialClub = window.SocialClub || {});

  function getRootConfig() {
    return window.__VIBE_FIREBASE_CONFIG__ || {};
  }

  function getVapidKey() {
    return (window.__VIBE_FCM_VAPID_KEY__ || '').trim();
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
      .social-club-wrap{width:100%;margin:0 auto;}
      .social-club-kicker{display:none;}
      .social-club-card{position:relative;overflow:hidden;border-radius:16px;padding:14px 20px;border:1px solid rgba(255,255,255,0.08);background:linear-gradient(135deg,rgba(255,255,255,0.06),rgba(255,255,255,0.015));backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);box-shadow:0 10px 40px rgba(0,0,0,0.25);transition:transform 180ms ease,box-shadow 180ms ease,border-color 180ms ease;}
      .social-club-card:hover{transform:translateY(-1px);border-color:rgba(99,32,233,0.35);box-shadow:0 14px 50px rgba(99,32,233,0.12),0 10px 40px rgba(0,0,0,0.3);}
      .social-club-row{display:flex;align-items:center;justify-content:space-between;gap:12px;}
      .social-club-left{display:flex;align-items:center;gap:10px;min-width:0;}
      .social-club-left h3{font-weight:800;font-size:1rem;letter-spacing:-0.01em;color:#fff;line-height:1.3;white-space:nowrap;}
      .social-club-dot{width:9px;height:9px;border-radius:9999px;background:rgba(148,163,184,0.5);box-shadow:none;flex-shrink:0;}
      .social-club-dot.live{background:#22c55e;box-shadow:0 0 0 5px rgba(34,197,94,0.15);}
      .social-club-status-text{font-size:0.75rem;font-weight:600;color:rgba(226,232,240,0.7);white-space:nowrap;}
      .social-club-desc{margin-top:8px;color:rgba(148,163,184,0.85);font-size:0.78rem;line-height:1.4;}
      .social-club-btn{appearance:none;-webkit-appearance:none;border:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:7px;padding:8px 16px;border-radius:10px;font-weight:700;font-size:0.8rem;line-height:1;color:#fff;background:linear-gradient(135deg,rgba(99,32,233,0.92),rgba(45,212,191,0.52));box-shadow:0 8px 24px rgba(99,32,233,0.22);transition:transform 160ms ease,box-shadow 160ms ease,filter 160ms ease;white-space:nowrap;flex-shrink:0;}
      .social-club-btn:hover{transform:translateY(-1px);filter:saturate(1.1);box-shadow:0 12px 32px rgba(99,32,233,0.3);}
      .social-club-btn:disabled{opacity:0.5;cursor:not-allowed;transform:none;box-shadow:none;}
      .social-club-btn-icon{width:19px;height:19px;border-radius:7px;display:inline-flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.18);border:1px solid rgba(255,255,255,0.1);}
      @media (max-width: 640px){
        .social-club-card{padding:12px 16px;}
        .social-club-left{gap:8px;}
        .social-club-left h3{font-size:0.9rem;}
        .social-club-status-text{font-size:0.7rem;}
        .social-club-btn{padding:7px 14px;font-size:0.75rem;}
        .social-club-desc{font-size:0.72rem;margin-top:6px;}
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

  function ensureEnterOverlay() {
    let el = document.getElementById('socialClubEnterOverlay');
    if (el) return el;
    el = document.createElement('div');
    el.id = 'socialClubEnterOverlay';
    el.className = 'fixed inset-0 z-[99999] hidden items-center justify-center';
    el.innerHTML = `
      <div class="absolute inset-0 bg-black/70 backdrop-blur-sm"></div>
      <div class="relative w-[92vw] max-w-sm rounded-2xl bg-[#15161C]/95 border border-white/10 shadow-2xl p-6">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-full border-2 border-primary/30 border-t-primary" style="animation: breathe 1.2s ease-in-out infinite;"></div>
          <div>
            <div class="text-base font-bold text-white">Entering Social Club…</div>
            <div id="socialClubEnterOverlayText" class="mt-1 text-sm text-slate-400">Preparing your guest profile…</div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(el);
    return el;
  }

  function showEnterOverlay(msg) {
    const el = ensureEnterOverlay();
    const t = el.querySelector('#socialClubEnterOverlayText');
    if (t && msg) t.textContent = msg;
    el.classList.remove('hidden');
    el.classList.add('flex');
  }

  function hideEnterOverlay() {
    const el = document.getElementById('socialClubEnterOverlay');
    if (!el) return;
    el.classList.add('hidden');
    el.classList.remove('flex');
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

  async function ensureGuestProfile(contextMood = 'social_club') {
    const user = await ensureSignedIn();
    const token = await user.getIdToken();
    const res = await fetch('/api/users/ensure-guest', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`
      },
      body: JSON.stringify({ mood: contextMood })
    });
    const text = await res.text();
    if (!res.ok) throw new Error(text || `HTTP ${res.status}`);
    let payload = null;
    try { payload = text ? JSON.parse(text) : null; } catch { payload = null; }

    try {
      if (payload?.user?.username) localStorage.setItem('guest_username', payload.user.username);
      if (user?.uid) localStorage.setItem('guest_uid', user.uid);
      localStorage.setItem('guest_timestamp', String(Date.now()));
    } catch { }

    return payload;
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
        ? `<span class="social-club-btn-icon"><span class="material-symbols-outlined" style="font-size:18px;">login</span></span><span>Enter Now</span>`
        : `<span class="social-club-btn-icon"><span class="material-symbols-outlined" style="font-size:18px;">playlist_add</span></span><span>Join Waitlist</span>`;
    }
  }

  SocialClub.mount = function mount(targetEl, options = {}) {
    if (!targetEl) return;
    ensureStyles();

    const wrap = createEl(`
      <section class="social-club-wrap" data-social-club-root="1">
        <div class="social-club-card">
          <div class="social-club-kicker" style="display:none;">Event will start soon</div>
          <div class="social-club-row">
            <div class="social-club-left">
              <span class="social-club-dot" data-social-dot="1"></span>
              <h3>Social Club</h3>
              <span class="social-club-status-text" data-social-status-text="1">Event will start soon</span>
            </div>
            <button type="button" class="social-club-btn" data-social-action="1" disabled>
              <span class="social-club-btn-icon"><span class="material-symbols-outlined" style="font-size:16px;">playlist_add</span></span>
              <span>Join Waitlist</span>
            </button>
          </div>
          <div class="social-club-desc">Event matchmaking room — join when it's live to meet new people</div>
        </div>
      </section>
    `);

    targetEl.appendChild(wrap);

    const btn = qs(wrap, '[data-social-action]');
    const card = qs(wrap, '.social-club-card');

    let pollTimer = null;
    let sse = null;

    const latestState = {
      isEventOpen: false,
      updatedAtMs: -1
    };

    function parseUpdatedAtMs(updatedAt) {
      if (!updatedAt) return null;
      const ms = Date.parse(updatedAt);
      return Number.isFinite(ms) ? ms : null;
    }

    function applyMergedState(nextState, source) {
      const isEventOpen = !!nextState?.isEventOpen;
      const updatedAtRaw = nextState?.updatedAt || null;
      const updatedAtMs = parseUpdatedAtMs(updatedAtRaw);

      // If the update does not include a valid timestamp, treat it as non-authoritative.
      // This prevents SSE messages with null updatedAt from overriding accurate poll results.
      if (updatedAtMs === null) {
        if (latestState.updatedAtMs >= 0) {
          try {
            console.log('🎭 [SocialClub] Ignoring non-timestamped state update', {
              source,
              isEventOpen,
              updatedAt: updatedAtRaw
            });
          } catch { }
          return;
        }

        // Allow the first-ever state set even without a timestamp.
        latestState.isEventOpen = isEventOpen;
        latestState.updatedAtMs = -1;
        setUiState(card, { isEventOpen, updatedAt: null });
        return;
      }

      if (updatedAtMs < latestState.updatedAtMs) {
        try {
          console.log('🎭 [SocialClub] Ignoring older state update', {
            source,
            isEventOpen,
            updatedAt: updatedAtRaw,
            updatedAtMs,
            currentUpdatedAtMs: latestState.updatedAtMs
          });
        } catch { }
        return;
      }

      latestState.isEventOpen = isEventOpen;
      latestState.updatedAtMs = updatedAtMs;
      setUiState(card, { isEventOpen, updatedAt: updatedAtRaw });
    }

    async function refresh() {
      try {
        const status = await getEventStatus();
        try { console.log('🎭 [SocialClub] Poll status:', status); } catch { }
        applyMergedState(status, 'poll');
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
            applyMergedState({ isEventOpen: !!event.isEventOpen, updatedAt: event.updatedAt || null }, 'sse');
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
        btn.disabled = true;
        try {
          showEnterOverlay('Signing you in anonymously…');
          await ensureSignedIn();
          showEnterOverlay('Creating your guest profile…');
          await ensureGuestProfile('social_club');
          showEnterOverlay('Entering chat…');
          window.location.href = '/chat.html?mode=social-club';
          return;
        } catch (err) {
          console.error('Social Club enter failed:', err);
          toast(err?.message ? String(err.message) : 'Failed to enter Social Club');
        } finally {
          hideEnterOverlay();
          btn.disabled = false;
        }
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
