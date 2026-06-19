/* ============================================
   index.js — All logic & functionality for index.html
   No HTML/CSS changes here — purely behavioral
   ============================================ */

/* ── 1. Silence console (optional, non-intrusive) ── */
(function () {
  try {
    if (window.__VIBE_SILENCE_CONSOLE__) return;
    window.__VIBE_SILENCE_CONSOLE__ = true;
    const noop = function () { };
    console.log = noop;
    console.info = noop;
    console.warn = noop;
    console.error = noop;
    console.debug = noop;
  } catch { }
})();

/* ── 2. Contact Support Modal ── */
(function () {
  const btn = document.getElementById('contactSupportBtn');
  const modal = document.getElementById('contactSupportModal');
  const copyBtn = document.getElementById('contactSupportCopyBtn');
  const emailEl = document.getElementById('contactSupportEmail');
  const hint = document.getElementById('contactSupportCopyHint');
  if (!btn || !modal || !copyBtn || !emailEl) return;

  const open = () => { modal.classList.remove('hidden'); modal.classList.add('flex'); };
  const close = () => { modal.classList.add('hidden'); modal.classList.remove('flex'); if (hint) hint.classList.add('hidden'); };

  btn.addEventListener('click', open);
  modal.addEventListener('click', (e) => {
    const t = e.target;
    if (t && t.closest && t.closest('[data-close="1"]')) close();
  });
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });

  copyBtn.addEventListener('click', async () => {
    const text = emailEl.textContent || 'contact@vibegra.com';
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        ta.remove();
      }
      if (hint) { hint.classList.remove('hidden'); setTimeout(() => hint.classList.add('hidden'), 1200); }
    } catch { }
  });
})();

/* ── 3. PWA Install ── */
try { window.VibePWA && window.VibePWA.init && window.VibePWA.init(); } catch { }

/* ── 4. Social Club Mount ── */
(function () {
  const mount = () => {
    try {
      const el = document.getElementById('socialClubMountIndex');
      if (el && window.SocialClub && typeof window.SocialClub.mount === 'function') {
        window.SocialClub.mount(el);
      }
    } catch { }
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mount);
  } else {
    mount();
  }
})();

/* ── 5. Social Club Owner Toggle ── */
(function () {
  const root = document.getElementById('socialClubOwnerToggleIndex');
  const checkbox = document.getElementById('socialClubOwnerToggleCheckboxIndex');
  const stateEl = document.getElementById('socialClubOwnerToggleStateIndex');
  const hintEl = document.getElementById('socialClubOwnerToggleHintIndex');
  if (!root || !checkbox || !stateEl) return;

  const OWNER_EMAIL = 'samirahmed1887@gmail.com';

  function ensureFirebaseReady() {
    try {
      if (typeof firebase === 'undefined') return false;
      if (!firebase.apps?.length) { firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__); }
      try { firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL); } catch { }
      return true;
    } catch { return false; }
  }

  async function authedHeaders() {
    if (!ensureFirebaseReady()) return null;
    const u = firebase?.auth?.().currentUser;
    if (!u) return null;
    const token = await u.getIdToken();
    return { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` };
  }

  async function isOwner() {
    try {
      const headers = await authedHeaders();
      if (!headers) return false;
      const res = await fetch('/api/users/me', { headers, credentials: 'same-origin' });
      if (!res.ok) return false;
      const me = await res.json();
      return String(me?.email || '').trim().toLowerCase() === OWNER_EMAIL;
    } catch { return false; }
  }

  async function refresh() {
    const headers = await authedHeaders();
    if (!headers) return;
    const res = await fetch('/api/admin/social_club/event-owner', { headers, credentials: 'same-origin' });
    if (!res.ok) throw new Error('Failed to fetch event state');
    const payload = await res.json();
    const open = !!payload?.event?.isEventOpen;
    checkbox.checked = open;
    stateEl.textContent = open ? 'ON' : 'OFF';
  }

  async function setOpen(nextOpen) {
    const headers = await authedHeaders();
    if (!headers) throw new Error('Not authenticated');
    const res = await fetch('/api/admin/social_club/event-owner', {
      method: 'POST',
      headers,
      credentials: 'same-origin',
      body: JSON.stringify({ isEventOpen: !!nextOpen })
    });
    if (!res.ok) { const t = await res.text(); throw new Error(t || 'Failed to update event'); }
    await refresh();
  }

  checkbox.addEventListener('change', async () => {
    const desired = checkbox.checked;
    checkbox.disabled = true;
    try { await setOpen(desired); }
    catch (e) {
      checkbox.checked = !desired;
      if (hintEl) hintEl.textContent = e?.message ? String(e.message) : 'Failed to update';
    } finally { checkbox.disabled = false; }
  });

  async function boot() {
    ensureFirebaseReady();
    const ok = await isOwner();
    if (!ok) return;
    root.classList.remove('hidden');
    try { await refresh(); }
    catch (e) { if (hintEl) hintEl.textContent = e?.message ? String(e.message) : 'Failed to load'; }
  }

  try { firebase?.auth?.().onAuthStateChanged(() => { boot(); }); } catch { boot(); }
})();

/* ── 6. Main App Logic (Mood Selection, Connect, Chat) ── */
(function () {
  const connectBtn = document.getElementById('connectNowBtn');
  const grid = document.getElementById('moodCardsGrid');

  const moods = [
    { id: 'happy', name: 'Happy', emoji: '😊', hint: 'Feeling good', tint: 'from-emerald-400/10' },
    { id: 'sad', name: 'Sad', emoji: '😭', hint: 'Feeling down', tint: 'from-indigo-500/10' },
    { id: 'angry', name: 'Angry', emoji: '😠', hint: 'Frustrated', tint: 'from-orange-500/10' },
    { id: 'lonely', name: 'Lonely', emoji: '😔', hint: 'Seeking connection', tint: 'from-blue-400/10' },
    { id: 'calm', name: 'Calm', emoji: '😌', hint: 'Peaceful', tint: 'from-teal-400/10' },
    { id: 'excited', name: 'Excited', emoji: '🤩', hint: 'High energy', tint: 'from-pink-500/10' },
    { id: 'tired', name: 'Tired', emoji: '😴', hint: 'Low energy', tint: 'from-primary/15' },
    { id: 'stressed', name: 'Stressed', emoji: '😣', hint: 'Overwhelmed', tint: 'from-red-400/10' },
    { id: 'confused', name: 'Confused', emoji: '😕', hint: 'Uncertain', tint: 'from-purple-400/10' },
  ];

  let selectedMood = null;

  function ensureOverlay() {
    let el = document.getElementById('guestConnectOverlay');
    if (el) return el;
    el = document.createElement('div');
    el.id = 'guestConnectOverlay';
    el.className = 'fixed inset-0 z-[9999] hidden items-center justify-center';
    el.innerHTML = `
      <div class="absolute inset-0 bg-black/70 backdrop-blur-sm"></div>
      <div class="relative w-[92vw] max-w-sm rounded-2xl bg-[#15161C]/95 border border-white/10 shadow-2xl p-6">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-full border-2 border-primary/30 border-t-primary" style="animation: breathe 1.2s ease-in-out infinite;"></div>
          <div>
            <div class="text-base font-bold text-white">Connecting…</div>
            <div id="guestConnectOverlayText" class="mt-1 text-sm text-slate-400">Preparing your guest profile…</div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(el);
    return el;
  }

  function showOverlay(msg) {
    const el = ensureOverlay();
    const t = el.querySelector('#guestConnectOverlayText');
    if (t && msg) t.textContent = msg;
    el.classList.remove('hidden');
    el.classList.add('flex');
  }

  function render() {
    if (!grid) return;
    grid.innerHTML = '';
    moods.forEach((m) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'group glass-card rounded-2xl p-5 flex flex-col items-center justify-center gap-3 text-center cursor-pointer relative overflow-hidden h-full';
      btn.dataset.mood = m.id;
      btn.innerHTML = `
        <div class="absolute inset-0 bg-gradient-to-br ${m.tint} to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
        <div class="w-16 h-16 flex items-center justify-center emoji-wrapper">
          <span class="emoji-3d" style="font-size: 3rem;">${m.emoji}</span>
        </div>
        <div class="relative z-10">
          <h3 class="text-base font-semibold text-white mb-1">${m.name}</h3>
          <p class="text-xs mood-hint font-light">${m.hint}</p>
        </div>
      `;
      btn.addEventListener('click', () => {
        selectedMood = m.id;
        try { localStorage.setItem('selectedMood', selectedMood); } catch { }
        updateSelection();
      });
      grid.appendChild(btn);
    });
    updateSelection();
  }

  function updateSelection() {
    const cards = grid ? Array.from(grid.querySelectorAll('button[data-mood]')) : [];
    cards.forEach((c) => {
      const isSelected = c.dataset.mood === selectedMood;
      c.classList.toggle('ring-2', isSelected);
      c.classList.toggle('ring-primary/50', isSelected);
      c.classList.toggle('shadow-[0_0_30px_rgba(99,32,233,0.15)]', isSelected);
      c.classList.toggle('transform', isSelected);
      c.classList.toggle('-translate-y-2', isSelected);
    });
    if (connectBtn) connectBtn.disabled = !selectedMood;
  }

  async function initFirebase() {
    if (typeof firebase === 'undefined') throw new Error('Firebase not loaded');
    if (!firebase.apps?.length) { firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__); }
    firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL);
  }

  async function connectNow() {
    if (!selectedMood) return;
    if (!connectBtn || connectBtn.disabled) return;
    connectBtn.disabled = true;
    showOverlay('Signing you in anonymously…');
    try {
      await initFirebase();
      let user = firebase.auth().currentUser;
      if (!user) {
        const credential = await firebase.auth().signInAnonymously();
        user = credential.user;
      }
      if (!user) throw new Error('Anonymous auth failed');
      showOverlay('Creating your guest profile…');
      const token = await user.getIdToken();
      const res = await fetch('/api/users/ensure-guest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ mood: selectedMood })
      });
      const text = await res.text();
      if (!res.ok) throw new Error(text || `HTTP ${res.status}`);
      let payload = null;
      try { payload = JSON.parse(text); } catch { }
      try {
        localStorage.setItem('guest_uid', user.uid);
        if (payload?.user?.username) localStorage.setItem('guest_username', payload.user.username);
        localStorage.setItem('guest_mood', selectedMood);
        localStorage.setItem('guest_timestamp', String(Date.now()));
        localStorage.setItem('selectedMood', selectedMood);
        localStorage.removeItem('currentRoom');
      } catch { }
      showOverlay('Entering chat…');
      window.location.href = '/chat.html';
    } catch (err) {
      const msg = (err && err.message) ? String(err.message) : 'Connection failed';
      alert(msg);
      const overlay = document.getElementById('guestConnectOverlay');
      if (overlay) { overlay.classList.add('hidden'); overlay.classList.remove('flex'); }
      connectBtn.disabled = !selectedMood;
    }
  }

  render();
  if (connectBtn) connectBtn.addEventListener('click', connectNow);

  try {
    const persisted = localStorage.getItem('selectedMood');
    if (persisted) { selectedMood = persisted; updateSelection(); }
  } catch { }
})();
