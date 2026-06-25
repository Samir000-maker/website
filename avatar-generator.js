(function () {
  const colors = [
    ['#10b981', '#064e3b'],
    ['#a7f3d0', '#047857'],
    ['#64748b', '#1e293b'],
    ['#f8fafc', '#94a3b8'],
    ['#c4b5fd', '#6d28d9'],
    ['#60a5fa', '#1e3a8a']
  ];
  const expressions = ['happy', 'chill', 'curious', 'tech'];
  const clothes = ['hoodie', 'jacket', 'sweater', 'overalls'];
  const accessories = ['headphones', 'goggles', 'antenna', 'cap', 'none'];

  function hash(value) {
    const str = String(value || 'user');
    let h = 2166136261;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return h >>> 0;
  }

  function pick(list, seed, offset) {
    return list[(seed >>> offset) % list.length];
  }

  function escapeXml(value) {
    return String(value || '').replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;'
    })[c]);
  }

  function face(type) {
    if (type === 'happy') return '<path d="M39 57c7 8 18 8 25 0" fill="none" stroke="#07120f" stroke-width="4" stroke-linecap="round"/><path d="M35 43c4-5 9-5 13 0M58 43c4-5 9-5 13 0" fill="none" stroke="#07120f" stroke-width="4" stroke-linecap="round"/>';
    if (type === 'chill') return '<path d="M34 44h14M58 44h14" stroke="#07120f" stroke-width="4" stroke-linecap="round"/><path d="M45 59c5 3 11 3 16 0" fill="none" stroke="#07120f" stroke-width="3" stroke-linecap="round"/>';
    if (type === 'curious') return '<circle cx="41" cy="45" r="4" fill="#07120f"/><circle cx="65" cy="45" r="4" fill="#07120f"/><ellipse cx="53" cy="60" rx="5" ry="7" fill="none" stroke="#07120f" stroke-width="3"/>';
    return '<rect x="31" y="37" width="45" height="16" rx="8" fill="#0f172a" stroke="#34d399" stroke-width="2"/><path d="M38 45h8l4-4 5 8 4-5h9" fill="none" stroke="#34d399" stroke-width="2" stroke-linecap="round"/><path d="M43 62c7 4 14 4 21 0" fill="none" stroke="#07120f" stroke-width="3" stroke-linecap="round"/>';
  }

  function clothing(type) {
    if (type === 'hoodie') return '<path d="M25 88c4-20 17-30 28-30s24 10 28 30" fill="#334155"/><path d="M39 67c5 6 23 6 28 0" fill="none" stroke="#10b981" stroke-width="4" stroke-linecap="round"/>';
    if (type === 'jacket') return '<path d="M24 88c5-19 18-30 29-30s24 11 30 30" fill="#1e293b"/><path d="M53 60v28M35 78h36" stroke="#10b981" stroke-width="3" stroke-linecap="round"/>';
    if (type === 'sweater') return '<path d="M25 88c5-18 17-28 28-28s23 10 28 28" fill="#475569"/><path d="M31 76h45" stroke="#10b981" stroke-width="5" stroke-linecap="round"/>';
    return '<path d="M27 88c4-18 15-28 26-28s22 10 26 28" fill="#334155"/><path d="M38 61v27M68 61v27" stroke="#94a3b8" stroke-width="5" stroke-linecap="round"/><circle cx="38" cy="71" r="3" fill="#10b981"/><circle cx="68" cy="71" r="3" fill="#10b981"/>';
  }

  function accessory(type) {
    if (type === 'headphones') return '<path d="M26 47c0-17 12-29 27-29s27 12 27 29" fill="none" stroke="#0f172a" stroke-width="5" stroke-linecap="round"/><rect x="21" y="43" width="10" height="18" rx="5" fill="#10b981"/><rect x="75" y="43" width="10" height="18" rx="5" fill="#10b981"/>';
    if (type === 'goggles') return '<rect x="27" y="35" width="52" height="21" rx="10" fill="rgba(15,23,42,.72)" stroke="#10b981" stroke-width="3"/><path d="M53 35v21" stroke="#10b981" stroke-width="2"/>';
    if (type === 'antenna') return '<path d="M53 21V8" stroke="#10b981" stroke-width="4" stroke-linecap="round"/><circle cx="53" cy="7" r="5" fill="#34d399"/>';
    if (type === 'cap') return '<path d="M29 31c9-14 37-14 48 1v8H29z" fill="#0f172a"/><path d="M76 38c9 0 15 3 18 7-8 2-15 1-22-3" fill="#10b981"/>';
    return '';
  }

  function svgFor(id, label) {
    const seed = hash(id || label);
    const palette = pick(colors, seed, 0);
    const bodyType = seed % 2;
    const expr = pick(expressions, seed, 4);
    const cloth = pick(clothes, seed, 9);
    const acc = pick(accessories, seed, 14);
    const ears = bodyType === 0
      ? '<circle cx="24" cy="44" r="9" fill="' + palette[0] + '"/><circle cx="82" cy="44" r="9" fill="' + palette[0] + '"/>'
      : '<path d="M30 28l-8-9M76 28l8-9" stroke="' + palette[0] + '" stroke-width="5" stroke-linecap="round"/>';
    const labelText = escapeXml((label || 'V').slice(0, 2).toUpperCase());
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 106 106" role="img" aria-label="${escapeXml(label || 'Avatar')}">
      <defs>
        <linearGradient id="bg" x1="0" x2="1" y1="0" y2="1"><stop stop-color="#0f172a"/><stop offset="1" stop-color="#020617"/></linearGradient>
        <linearGradient id="body" x1="0" x2="1" y1="0" y2="1"><stop stop-color="${palette[0]}"/><stop offset="1" stop-color="${palette[1]}"/></linearGradient>
      </defs>
      <rect width="106" height="106" rx="28" fill="url(#bg)"/>
      <circle cx="84" cy="18" r="17" fill="#10b981" opacity=".12"/>
      ${ears}
      <path d="M18 93c5-25 19-39 35-39s31 14 36 39" fill="#0f172a"/>
      ${clothing(cloth)}
      <path d="M22 48c0-22 13-35 31-35s31 13 31 35c0 20-13 33-31 33S22 68 22 48z" fill="url(#body)"/>
      <path d="M32 26c9-8 27-9 39 1" fill="none" stroke="#fff" stroke-opacity=".22" stroke-width="5" stroke-linecap="round"/>
      ${face(expr)}
      ${accessory(acc)}
      <text x="53" y="99" text-anchor="middle" font-family="Manrope, Arial, sans-serif" font-size="10" font-weight="800" fill="#a7f3d0" opacity=".72">${labelText}</text>
    </svg>`;
  }

  function dataUrl(id, label) {
    return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svgFor(id, label))}`;
  }

  function isDefaultUrl(url) {
    if (!url || typeof url !== 'string') return true;
    const u = url.trim();
    if (!u || u === 'null' || u === 'undefined') return true;
    return /ui-avatars\.com\/api\/.+name=User/i.test(u);
  }

  function profileUrl(profile, fallbackLabel) {
    const user = profile || {};
    const pfpUrl = typeof user.pfpUrl === 'string' ? user.pfpUrl.trim() : '';
    const username = (typeof user.username === 'string' && user.username.trim())
      ? user.username.trim()
      : (fallbackLabel || 'User');
    if (!isDefaultUrl(pfpUrl)) return pfpUrl;
    return dataUrl(user.userId || user._id || username, username);
  }

  function element(id, label, className) {
    const img = document.createElement('img');
    img.src = dataUrl(id, label);
    img.alt = label || 'Avatar';
    img.loading = 'lazy';
    if (className) img.className = className;
    return img;
  }

  window.VibeAvatar = { svgFor, dataUrl, element, isDefaultUrl, profileUrl };
})();
