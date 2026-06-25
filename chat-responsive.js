(function () {
  function qs(id) { return document.getElementById(id); }

  function isSmall() {
    return window.matchMedia('(max-width: 768px)').matches;
  }

  function setCollapsed(collapsed) {
    const sidebar = qs('sidebar');
    const overlay = qs('sidebarOverlay');
    const arrow = qs('mobileUsersArrow');
    if (!sidebar) return;
    sidebar.classList.toggle('show', !collapsed);
    overlay?.classList.toggle('active', !collapsed);
    if (arrow) arrow.textContent = collapsed ? 'keyboard_arrow_down' : 'keyboard_arrow_up';
  }

  function findMeCard() {
    return Array.from(document.querySelectorAll('#usersList .user-card')).find((card) => {
      return /\(You\)/i.test(card.textContent || '');
    }) || null;
  }

  function updateCurrentUserStrip() {
    const strip = qs('mobileCurrentUser');
    const avatarSlot = qs('mobileCurrentUserAvatar');
    const nameSlot = qs('mobileCurrentUserName');
    if (!strip || !avatarSlot || !nameSlot) return;

    strip.classList.toggle('hidden', !isSmall());
    if (!isSmall()) return;

    const meCard = findMeCard();
    const rawName = (meCard?.textContent || localStorage.getItem('guest_username') || 'You').replace(/\s*\(You\)\s*/i, '').trim() || 'You';
    nameSlot.textContent = rawName;
    avatarSlot.innerHTML = '';
    const img = meCard?.querySelector('img');
    if (img) {
      const clone = img.cloneNode(true);
      clone.className = 'h-full w-full object-cover';
      avatarSlot.appendChild(clone);
    } else if (window.VibeAvatar) {
      avatarSlot.appendChild(window.VibeAvatar.element(rawName, rawName, 'h-full w-full object-cover'));
    } else {
      avatarSlot.textContent = rawName.charAt(0).toUpperCase();
    }
  }

  function boot() {
    const toggle = qs('mobileUsersToggle');
    toggle?.addEventListener('click', () => {
      const sidebar = qs('sidebar');
      setCollapsed(sidebar?.classList.contains('show'));
    });

    qs('sidebarOverlay')?.addEventListener('click', () => setCollapsed(true));
    qs('sidebarClose')?.addEventListener('click', () => setCollapsed(true));
    window.addEventListener('resize', updateCurrentUserStrip, { passive: true });
    window.addEventListener('pageshow', updateCurrentUserStrip, { passive: true });

    const usersList = qs('usersList');
    if (usersList && 'MutationObserver' in window) {
      new MutationObserver(updateCurrentUserStrip).observe(usersList, { childList: true, subtree: true });
    }

    setCollapsed(true);
    updateCurrentUserStrip();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
