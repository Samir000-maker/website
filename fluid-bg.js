/**
 * Fluid Background – Diurnal Theme Engine
 * Lightweight, isolated vanilla JS.
 * Checks local system time every 60 seconds and applies the
 * appropriate theme class to <body>.
 */
(function () {
  'use strict';

  /* ── 1. Inject the #fluid-bg container if not present ── */
  function injectFluidContainer() {
    if (document.getElementById('fluid-bg')) return;
    var container = document.createElement('div');
    container.id = 'fluid-bg';
    container.setAttribute('aria-hidden', 'true');
    container.innerHTML =
      '<div class="fluid-blobs-wrapper">' +
      '<div class="fluid-blob fluid-blob--1"></div>' +
      '<div class="fluid-blob fluid-blob--2"></div>' +
      '<div class="fluid-blob fluid-blob--3"></div>' +
      '<div class="fluid-blob fluid-blob--4"></div>' +
      '</div>';
    document.body.insertBefore(container, document.body.firstChild);
  }

  /* ── 2. Determine theme from the current hour ── */
  function getThemeForHour(h) {
    if (h >= 6  && h < 11) return 'theme-morning';
    if (h >= 11 && h < 15) return 'theme-noon';
    if (h >= 15 && h < 18) return 'theme-afternoon';
    if (h >= 18 && h < 21) return 'theme-evening';
    if (h >= 21 || (h >= 0 && h < 1)) return 'theme-night';
    /* 01:00–05:59 */
    return 'theme-deepnight';
  }

  var ALL_THEMES = [
    'theme-morning',
    'theme-noon',
    'theme-afternoon',
    'theme-evening',
    'theme-night',
    'theme-deepnight'
  ];

  /* ── 3. Apply the correct theme class ── */
  function applyTheme() {
    var hour  = new Date().getHours();
    var theme = getThemeForHour(hour);
    var body  = document.body;

    /* Remove previous theme classes */
    for (var i = 0; i < ALL_THEMES.length; i++) {
      if (ALL_THEMES[i] !== theme) {
        body.classList.remove(ALL_THEMES[i]);
      }
    }
    /* Add the new one (no-op if already present) */
    body.classList.add(theme);
  }

  /* ── 4. Boot ── */
  function boot() {
    injectFluidContainer();
    applyTheme();
    /* Re-check every second */
    setInterval(applyTheme, 1000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
