(function () {
  const root = document.documentElement;
  const reference = { width: 1440, height: 900 };
  let rafId = 0;

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function getDeviceType(width) {
    if (width < 640) return 'mobile';
    if (width < 900) return 'tablet';
    if (width < 1200) return 'laptop';
    return 'desktop';
  }

  function scaleFor(width) {
    const raw = width / reference.width;
    const type = getDeviceType(width);
    if (type === 'desktop') return clamp(raw, 1, 1.2);
    if (type === 'laptop') return clamp(raw, 0.9, 1);
    if (type === 'tablet') return clamp(raw, 0.7, 0.9);
    return clamp(raw, 0.5, 0.8);
  }

  function applyMetrics() {
    const width = window.innerWidth || reference.width;
    const height = window.innerHeight || reference.height;
    const scale = scaleFor(width);
    const aspect = width / Math.max(height, 1);
    const device = getDeviceType(width);

    root.style.setProperty('--ui-scale', scale.toFixed(3));
    root.style.setProperty('--font-scale', clamp(scale, 0.82, 1.08).toFixed(3));
    root.style.setProperty('--spacing-scale', clamp(scale, 0.72, 1.12).toFixed(3));
    root.style.setProperty('--radius-scale', clamp(scale, 0.78, 1.08).toFixed(3));
    root.style.setProperty('--image-scale', clamp(scale, 0.72, 1.08).toFixed(3));
    root.style.setProperty('--vh', `${height * 0.01}px`);
    root.dataset.device = device;
    root.dataset.orientation = width >= height ? 'landscape' : 'portrait';
    root.dataset.aspect = aspect.toFixed(2);
    root.dataset.dpr = String(Math.round((window.devicePixelRatio || 1) * 100) / 100);
  }

  function schedule() {
    if (rafId) return;
    rafId = requestAnimationFrame(() => {
      rafId = 0;
      applyMetrics();
    });
  }

  applyMetrics();
  window.addEventListener('resize', schedule, { passive: true });
  window.addEventListener('orientationchange', schedule, { passive: true });
  window.addEventListener('pageshow', schedule, { passive: true });

  if ('ResizeObserver' in window) {
    const observer = new ResizeObserver(schedule);
    observer.observe(document.documentElement);
  }

  window.VibeResponsive = {
    refresh: schedule,
    getMetrics() {
      return {
        width: window.innerWidth,
        height: window.innerHeight,
        aspectRatio: window.innerWidth / Math.max(window.innerHeight, 1),
        deviceType: getDeviceType(window.innerWidth),
        pixelDensity: window.devicePixelRatio || 1,
        orientation: window.innerWidth >= window.innerHeight ? 'landscape' : 'portrait'
      };
    }
  };
})();
