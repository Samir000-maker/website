(function () {
  const KEY = 'vibe_persistent_chat_session_v1';

  function getSession() {
    try {
      const existing = JSON.parse(localStorage.getItem(KEY) || 'null');
      if (existing?.id) return existing;
    } catch { }
    const session = {
      id: `chat_${Date.now()}_${Math.random().toString(36).slice(2, 12)}`,
      createdAt: Date.now(),
      lastSeenAt: Date.now()
    };
    try { localStorage.setItem(KEY, JSON.stringify(session)); } catch { }
    return session;
  }

  function touch(roomId) {
    const session = getSession();
    session.lastSeenAt = Date.now();
    if (roomId) session.roomId = roomId;
    try { localStorage.setItem(KEY, JSON.stringify(session)); } catch { }
    return session;
  }

  function decorateJoinPayload(payload) {
    const session = getSession();
    const previousRoomId = session.roomId || null;
    const previousSeenAt = session.lastSeenAt || 0;
    const isResume = !!payload?.roomId
      && previousRoomId === payload.roomId
      && previousSeenAt
      && (Date.now() - previousSeenAt) < 10 * 60 * 1000;
    touch(payload?.roomId);
    return {
      ...(payload || {}),
      resume: !!isResume,
      persistentSessionId: session.id,
      lastSeenAt: previousSeenAt || null
    };
  }

  window.VibeChatConnection = {
    getSession,
    touch,
    decorateJoinPayload,
    isResumeLikely() {
      const session = getSession();
      return Date.now() - (session.lastSeenAt || 0) < 10 * 60 * 1000;
    }
  };

  window.addEventListener('online', () => touch(), { passive: true });
  window.addEventListener('visibilitychange', () => {
    if (!document.hidden) touch();
  }, { passive: true });
  window.addEventListener('pagehide', () => touch(), { passive: true });
})();
