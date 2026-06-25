(function () {
    const MoodApp = window.MoodApp || {};
    const _Auth = MoodApp.Auth;
    const _API = MoodApp.API;
    const _Toast = MoodApp.Toast;
    const _Loading = MoodApp.Loading;
    const _PageTransition = MoodApp.PageTransition;

    try {
        if (!firebase.apps?.length) {
            firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__);
        }
    } catch (error) {
        console.error('Firebase init error:', error);
    }

    const toastError = (msg) => {
        if (_Toast && typeof _Toast.error === 'function') _Toast.error(msg);
        else console.error('Toast.error:', msg);
    };
    const toastSuccess = (msg) => {
        if (_Toast && typeof _Toast.success === 'function') _Toast.success(msg);
        else console.log('Toast.success:', msg);
    };

    const ensureLoadingPopup = () => {
        let el = document.getElementById('usernameLoadingPopup');
        if (el) return el;
        el = document.createElement('div');
        el.id = 'usernameLoadingPopup';
        el.className = 'fixed inset-0 z-[9999] hidden items-center justify-center';
        el.innerHTML = `
          <div class="absolute inset-0 bg-black/60 backdrop-blur-sm"></div>
          <div class="relative w-[92vw] max-w-sm rounded-2xl bg-[#0f1115]/95 border border-white/10 shadow-2xl p-6">
            <div class="flex items-center gap-3">
              <div class="w-10 h-10 rounded-full border-2 border-primary/30 border-t-primary" style="animation: spin 1s linear infinite;"></div>
              <div>
                <div class="text-base font-bold text-white">Saving…</div>
                <div id="usernameLoadingText" class="mt-1 text-sm text-gray-400">Setting up your profile…</div>
              </div>
            </div>
          </div>
        `;
        document.body.appendChild(el);
        return el;
    };

    const loadingShow = (msg) => {
        try {
            if (_Loading && typeof _Loading.show === 'function') _Loading.show(msg);
        } catch { }
        const el = ensureLoadingPopup();
        const t = el.querySelector('#usernameLoadingText');
        if (t && msg) t.textContent = msg;
        el.classList.remove('hidden');
        el.classList.add('flex');
    };
    const loadingHide = () => {
        try {
            if (_Loading && typeof _Loading.hide === 'function') _Loading.hide();
        } catch { }
        const el = document.getElementById('usernameLoadingPopup');
        if (!el) return;
        el.classList.add('hidden');
        el.classList.remove('flex');
    };

    const debounce = (fn, wait = 300) => {
        let timer = null;
        return function (...args) {
            if (timer) clearTimeout(timer);
            timer = setTimeout(() => fn.apply(this, args), wait);
        };
    };

    const isValidUsername = (u) => {
        if (!u || typeof u !== 'string') return false;
        const trimmed = u.trim();
        return /^[a-zA-Z0-9_-]{3,20}$/.test(trimmed);
    };

    const usernameInput = document.getElementById('username');
    const submitBtn = document.getElementById('submitBtn');
    const statusIcon = document.getElementById('statusIcon');
    const feedback = document.getElementById('feedback');
    const usernameForm = document.getElementById('usernameForm');

    try {
        if (_Auth && typeof _Auth.requireAuth === 'function') {
            _Auth.requireAuth();
        }
    } catch (err) {
        console.error('Error calling Auth.requireAuth():', err);
    }

    function showFeedback(type, message) {
        if (!feedback) return;
        const color = type === 'success' ? 'emerald' : 'red';
        const icon = type === 'success' ? '\u2713' : '\u2717';
        feedback.innerHTML = `
            <div class="flex items-start gap-2 px-1">
                <span class="text-${color}-400 text-[18px] shrink-0">${icon}</span>
                <p class="text-sm font-medium text-${color}-400 leading-normal">${message}</p>
            </div>
        `;
    }

    const hideStatusIcon = () => {
        if (!statusIcon) return;
        statusIcon.classList.add('hidden');
        statusIcon.innerHTML = '';
    };

    const showSpinner = () => {
        if (!statusIcon) return;
        statusIcon.innerHTML = '<div class="loading-spinner" style="width:20px;height:20px;border-width:2px;border-radius:9999px;border-style:solid;border-color:#6b7280;border-top-color:transparent;animation:spin 1s linear infinite"></div>';
        statusIcon.classList.remove('hidden');
    };

    let isAvailable = false;
    let lastCheckedUsername = '';

    const getApiBaseUrl = () => {
        try {
            const { hostname, port } = window.location;
            if ((hostname === '127.0.0.1' || hostname === 'localhost') && port === '5500') {
                return 'http://localhost:3000';
            }
        } catch {}
        return window.location.origin;
    };

    const API_BASE_URL = getApiBaseUrl();

    const checkUsername = debounce(async (username) => {
        if (!isValidUsername(username)) {
            showFeedback('error', 'Username must be 3-20 characters: letters, numbers, underscores, hyphens');
            isAvailable = false;
            if (submitBtn) submitBtn.disabled = true;
            hideStatusIcon();
            return;
        }

        if (lastCheckedUsername === username && isAvailable) {
            return;
        }

        lastCheckedUsername = username;
        showSpinner();
        if (feedback) feedback.innerHTML = '';

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000);

        try {
            console.log('Checking username availability:', username);
            
            const response = await fetch(`${API_BASE_URL}/api/check-username`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ username }),
                signal: controller.signal
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                throw new Error(`Server returned ${response.status}`);
            }

            const result = await response.json();
            console.log('Username check result:', result);

            if (result && result.available) {
                isAvailable = true;
                if (submitBtn) submitBtn.disabled = false;
                statusIcon.innerHTML = '<span class="text-emerald-400 text-2xl">\u2713</span>';
                statusIcon.classList.remove('hidden');
                showFeedback('success', `Excellent! <strong>@${username}</strong> is available`);
            } else {
                isAvailable = false;
                if (submitBtn) submitBtn.disabled = true;
                statusIcon.innerHTML = '<span class="text-red-400 text-2xl">\u2717</span>';
                statusIcon.classList.remove('hidden');

                let suggestionsHtml = '';
                if (result && Array.isArray(result.suggestions) && result.suggestions.length) {
                    suggestionsHtml = `<br><small>Try: ${result.suggestions.map(s => `<strong>@${s}</strong>`).join(', ')}</small>`;
                }
                showFeedback('error', `Username taken${suggestionsHtml}`);
            }
        } catch (error) {
            clearTimeout(timeoutId);
            console.error('Error checking username:', error);
            
            hideStatusIcon();
            
            if (error.name === 'AbortError') {
                showFeedback('error', 'Request timeout. Please try again.');
            } else {
                showFeedback('error', 'Unable to check availability. Please try again.');
            }
            
            isAvailable = false;
            if (submitBtn) submitBtn.disabled = true;
        }
    }, 500);

    if (usernameInput) {
        usernameInput.addEventListener('input', (e) => {
            const username = (e.target.value || '').trim().toLowerCase();

            if (!username) {
                hideStatusIcon();
                if (feedback) feedback.innerHTML = '';
                if (submitBtn) submitBtn.disabled = true;
                isAvailable = false;
                lastCheckedUsername = '';
                return;
            }

            if (!isValidUsername(username)) {
                hideStatusIcon();
                showFeedback('error', 'Only letters, numbers, underscores, and hyphens allowed');
                if (submitBtn) submitBtn.disabled = true;
                isAvailable = false;
                return;
            }

            checkUsername(username);
        });
    }

    if (usernameForm) {
        usernameForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const username = usernameInput ? usernameInput.value.trim().toLowerCase() : '';

            console.log('Form submitted with username:', username, 'isAvailable:', isAvailable);

            if (!username || !isValidUsername(username)) {
                toastError('Please enter a valid username');
                return;
            }

            if (!isAvailable) {
                toastError('Please choose an available username');
                return;
            }

            loadingShow('Setting up your profile...');

            try {
                let result;
                
                if (_API && typeof _API.post === 'function') {
                    result = await _API.post('/api/users/profile', { username });
                } else {
                    const user = firebase.auth().currentUser;
                    if (!user) {
                        throw new Error('Not authenticated');
                    }
                    
                    const token = await user.getIdToken();
                    
                    const response = await fetch(`${API_BASE_URL}/api/users/profile`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${token}`
                        },
                        body: JSON.stringify({ username })
                    });

                    if (!response.ok) {
                        throw new Error(`Failed to save username: ${response.status}`);
                    }

                    result = await response.json();
                }

                console.log('Profile save result:', result);

                try {
                    localStorage.setItem('moodapp_username', username);
                } catch (err) {
                    console.warn('Could not save to localStorage:', err);
                }

                loadingHide();
                toastSuccess('Username saved!');

                setTimeout(() => {
                    if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
                        _PageTransition.navigateTo('/profile-picture.html');
                    } else {
                        window.location.href = '/profile-picture.html';
                    }
                }, 1000);
            } catch (error) {
                loadingHide();
                console.error('Failed to save username:', error);
                toastError(error && error.message ? error.message : 'Failed to save username. Please try again.');
            }
        });
    }
})();

(function () {
    const btn = document.getElementById('contactSupportBtn');
    const modal = document.getElementById('contactSupportModal');
    const copyBtn = document.getElementById('contactSupportCopyBtn');
    const emailEl = document.getElementById('contactSupportEmail');
    const hint = document.getElementById('contactSupportCopyHint');
    if (!btn || !modal || !copyBtn || !emailEl) return;

    const open = () => {
        modal.classList.remove('hidden');
        modal.classList.add('flex');
    };
    const close = () => {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
        if (hint) hint.classList.add('hidden');
    };

    btn.addEventListener('click', open);
    modal.addEventListener('click', (e) => {
        const t = e.target;
        if (t && t.closest && t.closest('[data-close="1"]')) close();
    });
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') close();
    });

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
            if (hint) {
                hint.classList.remove('hidden');
                setTimeout(() => hint.classList.add('hidden'), 1200);
            }
        } catch { }
    });
})();
