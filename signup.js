(async function() {
    const MoodApp = window.MoodApp || {};
    const _Auth = MoodApp.Auth;
    const _API = MoodApp.API;
    const _Toast = MoodApp.Toast;
    const _Loading = MoodApp.Loading;
    const _PageTransition = MoodApp.PageTransition;
    const _Validator = MoodApp.Validator;

    if (!MoodApp || Object.keys(MoodApp).length === 0) {
        console.warn('Warning: window.MoodApp is not defined. Make sure app.js creates window.MoodApp before this script runs.');
    }

    let isGoogleSignInInProgress = false;
    let isGuestSignInInProgress = false;

    const toastError = (msg) => {
        if (_Toast && typeof _Toast.error === 'function') _Toast.error(msg);
        else console.error('Toast.error:', msg);
    };
    const toastSuccess = (msg) => {
        if (_Toast && typeof _Toast.success === 'function') _Toast.success(msg);
        else console.log('Toast.success:', msg);
    };
    const loadingShow = (msg) => {
        if (_Loading && typeof _Loading.show === 'function') _Loading.show(msg);
    };
    const loadingHide = () => {
        if (_Loading && typeof _Loading.hide === 'function') _Loading.hide();
    };

    const ensureFirebaseReady = async () => {
        const startedAt = Date.now();
        while (typeof window.firebase === 'undefined' || !window.firebase.auth) {
            if (Date.now() - startedAt > 10000) {
                throw new Error('Firebase SDK did not load. Please refresh and try again.');
            }
            await new Promise(resolve => setTimeout(resolve, 50));
        }

        if (!firebase.apps?.length) {
            firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__);
        }

        try {
            await firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL);
        } catch { }

        return firebase.auth();
    };

    const getApiBaseUrl = () => {
        if (_API && typeof _API.BASE_URL !== 'undefined') {
            return _API.BASE_URL;
        }
        try {
            const { hostname, port } = window.location;
            if ((hostname === '127.0.0.1' || hostname === 'localhost') && port === '5500') {
                return 'http://localhost:3000';
            }
        } catch {}
        return window.location.origin;
    };

    let auth;
    try {
        auth = await ensureFirebaseReady();
    } catch (error) {
        toastError(error?.message || 'Authentication is unavailable. Please refresh and try again.');
        return;
    }

    auth.onAuthStateChanged(async (user) => {
        if (user && !isGoogleSignInInProgress && !isGuestSignInInProgress) {
            try {
                const idToken = await user.getIdToken();
                const apiBaseUrl = getApiBaseUrl();

                const checkResponse = await fetch(`${apiBaseUrl}/api/users/check-profile`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${idToken}`
                    }
                });

                const text = await checkResponse.text();
                let checkData = null;
                try { checkData = text ? JSON.parse(text) : null; } catch { }

                if (!checkResponse.ok) {
                    toastError('Unable to check your profile. Please try again.');
                    return;
                }

                const target = (checkData?.exists && checkData?.hasUsername) ? '/mood.html' : '/username.html';
                if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
                    _PageTransition.navigateTo(target);
                } else {
                    window.location.href = target;
                }
            } catch (error) {
                console.error('Error checking profile:', error);
            }
        }
    });

    const signupForm = document.getElementById('signupForm');
    const googleBtn = document.getElementById('googleSignIn');
    const guestBtn = document.getElementById('guestSignIn');

    if (signupForm) {
        signupForm.addEventListener('submit', async (e) => {
            e.preventDefault();

            const emailEl = document.getElementById('email');
            const passwordEl = document.getElementById('password');
            const confirmEl = document.getElementById('confirmPassword');

            const email = emailEl ? emailEl.value.trim() : '';
            const password = passwordEl ? passwordEl.value : '';
            const confirmPassword = confirmEl ? confirmEl.value : '';

            if (!_Validator || typeof _Validator.isValidEmail !== 'function') {
                toastError('Validation helper unavailable. Cannot validate input.');
                return;
            }
            if (!_Validator.isValidEmail(email)) {
                toastError('Please enter a valid email address');
                return;
            }
            if (!_Validator.isValidPassword || !_Validator.isValidPassword(password)) {
                toastError('Password must be at least 8 characters');
                return;
            }
            if (password !== confirmPassword) {
                toastError('Passwords do not match');
                return;
            }

            loadingShow('Creating your account...');

            try {
                const userCredential = await auth.createUserWithEmailAndPassword(email, password);
                const idToken = await userCredential.user.getIdToken();

                if (_Auth && typeof _Auth.setAuth === 'function') {
                    _Auth.setAuth(idToken, { email, uid: userCredential.user.uid });
                }

                loadingHide();
                toastSuccess('Account created successfully!');

                setTimeout(() => {
                    if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
                        _PageTransition.navigateTo('/username.html');
                    } else {
                        window.location.href = '/username.html';
                    }
                }, 1000);
            } catch (error) {
                loadingHide();
                console.error('Signup error:', error);
                toastError(error?.message || 'Failed to create account');
            }
        });
    }

    if (googleBtn) {
        googleBtn.addEventListener('click', async () => {
            isGoogleSignInInProgress = true;
            loadingShow('Signing in with Google...');

            try {
                const provider = new firebase.auth.GoogleAuthProvider();
                const result = await auth.signInWithPopup(provider);
                const idToken = await result.user.getIdToken();

                const apiBaseUrl = getApiBaseUrl();
                const authFetch = window.MoodApp?.authFetch;
                const checkResponse = authFetch
                    ? await authFetch(`/api/users/check-profile`, { method: 'POST' })
                    : await fetch(`${apiBaseUrl}/api/users/check-profile`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${idToken}`
                        }
                    });

                const checkData = await checkResponse.json();
                
                if (_Auth && typeof _Auth.setAuth === 'function') {
                    _Auth.setAuth(idToken, {
                        email: result.user.email,
                        uid: result.user.uid,
                        displayName: result.user.displayName,
                        photoURL: result.user.photoURL
                    });
                }

                loadingHide();
                isGoogleSignInInProgress = false;

                if (checkData.exists && checkData.hasUsername) {
                    toastSuccess('Welcome back!');
                    setTimeout(() => {
                        if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
                            _PageTransition.navigateTo('/mood.html');
                        } else {
                            window.location.href = '/mood.html';
                        }
                    }, 1000);
                } else {
                    toastSuccess('Account created! Please set up your profile');
                    setTimeout(() => {
                        if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
                            _PageTransition.navigateTo('/username.html');
                        } else {
                            window.location.href = '/username.html';
                        }
                    }, 1000);
                }
            } catch (error) {
                loadingHide();
                isGoogleSignInInProgress = false;
                console.error('Google sign in error:', error);
                toastError(error?.message || 'Failed to sign in with Google');
            }
        });
    }

    if (guestBtn) {
        guestBtn.addEventListener('click', async () => {
            isGuestSignInInProgress = true;
            loadingShow('Signing you in as a guest...');

            try {
                let user = auth.currentUser;
                if (!user) {
                    const credential = await auth.signInAnonymously();
                    user = credential?.user || null;
                }

                if (!user) throw new Error('Guest sign-in failed');

                loadingShow('Creating your guest profile...');
                const token = await user.getIdToken();

                const apiBaseUrl = getApiBaseUrl();
                const res = await fetch(`${apiBaseUrl}/api/users/ensure-guest`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${token}`
                    },
                    body: JSON.stringify({})
                });

                const text = await res.text();
                if (!res.ok) throw new Error(text || `HTTP ${res.status}`);

                let payload = null;
                try { payload = JSON.parse(text); } catch { }

                try {
                    localStorage.setItem('guest_uid', user.uid);
                    if (payload?.user?.username) localStorage.setItem('guest_username', payload.user.username);
                    localStorage.setItem('guest_timestamp', String(Date.now()));
                    localStorage.removeItem('currentRoom');
                } catch { }

                if (_Auth && typeof _Auth.setAuth === 'function') {
                    _Auth.setAuth(token, { uid: user.uid, isGuest: true });
                }

                loadingHide();
                toastSuccess('Welcome!');

                setTimeout(() => {
                    if (_PageTransition && typeof _PageTransition.navigateTo === 'function') {
                        _PageTransition.navigateTo('/mood.html');
                    } else {
                        window.location.href = '/mood.html';
                    }
                }, 700);
            } catch (error) {
                loadingHide();
                console.error('Guest sign-in error:', error);
                toastError(error?.message || 'Failed to continue as guest');
                try { await auth.signOut(); } catch { }
            } finally {
                isGuestSignInInProgress = false;
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
