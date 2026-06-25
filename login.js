const ensureFirebaseReady = async () => {
    const startedAt = Date.now();
    while (typeof window.firebase === 'undefined' || !window.firebase.auth) {
        if (Date.now() - startedAt > 10000) throw new Error('Firebase SDK did not load. Please refresh and try again.');
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

// Toast notification system
const Toast = {
    show: function(message, type = 'info', duration = 4000) {
        const container = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        
        const icons = {
            success: 'check_circle',
            error: 'error',
            info: 'info',
            warning: 'warning'
        };

        const colors = {
            success: 'from-green-500 to-emerald-500',
            error: 'from-red-500 to-rose-500',
            info: 'from-blue-500 to-cyan-500',
            warning: 'from-yellow-500 to-orange-500'
        };

        toast.className = `glass-card p-4 rounded-xl shadow-lg flex items-center gap-3 animate-slideIn min-w-[300px]`;
        toast.innerHTML = `
            <div class="flex items-center justify-center size-10 rounded-lg bg-gradient-to-br ${colors[type]} flex-shrink-0">
                <span class="material-symbols-outlined text-white text-[20px]">${icons[type]}</span>
            </div>
            <p class="text-sm font-medium text-white flex-1">${message}</p>
            <button onclick="this.parentElement.remove()" class="text-slate-400 hover:text-white transition-colors">
                <span class="material-symbols-outlined text-[20px]">close</span>
            </button>
        `;

        container.appendChild(toast);

        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            toast.style.transition = 'all 0.3s ease-out';
            setTimeout(() => toast.remove(), 300);
        }, duration);
    },
    success: function(message, duration) { this.show(message, 'success', duration); },
    error: function(message, duration) { this.show(message, 'error', duration); },
    info: function(message, duration) { this.show(message, 'info', duration); },
    warning: function(message, duration) { this.show(message, 'warning', duration); }
};

// Loading overlay system
const Loading = {
    overlay: null,
    show: function(message = 'Loading...') {
        if (!this.overlay) {
            this.overlay = document.createElement('div');
            this.overlay.className = 'fixed inset-0 bg-black/60 backdrop-blur-sm z-[70] flex items-center justify-center';
            this.overlay.innerHTML = `
                <div class="glass-card p-8 rounded-2xl flex flex-col items-center gap-4 min-w-[200px]">
                    <div class="relative">
                        <div class="size-12 border-4 border-primary/20 border-t-primary rounded-full animate-spin"></div>
                    </div>
                    <p class="text-sm font-medium text-white loading-message">${message}</p>
                </div>
            `;
        }
        document.body.appendChild(this.overlay);
    },
    hide: function() {
        if (this.overlay && this.overlay.parentNode) {
            this.overlay.remove();
        }
    }
};

// Email validator
const Validator = {
    isValidEmail: function(email) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(email);
    }
};

// Firebase error messages
const getFirebaseErrorMessage = (errorCode) => {
    const errorMessages = {
        'auth/invalid-email': 'Invalid email address format.',
        'auth/user-disabled': 'This account has been disabled.',
        'auth/user-not-found': 'No account found with this email.',
        'auth/wrong-password': 'Incorrect password.',
        'auth/invalid-credential': 'Invalid email or password.',
        'auth/too-many-requests': 'Too many failed attempts. Please try again later.',
        'auth/network-request-failed': 'Network error. Please check your connection.',
        'auth/popup-blocked': 'Popup was blocked. Please allow popups for this site.',
        'auth/popup-closed-by-user': 'Sign-in popup was closed.',
        'auth/cancelled-popup-request': 'Only one popup request is allowed at a time.',
        'auth/expired-action-code': 'The reset link has expired. Please request a new one.',
        'auth/invalid-action-code': 'The reset link is invalid. Please request a new one.',
    };
    return errorMessages[errorCode] || 'An error occurred. Please try again.';
};

// Global variables
let isGoogleSignInInProgress = false;
let isGuestSignInInProgress = false;
const _Toast = Toast;
const _Loading = Loading;
const _Validator = Validator;

// Password toggle functionality
const togglePassword = document.getElementById('togglePassword');
const passwordInput = document.getElementById('password');

if (togglePassword && passwordInput) {
    togglePassword.addEventListener('click', function() {
        const icon = this.querySelector('span');
        if (passwordInput.type === 'password') {
            passwordInput.type = 'text';
            icon.textContent = 'visibility_off';
        } else {
            passwordInput.type = 'password';
            icon.textContent = 'visibility';
        }
    });
}

// Forgot Password Modal functionality
const forgotPasswordModal = document.getElementById('forgotPasswordModal');
const forgotPasswordLink = document.getElementById('forgotPasswordLink');
const closeModal = document.getElementById('closeModal');
const cancelReset = document.getElementById('cancelReset');
const forgotPasswordForm = document.getElementById('forgotPasswordForm');

const openModal = () => {
    forgotPasswordModal.classList.add('active');
    document.body.style.overflow = 'hidden';
};

const closeModalFunc = () => {
    forgotPasswordModal.classList.remove('active');
    document.body.style.overflow = 'auto';
    document.getElementById('resetEmail').value = '';
};

forgotPasswordLink.addEventListener('click', (e) => {
    e.preventDefault();
    openModal();
});

closeModal.addEventListener('click', closeModalFunc);
cancelReset.addEventListener('click', closeModalFunc);

// Close modal when clicking outside
forgotPasswordModal.addEventListener('click', (e) => {
    if (e.target === forgotPasswordModal) {
        closeModalFunc();
    }
});

// Forgot Password Form submission
forgotPasswordForm.addEventListener('submit', async (e) => {
    e.preventDefault();

    const email = document.getElementById('resetEmail').value.trim();

    if (!Validator.isValidEmail(email)) {
        Toast.error('Please enter a valid email address');
        return;
    }

    Loading.show('Sending reset link...');

    try {
        // Send password reset email using Firebase
        const auth = await ensureFirebaseReady();
        await auth.sendPasswordResetEmail(email);
        
        Loading.hide();
        closeModalFunc();
        Toast.success('Password reset link sent! Check your email.', 5000);
    } catch (error) {
        Loading.hide();
        console.error('Password reset error:', error);
        const errorMessage = getFirebaseErrorMessage(error.code);
        Toast.error(errorMessage);
    }
});

// Main authentication logic
(async function() {
    let auth;
    try {
        auth = await ensureFirebaseReady();
    } catch (error) {
        Toast.error(error?.message || 'Authentication is unavailable. Please refresh and try again.');
        return;
    }

    // Check if user is already signed in
    auth.onAuthStateChanged(async (user) => {
        if (user && !isGoogleSignInInProgress && !isGuestSignInInProgress) {
            try {
                const idToken = await user.getIdToken();
                
                // Check if profile exists
                const apiBaseUrl = getApiBaseUrl();
                const response = await fetch(`${apiBaseUrl}/api/users/check-profile`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${idToken}`
                    }
                });

                const text = await response.text();
                let data = null;
                try { data = text ? JSON.parse(text) : null; } catch { }

                if (!response.ok) {
                    Toast.error('Unable to check your profile. Please try again.');
                    return;
                }

                if (data && data.exists && data.hasUsername) {
                    if (typeof _PageTransition !== 'undefined' && typeof _PageTransition.navigateTo === 'function') {
                        _PageTransition.navigateTo('/mood.html');
                    } else {
                        window.location.href = '/mood.html';
                    }
                } else {
                    if (typeof _PageTransition !== 'undefined' && typeof _PageTransition.navigateTo === 'function') {
                        _PageTransition.navigateTo('/username.html');
                    } else {
                        window.location.href = '/username.html';
                    }
                }
            } catch (error) {
                console.error('Error checking profile:', error);
            }
        }
    });

    const getApiBaseUrl = () => {
        if (typeof _API !== 'undefined' && typeof _API.BASE_URL !== 'undefined') {
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

    // Login form submission
    const loginForm = document.getElementById('loginForm');
    if (loginForm) {
        loginForm.addEventListener('submit', async (e) => {
            e.preventDefault();

            const email = document.getElementById('email').value.trim();
            const password = document.getElementById('password').value;

            if (!Validator.isValidEmail(email)) {
                Toast.error('Please enter a valid email address');
                return;
            }

            if (!password) {
                Toast.error('Please enter your password');
                return;
            }

            Loading.show('Signing in...');

            try {
                const userCredential = await auth.signInWithEmailAndPassword(email, password);
                const idToken = await userCredential.user.getIdToken();

                const apiBaseUrl = getApiBaseUrl();
                const checkRes = await fetch(`${apiBaseUrl}/api/users/check-profile`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${idToken}`
                    }
                });

                const checkText = await checkRes.text();
                let checkData = null;
                try { checkData = checkText ? JSON.parse(checkText) : null; } catch { }

                if (!checkRes.ok || (checkData && (checkData.error === 'User not found' || checkData.message === 'User not found'))) {
                    Loading.hide();
                    Toast.error('Account not found. Please sign up or continue as guest.');
                    return;
                }

                // Set auth if function exists
                if (typeof _Auth !== 'undefined' && typeof _Auth.setAuth === 'function') {
                    _Auth.setAuth(idToken, { email, uid: userCredential.user.uid });
                }

                Loading.hide();
                Toast.success('Welcome back!');

                // Navigate based on profile state
                setTimeout(() => {
                    const hasProfile = !!(checkData && checkData.exists);
                    const hasUsername = !!(checkData && checkData.hasUsername);
                    const target = (hasProfile && hasUsername) ? '/mood.html' : '/username.html';

                    if (typeof _PageTransition !== 'undefined' && typeof _PageTransition.navigateTo === 'function') {
                        _PageTransition.navigateTo(target);
                    } else {
                        window.location.href = target;
                    }
                }, 900);
            } catch (error) {
                Loading.hide();
                console.error('Login error:', error);
                const errorMessage = getFirebaseErrorMessage(error.code);
                Toast.error(errorMessage);
            }
        });
    }

    // Google Sign In
    const googleBtn = document.getElementById('googleSignIn');
    if (googleBtn) {
        googleBtn.addEventListener('click', async () => {
            isGoogleSignInInProgress = true;
            Loading.show('Signing in with Google...');

            try {
                const provider = new firebase.auth.GoogleAuthProvider();
                const result = await auth.signInWithPopup(provider);
                const idToken = await result.user.getIdToken();

                const apiBaseUrl = getApiBaseUrl();
                const checkResponse = await fetch(`${apiBaseUrl}/api/users/check-profile`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${idToken}`
                    }
                });

                const checkData = await checkResponse.json();

                if (typeof _Auth !== 'undefined' && typeof _Auth.setAuth === 'function') {
                    _Auth.setAuth(idToken, {
                        email: result.user.email,
                        uid: result.user.uid,
                        displayName: result.user.displayName,
                        photoURL: result.user.photoURL
                    });
                }

                Loading.hide();
                isGoogleSignInInProgress = false;

                if (checkData.exists && checkData.hasUsername) {
                    Toast.success('Welcome back!');
                    setTimeout(() => {
                        if (typeof _PageTransition !== 'undefined' && typeof _PageTransition.navigateTo === 'function') {
                            _PageTransition.navigateTo('/mood.html');
                        } else {
                            window.location.href = '/mood.html';
                        }
                    }, 1000);
                } else {
                    Toast.success('Please complete your profile setup');
                    setTimeout(() => {
                        if (typeof _PageTransition !== 'undefined' && typeof _PageTransition.navigateTo === 'function') {
                            _PageTransition.navigateTo('/username.html');
                        } else {
                            window.location.href = '/username.html';
                        }
                    }, 1000);
                }
            } catch (error) {
                Loading.hide();
                isGoogleSignInInProgress = false;
                console.error('Google sign in error:', error);
                const errorMessage = getFirebaseErrorMessage(error.code);
                Toast.error(errorMessage);
            }
        });
    }

    // Guest Sign In
    const guestBtn = document.getElementById('guestSignIn');
    if (guestBtn) {
        guestBtn.addEventListener('click', async () => {
            isGuestSignInInProgress = true;
            Loading.show('Signing you in as a guest...');

            try {
                let user = auth.currentUser;
                if (!user) {
                    const credential = await auth.signInAnonymously();
                    user = credential?.user || null;
                }

                if (!user) throw new Error('Guest sign-in failed');

                Loading.show('Creating your guest profile...');
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

                if (typeof _Auth !== 'undefined' && typeof _Auth.setAuth === 'function') {
                    _Auth.setAuth(token, { uid: user.uid, isGuest: true });
                }

                Loading.hide();
                Toast.success('Welcome!');

                setTimeout(() => {
                    if (typeof _PageTransition !== 'undefined' && typeof _PageTransition.navigateTo === 'function') {
                        _PageTransition.navigateTo('/mood.html');
                    } else {
                        window.location.href = '/mood.html';
                    }
                }, 700);
            } catch (error) {
                Loading.hide();
                console.error('Guest sign-in error:', error);
                const msg = (error && error.message) ? String(error.message) : 'Failed to continue as guest';
                Toast.error(msg);
                try { await auth.signOut(); } catch { }
            } finally {
                isGuestSignInInProgress = false;
            }
        });
    }
})();

(async function() {
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
