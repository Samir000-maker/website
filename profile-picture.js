(async () => {
    try {
        if (!firebase.apps?.length) {
            firebase.initializeApp(window.__VIBE_FIREBASE_CONFIG__);
        }
    } catch (error) {
        console.error('Firebase init error:', error);
    }

    const { Auth, API, Toast, Loading, PageTransition } = window.MoodApp;

    const toastError = (msg) => {
        try {
            if (Toast && typeof Toast.error === 'function') return Toast.error(msg);
        } catch { }
        console.error(msg);
    };
    const toastSuccess = (msg) => {
        try {
            if (Toast && typeof Toast.success === 'function') return Toast.success(msg);
        } catch { }
        console.log(msg);
    };

    const ensureLoadingPopup = () => {
        let el = document.getElementById('profileUploadLoadingPopup');
        if (el) return el;
        el = document.createElement('div');
        el.id = 'profileUploadLoadingPopup';
        el.className = 'fixed inset-0 z-[9999] hidden items-center justify-center';
        el.innerHTML = `
          <div class="absolute inset-0 bg-black/60 backdrop-blur-sm"></div>
          <div class="relative w-[92vw] max-w-sm rounded-2xl bg-[#0f1115]/95 border border-white/10 shadow-2xl p-6">
            <div class="flex items-center gap-3">
              <div class="w-10 h-10 rounded-full border-2 border-primary/30 border-t-primary" style="animation: spin 1s linear infinite;"></div>
              <div>
                <div class="text-base font-bold text-white">Uploading…</div>
                <div id="profileUploadLoadingText" class="mt-1 text-sm text-gray-400">Uploading your picture…</div>
              </div>
            </div>
          </div>
        `;
        document.body.appendChild(el);
        return el;
    };

    const loadingShow = (msg) => {
        try {
            if (Loading && typeof Loading.show === 'function') Loading.show(msg || 'Uploading...');
        } catch { }
        const el = ensureLoadingPopup();
        const t = el.querySelector('#profileUploadLoadingText');
        if (t && msg) t.textContent = msg;
        el.classList.remove('hidden');
        el.classList.add('flex');
    };

    const loadingHide = () => {
        try {
            if (Loading && typeof Loading.hide === 'function') Loading.hide();
        } catch { }
        const el = document.getElementById('profileUploadLoadingPopup');
        if (!el) return;
        el.classList.add('hidden');
        el.classList.remove('flex');
    };

    try {
        await Auth.requireAuth();
    } catch (err) {
        console.error('Auth required failed:', err);
        return;
    }

    const fileInput = document.getElementById('fileInput');
    const preview = document.getElementById('preview');
    const previewContainer = document.getElementById('previewContainer');
    const saveBtn = document.getElementById('saveBtn');
    const skipBtn = document.getElementById('skipBtn');

    let selectedFile = null;

    fileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (!file) return;

        if (file.size > 5 * 1024 * 1024) {
            toastError('File size must be under 5MB');
            return;
        }

        if (!file.type.startsWith('image/')) {
            toastError('Please select an image file');
            return;
        }

        selectedFile = file;

        const reader = new FileReader();
        reader.onload = (event) => {
            preview.src = event.target.result;
            preview.classList.remove('hidden');
            previewContainer
                .querySelector('.material-symbols-outlined')
                .classList.add('hidden');
            saveBtn.disabled = false;
        };
        reader.readAsDataURL(file);
    });

    saveBtn.addEventListener('click', async () => {
        if (!selectedFile) {
            toastError('Please select a file first');
            return;
        }

        loadingShow('Uploading your picture...');

        try {
            console.log('Uploading file:', selectedFile.name, selectedFile.type, selectedFile.size);
            
            const result = await API.uploadFile('/api/users/upload-pfp', selectedFile);
            
            console.log('Upload result:', result);
            
            loadingHide();
            toastSuccess('Profile picture saved!');

            setTimeout(() => {
                PageTransition.navigateTo('/mood.html');
            }, 1000);

        } catch (error) {
            loadingHide();
            console.error('Upload error:', error);

            let msg = 'Failed to upload picture. Please try again.';
            try {
                if (error && typeof error.message === 'string' && error.message.trim()) {
                    const maybeJson = error.message.trim();
                    if (maybeJson.startsWith('{') && maybeJson.endsWith('}')) {
                        const parsed = JSON.parse(maybeJson);
                        if (parsed && parsed.error) msg = parsed.error;
                        else msg = error.message;
                    } else {
                        msg = error.message;
                    }
                }
            } catch { }

            toastError(msg);
        }
    });

    skipBtn.addEventListener('click', () => {
        PageTransition.navigateTo('/mood.html');
    });
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
