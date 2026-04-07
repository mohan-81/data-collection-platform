/**
 * ConnectorManager
 * Centralizes all UI logic for connector status, authorization, and actions.
 */
class ConnectorManager {
    constructor(source, options = {}) {
        this.source = source;
        this.fieldIds = options.fieldIds || [];
        this.onStatusUpdate = options.onStatusUpdate || null;
        
        // Element IDs used across templates
        this.ids = {
            badge: 'statusBadge',
            connectedAs: 'connectedAs',
            syncContent: 'syncContent',
            accessOverlay: 'accessOverlay',
            credsBtn: 'saveCredsBtn',
            credsForm: 'credentialsForm',
            successState: 'successState',
            disconnectBtn: 'disconnectBtn',
            syncBtn: 'syncBtn',
            recoverBtn: 'recoverBtn',
            step1: 'step1-circle',
            step2: 'step2-circle',
            step3: 'step3-circle'
        };
    }

    init() {
        this.refreshStatus();
        console.log(`ConnectorManager initialized for [${this.source}]`);
    }

    async refreshStatus() {
        try {
            const res = await fetch(`/api/status/${this.source}`);
            const data = await res.json();
            this.updateUI(data);
            if (this.onStatusUpdate) this.onStatusUpdate(data);
        } catch (e) {
            console.error(`[${this.source}] Status check failed:`, e);
        }
    }

    updateUI(data) {
        const badge = document.getElementById(this.ids.badge);
        const connectedAs = document.getElementById(this.ids.connectedAs);
        const syncContent = document.getElementById(this.ids.syncContent);
        const accessOverlay = document.getElementById(this.ids.accessOverlay);
        const credsForm = document.getElementById(this.ids.credsForm);
        const successState = document.getElementById(this.ids.successState);

        if (data.connected) {
            this.setBadge('CONNECTED', 'emerald');
            if (data.email) {
                if (connectedAs) connectedAs.innerHTML = `Connected as <strong>${data.email}</strong>`;
            }
            this.updateFlow(3);
            if (syncContent) syncContent.classList.remove("opacity-65", "blur-sm", "pointer-events-none");
            if (accessOverlay) accessOverlay.classList.add("hidden");
            if (credsForm) credsForm.classList.add("hidden");
            if (successState) {
                successState.classList.remove("hidden");
                successState.classList.add("opacity-100");
            }
        } else if (data.has_credentials) {
            this.setBadge('CREDENTIALS SAVED', 'yellow');
            this.updateFlow(1);
            if (syncContent) syncContent.classList.add("opacity-65", "blur-sm", "pointer-events-none");
            if (accessOverlay) accessOverlay.classList.remove("hidden");
            if (credsForm) credsForm.classList.remove("hidden");
            if (successState) successState.classList.add("hidden");
        } else {
            this.setBadge('NOT CONNECTED', 'slate');
            this.updateFlow(0);
            if (syncContent) syncContent.classList.add("opacity-65", "blur-sm", "pointer-events-none");
            if (accessOverlay) accessOverlay.classList.remove("hidden");
            if (credsForm) credsForm.classList.remove("hidden");
            if (successState) successState.classList.add("hidden");
        }
    }

    setBadge(text, color) {
        const badge = document.getElementById(this.ids.badge);
        if (!badge) return;

        const colors = {
            emerald: {
                dot: 'bg-emerald-500 animate-pulse',
                bg: 'border-emerald-500/30 bg-emerald-500/5 text-emerald-400'
            },
            yellow: {
                dot: 'bg-yellow-500',
                bg: 'border-yellow-500/30 bg-yellow-500/5 text-yellow-400'
            },
            slate: {
                dot: 'bg-slate-600',
                bg: 'border-slate-300 dark:border-slate-700 bg-slate-100 dark:bg-slate-800/50 text-slate-500'
            }
        };

        const config = colors[color] || colors.slate;
        badge.innerHTML = `<span class="h-2 w-2 rounded-full ${config.dot}"></span> ${text}`;
        badge.className = `inline-flex items-center gap-2 px-3 py-1 rounded-full border ${config.bg} text-[10px] font-mono mb-8 uppercase tracking-widest`;
    }

    updateFlow(step) {
        const s1 = document.getElementById(this.ids.step1);
        const s2 = document.getElementById(this.ids.step2);
        const s3 = document.getElementById(this.ids.step3);
        const btn = document.getElementById(this.ids.credsBtn);

        const activeClass = "bg-emerald-500 text-slate-900 dark:text-white";
        const pendingClass = "bg-blue-300 text-slate-900";
        const inactiveClass = "bg-slate-700 text-slate-500";

        if (step >= 1) {
            if (s1) { s1.className = this.getStepClass(true); s1.innerHTML = "✓"; }
            if (s2) s2.className = this.getStepClass(false, true);
            if (btn) {
                btn.innerText = "Authorize & Link Account";
                btn.onclick = () => window.location.href = `/connectors/${this.source}/connect`;
            }
        } else {
            if (s1) { s1.className = this.getStepClass(false, true); s1.innerHTML = "1"; }
            if (s2) { s2.className = this.getStepClass(false, false); s2.innerHTML = "2"; }
            if (btn) {
                btn.innerText = "Save & Connect";
                btn.onclick = () => this.saveCredentials();
            }
        }

        if (step >= 3) {
            if (s2) { s2.className = this.getStepClass(true); s2.innerHTML = "✓"; }
            if (s3) { s3.className = this.getStepClass(true); s3.innerHTML = "✓"; }
        } else {
            if (s3) { s3.className = this.getStepClass(false, false); s3.innerHTML = "3"; }
        }
    }

    getStepClass(active, current = false) {
        const base = "w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold transition-all duration-300 ";
        if (active) return base + "bg-emerald-500 text-slate-900 dark:text-white";
        if (current) return base + "bg-blue-300 text-slate-900";
        return base + "bg-slate-700 text-slate-500";
    }

    async saveCredentials() {
        const payload = {};
        for (const id of this.fieldIds) {
            const el = document.getElementById(id);
            if (!el) continue;
            // Map common field names to backend expectations
            let key = id;
            if (id === 'access_token') key = 'access_token';
            if (id === 'clientId') key = 'client_id';
            if (id === 'clientSecret') key = 'client_secret';
            if (id === 'apiKey') key = 'api_key';
            
            payload[key] = el.value.trim();
        }

        if (Object.values(payload).some(v => v === "")) {
            alert("All credentials fields are required.");
            return;
        }

        try {
            const res = await fetch(`/connectors/${this.source}/save_app`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            if (res.ok) {
                // Success: trigger status update
                await this.refreshStatus();
            } else {
                const data = await res.json();
                alert(`Save failed: ${data.message || 'Unknown error'}`);
            }
        } catch (e) {
            alert(`Network error saving credentials: ${e.message}`);
        }
    }

    async disconnect() {
        if (!confirm(`Disconnecting will stop all future ${this.source} syncs.\n\nContinue?`)) return;
        
        const btn = document.getElementById(this.ids.disconnectBtn);
        const originalText = btn ? btn.innerText : "Disconnect";
        if (btn) { btn.disabled = true; btn.innerText = "Disconnecting..."; }

        try {
            const res = await fetch(`/connectors/${this.source}/disconnect`);
            if (res.ok) {
                alert("Connection revoked.");
                await this.refreshStatus();
            } else {
                alert("Failed to disconnect.");
                if (btn) { btn.disabled = false; btn.innerText = originalText; }
            }
        } catch (e) {
            alert("Disconnect error.");
            if (btn) { btn.disabled = false; btn.innerText = originalText; }
        }
    }

    async runSync() {
        const btn = document.getElementById(this.ids.syncBtn);
        if (!btn) return;
        const originalText = btn.innerText;
        btn.innerText = "SYNCING...";
        btn.disabled = true;

        try {
            const res = await fetch(`/connectors/${this.source}/sync`);
            const d = await res.json();
            btn.innerText = `SYNCED (${d.messages || 0} MSGS)`;
        } catch (e) {
            btn.innerText = "FAILED";
        } finally {
            setTimeout(() => {
                btn.innerText = originalText;
                btn.disabled = false;
            }, 3000);
        }
    }

    async recoverSync() {
        const btn = document.getElementById(this.ids.recoverBtn);
        if (!btn) return;
        const orgHtml = btn.innerHTML;
        btn.innerHTML = "Recovering...";
        btn.disabled = true;

        try {
            const res = await fetch(`/connectors/${this.source}/recover`, {
                method: "POST"
            });
            if (res.ok) {
                alert("Recovery triggered.");
            } else {
                alert("Recovery failed.");
            }
        } catch (e) {
            alert("Recovery error.");
        } finally {
            btn.innerHTML = orgHtml;
            btn.disabled = false;
        }
    }
}
