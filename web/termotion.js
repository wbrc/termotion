// termotion.js — WebSocket-driven progress renderer.
//
// Import it as an ES module and it will auto-activate any element carrying the
// `termotion` class together with a `data-termotion-url` attribute, e.g.
//
//   <div class="termotion" data-termotion-url="/termotion/ws"></div>
//   <script type="module" src="/termotion/termotion.js"></script>
//
// You can also drive it manually:
//
//   import { mount } from "/termotion/termotion.js";
//   const view = mount(document.querySelector("#progress"), "/termotion/ws");
//   // later: view.destroy();
//
// Options (third argument to mount, or data attributes for auto-init):
//   hiddenSummary / data-termotion-hidden-summary — draw hidden tasks as one
//   "✓ N tasks done" line per parent instead of dropping them silently.

const SPINNER = [
	"\u28fe", // ⣾
	"\u28fd", // ⣽
	"\u28fb", // ⣻
	"\u28bf", // ⢿
	"\u287f", // ⡿
	"\u28df", // ⣟
	"\u28ef", // ⣯
	"\u28f7"  // ⣷
];
const SYMBOL = {
	running: "\u25cf", // ●
	done: "\u2713", // ✓
	error: "\u26cc", // ⛌
	canceled: "\u23f9", // ⏹
};
const CONNECTOR_LAST = "\u2514\u2500\u2500 "; // └──
const CONNECTOR_MID = "\u251c\u2500\u2500 "; // ├──
const VERTICAL = "\u2502"; // │
const MAX_MSG_LINES = 5;

function resolveUrl(u) {
	if (/^wss?:\/\//.test(u)) return u;
	const proto = location.protocol === "https:" ? "wss:" : "ws:";
	if (u.startsWith("/")) return `${proto}//${location.host}${u}`;
	return `${proto}//${location.host}/${u}`;
}

function parseTime(s) {
	if (!s) return null;
	const d = new Date(s);
	return isNaN(d.getTime()) ? null : d;
}

// Canonical Duration Display: mirrored by formatDuration in duration.go — keep the two in step.
function fmtDuration(ms) {
	if (!isFinite(ms) || ms < 0) ms = 0;
	// Truncate to 100ms so the timer ticks like the terminal renderer.
	ms = Math.floor(ms / 100) * 100;
	const s = ms / 1000;
	if (s < 60) return `${s.toFixed(1)}s`;
	const sec = Math.floor(s % 60);
	const min = Math.floor(s / 60) % 60;
	const hrs = Math.floor(s / 3600);
	const pad = (n) => n.toString().padStart(2, "0");
	if (hrs === 0) return `${min}m${pad(sec)}s`;
	return `${hrs}h${pad(min)}m${pad(sec)}s`;
}

// Canonical Rate Display: mirrored by formatRate in rate.go — keep the two in
// step. The unit is producer-owned and reproduced verbatim; the magnitude is
// never rescaled.
function fmtRate(v, unit) {
	return `${fmtRateValue(v)} ${unit}`;
}

// Three significant figures, trailing zeros trimmed, never exponent notation.
function fmtRateValue(v) {
	if (typeof v !== "number" || !isFinite(v)) return "0";
	let sign = "";
	if (v < 0) {
		sign = "-";
		v = -v;
	}
	if (v === 0) return "0";
	const exp = Math.floor(Math.log10(v));
	if (exp >= 3) {
		const scale = Math.pow(10, exp - 2);
		return sign + (Math.round(v / scale) * scale).toFixed(0);
	}
	// Capped so toFixed stays within its legal range for tiny values.
	const dec = Math.min(Math.max(0, 2 - exp), 20);
	let s = v.toFixed(dec);
	if (s.includes(".")) s = s.replace(/0+$/, "").replace(/\.$/, "");
	return sign + s;
}

function esc(str) {
	return String(str)
		.replaceAll("&", "&amp;")
		.replaceAll("<", "&lt;")
		.replaceAll(">", "&gt;")
		.replaceAll('"', "&quot;")
		.replaceAll("'", "&#39;");
}

function newTask(m) {
	return {
		id: m.id,
		parent: m.parent || 0,
		title: m.title || "",
		startedAt: parseTime(m.startedAt),
		stoppedAt: parseTime(m.stoppedAt),
		finishAt: parseTime(m.finishAt),
		done: !!m.done,
		canceled: !!m.canceled,
		err: m.err || "",
		percent: typeof m.percent === "number" ? m.percent : 0,
		rate: typeof m.rate === "number" ? m.rate : 0,
		rateUnit: m.rateUnit || "",
		displaySpinner: !!m.displaySpinner,
		displayProgress: !!m.displayProgress,
		// Elapsed is shown by default, so an absent field means shown, not hidden.
		displayElapsed: m.displayElapsed !== false,
		hideWhenDone: !!m.hideWhenDone,
		messages: Array.isArray(m.messages) ? m.messages : [],
	};
}

class TermotionView {
	constructor(el, url, opts = {}) {
		this.el = el;
		this.url = resolveUrl(url);
		this.hiddenSummary = !!opts.hiddenSummary;
		this.el.classList.add("termotion");
		this.reset();
		this.closed = false;
		this.spinnerFrame = 0;
		this.connect();
		this.timer = setInterval(() => this.render(), 100);
	}

	reset() {
		this.state = {
			title: "",
			startedAt: null,
			stoppedAt: null,
			tasks: new Map(),
			order: [],
		};
	}

	connect() {
		if (this.closed) return;
		this.ws = new WebSocket(this.url);
		this.ws.onmessage = (e) => {
			try {
				this.onMessage(JSON.parse(e.data));
			} catch (_) {
				/* ignore malformed frames */
			}
		};
		this.ws.onclose = () => {
			if (this.closed) return;
			this.reconnectTimer = setTimeout(() => this.connect(), 1000);
		};
		this.ws.onerror = () => this.ws && this.ws.close();
	}

	onMessage(m) {
		switch (m.kind) {
			case "snapshot":
				this.applySnapshot(m);
				break;
			case "title":
				this.state.title = m.title || "";
				break;
			case "taskStart":
				this.onTaskStart(m);
				break;
			case "taskStop":
				this.onTaskStop(m);
				break;
			case "taskProgress": {
				const t = this.state.tasks.get(m.id);
				if (t && typeof m.percent === "number") t.percent = m.percent;
				break;
			}
			case "taskEta": {
				const t = this.state.tasks.get(m.id);
				if (t) t.finishAt = parseTime(m.finishAt);
				break;
			}
			case "taskRate": {
				const t = this.state.tasks.get(m.id);
				if (t) {
					t.rate = typeof m.rate === "number" ? m.rate : 0;
					t.rateUnit = typeof m.rateUnit === "string" ? m.rateUnit : "";
				}
				break;
			}
			case "taskDisplay":
				this.onTaskDisplay(m);
				break;
			case "taskMessage": {
				const t = this.state.tasks.get(m.id);
				if (t && Array.isArray(m.messages)) t.messages = m.messages;
				break;
			}
		}
		this.render();
	}

	applySnapshot(m) {
		this.reset();
		this.state.title = m.title || "";
		this.state.startedAt = parseTime(m.startedAt);
		this.state.stoppedAt = parseTime(m.stoppedAt);
		for (const wt of m.tasks || []) {
			const t = newTask(wt);
			this.state.tasks.set(t.id, t);
			this.state.order.push(t.id);
		}
	}

	onTaskStart(m) {
		if (m.id === 0) {
			this.state.startedAt = parseTime(m.startedAt);
			return;
		}
		if (this.state.tasks.has(m.id)) return;
		const t = newTask(m);
		this.state.tasks.set(t.id, t);
		this.state.order.push(t.id);
	}

	onTaskStop(m) {
		if (m.id === 0) {
			this.state.stoppedAt = parseTime(m.stoppedAt);
			return;
		}
		const t = this.state.tasks.get(m.id);
		if (!t) return;
		t.done = true;
		t.stoppedAt = parseTime(m.stoppedAt);
		t.canceled = !!m.canceled;
		t.err = m.err || "";
	}

	onTaskDisplay(m) {
		const t = this.state.tasks.get(m.id);
		if (!t) return;
		if (typeof m.title === "string") t.title = m.title;
		if (typeof m.displaySpinner === "boolean") t.displaySpinner = m.displaySpinner;
		if (typeof m.displayProgress === "boolean") t.displayProgress = m.displayProgress;
		if (typeof m.displayElapsed === "boolean") t.displayElapsed = m.displayElapsed;
		if (typeof m.hideWhenDone === "boolean") t.hideWhenDone = m.hideWhenDone;
	}

	elapsed(startedAt, stoppedAt) {
		if (!startedAt) return 0;
		const end = stoppedAt ? stoppedAt.getTime() : Date.now();
		return end - startedAt.getTime();
	}

	// remaining is the ETA countdown for a task: finishAt - now, or null when no
	// ETA should be shown (no finishAt, already past, or the task is done).
	remaining(t) {
		if (t.done || !t.finishAt) return null;
		const ms = t.finishAt.getTime() - Date.now();
		if (ms <= 0) return null;
		return ms;
	}

	childrenOf(id) {
		const visible = [];
		let hidden = 0;
		for (const tid of this.state.order) {
			const t = this.state.tasks.get(tid);
			if (!t || t.parent !== id) continue;
			// Hidden only after a successful stop; failed and canceled tasks stay.
			if (t.hideWhenDone && t.done && !t.err && !t.canceled) {
				hidden++;
				continue;
			}
			visible.push(t);
		}
		return { visible, hidden: this.hiddenSummary ? hidden : 0 };
	}

	renderHiddenSummary(n, prefix, isLast) {
		const connector = isLast ? CONNECTOR_LAST : CONNECTOR_MID;
		return (
			`<div class="tm-row">` +
			`<span class="tm-tree">${esc(prefix)}${connector}</span>` +
			`<span class="tm-status tm-done">${SYMBOL.done}</span> ` +
			`<span class="tm-done">${n} ${n === 1 ? "task" : "tasks"} done</span>` +
			`</div>`
		);
	}

	renderChildren(id, prefix) {
		const { visible, hidden } = this.childrenOf(id);
		let html = "";
		if (hidden) html += this.renderHiddenSummary(hidden, prefix, visible.length === 0);
		visible.forEach((c, i) => {
			html += this.renderTask(c, prefix, i === visible.length - 1);
		});
		return html;
	}

	spinner() {
		return SPINNER[Math.floor(Date.now() / 100) % SPINNER.length];
	}

	renderTask(t, prefix, isLast) {
		const connector = isLast ? CONNECTOR_LAST : CONNECTOR_MID;
		let statusClass, statusContent, nameClass;
		if (t.err) {
			statusClass = "error";
			statusContent = SYMBOL.error;
			nameClass = "error";
		} else if (t.canceled) {
			statusClass = "canceled";
			statusContent = SYMBOL.canceled;
			nameClass = "canceled";
		} else if (t.done) {
			statusClass = "done";
			statusContent = SYMBOL.done;
			nameClass = "done";
		} else {
			statusClass = "running";
			statusContent = t.displaySpinner ? this.spinner() : SYMBOL.running;
			nameClass = "name";
		}

		let bar = "";
		if (t.displayProgress && !t.done) {
			const pct = Math.max(0, Math.min(1, t.percent)) * 100;
			bar =
				` <span class="tm-bar"><span class="tm-bar-fill" style="width:${pct.toFixed(1)}%"></span></span>`;
		}

		// A rate is the producer's last declared sample, not a derivation: shown
		// while the unit is set and the task runs, never interpolated or aged out.
		const rate =
			!t.done && t.rateUnit
				? ` <span class="tm-rate">${esc(fmtRate(t.rate, t.rateUnit))}</span>`
				: "";

		const rem = this.remaining(t);
		const eta = rem === null ? "" : ` <span class="tm-time">ETA ${fmtDuration(rem)}</span>`;

		const time = t.displayElapsed
			? ` <span class="tm-time">(${fmtDuration(this.elapsed(t.startedAt, t.stoppedAt))})</span>`
			: "";

		let trailer = "";
		if (t.err) trailer = ` <span class="tm-error">${esc(t.err)}</span>`;
		else if (t.canceled) trailer = ` <span class="tm-canceled">canceled</span>`;

		let html =
			`<div class="tm-row">` +
			`<span class="tm-tree">${esc(prefix)}${connector}</span>` +
			`<span class="tm-status tm-${statusClass}">${statusContent}</span> ` +
			`<span class="tm-${nameClass}">${esc(t.title)}</span>` +
			bar +
			rate +
			eta +
			time +
			trailer +
			`</div>`;

		const { visible, hidden } = this.childrenOf(t.id);
		const childPrefix = prefix + (isLast ? "    " : `${VERTICAL}   `);

		// Show recent log lines while running or on error, matching the
		// terminal renderer.
		if (!t.done || t.err) {
			const msgPrefix = childPrefix + (visible.length || hidden ? VERTICAL : " ") + " ";
			let shown = 0;
			for (const line of t.messages) {
				if (shown >= MAX_MSG_LINES) break;
				html +=
					`<div class="tm-row"><span class="tm-tree">${esc(msgPrefix)}</span>` +
					`<span class="tm-msg">${esc(line)}</span></div>`;
				shown++;
			}
		}

		html += this.renderChildren(t.id, childPrefix);

		return html;
	}

	render() {
		let html = "";
		if (this.state.title) {
			const time = `<span class="tm-time">(${fmtDuration(
				this.elapsed(this.state.startedAt, this.state.stoppedAt),
			)})</span>`;
			html += `<div class="tm-row"><span class="tm-title">${esc(this.state.title)}</span> ${time}</div>`;
		}
		html += this.renderChildren(0, "");
		this.el.innerHTML = html;
	}

	destroy() {
		this.closed = true;
		clearInterval(this.timer);
		clearTimeout(this.reconnectTimer);
		if (this.ws) this.ws.close();
	}
}

/**
 * Mount a progress view onto an element, connecting to the given WebSocket URL.
 * Returns the view instance (call destroy() to tear it down).
 */
export function mount(el, url, opts = {}) {
	if (el.__termotion) return el.__termotion;
	const view = new TermotionView(el, url, opts);
	el.__termotion = view;
	return view;
}

function autoInit() {
	document.querySelectorAll(".termotion[data-termotion-url]").forEach((el) => {
		mount(el, el.getAttribute("data-termotion-url"), {
			hiddenSummary: el.hasAttribute("data-termotion-hidden-summary"),
		});
	});
}

if (typeof document !== "undefined") {
	if (document.readyState !== "loading") autoInit();
	else document.addEventListener("DOMContentLoaded", autoInit);
}

export default { mount };
