export function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

export function setMeta(node, text, isError = false) {
  if (!node) return;
  node.textContent = text || "";
  node.classList.toggle("status-error", Boolean(isError));
}

export function formatDuration(sec) {
  const v = Number(sec || 0);
  if (!Number.isFinite(v) || v <= 0) return "n/a";
  const h = Math.floor(v / 3600);
  const m = Math.floor((v % 3600) / 60);
  const s = Math.floor(v % 60);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  return `${m}m ${s}s`;
}

export function toInt(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}
