// Backward-compatible loader: old pages that still reference /app.js
// will load the new module-based UI entrypoint.
(async () => {
  try {
    await import("/js/main.js");
  } catch (err) {
    console.error("Failed to load module UI", err);
  }
})();
