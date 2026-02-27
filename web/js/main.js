import { apiGet, apiPost } from "./api.js";
import { state } from "./state.js";
import { escapeHtml, formatDuration, setMeta, toInt } from "./utils.js";

const el = {
  tabsNav: document.getElementById("tabsNav"),
  langSelect: document.getElementById("langSelect"),
  themeToggle: document.getElementById("themeToggle"),
  themeToggleLabel: document.getElementById("themeToggleLabel"),
  refreshBtn: document.getElementById("refreshBtn"),
  ingestForm: document.getElementById("ingestForm"),
  ingestBtn: document.getElementById("ingestBtn"),
  urlInput: document.getElementById("urlInput"),
  ingestMeta: document.getElementById("ingestMeta"),
  askForm: document.getElementById("askForm"),
  askBtn: document.getElementById("askBtn"),
  askModeSelect: document.getElementById("askModeSelect"),
  questionInput: document.getElementById("questionInput"),
  qaMeta: document.getElementById("qaMeta"),
  qaOutput: document.getElementById("qaOutput"),
  askSection: document.getElementById("askSection"),
  notesSearchInput: document.getElementById("notesSearchInput"),
  notesVideoList: document.getElementById("notesVideoList"),
  analyzeBtn: document.getElementById("analyzeBtn"),
  analyzeModeSelect: document.getElementById("analyzeModeSelect"),
  analyzeSection: document.getElementById("analyzeSection"),
  analysisMeta: document.getElementById("analysisMeta"),
  analysisOutput: document.getElementById("analysisOutput"),
  videoTitle: document.getElementById("videoTitle"),
  videoFacts: document.getElementById("videoFacts"),
  videoLink: document.getElementById("videoLink"),
  videoPreview: document.getElementById("videoPreview"),
  transcriptOutput: document.getElementById("transcriptOutput"),
  vaultSearchInput: document.getElementById("vaultSearchInput"),
  archiveSearchInput: document.getElementById("archiveSearchInput"),
  videoList: document.getElementById("videoList"),
  activeLiveList: document.getElementById("activeLiveList"),
  savedLiveList: document.getElementById("savedLiveList"),
  liveStartForm: document.getElementById("liveStartForm"),
  liveStartBtn: document.getElementById("liveStartBtn"),
  liveUrlInput: document.getElementById("liveUrlInput"),
  liveStartMeta: document.getElementById("liveStartMeta"),
  activeLiveBlock: document.getElementById("activeLiveBlock"),
  directRetentionMeta: document.getElementById("directRetentionMeta"),
  liveRetentionMeta: document.getElementById("liveRetentionMeta"),
  researchSearchInput: document.getElementById("researchSearchInput"),
  researchList: document.getElementById("researchList"),
  researchTitle: document.getElementById("researchTitle"),
  researchMeta: document.getElementById("researchMeta"),
  researchOutput: document.getElementById("researchOutput"),
  directForm: document.getElementById("directForm"),
  directUrlInput: document.getElementById("directUrlInput"),
  directPrepareBtn: document.getElementById("directPrepareBtn"),
  directMeta: document.getElementById("directMeta"),
  directPreview: document.getElementById("directPreview"),
  directOutput: document.getElementById("directOutput"),
  directRecentList: document.getElementById("directRecentList"),
  juiceForm: document.getElementById("juiceForm"),
  juiceRunBtn: document.getElementById("juiceRunBtn"),
  juiceTopicInput: document.getElementById("juiceTopicInput"),
  juiceFiltersToggle: document.getElementById("juiceFiltersToggle"),
  juiceFiltersPanel: document.getElementById("juiceFiltersPanel"),
  juiceMaxVideos: document.getElementById("juiceMaxVideos"),
  juiceMaxQueries: document.getElementById("juiceMaxQueries"),
  juicePerQuery: document.getElementById("juicePerQuery"),
  juiceMinDuration: document.getElementById("juiceMinDuration"),
  juiceMaxDuration: document.getElementById("juiceMaxDuration"),
  juiceFast: document.getElementById("juiceFast"),
  juicePrivate: document.getElementById("juicePrivate"),
  juiceMeta: document.getElementById("juiceMeta"),
  wsState: document.getElementById("wsState"),
  activeBrewList: document.getElementById("activeBrewList"),
  brewMeta: document.getElementById("brewMeta"),
  brewProgressBar: document.getElementById("brewProgressBar"),
  brewConfigMeta: document.getElementById("brewConfigMeta"),
  currentReviewWrap: document.getElementById("currentReviewWrap"),
  currentReview: document.getElementById("currentReview"),
  reviewedWrap: document.getElementById("reviewedWrap"),
  reviewedVideos: document.getElementById("reviewedVideos"),
  candidateWrap: document.getElementById("candidateWrap"),
  candidateVideos: document.getElementById("candidateVideos"),
  juiceResultWrap: document.getElementById("juiceResultWrap"),
  juiceResultMeta: document.getElementById("juiceResultMeta"),
  juiceOutput: document.getElementById("juiceOutput"),
  brewDetailPanel: document.getElementById("brewDetailPanel"),
  stackRefreshBtn: document.getElementById("stackRefreshBtn"),
  stackMeta: document.getElementById("stackMeta"),
  stackWebList: document.getElementById("stackWebList"),
  stackTgList: document.getElementById("stackTgList"),
  componentTypeSelect: document.getElementById("componentTypeSelect"),
  componentRunBtn: document.getElementById("componentRunBtn"),
  componentMeta: document.getElementById("componentMeta"),
  componentProgressBar: document.getElementById("componentProgressBar"),
  componentStats: document.getElementById("componentStats"),
  componentJobsList: document.getElementById("componentJobsList"),
  componentCaseList: document.getElementById("componentCaseList"),
  componentLog: document.getElementById("componentLog"),
};

function _normalizePage(raw) {
  const page = String(raw || "").trim().toLowerCase();
  const allowed = new Set(["direct", "archive", "transcript", "juice", "advanced"]);
  return allowed.has(page) ? page : "direct";
}

function switchPage(page, persist = true) {
  const safePage = _normalizePage(page);
  state.page = safePage;
  document.querySelectorAll(".tab").forEach((tab) => tab.classList.toggle("active", tab.dataset.page === safePage));
  document.querySelectorAll(".page").forEach((p) => p.classList.toggle("active", p.id === `page-${safePage}`));
  if (persist) writeUiPrefs();
}

const RECENT_DIRECT_KEY = "ytbot_recent_direct_searches_v2";
const RECENT_DIRECT_KEY_LEGACY = "ytbot_recent_direct_searches_v1";
const RECENT_DIRECT_LIMIT = 10;
const DIRECT_SAVE_STALE_MS = 20 * 60 * 1000;
const JUICE_FILTERS_KEY = "ytbot_juice_filters_v1";
const UI_PREFS_KEY = "ytbot_ui_prefs_v1";
const EXEC_MODE_PREFS_KEY = "ytbot_exec_mode_prefs_v2";
const NO_CANDIDATES_TEXT = "No candidate videos found. Try a broader goal.";
const GENERIC_NO_CANDIDATES_ERROR = "No candidate videos matched your topic/filters. Try a broader topic or adjust limits.";
const BROWSER_WEBLLM_IMPORT = "https://esm.run/@mlc-ai/web-llm";
const BROWSER_WEBLLM_MODELS = [
  "Llama-3.2-3B-Instruct-q4f16_1-MLC",
  "Qwen2.5-1.5B-Instruct-q4f16_1-MLC",
  "Llama-3.2-1B-Instruct-q4f16_1-MLC",
];

const browserLlmRuntime = {
  provider: "",
  model: "",
  session: null,
  engine: null,
  loading: null,
};

const I18N = {
  en: {
    "brand.sub": "YouTube operations console",
    "label.language": "Language",
    "label.exec_mode": "Mode",
    "mode.browser_fast": "Browser (faster)",
    "mode.server_side": "Server side (longer)",
    "btn.theme_to_night": "Night Mode",
    "btn.theme_to_day": "Day Mode",
    "btn.refresh": "Refresh",
    "btn.start": "Start",
    "btn.start_saving_live": "Start Saving Live",
    "btn.save_transcript": "Save Transcript",
    "btn.ask": "Ask",
    "btn.run_analysis": "Run Analysis",
    "btn.filters": "Filters",
    "btn.start_brewing": "Start Brewing",
    "btn.download_video": "Download Video",
    "btn.download_audio": "Download Audio",
    "btn.download_to_server": "Download To Server",
    "btn.download_from_server": "Download From Server",
    "btn.open_notes": "Open Notes",
    "btn.stop_saving": "Stop Saving",
    "btn.stop_saving_if_running": "Stop Saving (If Running)",
    "btn.open_saved_file": "Open Saved File",
    "btn.youtube": "YouTube",
    "nav.modules": "Workspaces",
    "tab.direct": "Direct Download",
    "tab.archive": "Save Live",
    "tab.transcript": "Video Notes",
    "tab.juice": "Knowledge Brew",
    "tab.info": "Info",
    "direct.head.title": "Video/Audio Download",
    "direct.head.desc": "Paste a YouTube URL, preview it, then generate download buttons.",
    "direct.saved_videos": "Saved Videos",
    "direct.recent_searches": "Recent Searches",
    "archive.head.title": "Save Live",
    "archive.head.desc": "Start/stop live captures and access saved live archives.",
    "archive.currently_saving": "Currently Saving Live",
    "archive.saved_archive": "Saved Live Archive",
    "notes.head.title": "Video Notes",
    "notes.head.desc": "Save transcript, then ask questions and run analysis per selected video.",
    "notes.transcript_intake": "Transcript Intake",
    "notes.rebuild_if_cached": "rebuild if cached",
    "notes.newest_transcripts": "Newest Transcripts",
    "notes.kicker": "Video Notes Studio",
    "notes.selected_video_notes": "Selected Video Notes",
    "notes.ask_transcript": "Ask Transcript",
    "notes.analysis": "Analysis",
    "notes.force_fresh": "force fresh",
    "notes.transcript_preview": "Full Transcript",
    "juice.head.title": "Knowledge Brew",
    "juice.head.desc": "Run caption-aware topic research and follow live progress by step.",
    "juice.how_title": "How this works",
    "juice.how_line_1": "Brew compares multiple owner videos on one topic, then extracts common patterns and differences.",
    "juice.how_line_2": "Use it to compress many hours of watch time into actionable lessons and practical next steps.",
    "juice.how_line_3": "Great for success-story learning, business model understanding, or comparing creators in the same niche.",
    "juice.brew_title": "Brew Knowledge Juice",
    "juice.max_videos": "Max videos",
    "juice.max_queries": "Max queries",
    "juice.per_query": "Per query",
    "juice.min_duration": "Min duration (sec)",
    "juice.max_duration": "Max duration (sec)",
    "juice.fast_mode": "make fast (captions only)",
    "juice.private_run": "private run (do not save publicly)",
    "juice.captions_note": "With captions: no duration cap. Without captions: max 10 minutes.",
    "juice.active_brewings": "Active Brewings",
    "juice.currently_under_review": "Currently Under Review",
    "juice.reviewed_videos": "Reviewed Videos",
    "juice.candidate_pool": "Candidate Pool",
    "juice.brew_result": "Brew Result",
    "research.public_researches": "Public Researches",
    "research.detail": "Research Detail",
    "placeholder.search": "Search",
    "placeholder.youtube_url": "https://www.youtube.com/watch?v=...",
    "placeholder.live_url": "https://www.youtube.com/watch?v=... or /live/...",
    "placeholder.ask_transcript": "Ask about selected video transcript...",
    "placeholder.topic": "bakery, mechanic, etc.",
    "status.connecting_live_updates": "Connecting live updates...",
    "status.no_recent_searches": "No recent searches yet.",
    "status.no_saved_live_items": "No saved live items.",
    "status.no_saved_videos": "No saved videos yet.",
    "status.no_transcripts": "No transcripts yet.",
    "status.no_public_researches": "No public researches.",
    "status.no_completed_public_researches": "No completed public researches yet.",
    "status.no_items": "No items.",
    "status.select_video_from_list": "Select a video from Video Notes List",
    "status.no_video_selected": "No video selected.",
    "status.open_on_youtube": "Open on YouTube",
    "status.loaded_saved_analysis": "Loaded saved analysis.",
    "status.no_analysis_saved": "No analysis saved yet.",
    "status.select_video_first": "Select a video first.",
    "status.question_required": "Question is required.",
    "status.youtube_url_required": "YouTube URL is required.",
    "status.live_url_required": "Live URL is required.",
    "status.topic_required": "Topic is required.",
    "status.saved_file": "saved file",
    "status.not_available_yet": "not available yet",
    "status.status": "status",
    "status.running_analysis": "Running analysis...",
    "status.asking_transcript": "Asking transcript...",
    "status.analysis_already_running": "Analysis is already in progress. Please wait.",
    "status.ask_already_running": "Ask is already in progress. Please wait.",
    "status.analysis_blocked_by_ask": "Ask is running. Wait until it finishes.",
    "status.ask_blocked_by_analysis": "Analysis is running. Wait until it finishes.",
    "status.saving_transcript": "Saving transcript...",
    "status.preparing_download_links": "Preparing download links...",
    "status.starting_live_recording": "Starting live recording...",
    "status.starting_brewing": "Starting brewing job...",
    "status.brewing_started": "Brewing started.",
    "status.refreshing": "Refreshing...",
    "status.data_refreshed": "Data refreshed.",
    "status.connected": "Connected.",
    "status.live_updates_on_polling": "Live updates on (polling)",
    "status.live_updates_on": "Live updates on",
    "status.live_updates_polling_fallback": "Live updates on (polling fallback)",
    "status.saved_files_retention": "Saved files are auto-deleted after {days} days.",
    "status.preparing_search_plan": "Preparing search plan…",
    "status.searching_youtube_videos": "Searching YouTube videos…",
    "status.reviewing_videos": "Reviewing {reviewed}/{total} videos",
    "status.comparing_insights": "Comparing insights from {reviewed} reviewed videos…",
    "status.fast_mode": "fast mode",
    "status.updated_at": "Updated: {time}",
    "status.error": "Error: {error}",
    "status.browser_loading_model": "Preparing browser model...",
    "status.browser_model_ready": "Browser model ready.",
  },
  uk: {
    "brand.sub": "Консоль YouTube-операцій",
    "label.language": "Мова",
    "label.exec_mode": "Режим",
    "mode.browser_fast": "Браузер (швидше)",
    "mode.server_side": "Сервер (довше)",
    "btn.theme_to_night": "Нічний режим",
    "btn.theme_to_day": "Денний режим",
    "btn.refresh": "Оновити",
    "btn.start": "Старт",
    "btn.start_saving_live": "Почати збереження LIVE",
    "btn.save_transcript": "Зберегти транскрипт",
    "btn.ask": "Запитати",
    "btn.run_analysis": "Запустити аналіз",
    "btn.filters": "Фільтри",
    "btn.start_brewing": "Почати збір",
    "btn.download_video": "Завантажити відео",
    "btn.download_audio": "Завантажити аудіо",
    "btn.download_to_server": "Завантажити на сервер",
    "btn.download_from_server": "Завантажити з сервера",
    "btn.open_notes": "Відкрити нотатки",
    "btn.stop_saving": "Зупинити збереження",
    "btn.stop_saving_if_running": "Зупинити збереження (якщо активне)",
    "btn.open_saved_file": "Відкрити збережений файл",
    "btn.youtube": "YouTube",
    "nav.modules": "Робочі зони",
    "tab.direct": "Пряме завантаження",
    "tab.archive": "Зберегти LIVE",
    "tab.transcript": "Нотатки відео",
    "tab.juice": "Knowledge Brew",
    "tab.info": "Інфо",
    "direct.head.title": "Завантаження відео/аудіо",
    "direct.head.desc": "Вставте URL YouTube, перегляньте превʼю та згенеруйте кнопки завантаження.",
    "direct.saved_videos": "Збережені відео",
    "direct.recent_searches": "Останні пошуки",
    "archive.head.title": "Зберегти LIVE",
    "archive.head.desc": "Запускайте/зупиняйте LIVE-записи та переглядайте архів.",
    "archive.currently_saving": "Зараз записується LIVE",
    "archive.saved_archive": "Збережений LIVE-архів",
    "notes.head.title": "Нотатки відео",
    "notes.head.desc": "Збережіть транскрипт, потім ставте питання і запускайте аналіз для вибраного відео.",
    "notes.transcript_intake": "Збір транскрипту",
    "notes.rebuild_if_cached": "перебудувати, якщо є кеш",
    "notes.newest_transcripts": "Нові транскрипти",
    "notes.kicker": "Студія нотаток",
    "notes.selected_video_notes": "Нотатки вибраного відео",
    "notes.ask_transcript": "Запитати по транскрипту",
    "notes.analysis": "Аналіз",
    "notes.force_fresh": "примусово свіжий",
    "notes.transcript_preview": "Повний транскрипт",
    "juice.head.title": "Knowledge Brew",
    "juice.head.desc": "Запускайте дослідження теми та відстежуйте прогрес крок за кроком.",
    "juice.how_title": "Як це працює",
    "juice.how_line_1": "Brew порівнює кілька відео власників в одній темі та виділяє спільні патерни і відмінності.",
    "juice.how_line_2": "Це стискає багато годин перегляду в практичні висновки та конкретні наступні кроки.",
    "juice.how_line_3": "Підходить для розбору success-історій, бізнес-моделей або порівняння авторів у ніші.",
    "juice.brew_title": "Зібрати Knowledge Juice",
    "juice.max_videos": "Макс. відео",
    "juice.max_queries": "Макс. запитів",
    "juice.per_query": "На запит",
    "juice.min_duration": "Мін. тривалість (сек)",
    "juice.max_duration": "Макс. тривалість (сек)",
    "juice.fast_mode": "швидко (лише субтитри)",
    "juice.private_run": "приватний запуск (не зберігати публічно)",
    "juice.captions_note": "Із субтитрами: без обмеження тривалості. Без субтитрів: максимум 10 хв.",
    "juice.active_brewings": "Активні запуски",
    "juice.currently_under_review": "Зараз перевіряється",
    "juice.reviewed_videos": "Переглянуті відео",
    "juice.candidate_pool": "Пул кандидатів",
    "juice.brew_result": "Результат",
    "research.public_researches": "Публічні дослідження",
    "research.detail": "Деталі дослідження",
    "placeholder.search": "Пошук",
    "placeholder.youtube_url": "https://www.youtube.com/watch?v=...",
    "placeholder.live_url": "https://www.youtube.com/watch?v=... або /live/...",
    "placeholder.ask_transcript": "Запитайте про транскрипт вибраного відео...",
    "placeholder.topic": "пекарня, механік, тощо",
    "status.connecting_live_updates": "Підключення live-оновлень...",
    "status.no_recent_searches": "Ще немає останніх пошуків.",
    "status.no_saved_live_items": "Ще немає збережених LIVE.",
    "status.no_saved_videos": "Ще немає збережених відео.",
    "status.no_transcripts": "Ще немає транскриптів.",
    "status.no_public_researches": "Немає публічних досліджень.",
    "status.no_completed_public_researches": "Ще немає завершених публічних досліджень.",
    "status.no_items": "Немає елементів.",
    "status.select_video_from_list": "Виберіть відео зі списку нотаток",
    "status.no_video_selected": "Відео не вибрано.",
    "status.open_on_youtube": "Відкрити на YouTube",
    "status.loaded_saved_analysis": "Завантажено збережений аналіз.",
    "status.no_analysis_saved": "Збереженого аналізу ще немає.",
    "status.select_video_first": "Спочатку виберіть відео.",
    "status.question_required": "Питання обов'язкове.",
    "status.youtube_url_required": "Потрібен URL YouTube.",
    "status.live_url_required": "Потрібен URL LIVE.",
    "status.topic_required": "Тема обов'язкова.",
    "status.saved_file": "збережений файл",
    "status.not_available_yet": "ще недоступно",
    "status.status": "статус",
    "status.running_analysis": "Виконується аналіз...",
    "status.asking_transcript": "Ставимо питання до транскрипту...",
    "status.analysis_already_running": "Аналіз уже виконується. Будь ласка, зачекайте.",
    "status.ask_already_running": "Запит уже виконується. Будь ласка, зачекайте.",
    "status.analysis_blocked_by_ask": "Запит уже виконується. Дочекайтеся завершення.",
    "status.ask_blocked_by_analysis": "Аналіз уже виконується. Дочекайтеся завершення.",
    "status.saving_transcript": "Зберігаємо транскрипт...",
    "status.preparing_download_links": "Готуємо посилання для завантаження...",
    "status.starting_live_recording": "Запускаємо LIVE-запис...",
    "status.starting_brewing": "Запуск brewing-задачі...",
    "status.brewing_started": "Brewing запущено.",
    "status.refreshing": "Оновлення...",
    "status.data_refreshed": "Дані оновлено.",
    "status.connected": "Підключено.",
    "status.live_updates_on_polling": "Live-оновлення увімкнено (polling)",
    "status.live_updates_on": "Live-оновлення увімкнено",
    "status.live_updates_polling_fallback": "Live-оновлення увімкнено (fallback polling)",
    "status.saved_files_retention": "Збережені файли авто-видаляються через {days} днів.",
    "status.preparing_search_plan": "Підготовка плану пошуку…",
    "status.searching_youtube_videos": "Пошук відео на YouTube…",
    "status.reviewing_videos": "Перегляд {reviewed}/{total} відео",
    "status.comparing_insights": "Порівняння інсайтів із {reviewed} переглянутих відео…",
    "status.fast_mode": "швидкий режим",
    "status.updated_at": "Оновлено: {time}",
    "status.error": "Помилка: {error}",
    "status.browser_loading_model": "Підготовка браузерної моделі...",
    "status.browser_model_ready": "Браузерна модель готова.",
  },
};

let _uiLang = "en";
let _uiTheme = "day";

function _fillTemplate(text, vars = {}) {
  return String(text || "").replace(/\{([a-zA-Z0-9_]+)\}/g, (_m, k) => String(vars[k] ?? ""));
}

function t(key, vars = {}) {
  const langPack = I18N[_uiLang] || I18N.en;
  const basePack = I18N.en || {};
  const raw = Object.prototype.hasOwnProperty.call(langPack, key)
    ? langPack[key]
    : (Object.prototype.hasOwnProperty.call(basePack, key) ? basePack[key] : key);
  return _fillTemplate(raw, vars);
}

function _normalizeLang(raw) {
  const v = String(raw || "").trim().toLowerCase();
  return v === "uk" ? "uk" : "en";
}

function _normalizeTheme(raw) {
  const v = String(raw || "").trim().toLowerCase();
  return v === "night" ? "night" : "day";
}

function readUiPrefs() {
  try {
    const raw = JSON.parse(window.localStorage.getItem(UI_PREFS_KEY) || "{}");
    const themeRaw = typeof raw.theme === "string" && raw.theme.trim() ? raw.theme : "day";
    return {
      lang: _normalizeLang(raw.lang),
      theme: _normalizeTheme(themeRaw),
      page: _normalizePage(raw.page),
      selected_research_id: String(raw.selected_research_id || "").trim(),
      selected_job_id: String(raw.selected_job_id || "").trim(),
      selected_video_id: String(raw.selected_video_id || "").trim(),
    };
  } catch (_err) {
    return { lang: "en", theme: "day", page: "direct", selected_research_id: "", selected_job_id: "", selected_video_id: "" };
  }
}

function writeUiPrefs() {
  try {
    window.localStorage.setItem(
      UI_PREFS_KEY,
      JSON.stringify({
        lang: _uiLang,
        theme: _uiTheme,
        page: _normalizePage(state.page),
        selected_research_id: String(state.selectedResearchId || "").trim(),
        selected_job_id: String(state.selectedJobId || "").trim(),
        selected_video_id: String(state.selectedVideoId || "").trim(),
      })
    );
  } catch (_err) {
    // no-op
  }
}

function setThemeButtonLabel() {
  if (!el.themeToggleLabel) return;
  const key = _uiTheme === "night" ? "btn.theme_to_day" : "btn.theme_to_night";
  el.themeToggleLabel.textContent = t(key);
  el.themeToggleLabel.dataset.i18n = key;
  if (el.themeToggle) el.themeToggle.checked = _uiTheme === "night";
}

function applyTheme(theme, persist = true) {
  _uiTheme = _normalizeTheme(theme);
  document.documentElement.setAttribute("data-theme", _uiTheme);
  setThemeButtonLabel();
  if (persist) writeUiPrefs();
}

function applyLanguage(lang, persist = true) {
  _uiLang = _normalizeLang(lang);
  document.documentElement.lang = _uiLang;
  if (el.langSelect) el.langSelect.value = _uiLang;

  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = String(node.getAttribute("data-i18n") || "").trim();
    if (!key) return;
    node.textContent = t(key);
    if (node instanceof HTMLButtonElement) delete node.dataset.baseLabel;
  });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((node) => {
    const key = String(node.getAttribute("data-i18n-placeholder") || "").trim();
    if (!key) return;
    node.setAttribute("placeholder", t(key));
  });

  setJuiceFiltersToggleLabel();
  if (persist) writeUiPrefs();
}

function _normalizeExecMode(raw) {
  return String(raw || "").trim().toLowerCase() === "browser" ? "browser" : "server";
}

function readExecModePrefs() {
  const defaults = { ask_mode: "browser", analyze_mode: "browser" };
  try {
    const raw = JSON.parse(window.localStorage.getItem(EXEC_MODE_PREFS_KEY) || "{}");
    return {
      ask_mode: _normalizeExecMode(raw.ask_mode || defaults.ask_mode),
      analyze_mode: _normalizeExecMode(raw.analyze_mode || defaults.analyze_mode),
    };
  } catch (_err) {
    return defaults;
  }
}

function writeExecModePrefs(prefs) {
  const payload = prefs && typeof prefs === "object" ? prefs : readExecModePrefs();
  try {
    window.localStorage.setItem(EXEC_MODE_PREFS_KEY, JSON.stringify(payload));
  } catch (_err) {
    // no-op
  }
}

function applyExecModePrefs() {
  const prefs = readExecModePrefs();
  if (el.askModeSelect) el.askModeSelect.value = _normalizeExecMode(prefs.ask_mode);
  if (el.analyzeModeSelect) el.analyzeModeSelect.value = _normalizeExecMode(prefs.analyze_mode);
}

function persistExecModePrefs() {
  writeExecModePrefs({
    ask_mode: _normalizeExecMode(el.askModeSelect?.value || "browser"),
    analyze_mode: _normalizeExecMode(el.analyzeModeSelect?.value || "browser"),
  });
}

function currentAskMode() {
  return _normalizeExecMode(el.askModeSelect?.value || "browser");
}

function currentAnalyzeMode() {
  return _normalizeExecMode(el.analyzeModeSelect?.value || "browser");
}

function getVideoThumb(videoId) {
  const vid = String(videoId || "").trim();
  if (!vid) return "";
  return `https://i.ytimg.com/vi/${encodeURIComponent(vid)}/hqdefault.jpg`;
}

function extractYouTubeVideoId(rawUrl) {
  const text = String(rawUrl || "").trim();
  if (!text) return "";
  const shortId = text.match(/^[A-Za-z0-9_-]{11}$/);
  if (shortId) return shortId[0];
  try {
    const u = new URL(text);
    const host = u.hostname.toLowerCase();
    if (host.includes("youtu.be")) {
      const seg = (u.pathname || "").split("/").filter(Boolean)[0] || "";
      return /^[A-Za-z0-9_-]{6,20}$/.test(seg) ? seg : "";
    }
    const v = (u.searchParams.get("v") || "").trim();
    if (/^[A-Za-z0-9_-]{6,20}$/.test(v)) return v;
    const parts = (u.pathname || "").split("/").filter(Boolean);
    const shortsIdx = parts.findIndex((p) => p === "shorts" || p === "live" || p === "embed");
    if (shortsIdx >= 0 && parts[shortsIdx + 1] && /^[A-Za-z0-9_-]{6,20}$/.test(parts[shortsIdx + 1])) {
      return parts[shortsIdx + 1];
    }
  } catch (_err) {
    // ignore parse error
  }
  return "";
}

function safeHref(rawUrl) {
  const text = String(rawUrl || "").trim();
  if (!/^https?:\/\//i.test(text)) return "#";
  return text.replace(/"/g, "%22").replace(/'/g, "%27");
}

function toFriendlyJuiceError(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";
  if (text === NO_CANDIDATES_TEXT) return GENERIC_NO_CANDIDATES_ERROR;
  return text;
}

function toFriendlyResearchReport(item, rawReportText) {
  const report = String(rawReportText || "");
  if (report.trim() === NO_CANDIDATES_TEXT) {
    return GENERIC_NO_CANDIDATES_ERROR;
  }
  return stripMoneySection(report);
}

function stripMoneySection(rawText) {
  const src = String(rawText || "");
  if (!src.trim()) return "";
  const cleaned = src.replace(
    /(^|\n)Money\s*\/\s*Profit Signals[\s\S]*?(?=\n\n(?:Summary|Steps To Do|Similarities|Differences)\b|$)/gi,
    "\n"
  );
  return cleaned.replace(/\n{3,}/g, "\n\n").trim();
}

function cleanResearchText(raw) {
  return String(raw || "")
    .replace(/[\[\]{}|`]/g, " ")
    .replace(/["']/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseReportSections(reportText) {
  const out = {
    summaryLines: [],
    similarities: [],
    differences: [],
    steps: [],
  };
  const text = String(reportText || "");
  const lines = text.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  let section = "summary";
  for (const line of lines) {
    const low = line.toLowerCase();
    if (low.includes("similarities")) {
      section = "similarities";
      continue;
    }
    if (low.includes("differences")) {
      section = "differences";
      continue;
    }
    if (low.includes("recommended next actions")) {
      section = "steps";
      continue;
    }
    if (/^top videos:?$/i.test(line)) continue;
    if (line.startsWith("•")) {
      const bullet = cleanResearchText(line.replace(/^•\s*/, ""));
      if (!bullet) continue;
      if (section === "similarities") out.similarities.push(bullet);
      else if (section === "differences") out.differences.push(bullet);
      else if (section === "steps") out.steps.push(bullet);
      else out.summaryLines.push(bullet);
      continue;
    }
    if (section === "summary") {
      const normalized = cleanResearchText(line.replace(/^[^\w]+/, ""));
      if (normalized) out.summaryLines.push(normalized);
    }
  }
  return out;
}

function arrayOfStrings(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map((x) => cleanResearchText(x)).filter(Boolean);
}

function researchHasResult(item) {
  const obj = item && typeof item === "object" ? item : {};
  const status = String(obj.status || "").trim().toLowerCase();
  const excerpt = String(obj.report_excerpt || "").trim();
  const reportText = String(obj.report_text || "").trim();
  const lowCombined = `${excerpt}\n${reportText}`.toLowerCase();
  const notFailed = !lowCombined.includes("research failed:");
  const summaryObj = obj && typeof obj.summary === "object" ? obj.summary : {};
  const hasSummaryData = summaryObj && Object.keys(summaryObj).length > 0;
  const hasReportData = Boolean(excerpt || reportText || hasSummaryData);
  return ["completed", "done"].includes(status) && hasReportData && notFailed;
}

function formatResearchDetail(item) {
  const report = toFriendlyResearchReport(item, item.report_text || "");
  const parsed = parseReportSections(report);
  const summaryObj = item && typeof item.summary === "object" ? item.summary : {};
  const comparison = summaryObj && typeof summaryObj.comparison === "object" ? summaryObj.comparison : {};

  const similarities = [
    ...arrayOfStrings(comparison.similarities),
    ...parsed.similarities,
  ].filter((x, idx, arr) => arr.indexOf(x) === idx).slice(0, 10);
  const differences = [
    ...arrayOfStrings(comparison.differences),
    ...parsed.differences,
  ].filter((x, idx, arr) => arr.indexOf(x) === idx).slice(0, 10);
  const steps = [
    ...arrayOfStrings(comparison.recommendations),
    ...parsed.steps,
  ].filter((x, idx, arr) => arr.indexOf(x) === idx).slice(0, 10);

  const summaryBase = cleanResearchText(
    [
      ...(parsed.summaryLines || []),
      steps.length ? "" : cleanResearchText(report.slice(0, 700)),
    ]
      .filter(Boolean)
      .join(" ")
  );
  const stepsBlock = steps.length ? steps.map((x, i) => `${i + 1}. ${x}`).join("\n") : "No clear steps extracted yet.";
  let summaryAndSteps = `Summary\n${summaryBase || "No summary available."}\n\nSteps To Do\n${stepsBlock}`.trim();
  if (summaryAndSteps.length > 1000) summaryAndSteps = `${summaryAndSteps.slice(0, 997).trim()}...`;

  const simBlock = similarities.length ? similarities.map((x) => `- ${x}`).join("\n") : "- Not enough overlap extracted.";
  const diffBlock = differences.length ? differences.map((x) => `- ${x}`).join("\n") : "- Not enough contrasts extracted.";
  return `${summaryAndSteps}\n\nSimilarities\n${simBlock}\n\nDifferences\n${diffBlock}`.trim();
}

function researchVideoThumb(video) {
  const row = video && typeof video === "object" ? video : {};
  const direct = String(row.thumbnail_url || "").trim();
  if (direct) return direct;
  const vid = String(row.video_id || "").trim() || extractYouTubeVideoId(String(row.url || "").trim());
  return vid ? `https://i.ytimg.com/vi/${vid}/hqdefault.jpg` : "";
}

function researchDisplayTitle(item) {
  const row = item && typeof item === "object" ? item : {};
  const display = String(row.display_title || "").trim();
  if (display) return display;
  const goal = String(row.goal_text || "").trim();
  if (goal) return goal;
  return String(row.run_id || "").trim() || "Research";
}

function researchPreviewThumbs(item) {
  const row = item && typeof item === "object" ? item : {};
  const previews = Array.isArray(row.preview_videos) ? row.preview_videos : [];
  return previews
    .slice(0, 4)
    .map((x) => {
      const one = x && typeof x === "object" ? x : {};
      const thumb = String(one.thumbnail_url || "").trim() || researchVideoThumb(one);
      if (!thumb) return "";
      return `<img class="research-list-thumb" src="${escapeHtml(thumb)}" alt="" loading="lazy" />`;
    })
    .filter(Boolean)
    .join("");
}

function formatResearchDetailHtml(item) {
  const summaryText = formatResearchDetail(item);
  const videos = Array.isArray(item && item.videos) ? item.videos : [];
  const videosHtml = videos.length
    ? videos.map((v, idx) => {
      const row = v && typeof v === "object" ? v : {};
      const title = String(row.title || row.video_id || `Video ${idx + 1}`).trim();
      const channel = String(row.channel || "").trim();
      const videoId = String(row.video_id || "").trim();
      const url = String(row.url || (videoId ? `https://www.youtube.com/watch?v=${videoId}` : "")).trim();
      const safeUrlVal = safeHref(url || "#");
      const thumb = researchVideoThumb(row);
      const transcript = String(row.transcript_text || "").replace(/\r/g, "").trim();
      const transcriptLow = transcript.toLowerCase();
      const transcriptPlaceholder = transcriptLow === "transcript not saved."
        || transcriptLow === "transcript not available."
        || transcriptLow === "transcript not available";
      const transcriptSaved = Boolean(transcript) && !transcriptPlaceholder;
      const transcriptChars = Number(row.transcript_chars || 0);
      const truncated = Boolean(row.transcript_truncated);
      const transcriptLabel = transcriptSaved
        ? `saved${transcriptChars > 0 ? ` • ${transcriptChars} chars` : ""}${truncated ? " • truncated" : ""}`
        : "not saved";
      return `
        <article class="video-card research-video-card">
          ${
  thumb
    ? (
      url
        ? `<a class="research-video-thumb-link" href="${escapeHtml(safeUrlVal)}" target="_blank" rel="noreferrer"><img class="research-video-thumb" src="${escapeHtml(thumb)}" alt="" loading="lazy" /></a>`
        : `<img class="research-video-thumb" src="${escapeHtml(thumb)}" alt="" loading="lazy" />`
    )
    : ""
}
          <div class="body">
            <p class="title">${
  url
    ? `<a href="${escapeHtml(safeUrlVal)}" target="_blank" rel="noreferrer">${escapeHtml(title)}</a>`
    : escapeHtml(title)
}</p>
            <p class="meta-line">${escapeHtml(channel || "Unknown channel")}</p>
            ${
  transcriptSaved
    ? `<p class="meta-line">Transcript: ${escapeHtml(transcriptLabel)}</p><pre class="research-transcript">${escapeHtml(transcript)}</pre>`
    : ``
}
          </div>
        </article>
      `;
    }).join("")
    : `<p class="meta">No research videos saved.</p>`;

  return `
    <div class="research-detail-rich">
      <h4 class="brew-section-label">Videos And Transcripts</h4>
      <div class="cards media-feed research-videos-feed">${videosHtml}</div>
      <pre class="research-summary">${escapeHtml(summaryText)}</pre>
    </div>
  `;
}

function normalizeRecentDirectEntry(item) {
  const entry = item && typeof item === "object" ? item : {};
  const url = String(entry.url || "").trim();
  if (!url) return null;

  const videoId = String(entry.video_id || "").trim();
  const title = String(entry.title || "").trim();
  const inputLinks = entry.links && typeof entry.links === "object" ? entry.links : {};
  const links = {
    video: String(inputLinks.video || entry.video_url || "").trim(),
    audio: String(inputLinks.audio || entry.audio_url || "").trim(),
  };
  const savedVideoUrl = String(
    entry.saved_video_url || entry.saved_url || (inputLinks.saved || "")
  ).trim();
  const saveJobId = String(entry.save_job_id || "").trim();
  const saveStatus = String(entry.save_status || "").trim();
  const saveRequestedAt = String(entry.save_requested_at || "").trim();
  const createdAt = String(entry.created_at || entry.updated_at || new Date().toISOString());
  const updatedAt = String(entry.updated_at || createdAt || new Date().toISOString());
  const thumbnail = String(entry.thumbnail_url || "").trim() || getVideoThumb(videoId);
  return {
    url,
    video_id: videoId,
    title,
    thumbnail_url: thumbnail,
    links,
    saved_video_url: savedVideoUrl,
    save_job_id: saveJobId,
    save_status: saveStatus,
    save_requested_at: saveRequestedAt,
    created_at: createdAt,
    updated_at: updatedAt,
  };
}

function _parseTimeMs(raw) {
  const ts = Date.parse(String(raw || "").trim());
  return Number.isFinite(ts) ? ts : 0;
}

function _saveStatus(raw) {
  return String(raw || "").trim().toLowerCase();
}

function _saveActivityMs(entry) {
  const row = entry && typeof entry === "object" ? entry : {};
  return Math.max(
    _parseTimeMs(row.save_requested_at),
    _parseTimeMs(row.updated_at),
    _parseTimeMs(row.created_at)
  );
}

function _isStaleRunningSave(entry, nowMs = Date.now()) {
  const row = entry && typeof entry === "object" ? entry : {};
  const status = _saveStatus(row.save_status);
  if (status !== "started" && status !== "running") return false;
  const activityMs = _saveActivityMs(row);
  if (activityMs <= 0) return true;
  return (nowMs - activityMs) > DIRECT_SAVE_STALE_MS;
}

function effectiveRecentSaveStatus(entry, nowMs = Date.now()) {
  const row = entry && typeof entry === "object" ? entry : {};
  if (String(row.saved_video_url || "").trim()) return "saved";
  const status = _saveStatus(row.save_status);
  if ((status === "started" || status === "running") && _isStaleRunningSave(row, nowMs)) {
    return "error";
  }
  return status;
}

function resolveRecentVideoTitle(item) {
  const entry = item && typeof item === "object" ? item : {};
  const rawTitle = String(entry.title || "").trim();
  if (rawTitle && !isVideoIdLike(rawTitle)) return rawTitle;
  const videoId = String(entry.video_id || "").trim();
  if (videoId) {
    const known = state.videos.find((v) => String(v.video_id || "").trim() === videoId);
    const knownTitle = String((known || {}).title || "").trim();
    if (knownTitle && !isVideoIdLike(knownTitle)) return knownTitle;
  }
  return videoId || extractYouTubeVideoId(entry.url || "") || "Video";
}

function preferredDirectTitle({ rawTitle, knownTitle, recentTitle, videoId, url }) {
  const picked = [rawTitle, knownTitle, recentTitle]
    .map((x) => String(x || "").trim())
    .find((x) => x && !isVideoIdLike(x));
  if (picked) return picked;
  return String(videoId || "").trim() || extractYouTubeVideoId(url || "") || "Video";
}

function readRecentDirectSearches() {
  try {
    const raw = window.localStorage.getItem(RECENT_DIRECT_KEY) || window.localStorage.getItem(RECENT_DIRECT_KEY_LEGACY);
    const arr = JSON.parse(raw || "[]");
    if (!Array.isArray(arr)) return [];
    let changed = false;
    const nowIso = new Date().toISOString();
    const nowMs = Date.now();
    const next = arr
      .map((x) => normalizeRecentDirectEntry(x))
      .filter(Boolean)
      .map((x) => {
        const effectiveStatus = effectiveRecentSaveStatus(x, nowMs);
        if (_saveStatus(x.save_status) !== effectiveStatus) {
          changed = true;
          return {
            ...x,
            save_status: effectiveStatus,
            updated_at: nowIso,
          };
        }
        return x;
      })
      .sort((a, b) => String(b.updated_at || "").localeCompare(String(a.updated_at || "")));
    if (changed) writeRecentDirectSearches(next);
    return next;
  } catch (_err) {
    return [];
  }
}

function writeRecentDirectSearches(items) {
  try {
    const normalized = Array.isArray(items) ? items.map((x) => normalizeRecentDirectEntry(x)).filter(Boolean) : [];
    window.localStorage.setItem(RECENT_DIRECT_KEY, JSON.stringify(normalized));
  } catch (_err) {
    // no-op
  }
}

function rememberRecentDirectSearch(item) {
  const entry = item && typeof item === "object" ? item : {};
  const mediaType = String(entry.media_type || "").trim();
  const mediaLink = String(entry.download_url || "").trim();
  const normalized = normalizeRecentDirectEntry({
    ...entry,
    updated_at: new Date().toISOString(),
  });
  if (!normalized) return;
  if (mediaType === "video" && mediaLink) normalized.links.video = mediaLink;
  if (mediaType === "audio" && mediaLink) normalized.links.audio = mediaLink;

  const current = readRecentDirectSearches();
  const matched = current.find((x) => x.url === normalized.url);
  if (matched) {
    normalized.links.video = normalized.links.video || matched.links.video || "";
    normalized.links.audio = normalized.links.audio || matched.links.audio || "";
    normalized.video_id = normalized.video_id || matched.video_id || "";
    normalized.title = normalized.title || matched.title || "";
    normalized.thumbnail_url = normalized.thumbnail_url || matched.thumbnail_url || getVideoThumb(normalized.video_id);
    normalized.saved_video_url = normalized.saved_video_url || matched.saved_video_url || "";
    normalized.save_job_id = normalized.save_job_id || matched.save_job_id || "";
    normalized.save_status = normalized.save_status || matched.save_status || "";
    normalized.save_requested_at = normalized.save_requested_at || matched.save_requested_at || "";
    normalized.created_at = matched.created_at || normalized.created_at;
  }

  const next = [normalized, ...current.filter((x) => x.url !== normalized.url)]
    .slice(0, RECENT_DIRECT_LIMIT)
    .sort((a, b) => String(b.updated_at || "").localeCompare(String(a.updated_at || "")));
  writeRecentDirectSearches(next);
}

function patchRecentDirectSearch(url, patch = {}) {
  const targetUrl = String(url || "").trim();
  if (!targetUrl) return;
  const current = readRecentDirectSearches();
  if (!current.length) return;
  const nowIso = new Date().toISOString();
  const next = current.map((entry) => {
    if (String(entry.url || "").trim() !== targetUrl) return entry;
    const merged = {
      ...entry,
      ...patch,
      links: {
        ...(entry.links || {}),
        ...((patch && typeof patch.links === "object") ? patch.links : {}),
      },
      updated_at: nowIso,
    };
    return normalizeRecentDirectEntry(merged) || entry;
  });
  writeRecentDirectSearches(next);
}

function syncRecentSavedLinksFromVideos() {
  const current = readRecentDirectSearches();
  if (!current.length) return false;

  const savedByVideoId = new Map();
  const titleByVideoId = new Map();
  for (const v of state.videos || []) {
    const videoId = String(v.video_id || "").trim();
    const publicUrl = String(v.public_url || "").trim();
    const title = String(v.title || "").trim();
    if (videoId && title && !isVideoIdLike(title)) titleByVideoId.set(videoId, title);
    if (!videoId || !publicUrl) continue;
    savedByVideoId.set(videoId, publicUrl);
  }
  if (!savedByVideoId.size && !titleByVideoId.size) return false;

  let changed = false;
  const next = current.map((entry) => {
    const entryVideoId = String(entry.video_id || "").trim() || extractYouTubeVideoId(entry.url || "");
    if (!entryVideoId) return entry;
    const savedUrl = String(savedByVideoId.get(entryVideoId) || "").trim();
    const knownTitle = String(titleByVideoId.get(entryVideoId) || "").trim();
    const entryTitle = String(entry.title || "").trim();
    let updated = entry;

    if (savedUrl && String(entry.saved_video_url || "").trim() !== savedUrl) {
      changed = true;
      updated = {
        ...updated,
        video_id: entryVideoId,
        saved_video_url: savedUrl,
        save_status: "saved",
        updated_at: new Date().toISOString(),
      };
    }

    if (knownTitle && (isVideoIdLike(entryTitle) || !entryTitle)) {
      changed = true;
      updated = {
        ...updated,
        title: knownTitle,
        updated_at: new Date().toISOString(),
      };
    }

    return updated;
  });

  if (changed) writeRecentDirectSearches(next);
  return changed;
}

async function copyText(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) return false;
  try {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch (_err) {
    // fallback below
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = value;
    ta.setAttribute("readonly", "readonly");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return Boolean(ok);
  } catch (_err) {
    return false;
  }
}

function juiceDefaultPrefs() {
  return {
    max_videos: 6,
    max_queries: 8,
    per_query: 8,
    min_duration_sec: 0,
    max_duration_sec: 0,
    captions_only: true,
    private_run: false,
    filters_open: false,
  };
}

function readJuicePrefs() {
  const defaults = juiceDefaultPrefs();
  try {
    const raw = window.localStorage.getItem(JUICE_FILTERS_KEY);
    const parsed = JSON.parse(raw || "{}");
    if (!parsed || typeof parsed !== "object") return defaults;
    return {
      ...defaults,
      max_videos: toInt(parsed.max_videos, defaults.max_videos),
      max_queries: toInt(parsed.max_queries, defaults.max_queries),
      per_query: toInt(parsed.per_query, defaults.per_query),
      min_duration_sec: toInt(parsed.min_duration_sec, defaults.min_duration_sec),
      max_duration_sec: toInt(parsed.max_duration_sec, defaults.max_duration_sec),
      captions_only: Object.prototype.hasOwnProperty.call(parsed, "captions_only")
        ? Boolean(parsed.captions_only)
        : defaults.captions_only,
      private_run: Object.prototype.hasOwnProperty.call(parsed, "private_run")
        ? Boolean(parsed.private_run)
        : defaults.private_run,
      filters_open: Object.prototype.hasOwnProperty.call(parsed, "filters_open")
        ? Boolean(parsed.filters_open)
        : defaults.filters_open,
    };
  } catch (_err) {
    return defaults;
  }
}

function writeJuicePrefs(prefs) {
  const payload = prefs && typeof prefs === "object" ? prefs : juiceDefaultPrefs();
  try {
    window.localStorage.setItem(JUICE_FILTERS_KEY, JSON.stringify(payload));
  } catch (_err) {
    // no-op
  }
}

function collectJuicePrefs() {
  return {
    max_videos: toInt(el.juiceMaxVideos?.value, 6),
    max_queries: toInt(el.juiceMaxQueries?.value, 8),
    per_query: toInt(el.juicePerQuery?.value, 8),
    min_duration_sec: toInt(el.juiceMinDuration?.value, 0),
    max_duration_sec: toInt(el.juiceMaxDuration?.value, 0),
    captions_only: Boolean(el.juiceFast?.checked),
    private_run: Boolean(el.juicePrivate?.checked),
    filters_open: Boolean(el.juiceFiltersPanel && !el.juiceFiltersPanel.hidden),
  };
}

function setJuiceFiltersToggleLabel() {
  if (!el.juiceFiltersToggle) return;
  el.juiceFiltersToggle.textContent = t("btn.filters");
  delete el.juiceFiltersToggle.dataset.baseLabel;
}

function applyJuicePrefs() {
  const prefs = readJuicePrefs();
  if (el.juiceMaxVideos) el.juiceMaxVideos.value = String(prefs.max_videos);
  if (el.juiceMaxQueries) el.juiceMaxQueries.value = String(prefs.max_queries);
  if (el.juicePerQuery) el.juicePerQuery.value = String(prefs.per_query);
  if (el.juiceMinDuration) el.juiceMinDuration.value = String(prefs.min_duration_sec);
  if (el.juiceMaxDuration) el.juiceMaxDuration.value = String(prefs.max_duration_sec);
  if (el.juiceFast) el.juiceFast.checked = Boolean(prefs.captions_only);
  if (el.juicePrivate) el.juicePrivate.checked = Boolean(prefs.private_run);
  if (el.juiceFiltersPanel) el.juiceFiltersPanel.hidden = true;
  setJuiceFiltersToggleLabel();
}

function persistJuicePrefs() {
  const prefs = collectJuicePrefs();
  writeJuicePrefs(prefs);
  setJuiceFiltersToggleLabel();
}

function directResultButtonsHtml({ url, videoLink, audioLink, allowBuild = true }) {
  const safeUrl = String(url || "").trim();
  const safeVideo = String(videoLink || "").trim();
  const safeAudio = String(audioLink || "").trim();
  const canBuild = allowBuild !== false;
  const vBtn = safeVideo
    ? `<a class="btn ghost direct-download-link" href="${escapeHtml(safeHref(safeVideo))}" target="_blank" rel="noreferrer" download data-url="${encodeURIComponent(safeUrl)}">${escapeHtml(t("btn.download_video"))}</a>`
    : (canBuild
      ? `<button class="btn ghost direct-build-link" type="button" data-kind="video" data-url="${encodeURIComponent(safeUrl)}">${escapeHtml(t("btn.download_video"))}</button>`
      : "");
  const aBtn = safeAudio
    ? `<a class="btn ghost direct-download-link" href="${escapeHtml(safeHref(safeAudio))}" target="_blank" rel="noreferrer" download data-url="${encodeURIComponent(safeUrl)}">${escapeHtml(t("btn.download_audio"))}</a>`
    : (canBuild
      ? `<button class="btn ghost direct-build-link" type="button" data-kind="audio" data-url="${encodeURIComponent(safeUrl)}">${escapeHtml(t("btn.download_audio"))}</button>`
      : "");
  return `
    ${vBtn}
    ${aBtn}
  `;
}

function renderDirectPreview(url, title = "") {
  if (!el.directPreview) return;
  void url;
  void title;
  el.directPreview.innerHTML = "";
}

function directSaveProgressHtml(url, entry = null) {
  const key = String(url || "").trim();
  if (!key) return "";
  const liveRow = state.directSaveProgress.get(key);
  const fallback = entry && typeof entry === "object" ? entry : {};
  let row = liveRow;
  if (!row || !String(row.status || "").trim()) {
    const fallbackStatus = effectiveRecentSaveStatus(fallback);
    if (fallbackStatus === "started" || fallbackStatus === "running") {
      row = {
        status: "running",
        percent: 18,
        message: "Server save in progress...",
      };
    } else if (fallbackStatus === "error") {
      row = {
        status: "error",
        percent: 100,
        message: "Save failed.",
      };
    }
  }
  if (!row || !String(row.status || "").trim()) return "";
  const status = String(row.status || "").trim().toLowerCase();
  const percentRaw = Number(row.percent || 0);
  const percent = Math.max(0, Math.min(100, Number.isFinite(percentRaw) ? percentRaw : 0));
  const barClass = status === "done" ? "is-done" : (status === "error" ? "is-error" : "is-running");
  const msg = String(row.message || "").trim()
    || (status === "done" ? "Saved on server." : (status === "error" ? "Save failed." : "Saving on server..."));
  return `
    <div class="direct-save-progress ${barClass}">
      <div class="direct-save-progress-bar" style="width:${percent}%"></div>
    </div>
    <p class="meta-line save-progress-line">${escapeHtml(msg)}</p>
  `;
}

function setDirectSaveProgress(url, patch = {}) {
  const key = String(url || "").trim();
  if (!key) return;
  const prev = state.directSaveProgress.get(key) || {};
  const merged = { ...prev, ...patch };
  const pct = Number(merged.percent || 0);
  merged.percent = Math.max(0, Math.min(100, Number.isFinite(pct) ? pct : 0));
  merged.status = String(merged.status || "").trim().toLowerCase();
  state.directSaveProgress.set(key, merged);
  renderRecentDirectSearches();
  const activeUrl = String((state.directResultContext || {}).url || "").trim();
  if (activeUrl && activeUrl === key) renderDirectResultCard(state.directResultContext);
  renderSelectedJob();
}

function renderDirectResultCard(ctx, options = {}) {
  if (!el.directOutput) return;
  if (!ctx || typeof ctx !== "object") {
    el.directOutput.innerHTML = "";
    return;
  }
  const allowBuild = Object.prototype.hasOwnProperty.call(options, "allowBuild")
    ? options.allowBuild !== false
    : (ctx.allowBuild !== false);
  const url = String(ctx.url || "").trim();
  const title = String(ctx.title || "").trim() || extractYouTubeVideoId(url) || "Video";
  const thumb = String(ctx.thumb || "").trim() || getVideoThumb(extractYouTubeVideoId(url));
  const videoLink = String(ctx.videoLink || "").trim();
  const audioLink = String(ctx.audioLink || "").trim();
  const recent = readRecentDirectSearches().find((x) => String(x.url || "").trim() === url) || {};
  const savedUrl = String(ctx.savedVideoUrl || recent.saved_video_url || "").trim();

  el.directOutput.innerHTML = `
    <article class="direct-result-card">
      <img src="${escapeHtml(thumb)}" alt="" loading="lazy" />
      <div class="body">
        <p class="title">${escapeHtml(title)}</p>
        ${savedUrl ? `<p class="meta-line"><a href="${escapeHtml(safeHref(savedUrl))}" target="_blank" rel="noreferrer" download>Saved video</a></p>` : ""}
        <div class="row direct-action-row">
          ${directResultButtonsHtml({ url, videoLink, audioLink, allowBuild })}
          <a class="btn ghost" href="${escapeHtml(safeHref(url))}" target="_blank" rel="noreferrer">Open source</a>
        </div>
        ${directSaveProgressHtml(url)}
      </div>
    </article>
  `;

  el.directOutput.querySelectorAll(".direct-build-link").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const rawUrl = decodeURIComponent(btn.dataset.url || "");
      const kind = String(btn.dataset.kind || "video");
      try {
        setMeta(el.directMeta, `Preparing ${kind} link...`);
        await buildAndRememberDirectLink(rawUrl, kind);
        await runDirectPrepare();
      } catch (err) {
        setMeta(el.directMeta, String(err.message || err), true);
      }
    });
  });
}

async function monitorDirectSaveProgress(url, videoId = "") {
  const key = String(url || "").trim();
  if (!key) return;
  const vid = String(videoId || "").trim();
  let percent = 22;
  for (let i = 0; i < 45; i += 1) {
    try {
      const data = await apiGet("/api/videos");
      const items = Array.isArray(data.items) ? data.items : [];
      const row = items.find((x) => {
        const xVid = String(x.video_id || "").trim();
        if (vid && xVid && xVid === vid) return true;
        const src = String(x.source_url || x.youtube_url || "").trim();
        return src && src === key;
      });
      const saved = String((row || {}).public_url || "").trim();
      if (saved) {
        const rowTitle = String((row || {}).title || "").trim();
        patchRecentDirectSearch(key, {
          video_id: String((row || {}).video_id || vid || extractYouTubeVideoId(key)),
          title: rowTitle,
          save_status: "saved",
          saved_video_url: saved,
        });
        setDirectSaveProgress(key, {
          status: "done",
          percent: 100,
          message: "Saved on server.",
        });
        loadVideos(true).catch(() => {});
        return;
      }
    } catch (_err) {
      // keep polling
    }
    percent = Math.min(95, percent + (i < 10 ? 4 : 2));
    setDirectSaveProgress(key, {
      status: "running",
      percent,
      message: `Saving on server... ${Math.round(percent)}%`,
    });
    await new Promise((resolve) => window.setTimeout(resolve, 2000));
  }
  setDirectSaveProgress(key, {
    status: "error",
    percent: 100,
    message: "Save status timed out. Refresh and retry if file is still missing.",
  });
  patchRecentDirectSearch(key, { save_status: "error" });
}

async function buildAndRememberDirectLink(url, kind) {
  const isVideo = kind === "video";
  const path = isVideo ? "/api/direct_video" : "/api/direct_audio";
  const data = await apiPost(path, { url });
  const directUrl = String(data.download_url || "").trim();
  const videoId = String(data.video_id || "").trim();
  const saveStarted = Boolean(data.save_started);
  const saveStatusRaw = String(data.save_status || "").trim().toLowerCase();
  const saveBusy = Boolean(data.save_busy) || saveStatusRaw === "busy";
  const savedVideoUrl = String(data.public_url || "").trim();
  const saveJobId = String(data.save_job_id || "").trim();
  const rawTitle = String(data.title || "").trim();
  const known = state.videos.find((v) => String(v.video_id || "").trim() === videoId);
  const knownTitle = String((known || {}).title || "").trim();
  const prior = readRecentDirectSearches().find((x) => String(x.url || "").trim() === String(url || "").trim()) || {};
  const recentTitle = String(prior.title || "").trim();
  const title = preferredDirectTitle({ rawTitle, knownTitle, recentTitle, videoId, url });
  const thumb = getVideoThumb(videoId || extractYouTubeVideoId(url));
  rememberRecentDirectSearch({
    url,
    title,
    media_type: data.media_type || (isVideo ? "video" : "audio"),
    video_id: videoId || extractYouTubeVideoId(url),
    thumbnail_url: thumb,
    download_url: directUrl,
    save_status: savedVideoUrl ? "saved" : (saveStarted ? "started" : (saveStatusRaw || "")),
    saved_video_url: savedVideoUrl,
    save_job_id: saveJobId,
  });
  if (saveStarted) {
    if (savedVideoUrl) {
      setDirectSaveProgress(url, {
        status: "done",
        percent: 100,
        message: "Saved on server.",
      });
    } else {
      setDirectSaveProgress(url, {
        status: "running",
        percent: 18,
        message: "Server save started (direct links blocked by YouTube).",
      });
      monitorDirectSaveProgress(url, videoId || extractYouTubeVideoId(url)).catch(() => {});
    }
  } else if (saveBusy) {
    setDirectSaveProgress(url, {
      status: "error",
      percent: 100,
      message: String(data.save_busy_message || "Another save is already running."),
    });
  }
  return {
    download_url: directUrl,
    video_id: videoId,
    title,
    thumbnail_url: thumb,
    media_type: String(data.media_type || "").trim(),
    save_started: saveStarted,
    save_status: saveStatusRaw,
    save_busy: saveBusy,
    save_busy_message: String(data.save_busy_message || ""),
    public_url: savedVideoUrl,
    save_job_id: saveJobId,
  };
}

function shouldStartDirectServerSave(url) {
  const key = String(url || "").trim();
  if (!key) return false;
  const recent = readRecentDirectSearches().find((x) => String(x.url || "").trim() === key) || {};
  if (String(recent.saved_video_url || "").trim()) return false;

  const runningElsewhere = Array.from(state.directSaveProgress.entries()).find(([savedUrl, row]) => {
    const status = String((row || {}).status || "").trim().toLowerCase();
    if (savedUrl === key) return false;
    return status === "running" || status === "started";
  });
  if (runningElsewhere) return false;

  const saveStatus = effectiveRecentSaveStatus(recent);
  if (saveStatus === "saved" || saveStatus === "started" || saveStatus === "running") return false;

  const progress = state.directSaveProgress.get(key) || {};
  const progressStatus = String(progress.status || "").trim().toLowerCase();
  if (progressStatus && progressStatus !== "error") return false;

  return true;
}

async function runSaveOnServer(url) {
  const srcUrl = String(url || "").trim();
  if (!srcUrl) {
    setMeta(el.directMeta, "YouTube URL is required.", true);
    return null;
  }
  const runningElsewhere = Array.from(state.directSaveProgress.entries()).find(([savedUrl, row]) => {
    const status = String((row || {}).status || "").trim().toLowerCase();
    if (savedUrl === srcUrl) return false;
    return status === "running" || status === "started";
  });
  if (runningElsewhere) {
    const activeUrl = String(runningElsewhere[0] || "").trim();
    setMeta(el.directMeta, `Another save is already running. Finish it first: ${activeUrl}`, true);
    return null;
  }
  setDirectSaveProgress(srcUrl, {
    status: "running",
    percent: 8,
    message: "Starting server save...",
  });
  setMeta(el.directMeta, "Starting server save...");
  try {
    const data = await apiPost("/api/direct_save_server", { url: srcUrl });
    const resolvedVideoId = String(data.video_id || "").trim() || extractYouTubeVideoId(srcUrl);
    const rawTitle = String(data.title || "").trim();
    const known = state.videos.find((v) => String(v.video_id || "").trim() === resolvedVideoId);
    const knownTitle = String((known || {}).title || "").trim();
    const prior = readRecentDirectSearches().find((x) => String(x.url || "").trim() === srcUrl) || {};
    const recentTitle = String(prior.title || "").trim();
    const resolvedTitle = preferredDirectTitle({
      rawTitle,
      knownTitle,
      recentTitle,
      videoId: resolvedVideoId,
      url: srcUrl,
    });
    const status = String(data.status || "").trim().toLowerCase();
    const busy = Boolean(data.busy) || status === "busy";
    const immediateSavedUrl = String(data.public_url || "").trim();
    const isSaved = Boolean(immediateSavedUrl) || status === "already_saved";
    const inProgress = status === "started" || status === "already_running";
    patchRecentDirectSearch(srcUrl, {
      video_id: resolvedVideoId,
      title: resolvedTitle,
      save_job_id: String(data.save_job_id || "").trim(),
      save_status: isSaved ? "saved" : (busy ? "busy" : (inProgress ? "started" : String(status || "started"))),
      save_requested_at: new Date().toISOString(),
      saved_video_url: immediateSavedUrl,
    });
    if (isSaved) {
      setDirectSaveProgress(srcUrl, {
        status: "done",
        percent: 100,
        message: "Saved on server.",
      });
      setMeta(el.directMeta, `Already saved on server: ${resolvedTitle}.`);
    } else if (busy) {
      setDirectSaveProgress(srcUrl, {
        status: "error",
        percent: 100,
        message: String(data.busy_message || "Another save is already running."),
      });
      setMeta(el.directMeta, String(data.busy_message || "Another save is already running. Please wait."), true);
      return data;
    } else {
      setDirectSaveProgress(srcUrl, {
        status: "running",
        percent: 18,
        message: "Server save started...",
      });
      monitorDirectSaveProgress(srcUrl, resolvedVideoId).catch(() => {});
    }
    if (syncRecentSavedLinksFromVideos()) renderRecentDirectSearches();
    setMeta(
      el.directMeta,
      `Server save started for ${resolvedTitle}. It will appear in storage when download completes.`
    );
    return data;
  } catch (err) {
    const msg = String(err.message || err);
    setDirectSaveProgress(srcUrl, {
      status: "error",
      percent: 100,
      message: msg.length > 140 ? `${msg.slice(0, 137)}...` : msg,
    });
    setMeta(el.directMeta, String(err.message || err), true);
    return null;
  }
}

function directServerActionButtonHtml(entry) {
  const row = entry && typeof entry === "object" ? entry : {};
  const srcUrl = String(row.url || "").trim();
  if (!srcUrl) return "";
  const savedUrl = resolvedSavedVideoUrl(row);
  if (savedUrl) {
    return `<a class="btn ghost" href="${escapeHtml(safeHref(savedUrl))}" target="_blank" rel="noreferrer" download>${escapeHtml(t("btn.download_from_server"))}</a>`;
  }
  const progress = state.directSaveProgress.get(srcUrl) || {};
  const saveStatus = String(progress.status || "").trim().toLowerCase() || effectiveRecentSaveStatus(row);
  if (saveStatus === "running" || saveStatus === "started") {
    return `<button class="btn ghost" type="button" disabled>Saving...</button>`;
  }
  const runningElsewhere = Array.from(state.directSaveProgress.entries()).find(([savedUrlKey, rec]) => {
    const status = String((rec || {}).status || "").trim().toLowerCase();
    if (savedUrlKey === srcUrl) return false;
    return status === "running" || status === "started";
  });
  if (runningElsewhere) {
    return `<button class="btn ghost" type="button" disabled>Save Busy</button>`;
  }
  const label = saveStatus === "error" ? "Retry Save" : t("btn.download_to_server");
  return `<button class="btn ghost direct-save-server-btn" type="button" data-url="${encodeURIComponent(srcUrl)}">${escapeHtml(label)}</button>`;
}

function ensureRecentDirectMonitor(entry) {
  const row = entry && typeof entry === "object" ? entry : {};
  const srcUrl = String(row.url || "").trim();
  if (!srcUrl) return;
  if (resolvedSavedVideoUrl(row)) return;
  const saveStatus = effectiveRecentSaveStatus(row);
  if (saveStatus !== "started" && saveStatus !== "running") return;
  if (state.directSaveWatchers.has(srcUrl)) return;
  state.directSaveWatchers.add(srcUrl);
  monitorDirectSaveProgress(srcUrl, String(row.video_id || "").trim())
    .catch(() => {})
    .finally(() => {
      state.directSaveWatchers.delete(srcUrl);
    });
}

function isDirectVaultCandidate(video) {
  const row = video && typeof video === "object" ? video : {};
  const source = String(row.transcript_source || "").trim().toLowerCase();
  const hasKnownSource = Boolean(source && source !== "unknown");
  const hasSavedFile = Boolean(String(row.public_url || "").trim());
  return hasKnownSource || hasSavedFile || Boolean(row.has_transcript);
}

function _directRecentKey(entry) {
  const row = entry && typeof entry === "object" ? entry : {};
  const rawUrl = String(row.url || "").trim();
  const vid = String(row.video_id || "").trim() || extractYouTubeVideoId(rawUrl);
  if (vid) return `vid:${vid}`;
  return rawUrl ? `url:${rawUrl}` : "";
}

function _savedVideosRecentEntries() {
  const out = [];
  for (const row of (state.vaultVideos || [])) {
    if (!isDirectVaultCandidate(row)) continue;
    const videoId = String(row.video_id || "").trim();
    const sourceUrl = String(row.source_url || "").trim();
    const youtubeUrl = String(row.youtube_url || "").trim() || (videoId ? `https://www.youtube.com/watch?v=${videoId}` : "");
    const url = sourceUrl || youtubeUrl;
    if (!url) continue;
    const context = existingDirectContextFromVideo(row);
    const resolvedVideoId = videoId || extractYouTubeVideoId(url);
    const transcriptTs = Number(row.transcript_updated_at_epoch || 0);
    const analysisTs = Number(row.analysis_saved_at_epoch || 0);
    const bestTs = Math.max(
      0,
      Number.isFinite(transcriptTs) ? transcriptTs : 0,
      Number.isFinite(analysisTs) ? analysisTs : 0
    );
    const updatedAt = bestTs > 0 ? new Date(bestTs * 1000).toISOString() : new Date().toISOString();
    const savedVideoUrl = String(row.public_url || context.savedVideoUrl || "").trim();
    const recent = normalizeRecentDirectEntry({
      url,
      video_id: resolvedVideoId,
      title: preferredDirectTitle({
        rawTitle: String(row.title || "").trim(),
        knownTitle: String(context.title || "").trim(),
        recentTitle: "",
        videoId: resolvedVideoId,
        url,
      }),
      thumbnail_url: String(row.thumbnail_url || context.thumb || getVideoThumb(resolvedVideoId)).trim(),
      links: {
        video: String(context.videoLink || "").trim(),
        audio: String(context.audioLink || "").trim(),
      },
      saved_video_url: savedVideoUrl,
      save_status: savedVideoUrl ? "saved" : "",
      created_at: updatedAt,
      updated_at: updatedAt,
    });
    if (recent) out.push(recent);
  }
  return out;
}

function _videoIdFromRecentLike(entry) {
  const row = entry && typeof entry === "object" ? entry : {};
  const direct = String(row.video_id || "").trim();
  if (direct) return direct;
  const u1 = String(row.url || "").trim();
  if (u1) {
    const x1 = extractYouTubeVideoId(u1);
    if (x1) return x1;
  }
  const u2 = String(row.source_url || row.youtube_url || "").trim();
  if (u2) {
    const x2 = extractYouTubeVideoId(u2);
    if (x2) return x2;
  }
  return "";
}

function _urlsMatchByVideo(urlA, urlB) {
  const a = String(urlA || "").trim();
  const b = String(urlB || "").trim();
  if (!a || !b) return false;
  if (a === b) return true;
  const aVid = extractYouTubeVideoId(a);
  const bVid = extractYouTubeVideoId(b);
  return Boolean(aVid && bVid && aVid === bVid);
}

function findSavedVideoRowForRecent(entry) {
  const row = entry && typeof entry === "object" ? entry : {};
  const vid = _videoIdFromRecentLike(row);
  const src = String(row.url || "").trim();
  const videos = Array.isArray(state.videos) ? state.videos : [];
  return (
    videos.find((x) => {
      const xVid = _videoIdFromRecentLike(x);
      return Boolean(vid && xVid && vid === xVid);
    })
    || videos.find((x) => {
      const xSrc = String(x.source_url || x.youtube_url || "").trim();
      return Boolean(src && xSrc && _urlsMatchByVideo(xSrc, src));
    })
    || null
  );
}

function resolvedSavedVideoUrl(entry) {
  const row = entry && typeof entry === "object" ? entry : {};
  const direct = String(row.saved_video_url || "").trim();
  if (direct) return direct;
  const savedRow = findSavedVideoRowForRecent(row);
  return String((savedRow || {}).public_url || "").trim();
}

function mergedRecentDirectSearches() {
  const localItems = readRecentDirectSearches();
  const savedItems = _savedVideosRecentEntries();
  const merged = new Map();

  for (const row of savedItems) {
    const key = _directRecentKey(row);
    if (!key) continue;
    merged.set(key, row);
  }

  for (const row of localItems) {
    const key = _directRecentKey(row);
    if (!key) continue;
    const prev = merged.get(key) || {};
    const prevLinks = prev.links && typeof prev.links === "object" ? prev.links : {};
    const rowLinks = row.links && typeof row.links === "object" ? row.links : {};
    const url = String(row.url || prev.url || "").trim();
    const videoId = String(row.video_id || prev.video_id || extractYouTubeVideoId(url)).trim();
    const savedVideoUrl = String(row.saved_video_url || prev.saved_video_url || "").trim();
    const normalized = normalizeRecentDirectEntry({
      ...prev,
      ...row,
      url,
      video_id: videoId,
      title: preferredDirectTitle({
        rawTitle: String(row.title || "").trim(),
        knownTitle: String(prev.title || "").trim(),
        recentTitle: "",
        videoId,
        url,
      }),
      thumbnail_url: String(row.thumbnail_url || prev.thumbnail_url || getVideoThumb(videoId)).trim(),
      links: {
        video: String(rowLinks.video || prevLinks.video || "").trim(),
        audio: String(rowLinks.audio || prevLinks.audio || "").trim(),
      },
      saved_video_url: savedVideoUrl,
      save_status: _saveStatus(row.save_status || prev.save_status || (savedVideoUrl ? "saved" : "")),
      created_at: String(row.created_at || prev.created_at || new Date().toISOString()),
      updated_at: String(row.updated_at || prev.updated_at || new Date().toISOString()),
    });
    if (normalized) merged.set(key, normalized);
  }

  return Array.from(merged.values()).sort((a, b) => String(b.updated_at || "").localeCompare(String(a.updated_at || "")));
}

function renderRecentDirectSearches() {
  if (!el.directRecentList) return;
  const items = mergedRecentDirectSearches();
  el.directRecentList.innerHTML = items.length
    ? items
        .map(
          (x) => {
            const savedUrl = resolvedSavedVideoUrl(x);
            const links = x.links && typeof x.links === "object" ? x.links : {};
            return `
            <article class="direct-recent-card" data-url="${encodeURIComponent(String(x.url || ""))}">
              <img src="${escapeHtml(x.thumbnail_url || getVideoThumb(x.video_id || ""))}" alt="" loading="lazy" />
              <div class="body">
                <p class="title">${escapeHtml(resolveRecentVideoTitle(x))}</p>
                <p class="meta-line mono">${escapeHtml(formatTime(x.updated_at || x.created_at))}</p>
                <p class="meta-line"><a href="${escapeHtml(safeHref(x.url || "#"))}" target="_blank" rel="noreferrer">Open source</a></p>
                ${
                  savedUrl
                    ? `<p class="meta-line"><a href="${escapeHtml(safeHref(savedUrl))}" target="_blank" rel="noreferrer" download>Saved video</a></p>`
                    : ""
                }
                <div class="row direct-action-row">
                  ${directServerActionButtonHtml({ ...x, saved_video_url: savedUrl })}
                  ${directResultButtonsHtml({
                    url: x.url,
                    videoLink: String(links.video || "").trim(),
                    audioLink: String(links.audio || "").trim(),
                    allowBuild: true,
                  })}
                </div>
                ${directSaveProgressHtml(x.url, x)}
              </div>
            </article>
          `;
          }
        )
        .join("")
    : `<p class="meta">${escapeHtml(t("status.no_recent_searches"))}</p>`;

  el.directRecentList.querySelectorAll(".direct-recent-card").forEach((card) => {
    card.addEventListener("click", (ev) => {
      const tgt = ev.target;
      if (tgt instanceof HTMLElement && (tgt.closest(".btn") || tgt.closest("a"))) return;
      if (el.directUrlInput) el.directUrlInput.value = decodeURIComponent(card.dataset.url || "");
      el.directUrlInput?.focus();
    });
  });
  el.directRecentList.querySelectorAll(".direct-build-link").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const rawUrl = decodeURIComponent(String(btn.dataset.url || ""));
      const kind = String(btn.dataset.kind || "video");
      if (!rawUrl) return;
      btn.disabled = true;
      try {
        setMeta(el.directMeta, `Preparing ${kind} link...`);
        await buildAndRememberDirectLink(rawUrl, kind);
        renderRecentDirectSearches();
      } catch (err) {
        setMeta(el.directMeta, String(err.message || err), true);
      } finally {
        btn.disabled = false;
      }
    });
  });
  el.directRecentList.querySelectorAll(".direct-save-server-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const rawUrl = decodeURIComponent(String(btn.dataset.url || ""));
      if (!rawUrl) return;
      if (!shouldStartDirectServerSave(rawUrl)) {
        setMeta(el.directMeta, "Server save already in progress or already available.");
        return;
      }
      btn.disabled = true;
      try {
        await runSaveOnServer(rawUrl);
      } finally {
        btn.disabled = false;
      }
    });
  });
  items.forEach((entry) => ensureRecentDirectMonitor(entry));
}

function makeListItem(item, active, line1, line2) {
  return `
    <button class="item${active ? " active" : ""}" data-id="${escapeHtml(item)}">
      <div class="line-1">${escapeHtml(line1)}</div>
      <div class="line-2">${escapeHtml(line2)}</div>
    </button>
  `;
}

function findRecentDirectEntryForVideo(video) {
  const row = video && typeof video === "object" ? video : {};
  const items = readRecentDirectSearches();
  const vid = String(row.video_id || "").trim();
  const src = String(row.source_url || row.youtube_url || "").trim();
  if (!items.length) return null;
  return (
    items.find((x) => String(x.video_id || "").trim() === vid)
    || items.find((x) => String(x.url || "").trim() === src)
    || items.find((x) => {
      const xId = extractYouTubeVideoId(String(x.url || "").trim());
      return Boolean(vid && xId && xId === vid);
    })
    || null
  );
}

function existingDirectContextFromVideo(video) {
  const row = video && typeof video === "object" ? video : {};
  const recent = findRecentDirectEntryForVideo(row) || {};
  const links = recent.links && typeof recent.links === "object" ? recent.links : {};
  const url = String(row.source_url || row.youtube_url || recent.url || "").trim();
  const videoFileUrl = String(row.public_url || recent.saved_video_url || "").trim();
  const directVideoUrl = String(links.video || "").trim();
  const audioUrl = String(links.audio || "").trim();
  return {
    url,
    title: String(row.title || recent.title || row.video_id || "Video").trim(),
    thumb: String(row.thumbnail_url || recent.thumbnail_url || getVideoThumb(row.video_id || "")).trim(),
    videoLink: videoFileUrl || directVideoUrl,
    audioLink: audioUrl,
    savedVideoUrl: videoFileUrl,
    allowBuild: false,
  };
}

function formatTime(ts) {
  if (!ts) return "unknown";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return String(ts);
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatStatus(status) {
  const raw = String(status || "").toLowerCase();
  if (!raw) return "Unknown";
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function formatLlmBackendLabel(rawBackend) {
  const raw = String(rawBackend || "").trim().toLowerCase();
  if (raw === "claude" || raw === "openai") return "Remote";
  if (raw === "local" || raw === "local_fallback") return "Local";
  return "Unknown";
}

function llmModeFromBackend(rawBackend) {
  const label = formatLlmBackendLabel(rawBackend);
  return label === "Unknown" ? "" : label.toLowerCase();
}

function llmModeFromVideoItem(item) {
  const mode = String(item?.analysis_llm_mode || "").trim().toLowerCase();
  if (mode === "local" || mode === "remote") return mode;
  return llmModeFromBackend(item?.analysis_llm_backend);
}

function llmModeText(mode) {
  const m = String(mode || "").trim().toLowerCase();
  if (m === "local") return "local";
  if (m === "remote") return "remote";
  return "Unknown";
}

const FUN_STATUS_LINES = {
  analyze: {
    en: [
      "Combobulating",
      "Polishing",
      "Packing",
      "Brewing",
      "Distilling",
      "Filtering",
      "Sifting",
      "Mapping",
      "Scanning",
      "Indexing",
      "Comparing",
      "Refining",
      "Finalizing",
      "Synthesizing",
    ],
    uk: [
      "Combobulating",
      "Polishing",
      "Packing",
      "Brewing",
      "Distilling",
      "Filtering",
      "Sifting",
      "Mapping",
      "Scanning",
      "Indexing",
      "Comparing",
      "Refining",
      "Finalizing",
      "Synthesizing",
    ],
  },
  brew: {
    en: [
      "Combobulating",
      "Polishing",
      "Packing",
      "Brewing",
      "Distilling",
      "Filtering",
      "Sifting",
      "Mapping",
      "Scanning",
      "Indexing",
      "Comparing",
      "Refining",
      "Finalizing",
      "Synthesizing",
    ],
    uk: [
      "Combobulating",
      "Polishing",
      "Packing",
      "Brewing",
      "Distilling",
      "Filtering",
      "Sifting",
      "Mapping",
      "Scanning",
      "Indexing",
      "Comparing",
      "Refining",
      "Finalizing",
      "Synthesizing",
    ],
  },
};

function funStatusLine(kind, seed = 0) {
  const bucket = FUN_STATUS_LINES[kind] || {};
  const lang = _uiLang === "uk" ? "uk" : "en";
  const list = Array.isArray(bucket[lang]) && bucket[lang].length
    ? bucket[lang]
    : (Array.isArray(bucket.en) ? bucket.en : []);
  if (!list.length) return "";
  const n = Math.max(0, Number.isFinite(Number(seed)) ? Math.trunc(Number(seed)) : 0);
  return String(list[n % list.length] || "").trim();
}

function setButtonLoading(button, isLoading, loadingLabel) {
  if (!button) return;
  const baseLabel = String(button.dataset.baseLabel || button.textContent || "").trim();
  if (!button.dataset.baseLabel) button.dataset.baseLabel = baseLabel;
  if (isLoading) {
    button.classList.add("is-loading");
    button.setAttribute("aria-busy", "true");
    if (loadingLabel) button.textContent = loadingLabel;
    return;
  }
  button.classList.remove("is-loading");
  button.removeAttribute("aria-busy");
  button.textContent = String(button.dataset.baseLabel || baseLabel || button.textContent || "").trim();
}

function setSectionLoading(section, metaNode, isLoading) {
  if (section) section.classList.toggle("is-busy", Boolean(isLoading));
  if (metaNode) metaNode.classList.toggle("meta-loading", Boolean(isLoading));
}

function activeNotesTask() {
  const localTask = String(state.notesBusyTask || "").trim().toLowerCase();
  if (localTask) return localTask;
  return String(state.notesRemoteTask || "").trim().toLowerCase();
}

function syncNotesActionButtons() {
  const active = activeNotesTask();
  const busy = active === "ask" || active === "analyze";
  if (el.askBtn) el.askBtn.disabled = busy;
  if (el.analyzeBtn) el.analyzeBtn.disabled = busy;
}

function tryStartNotesTask(task, metaNode) {
  const wanted = String(task || "").trim().toLowerCase();
  if (!wanted) return false;
  const active = activeNotesTask();
  if (active) {
    if (active === wanted) {
      const key = wanted === "ask" ? "status.ask_already_running" : "status.analysis_already_running";
      setMeta(metaNode, t(key));
    } else {
      const key = active === "ask" ? "status.analysis_blocked_by_ask" : "status.ask_blocked_by_analysis";
      setMeta(metaNode, t(key), true);
    }
    syncNotesActionButtons();
    return false;
  }
  state.notesBusyTask = wanted;
  state.notesRemoteTask = "";
  syncNotesActionButtons();
  return true;
}

function finishNotesTask(task) {
  const wanted = String(task || "").trim().toLowerCase();
  if (String(state.notesBusyTask || "").trim().toLowerCase() === wanted) {
    state.notesBusyTask = "";
  }
  syncNotesActionButtons();
}

function _isTaskRunningStatus(rawStatus) {
  const status = String(rawStatus || "").trim().toLowerCase();
  return status === "running" || status === "started" || status === "queued";
}

function applyServerNotesProgress(videoId, raw) {
  const selected = String(state.selectedVideoId || "").trim();
  const target = String(videoId || "").trim();
  if (!selected || !target || selected !== target) return;

  const payload = raw && typeof raw === "object" ? raw : {};
  const ask = payload.ask && typeof payload.ask === "object" ? payload.ask : {};
  const analyze = payload.analyze && typeof payload.analyze === "object" ? payload.analyze : {};
  const askRunning = Boolean(ask.in_progress) || _isTaskRunningStatus(ask.status);
  const analyzeRunning = Boolean(analyze.in_progress) || _isTaskRunningStatus(analyze.status);
  const prevRemote = String(state.notesRemoteTask || "").trim().toLowerCase();
  state.notesRemoteTask = askRunning ? "ask" : (analyzeRunning ? "analyze" : "");
  syncNotesActionButtons();

  if (state.notesBusyTask) return;

  if (askRunning) {
    const elapsed = Math.max(0, Math.round(Number(ask.elapsed_sec || 0)));
    const msg = String(ask.message || t("status.asking_transcript")).trim() || t("status.asking_transcript");
    setSectionLoading(el.askSection, el.qaMeta, true);
    setButtonLoading(el.askBtn, true, `${t("btn.ask")}...`);
    setMeta(el.qaMeta, elapsed > 0 ? `${msg} ${elapsed}s` : msg);
  } else if (prevRemote === "ask") {
    setSectionLoading(el.askSection, el.qaMeta, false);
    setButtonLoading(el.askBtn, false);
    if (!String(el.qaOutput?.textContent || "").trim()) setMeta(el.qaMeta, "");
  }

  if (analyzeRunning) {
    const elapsed = Math.max(0, Math.round(Number(analyze.elapsed_sec || 0)));
    const total = Math.max(0, Math.trunc(Number(analyze.chunk_total || 0)));
    const done = Math.max(0, Math.trunc(Number(analyze.chunk_completed || 0)));
    const msg = String(analyze.message || t("status.running_analysis")).trim() || t("status.running_analysis");
    const parts = total > 0 ? ` | parts ${Math.min(done, total)}/${total}` : "";
    const elapsedTxt = elapsed > 0 ? ` ${elapsed}s` : "";
    setSectionLoading(el.analyzeSection, el.analysisMeta, true);
    setButtonLoading(el.analyzeBtn, true, `${t("btn.run_analysis")}...`);
    setMeta(el.analysisMeta, `${msg}${elapsedTxt}${parts}`);
  } else if (prevRemote === "analyze") {
    setSectionLoading(el.analyzeSection, el.analysisMeta, false);
    setButtonLoading(el.analyzeBtn, false);
    const hasAnalysis = Boolean(String(el.analysisOutput?.textContent || "").trim());
    setMeta(el.analysisMeta, hasAnalysis ? t("status.loaded_saved_analysis") : t("status.no_analysis_saved"));
  }
}

function rotatingFunStatus(kind, startedAt, offset = 0, stepMs = 1200) {
  const base = Math.max(250, Number.isFinite(Number(stepMs)) ? Math.trunc(Number(stepMs)) : 1200);
  const seed = Math.max(0, Math.floor((Date.now() - Number(startedAt || Date.now())) / base)) + Math.max(0, Number(offset || 0));
  return funStatusLine(kind, seed);
}

function extractLlmDetailFromText(rawText) {
  const text = String(rawText || "");
  if (!text) return "";
  const lines = text.split(/\r?\n/);
  for (const line of lines) {
    const low = line.toLowerCase();
    const pos = low.indexOf("backend:");
    if (pos < 0) continue;
    const detail = line.slice(pos + "backend:".length).replace(/^[\s:-]+/, "").trim().replace(/\s+/g, " ");
    if (detail) return detail;
  }
  return "";
}

function llmDetailText(rawDetail, rawBackend, textFallback = "") {
  const direct = String(rawDetail || "").trim();
  if (direct && direct.toLowerCase() !== "unknown") return direct;
  const parsed = extractLlmDetailFromText(textFallback);
  if (parsed) return parsed;
  const mode = llmModeText(llmModeFromBackend(rawBackend));
  return mode === "Unknown" ? "unknown" : mode;
}

function isVideoIdLike(raw) {
  return /^[A-Za-z0-9_-]{6,20}$/.test(String(raw || "").trim());
}

function statusClass(status) {
  const raw = String(status || "").toLowerCase();
  if (raw === "running") return "is-running";
  if (raw === "queued") return "is-queued";
  if (raw === "done") return "is-done";
  return "is-default";
}

function brewStageLabel(job) {
  const progress = job && typeof job.progress === "object" ? job.progress : {};
  const totalSteps = Number(progress.total_steps || 5) || 5;
  const step = Math.max(0, Math.min(totalSteps, Number(progress.step || 0)));
  const stepTitles = {
    1: "Understanding goal",
    2: "Generating queries",
    3: "Searching videos",
    4: "Extracting + analyzing transcripts",
    5: "Comparing and finalizing",
  };
  const etype = String((job || {}).last_event_type || "").trim().toLowerCase();
  if (etype === "queries_ready") return "Generating queries";
  if (etype === "search_query_started" || etype === "search_query_processed" || etype === "candidates_ready") {
    return "Searching videos";
  }
  if (etype === "processing_video") return "Downloading transcript";
  if (etype === "video_processed") return "Analyzing transcript";
  if (etype === "comparing") return "Comparing transcripts";
  if (etype === "completed") return "Completed";
  if (etype === "failed") return "Failed";
  return stepTitles[step] || "Preparing";
}

function searchStatsSummary(job) {
  const searchStats = job && typeof job.search_stats === "object" ? job.search_stats : {};
  const queryStats = Array.isArray(job && job.query_stats) ? job.query_stats : [];
  const queries = Number(searchStats.query_count || queryStats.length || 0);
  const seen = Number(searchStats.seen_total || 0);
  const eligible = Number(searchStats.eligible_total || 0);
  if (!queries && !seen && !eligible) return "";
  const topQueries = queryStats
    .slice(0, 2)
    .map((x) => `${String(x.query || "").slice(0, 28)}: ${Number(x.returned || 0)}`)
    .join(" | ");
  const base = `Searched ${queries} ${queries === 1 ? "query" : "queries"}, got ${seen} results, kept ${eligible}.`;
  return topQueries ? `${base} ${topQueries}` : base;
}

function brewActiveThumbs(job, maxItems = 3) {
  const out = [];
  const seen = new Set();
  const pushThumb = (video) => {
    if (!video || typeof video !== "object") return;
    const src = String(video.thumbnail_url || "").trim();
    if (!src || seen.has(src)) return;
    seen.add(src);
    out.push(src);
  };
  pushThumb(job.current_video || {});
  (job.reviewed_videos || []).slice(-2).forEach(pushThumb);
  (job.candidate_videos || []).slice(0, 2).forEach(pushThumb);
  return out.slice(0, Math.max(1, maxItems));
}

function makeBrewingItem(job, active) {
  const ratio = Math.max(0, Math.min(1, Number((job.progress || {}).ratio || 0)));
  const percent = Math.round(ratio * 100);
  const status = formatStatus(job.status);
  const sClass = statusClass(job.status);
  const stage = brewStageLabel(job);
  const thumbs = brewActiveThumbs(job, 3);
  const thumbsHtml = thumbs.length
    ? `<div class="brew-thumb-strip">${thumbs.map((src) => `<img src="${escapeHtml(src)}" alt="" loading="lazy" />`).join("")}</div>`
    : "";
  return `
    <button class="item brewing-item${active ? " active" : ""}" data-id="${escapeHtml(job.job_id || "")}">
      <div class="line-1">
        <span class="topic">${escapeHtml(job.topic || "Topic")}</span>
        <span class="status-chip ${sClass}">${escapeHtml(status)}</span>
      </div>
      <p class="brew-stage-line">${escapeHtml(stage)} · ${percent}%</p>
      ${thumbsHtml}
      <div class="item-progress-wrap"><div class="item-progress-bar" style="width:${percent}%"></div></div>
    </button>
  `;
}

function makeLiveCard(v, isActive = false) {
  const videoId = String(v.video_id || "");
  const thumb = v.thumbnail_url || getVideoThumb(videoId);
  const status = formatStatus(v.archive_status_effective || v.archive_status || "saved");
  const category = String(v.archive_service_label || "Live");
  const youtubeUrlRaw = String(v.youtube_url || v.source_url || "").trim();
  const youtubeUrl = safeHref(youtubeUrlRaw || "#");
  const publicUrlRaw = String(v.public_url || "").trim();
  const publicUrl = safeHref(publicUrlRaw);
  const hasTranscript = Boolean(v.has_transcript);
  const canStop = Boolean(v.can_stop_live);
  const savedFileLine = publicUrlRaw
    ? `<p class="meta-line">${escapeHtml(t("status.saved_file"))}: <a href="${escapeHtml(publicUrl)}" target="_blank" rel="noreferrer" download>${escapeHtml(t("btn.open_saved_file"))}</a></p>`
    : `<p class="meta-line">${escapeHtml(t("status.saved_file"))}: ${escapeHtml(t("status.not_available_yet"))}</p>`;
  return `
    <article class="video-card live-card">
      <img src="${escapeHtml(thumb)}" alt="" loading="lazy" />
      <div class="body">
        <p class="title"><a href="${escapeHtml(youtubeUrl)}" target="_blank" rel="noreferrer">${escapeHtml(
    v.title || videoId || "Live"
  )}</a></p>
        <p class="meta-line">${escapeHtml(v.channel || "Unknown")} | ${escapeHtml(category)}</p>
        <p class="meta-line">${escapeHtml(t("status.status"))}: ${escapeHtml(status)} | ${escapeHtml(v.archive_date_key || "")}</p>
        ${savedFileLine}
        <div class="row">
          ${
            hasTranscript
              ? `<button class="btn ghost live-notes-btn" type="button" data-id="${escapeHtml(videoId)}">${escapeHtml(t("btn.open_notes"))}</button>`
              : ""
          }
          ${
            canStop
              ? `<button class="btn ghost live-stop-btn" type="button" data-id="${escapeHtml(videoId)}">${
                  escapeHtml(isActive ? t("btn.stop_saving") : t("btn.stop_saving_if_running"))
                }</button>`
              : ""
          }
          ${
            publicUrlRaw
              ? `<a class="btn ghost live-link-btn" href="${escapeHtml(publicUrl)}" target="_blank" rel="noreferrer" download>${escapeHtml(t("btn.open_saved_file"))}</a>`
              : ""
          }
          ${
            youtubeUrlRaw
              ? `<a class="btn ghost live-link-btn" href="${escapeHtml(youtubeUrl)}" target="_blank" rel="noreferrer">${escapeHtml(t("btn.youtube"))}</a>`
              : ""
          }
        </div>
      </div>
    </article>
  `;
}

function renderLiveLists(archiveItems) {
  const query = (el.archiveSearchInput?.value || "").trim().toLowerCase();
  const all = Array.isArray(archiveItems) ? archiveItems : [];
  const active = all.filter((v) => Boolean(v.is_live_active));
  const archived = all.filter((v) => !v.is_live_active);
  const filtered = archived.filter((v) => {
    if (!query) return true;
    return `${v.title || ""} ${v.video_id || ""} ${v.archive_service_label || ""}`.toLowerCase().includes(query);
  });

  if (el.activeLiveBlock) el.activeLiveBlock.hidden = active.length === 0;
  if (el.activeLiveList) {
    el.activeLiveList.innerHTML = active.length ? active.map((v) => makeLiveCard(v, true)).join("") : "";
  }
  if (el.savedLiveList) {
    el.savedLiveList.innerHTML = filtered.length
      ? filtered.map((v) => makeLiveCard(v, false)).join("")
      : `<p class="meta">${escapeHtml(t("status.no_saved_live_items"))}</p>`;
  }

  document.querySelectorAll(".live-notes-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await selectVideo(btn.dataset.id);
      switchPage("transcript");
    });
  });
  document.querySelectorAll(".live-stop-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const videoId = String(btn.dataset.id || "").trim();
      if (!videoId) return;
      btn.disabled = true;
      setMeta(el.liveStartMeta, `Stopping live save for ${videoId}...`);
      try {
        const data = await apiPost("/api/live/stop", { video_id: videoId });
        if (String(data.status || "") === "already_finished") {
          setMeta(el.liveStartMeta, "Live save already ended.");
        } else {
          setMeta(el.liveStartMeta, `Stop requested for ${videoId}.`);
        }
        await loadVideos(true);
      } catch (err) {
        const msg = String(err.message || err);
        if (msg.toLowerCase().includes("live recording not found")) {
          setMeta(el.liveStartMeta, "Live save already ended.");
        } else {
          setMeta(el.liveStartMeta, msg, true);
        }
      } finally {
        btn.disabled = false;
      }
    });
  });
}

function renderVideoLists() {
  const vq = (el.vaultSearchInput?.value || "").trim().toLowerCase();
  const vault = state.vaultVideos.filter((v) => {
    if (!isDirectVaultCandidate(v)) return false;
    if (!vq) return true;
    return `${v.title || ""} ${v.video_id || ""}`.toLowerCase().includes(vq);
  });

  if (el.videoList) {
    el.videoList.innerHTML = vault.length
      ? vault
          .map((v) => {
            const rawSource = String(v.transcript_source || "").trim();
            const sourceLabel = rawSource && rawSource.toLowerCase() !== "unknown"
              ? rawSource
              : (String(v.public_url || "").trim() ? "saved file" : "no transcript");
            return makeListItem(
              v.video_id,
              state.selectedVideoId === v.video_id,
              v.title || v.video_id,
              `${v.video_id} | ${sourceLabel}`
            );
          })
          .join("")
      : `<p class="meta">${escapeHtml(t("status.no_saved_videos"))}</p>`;

    el.videoList.querySelectorAll(".item").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const videoId = String(btn.dataset.id || "").trim();
        if (!videoId) return;
        const clickedFromDirect = state.page === "direct";
        if (clickedFromDirect) {
          const picked = state.videos.find((v) => String(v.video_id || "").trim() === videoId) || {};
          const existing = existingDirectContextFromVideo(picked);
          if (existing.url && el.directUrlInput) el.directUrlInput.value = existing.url;
          state.directResultContext = existing;
          renderDirectPreview(existing.url, existing.title);
          renderDirectResultCard(existing, { allowBuild: false });
          const hasAnyLink = Boolean(String(existing.videoLink || "").trim() || String(existing.audioLink || "").trim());
          setMeta(
            el.directMeta,
            hasAnyLink
              ? "Showing existing saved/download links only."
              : "No saved video/audio links for this item."
          );
          return;
        }
        await selectVideo(videoId);
        switchPage("transcript");
        await autoAnalyzeIfTranscript(videoId);
      });
    });
  }

  renderNotesVideoList();
  renderLiveLists(state.archiveVideos);
}

function renderNotesVideoList() {
  if (!el.notesVideoList) return;
  const query = (el.notesSearchInput?.value || "").trim().toLowerCase();
  const noteVideos = state.videos.filter((v) => Boolean(v.has_transcript));
  const filtered = noteVideos
    .slice()
    .sort((a, b) => Number(b.transcript_updated_at_epoch || 0) - Number(a.transcript_updated_at_epoch || 0))
    .filter((v) => {
    if (!query) return true;
    return `${v.title || ""} ${v.video_id || ""}`.toLowerCase().includes(query);
  });

  el.notesVideoList.innerHTML = filtered.length
    ? filtered
        .map((v) => {
          const thumb = String(v.thumbnail_url || "").trim() || getVideoThumb(v.video_id || "");
          const updatedAt = formatTime(
            v.transcript_updated_at_epoch
              ? new Date(Number(v.transcript_updated_at_epoch) * 1000).toISOString()
              : ""
          );
          return `
            <button class="item notes-list-item${state.selectedVideoId === v.video_id ? " active" : ""}" data-id="${escapeHtml(v.video_id)}">
              ${thumb ? `<img class="notes-list-item-thumb" src="${escapeHtml(thumb)}" alt="" loading="lazy" />` : `<div class="notes-list-item-thumb" aria-hidden="true"></div>`}
              <div class="notes-list-item-body">
                <div class="line-1">${escapeHtml(v.title || v.video_id)}</div>
                <div class="line-2">${escapeHtml(`transcript saved | ${updatedAt}`)}</div>
              </div>
            </button>
          `;
        })
        .join("")
    : `<p class="meta">${escapeHtml(t("status.no_transcripts"))}</p>`;

  el.notesVideoList.querySelectorAll(".item").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await selectVideo(btn.dataset.id);
    });
  });
}

function clearSelectedVideoDetail() {
  state.selectedVideoId = "";
  state.notesRemoteTask = "";
  writeUiPrefs();
  if (el.videoTitle) el.videoTitle.textContent = t("status.select_video_from_list");
  if (el.videoFacts) el.videoFacts.textContent = t("status.no_video_selected");
  if (el.videoLink) {
    el.videoLink.href = "#";
    el.videoLink.textContent = "";
    el.videoLink.title = "";
  }
  if (el.videoPreview) {
    el.videoPreview.removeAttribute("src");
    el.videoPreview.alt = "";
    el.videoPreview.hidden = true;
  }
  if (el.analysisOutput) el.analysisOutput.textContent = "";
  if (el.transcriptOutput) el.transcriptOutput.textContent = "";
  if (el.qaOutput) el.qaOutput.textContent = "";
  if (el.qaOutput) el.qaOutput.hidden = true;
  if (el.askSection) el.askSection.hidden = true;
  if (el.analyzeSection) el.analyzeSection.hidden = true;
  setSectionLoading(el.askSection, el.qaMeta, false);
  setSectionLoading(el.analyzeSection, el.analysisMeta, false);
  setButtonLoading(el.askBtn, false);
  setButtonLoading(el.analyzeBtn, false);
  setMeta(el.analysisMeta, "");
  setMeta(el.qaMeta, "");
  syncNotesActionButtons();
}

async function loadVideos(keepSelected = true) {
  const data = await apiGet("/api/videos");
  state.videos = data.items || [];
  syncRecentSavedLinksFromVideos();
  state.vaultVideos = state.videos.filter((v) => !v.is_archive);
  state.archiveVideos = state.videos.filter((v) => Boolean(v.is_archive));
  renderRecentDirectSearches();
  if (!keepSelected || !state.videos.some((x) => x.video_id === state.selectedVideoId)) state.selectedVideoId = "";
  renderVideoLists();
  if (state.selectedVideoId) {
    await loadVideoDetail(state.selectedVideoId);
  } else {
    clearSelectedVideoDetail();
  }
}

async function loadVideoDetail(videoId) {
  if (!videoId) return;
  const previousVideoId = String(state.selectedVideoId || "");
  const data = await apiGet(`/api/video?video_id=${encodeURIComponent(videoId)}`);
  const item = data.item || {};
  state.selectedVideoId = item.video_id || videoId;
  writeUiPrefs();
  const currentVideoId = String(state.selectedVideoId || "");
  const isSameSelectedVideo = previousVideoId && previousVideoId === currentVideoId;
  const candidateTitle = String(item.title || "").trim();
  const cached = state.videos.find((v) => String(v.video_id || "") === String(state.selectedVideoId || ""));
  const cachedTitle = String((cached || {}).title || "").trim();
  const resolvedTitle = [candidateTitle, cachedTitle].find((x) => x && !isVideoIdLike(x)) || "Video";
  el.videoTitle.textContent = resolvedTitle;
  const mod = item.is_archive ? "Live Archive" : "Saved Video";
  const analysisLlm = String(item.analysis_llm_detail || "").trim();
  const llmMode = llmModeFromVideoItem(item);
  const llmPart = analysisLlm && analysisLlm.toLowerCase() !== "unknown"
    ? ` | analysis llm: ${analysisLlm}`
    : (llmMode ? ` | analysis llm: ${llmModeText(llmMode)}` : "");
  el.videoFacts.textContent = `${mod} | source: ${item.transcript_source || "unknown"} | chars: ${item.transcript_chars || 0}${llmPart}`;
  const youtubeUrl = String(item.youtube_url || "").trim();
  el.videoLink.href = youtubeUrl || "#";
  el.videoLink.textContent = youtubeUrl ? t("status.open_on_youtube") : "";
  el.videoLink.title = youtubeUrl;
  if (el.videoPreview) {
    const thumb = String(item.thumbnail_url || "").trim() || getVideoThumb(state.selectedVideoId);
    if (thumb) {
      el.videoPreview.src = thumb;
      el.videoPreview.alt = resolvedTitle || "Video preview";
      el.videoPreview.hidden = false;
    } else {
      el.videoPreview.removeAttribute("src");
      el.videoPreview.alt = "";
      el.videoPreview.hidden = true;
    }
  }
  el.analysisOutput.textContent = item.analysis_text || "";
  el.transcriptOutput.textContent = item.transcript_preview || "";
  const hasTranscript = Boolean(item.transcript_exists);
  if (el.askSection) el.askSection.hidden = !hasTranscript;
  if (el.analyzeSection) el.analyzeSection.hidden = !hasTranscript;
  const analyzeIsBusy = Boolean(el.analyzeSection?.classList.contains("is-busy"));
  if (!analyzeIsBusy) {
    setMeta(el.analysisMeta, item.analysis_text ? t("status.loaded_saved_analysis") : t("status.no_analysis_saved"));
  }
  if (!isSameSelectedVideo) {
    setMeta(el.qaMeta, "");
    el.qaOutput.textContent = "";
    el.qaOutput.hidden = true;
  }
  applyServerNotesProgress(currentVideoId, item.notes_progress || {});
  renderVideoLists();
}

async function selectVideo(videoId) {
  await loadVideoDetail(videoId);
}

async function autoAnalyzeIfTranscript(videoId) {
  const vid = String(videoId || "").trim();
  if (!vid) return;
  const row = state.videos.find((v) => String(v.video_id || "").trim() === vid);
  if (!row || !Boolean(row.has_transcript)) return;
  await runAnalyze();
}

async function ensureSelectedTranscriptText() {
  let txt = String(el.transcriptOutput?.textContent || "").trim();
  if (txt) return txt;
  if (state.selectedVideoId) {
    await loadVideoDetail(state.selectedVideoId);
    txt = String(el.transcriptOutput?.textContent || "").trim();
  }
  return txt;
}

function _tokenizeForScoring(text) {
  return String(text || "")
    .toLowerCase()
    .match(/[a-zA-Z0-9]{3,}/g) || [];
}

function buildBrowserQaContext(transcript, question, maxChars = 12000) {
  const source = String(transcript || "").trim();
  if (!source) return "";
  const blocks = source.split(/\n{2,}/).map((x) => String(x || "").trim()).filter(Boolean);
  if (!blocks.length) return source.slice(0, maxChars);
  const qTokens = Array.from(new Set(_tokenizeForScoring(question))).slice(0, 22);
  const scored = blocks.map((block, idx) => {
    const low = block.toLowerCase();
    let score = 0;
    for (const token of qTokens) {
      if (low.includes(token)) score += 1 + Math.min(2, low.split(token).length - 1);
    }
    return { idx, score, block };
  });
  scored.sort((a, b) => b.score - a.score);
  const picked = new Set(scored.slice(0, 8).map((x) => x.idx));
  for (const idx of Array.from(picked)) {
    if (idx - 1 >= 0) picked.add(idx - 1);
    if (idx + 1 < blocks.length) picked.add(idx + 1);
  }
  let out = "";
  for (const idx of Array.from(picked).sort((a, b) => a - b)) {
    const part = blocks[idx];
    if (!part) continue;
    if ((out.length + part.length + 2) > maxChars && out) break;
    out += `${out ? "\n\n" : ""}${part}`;
  }
  return (out || source.slice(0, maxChars)).slice(0, maxChars);
}

function answerLooksLikeNoEvidence(text) {
  const low = String(text || "").trim().toLowerCase();
  if (!low) return true;
  const markers = [
    "does not contain enough evidence",
    "not contain enough evidence",
    "not enough evidence",
    "insufficient evidence",
    "cannot determine from the transcript",
    "cannot answer from the transcript",
    "transcript does not provide enough",
    "not provided in the transcript",
    "not mentioned in the transcript",
  ];
  return markers.some((m) => low.includes(m));
}

function _splitTranscriptSegmentsForQa(transcript) {
  const src = String(transcript || "").replace(/\r/g, "\n").trim();
  if (!src) return [];
  const lines = src.split(/\n+/).map((x) => String(x || "").trim()).filter(Boolean);
  if (!lines.length) return [];
  const out = [];
  let cur = "";
  for (const line of lines) {
    if (/^\[\d{1,2}:\d{2}\]/.test(line) && cur) {
      out.push(cur.trim());
      cur = line;
      continue;
    }
    cur = cur ? `${cur} ${line}` : line;
  }
  if (cur) out.push(cur.trim());
  return out;
}

function buildBrowserQaEvidenceContext(transcript, question, maxChars = 9000) {
  const source = String(transcript || "").trim();
  if (!source) return "";
  const segments = _splitTranscriptSegmentsForQa(source);
  const blocks = segments.length ? segments : source.split(/\n{2,}/).map((x) => String(x || "").trim()).filter(Boolean);
  if (!blocks.length) return source.slice(0, maxChars);

  const qTokens = Array.from(new Set(_tokenizeForScoring(question))).slice(0, 30);
  const coreTokens = [
    "salesperson",
    "authority",
    "manager",
    "pricing",
    "payment",
    "negotiat",
    "closer",
    "proposal",
    "deal",
  ];
  const tokens = Array.from(new Set([...qTokens, ...coreTokens]));

  const scored = blocks.map((block, idx) => {
    const low = block.toLowerCase();
    let score = 0;
    for (const token of tokens) {
      if (!token || token.length < 3) continue;
      const hits = low.split(token).length - 1;
      if (hits > 0) score += 1 + Math.min(4, hits);
    }
    return { idx, score, block };
  });
  scored.sort((a, b) => b.score - a.score);

  const picked = scored.filter((x) => x.score > 0).slice(0, 10).map((x) => x.idx);
  const base = picked.length ? picked : scored.slice(0, 6).map((x) => x.idx);
  const expanded = new Set(base);
  for (const idx of base) {
    if (idx - 1 >= 0) expanded.add(idx - 1);
    if (idx + 1 < blocks.length) expanded.add(idx + 1);
  }

  let out = "";
  for (const idx of Array.from(expanded).sort((a, b) => a - b)) {
    const part = blocks[idx];
    if (!part) continue;
    if ((out.length + part.length + 2) > maxChars && out) break;
    out += `${out ? "\n\n" : ""}${part}`;
  }
  return (out || source.slice(0, maxChars)).slice(0, maxChars);
}

function splitTextWindowsLocal(text, windowChars = 7000, overlapChars = 320, maxWindows = 4) {
  const src = String(text || "").trim();
  if (!src) return [];
  const out = [];
  const win = Math.max(1800, Number(windowChars || 0));
  const overlap = Math.max(0, Math.min(Number(overlapChars || 0), Math.floor(win / 3)));
  let start = 0;
  while (start < src.length && out.length < Math.max(1, Number(maxWindows || 1))) {
    let end = Math.min(src.length, start + win);
    if (end < src.length) {
      const cut = src.lastIndexOf("\n", end);
      if (cut > start + Math.floor(win * 0.55)) end = cut;
    }
    const chunk = src.slice(start, end).trim();
    if (chunk) out.push(chunk);
    if (end >= src.length) break;
    start = Math.max(end - overlap, start + 1);
  }
  return out;
}

function browserLlmDetail() {
  const provider = String(browserLlmRuntime.provider || "browser").trim();
  const model = String(browserLlmRuntime.model || "unknown").trim();
  return `${provider}:${model}`;
}

async function _runWindowAiPrompt(session, systemPrompt, userPrompt) {
  const prompt = `${String(systemPrompt || "").trim()}\n\n${String(userPrompt || "").trim()}`.trim();
  if (session && typeof session.prompt === "function") {
    const out = await session.prompt(prompt);
    if (typeof out === "string") return out.trim();
    if (out && typeof out === "object") {
      return String(out.text || out.output || out.content || "").trim();
    }
  }
  if (session && typeof session.complete === "function") {
    const out = await session.complete(prompt);
    if (typeof out === "string") return out.trim();
    if (out && typeof out === "object") {
      return String(out.text || out.output || out.content || "").trim();
    }
  }
  throw new Error("Browser AI prompt API is unavailable.");
}

async function _runWebLlmPrompt(engine, systemPrompt, userPrompt) {
  const resp = await engine.chat.completions.create({
    messages: [
      { role: "system", content: String(systemPrompt || "") },
      { role: "user", content: String(userPrompt || "") },
    ],
    temperature: 0.2,
  });
  const choice = Array.isArray(resp?.choices) ? resp.choices[0] : null;
  let content = choice?.message?.content;
  if (Array.isArray(content)) {
    content = content.map((x) => (typeof x === "string" ? x : String(x?.text || ""))).join("");
  }
  return String(content || "").trim();
}

async function ensureBrowserLlm(onStatus) {
  if (browserLlmRuntime.session || browserLlmRuntime.engine) return browserLlmRuntime;
  if (browserLlmRuntime.loading) return browserLlmRuntime.loading;
  const report = typeof onStatus === "function" ? onStatus : (() => {});
  browserLlmRuntime.loading = (async () => {
    const aiApi = window.ai && window.ai.languageModel;
    if (aiApi && typeof aiApi.create === "function") {
      report(t("status.browser_loading_model"));
      const session = await aiApi.create();
      browserLlmRuntime.provider = "window.ai";
      browserLlmRuntime.model = "languageModel";
      browserLlmRuntime.session = session;
      report(t("status.browser_model_ready"));
      return browserLlmRuntime;
    }

    if (!navigator.gpu) {
      throw new Error("Browser acceleration unavailable: WebGPU is not supported.");
    }
    report(t("status.browser_loading_model"));
    const webllm = await import(BROWSER_WEBLLM_IMPORT);
    const engine = new webllm.MLCEngine();
    let loadedModel = "";
    let lastErr = "";
    for (const model of BROWSER_WEBLLM_MODELS) {
      try {
        await engine.reload(model, {
          initProgressCallback: (p) => {
            const ratio = Number(p?.progress || 0);
            const pct = Math.max(0, Math.min(100, Math.round(ratio * 100)));
            report(`${t("status.browser_loading_model")} ${pct}%`);
          },
        });
        loadedModel = model;
        break;
      } catch (err) {
        lastErr = String(err?.message || err || "");
      }
    }
    if (!loadedModel) {
      throw new Error(lastErr || "Could not load browser model.");
    }
    browserLlmRuntime.provider = "webllm";
    browserLlmRuntime.model = loadedModel;
    browserLlmRuntime.engine = engine;
    report(t("status.browser_model_ready"));
    return browserLlmRuntime;
  })()
    .finally(() => {
      browserLlmRuntime.loading = null;
    });
  return browserLlmRuntime.loading;
}

async function runBrowserChat(systemPrompt, userPrompt, onStatus) {
  const rt = await ensureBrowserLlm(onStatus);
  if (rt.session) return _runWindowAiPrompt(rt.session, systemPrompt, userPrompt);
  if (rt.engine) return _runWebLlmPrompt(rt.engine, systemPrompt, userPrompt);
  throw new Error("Browser model is not initialized.");
}

async function runAnalyze(opts = {}) {
  if (!state.selectedVideoId) {
    setMeta(el.analysisMeta, t("status.select_video_first"), true);
    return;
  }
  if (!tryStartNotesTask("analyze", el.analysisMeta)) return;
  const videoId = String(state.selectedVideoId || "").trim();
  if (!videoId) {
    finishNotesTask("analyze");
    setMeta(el.analysisMeta, t("status.select_video_first"), true);
    return;
  }
  const save = Object.prototype.hasOwnProperty.call(opts, "save") ? Boolean(opts.save) : true;
  const mode = currentAnalyzeMode();
  if (mode === "browser") {
    const startedAtBrowser = Date.now();
    setSectionLoading(el.analyzeSection, el.analysisMeta, true);
    setButtonLoading(el.analyzeBtn, true, `${t("btn.run_analysis")}...`);
    el.analyzeBtn.disabled = true;
    try {
      const transcript = await ensureSelectedTranscriptText();
      if (!transcript) {
        throw new Error("Transcript is required for browser analysis.");
      }
      const title = String(el.videoTitle?.textContent || state.selectedVideoId || "Video").trim();
      const trimmed = transcript.slice(0, 22000);
      const chunks = splitTextWindowsLocal(trimmed, 6500, 280, 4);
      if (!chunks.length) throw new Error("Transcript is empty.");

      await ensureBrowserLlm((msg) => {
        const elapsed = Math.max(1, Math.round((Date.now() - startedAtBrowser) / 1000));
        const fun = rotatingFunStatus("analyze", startedAtBrowser, 1, 1000);
        setMeta(el.analysisMeta, `${msg}\n${fun}`);
      });

      const notes = [];
      for (let i = 0; i < chunks.length; i += 1) {
        const idx = i + 1;
        const elapsed = Math.max(1, Math.round((Date.now() - startedAtBrowser) / 1000));
        const fun = rotatingFunStatus("analyze", startedAtBrowser, i + 2, 1000);
        setMeta(el.analysisMeta, `${t("status.running_analysis")} ${elapsed}s | parts ${idx}/${chunks.length}\n${fun}`);
        const part = await runBrowserChat(
          "Analyze this transcript chunk. Return concise notes: idea, key points, practical actions, risks.",
          `Title: ${title}\nChunk ${idx}/${chunks.length}\n\n${chunks[i]}`,
          null
        );
        const clean = String(part || "").trim();
        if (clean) notes.push(clean);
      }
      if (!notes.length) throw new Error("Browser model returned empty analysis.");

      let finalBody = notes.length === 1 ? notes[0] : "";
      if (notes.length > 1) {
        finalBody = await runBrowserChat(
          "Merge chunk notes into one coherent analysis with sections: Idea, Key Points, Actions, Risks.",
          notes.map((x, i) => `Part ${i + 1}/${notes.length}\n${x}`).join("\n\n"),
          null
        );
      }
      finalBody = String(finalBody || "").trim() || notes.join("\n\n");
      const llmDetail = browserLlmDetail();
      const analysisText = `🧠 Browser Analysis\n🖥️ Backend: browser (${llmDetail})\n\n${finalBody}`;
      el.analysisOutput.textContent = analysisText;

      let savedNotePath = "";
      if (save) {
        try {
          const store = await apiPost("/api/analyze_store", {
            video_id: videoId,
            analysis: analysisText,
            llm_backend: "browser",
            llm_backend_detail: llmDetail,
          });
          savedNotePath = String(store.analysis_md_path || "").trim();
          await loadVideos(true);
        } catch (_err) {
          // keep browser result visible even if persistence fails
        }
      }
      const elapsed = Math.max(1, Math.round((Date.now() - startedAtBrowser) / 1000));
      const saveMeta = save ? (savedNotePath ? " | saved" : " | not saved") : " | not saved";
      setMeta(el.analysisMeta, `Done in ${elapsed}s | mode=browser | llm=${llmDetail}${saveMeta}`);
    } catch (err) {
      setMeta(el.analysisMeta, String(err.message || err), true);
    } finally {
      el.analyzeBtn.disabled = false;
      setButtonLoading(el.analyzeBtn, false);
      setSectionLoading(el.analyzeSection, el.analysisMeta, false);
      finishNotesTask("analyze");
    }
    return;
  }
  const startedAt = Date.now();
  let analysisTimer = null;
  let pollInFlight = false;
  let progressItem = null;
  let handoffProgress = null;

  const summarizeProgress = () => {
    const elapsed = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
    const snap = progressItem && typeof progressItem === "object" ? progressItem : {};
    const totalRaw = Number(snap.chunk_total || 0);
    const doneRaw = Number(snap.chunk_completed || 0);
    const total = Number.isFinite(totalRaw) ? Math.max(0, Math.trunc(totalRaw)) : 0;
    const done = Number.isFinite(doneRaw) ? Math.max(0, Math.trunc(doneRaw)) : 0;
    const charsRaw = Number(snap.generated_chars || 0);
    const chars = Number.isFinite(charsRaw) ? Math.max(0, Math.trunc(charsRaw)) : 0;
    const statusMsg = String(snap.message || "").trim();
    let tail = "";
    if (total > 0) tail += ` | parts ${Math.min(done, total)}/${total}`;
    if (chars > 0) tail += ` | ${chars} chars`;
    if (!tail && statusMsg) tail += ` | ${statusMsg}`;
    const fun = rotatingFunStatus("analyze", startedAt, 0, 1000);
    const funLine = fun ? `\n${fun}` : "";
    setMeta(el.analysisMeta, `${t("status.running_analysis")} ${elapsed}s${tail}${funLine}`);
  };

  const pollProgress = async () => {
    if (pollInFlight) return;
    pollInFlight = true;
    try {
      const payload = await apiGet(`/api/analyze_progress?video_id=${encodeURIComponent(videoId)}`);
      const item = payload && typeof payload.item === "object" ? payload.item : null;
      if (item) progressItem = item;
    } catch (_err) {
      // Ignore polling errors while the main analyze request is running.
    } finally {
      pollInFlight = false;
    }
  };

  setSectionLoading(el.analyzeSection, el.analysisMeta, true);
  setButtonLoading(el.analyzeBtn, true, `${t("btn.run_analysis")}...`);
  el.analyzeBtn.disabled = true;
  setMeta(el.analysisMeta, `${t("status.running_analysis")}\n${rotatingFunStatus("analyze", startedAt, 0, 1000)}`);
  void pollProgress();
  analysisTimer = window.setInterval(() => {
    summarizeProgress();
    void pollProgress();
  }, 1200);
  try {
    const data = await apiPost("/api/analyze", {
      video_id: videoId,
      save,
    });
    if (String(data.status || "").trim().toLowerCase() === "already_running" || Boolean(data.in_progress)) {
      handoffProgress = {
        busy_task: "analyze",
        analyze: {
          ...(data.item && typeof data.item === "object" ? data.item : {}),
          status: "running",
          in_progress: true,
        },
      };
      setMeta(el.analysisMeta, String((data.item || {}).message || t("status.analysis_already_running")));
      return;
    }
    await pollProgress();
    el.analysisOutput.textContent = data.analysis || "";
    const mode = data.cached ? `cache (${data.cache_age_sec}s old)` : "fresh";
    const llm = llmDetailText(data.llm_backend_detail, data.llm_backend, data.analysis || "");
    const finalTotalRaw = Number(
      data.chunk_total
      || ((progressItem && typeof progressItem === "object") ? progressItem.chunk_total : 0)
      || 0
    );
    const finalDoneRaw = Number(
      data.chunk_completed
      || ((progressItem && typeof progressItem === "object") ? progressItem.chunk_completed : 0)
      || 0
    );
    const finalTotal = Number.isFinite(finalTotalRaw) ? Math.max(0, Math.trunc(finalTotalRaw)) : 0;
    const finalDone = Number.isFinite(finalDoneRaw) ? Math.max(0, Math.trunc(finalDoneRaw)) : 0;
    const partsMeta = finalTotal > 0 ? ` | parts ${Math.min(finalDone, finalTotal)}/${finalTotal}` : "";
    setMeta(el.analysisMeta, `Done in ${data.elapsed_sec}s | ${mode} | lang=${data.lang} | llm=${llm}${partsMeta}`);
    await loadVideos(true);
  } catch (err) {
    setMeta(el.analysisMeta, String(err.message || err), true);
  } finally {
    if (analysisTimer) window.clearInterval(analysisTimer);
    el.analyzeBtn.disabled = false;
    setButtonLoading(el.analyzeBtn, false);
    setSectionLoading(el.analyzeSection, el.analysisMeta, false);
    finishNotesTask("analyze");
    if (handoffProgress) applyServerNotesProgress(videoId, handoffProgress);
  }
}

async function runAsk(ev) {
  ev.preventDefault();
  if (!state.selectedVideoId) {
    setMeta(el.qaMeta, t("status.select_video_first"), true);
    return;
  }
  if (!tryStartNotesTask("ask", el.qaMeta)) return;
  const question = (el.questionInput.value || "").trim();
  if (!question) {
    finishNotesTask("ask");
    setMeta(el.qaMeta, t("status.question_required"), true);
    return;
  }
  const mode = currentAskMode();
  if (mode === "browser") {
    const startedAtBrowser = Date.now();
    let askMetaPrefix = t("status.asking_transcript");
    let askTick = 0;
    const renderAskMeta = () => {
      const elapsed = Math.max(1, Math.round((Date.now() - startedAtBrowser) / 1000));
      const fun = funStatusLine("analyze", askTick);
      askTick += 1;
      setMeta(el.qaMeta, `${askMetaPrefix} ${elapsed}s\n${fun}`);
    };
    const askTimer = window.setInterval(renderAskMeta, 1200);
    setSectionLoading(el.askSection, el.qaMeta, true);
    setButtonLoading(el.askBtn, true, `${t("btn.ask")}...`);
    el.askBtn.disabled = true;
    renderAskMeta();
    el.qaOutput.textContent = "";
    el.qaOutput.hidden = true;
    try {
      const transcript = await ensureSelectedTranscriptText();
      if (!transcript) throw new Error("Transcript is required for browser ask mode.");
      const context = buildBrowserQaContext(transcript, question, 15000);
      const title = String(el.videoTitle?.textContent || state.selectedVideoId || "Video").trim();

      await ensureBrowserLlm((msg) => {
        askMetaPrefix = String(msg || "").trim() || t("status.asking_transcript");
        renderAskMeta();
      });
      askMetaPrefix = t("status.asking_transcript");
      renderAskMeta();
      let finalAnswer = String(await runBrowserChat(
        "Answer from transcript context only. If evidence exists, give a direct answer and cite up to 3 short timestamped quotes at the end under 'Evidence'. Only say insufficient evidence when the context truly lacks support.",
        `Title: ${title}\nQuestion: ${question}\n\nTranscript context:\n${context}`,
        null
      ) || "").trim();

      if (answerLooksLikeNoEvidence(finalAnswer)) {
        const evidenceContext = buildBrowserQaEvidenceContext(transcript, question, 10000);
        if (evidenceContext) {
          finalAnswer = String(await runBrowserChat(
            "You are performing focused evidence extraction from transcript lines. Return a direct answer first, then 'Evidence:' with 2-4 short timestamped snippets from the transcript.",
            `Question: ${question}\n\nTranscript evidence candidates:\n${evidenceContext}`,
            null
          ) || "").trim() || finalAnswer;
        }
      }

      if (!finalAnswer) throw new Error("Browser model returned an empty answer.");
      el.qaOutput.textContent = finalAnswer;
      el.qaOutput.hidden = false;
      const elapsed = Math.max(1, Math.round((Date.now() - startedAtBrowser) / 1000));
      setMeta(el.qaMeta, `Answered in ${elapsed}s | mode=browser | llm=${browserLlmDetail()}.`);
    } catch (err) {
      setMeta(el.qaMeta, String(err.message || err), true);
      el.qaOutput.hidden = true;
    } finally {
      window.clearInterval(askTimer);
      el.askBtn.disabled = false;
      setButtonLoading(el.askBtn, false);
      setSectionLoading(el.askSection, el.qaMeta, false);
      finishNotesTask("ask");
    }
    return;
  }
  const startedAt = Date.now();
  let askTimer = null;
  let handoffProgress = null;
  setSectionLoading(el.askSection, el.qaMeta, true);
  setButtonLoading(el.askBtn, true, `${t("btn.ask")}...`);
  el.askBtn.disabled = true;
  setMeta(el.qaMeta, `${t("status.asking_transcript")}\n${rotatingFunStatus("analyze", startedAt, 0, 1000)}`);
  askTimer = window.setInterval(() => {
    const elapsed = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
    const fun = rotatingFunStatus("analyze", startedAt, 1, 1000);
    const funLine = fun ? `\n${fun}` : "";
    setMeta(el.qaMeta, `${t("status.asking_transcript")} ${elapsed}s${funLine}`);
  }, 1200);
  el.qaOutput.textContent = "";
  el.qaOutput.hidden = true;
  try {
    const data = await apiPost("/api/ask", { video_id: state.selectedVideoId, question });
    if (String(data.status || "").trim().toLowerCase() === "already_running" || Boolean(data.in_progress)) {
      handoffProgress = {
        busy_task: "ask",
        ask: {
          ...(data.item && typeof data.item === "object" ? data.item : {}),
          status: "running",
          in_progress: true,
        },
      };
      setMeta(el.qaMeta, String((data.item || {}).message || t("status.ask_already_running")));
      return;
    }
    const answer = String(data.answer || "");
    el.qaOutput.textContent = answer;
    el.qaOutput.hidden = !answer.trim();
    const llm = llmDetailText(data.llm_backend_detail, data.llm_backend, answer);
    const mode = Boolean(data.cached) ? "cache" : "fresh";
    setMeta(el.qaMeta, `Answered in ${data.elapsed_sec}s | ${mode} | llm=${llm}.`);
  } catch (err) {
    setMeta(el.qaMeta, String(err.message || err), true);
    el.qaOutput.hidden = true;
  } finally {
    if (askTimer) window.clearInterval(askTimer);
    el.askBtn.disabled = false;
    setButtonLoading(el.askBtn, false);
    setSectionLoading(el.askSection, el.qaMeta, false);
    finishNotesTask("ask");
    if (handoffProgress) applyServerNotesProgress(state.selectedVideoId, handoffProgress);
  }
}

async function runIngest(ev) {
  ev.preventDefault();
  const url = (el.urlInput.value || "").trim();
  if (!url) {
    setMeta(el.ingestMeta, t("status.youtube_url_required"), true);
    return;
  }
  el.ingestBtn.disabled = true;
  setMeta(el.ingestMeta, t("status.saving_transcript"));
  try {
    const data = await apiPost("/api/save_transcript", { url, force: false });
    setMeta(
      el.ingestMeta,
      `Ready in ${data.elapsed_sec}s | ${data.video_id} | ${data.cached ? "cached" : data.source || "new"}`
    );
    await loadVideos(false);
  } catch (err) {
    setMeta(el.ingestMeta, String(err.message || err), true);
  } finally {
    el.ingestBtn.disabled = false;
  }
}

async function runDirectPrepare(ev) {
  ev?.preventDefault?.();
  const url = (el.directUrlInput?.value || "").trim();
  if (!url) {
    setMeta(el.directMeta, t("status.youtube_url_required"), true);
    return;
  }
  renderDirectPreview();
  if (el.directPrepareBtn) el.directPrepareBtn.disabled = true;
  setMeta(el.directMeta, t("status.preparing_download_links"));
  if (el.directOutput) el.directOutput.innerHTML = "";
  try {
    let videoOk = null;
    let audioOk = null;
    let firstErr = null;
    try {
      videoOk = await buildAndRememberDirectLink(url, "video");
    } catch (err) {
      firstErr = firstErr || err;
    }
    try {
      audioOk = await buildAndRememberDirectLink(url, "audio");
    } catch (err) {
      firstErr = firstErr || err;
    }
    if (!videoOk && !audioOk) {
      throw firstErr || new Error("Could not prepare video/audio links.");
    }

    const mergedVideoId = String(videoOk?.video_id || audioOk?.video_id || extractYouTubeVideoId(url) || "");
    const mergedTitle = preferredDirectTitle({
      rawTitle: String(videoOk?.title || audioOk?.title || "").trim(),
      knownTitle: "",
      recentTitle: "",
      videoId: mergedVideoId,
      url,
    });
    const thumb = String(videoOk?.thumbnail_url || audioOk?.thumbnail_url || getVideoThumb(mergedVideoId));
    state.directResultContext = {
      url,
      title: mergedTitle,
      thumb,
      videoLink: String(videoOk?.download_url || "").trim(),
      audioLink: String(audioOk?.download_url || "").trim(),
    };
    renderDirectResultCard(state.directResultContext);
    const bestLink = String(videoOk?.download_url || audioOk?.download_url || "").trim();
    const copied = bestLink ? await copyText(bestLink) : false;
    const manualSaveNeeded = [videoOk, audioOk].some((x) => {
      const row = x && typeof x === "object" ? x : {};
      const reason = String(row.fallback_reason || "").trim().toLowerCase();
      const saveStatus = String(row.save_status || "").trim().toLowerCase();
      return reason === "youtube_antibot_direct_blocked" || saveStatus === "manual_required";
    });
    if (!bestLink && manualSaveNeeded) {
      setMeta(el.directMeta, "Direct links blocked by YouTube. Click 'Download To Server' to start save manually.");
    } else if (!bestLink) {
      setMeta(el.directMeta, "No direct links available right now. Try again later or use 'Download To Server'.", true);
    } else {
      setMeta(
        el.directMeta,
        `Buttons ready${copied ? " (path copied)" : ""}. Links are temporary and may expire.`
      );
    }
    renderRecentDirectSearches();
  } catch (err) {
    setMeta(el.directMeta, String(err.message || err), true);
  } finally {
    if (el.directPrepareBtn) el.directPrepareBtn.disabled = false;
  }
}

async function runLiveStart(ev) {
  ev.preventDefault();
  const url = (el.liveUrlInput?.value || "").trim();
  if (!url) {
    setMeta(el.liveStartMeta, t("status.live_url_required"), true);
    return;
  }
  if (el.liveStartBtn) el.liveStartBtn.disabled = true;
  setMeta(el.liveStartMeta, t("status.starting_live_recording"));
  try {
    const data = await apiPost("/api/live/start", { url });
    const status = String(data.startup_status || data.status || "").trim().toLowerCase();
    const startupMsg = String(data.startup_message || "").trim();
    const elapsed = Number(data.elapsed_sec || 0);

    if (status === "failed") {
      setMeta(
        el.liveStartMeta,
        startupMsg || `Live start failed after ${elapsed}s. Check service logs for details.`,
        true
      );
    } else if (status === "upcoming") {
      setMeta(el.liveStartMeta, startupMsg || "Live is upcoming. Waiting for it to actually start.");
      if (el.liveUrlInput) el.liveUrlInput.value = "";
    } else if (status === "already_running") {
      setMeta(el.liveStartMeta, startupMsg || "This LIVE is already being recorded.");
      if (el.liveUrlInput) el.liveUrlInput.value = "";
    } else if (status === "archived") {
      setMeta(el.liveStartMeta, startupMsg || "Archived LIVE save started.");
      if (el.liveUrlInput) el.liveUrlInput.value = "";
    } else if (status === "started") {
      setMeta(el.liveStartMeta, startupMsg || `LIVE recording started in ${elapsed}s.`);
      if (el.liveUrlInput) el.liveUrlInput.value = "";
    } else {
      setMeta(
        el.liveStartMeta,
        startupMsg || `Live start requested in ${elapsed}s. It should appear in 'Currently Saving Live' soon.`
      );
      if (el.liveUrlInput) el.liveUrlInput.value = "";
    }
    await loadVideos(true);
  } catch (err) {
    setMeta(el.liveStartMeta, String(err.message || err), true);
  } finally {
    if (el.liveStartBtn) el.liveStartBtn.disabled = false;
  }
}

function renderResearches() {
  if (!el.researchList) return;
  const q = (el.researchSearchInput?.value || "").trim().toLowerCase();
  const items = state.researches.filter((r) => researchHasResult(r)).filter((r) => {
    if (!q) return true;
    const tags = (r.topics || []).map((t) => String(t.tag || "").toLowerCase()).join(" ");
    return `${researchDisplayTitle(r)} ${r.goal_text || ""} ${r.run_id || ""} ${tags}`.toLowerCase().includes(q);
  });

  el.researchList.innerHTML = items.length
    ? items
        .map((r) => {
          const tags = (r.topics || []).map((t) => t.tag).filter(Boolean).slice(0, 3).join(", ");
          const thumbs = researchPreviewThumbs(r);
          return `
            <button class="item research-list-item${state.selectedResearchId === r.run_id ? " active" : ""}" data-id="${escapeHtml(r.run_id)}">
              <div class="line-1">${escapeHtml(researchDisplayTitle(r))}</div>
              <div class="line-2">${escapeHtml(`${r.run_kind || "research"} | ${r.status || ""} | ${tags || "no tags"}`)}</div>
              ${thumbs ? `<div class="research-list-thumbs">${thumbs}</div>` : ""}
            </button>
          `;
        })
        .join("")
    : `<p class="meta">${escapeHtml(t("status.no_completed_public_researches"))}</p>`;

  el.researchList.querySelectorAll(".item").forEach((btn) => btn.addEventListener("click", () => loadResearchDetail(btn.dataset.id)));
}

async function loadResearches(keepSelected = true) {
  if (!el.researchList) return;
  const data = await apiGet("/api/researches");
  state.researches = data.items || [];
  const withSummary = state.researches.filter((r) => researchHasResult(r));
  if (!keepSelected || !withSummary.some((r) => r.run_id === state.selectedResearchId)) {
    state.selectedResearchId = (withSummary[0] || {}).run_id || "";
  }
  writeUiPrefs();
  renderResearches();
  if (state.selectedResearchId) await loadResearchDetail(state.selectedResearchId);
  else {
    if (el.researchTitle) el.researchTitle.textContent = t("research.detail");
    setMeta(el.researchMeta, t("status.no_completed_public_researches"));
    if (el.researchOutput) el.researchOutput.textContent = "";
  }
}

async function loadResearchDetail(runId) {
  if (!runId || !el.researchTitle || !el.researchMeta || !el.researchOutput) return;
  const data = await apiGet(`/api/research?run_id=${encodeURIComponent(runId)}`);
  const item = data.item || {};
  if (!researchHasResult(item)) {
    if (state.selectedResearchId === runId) state.selectedResearchId = "";
    writeUiPrefs();
    renderResearches();
    el.researchTitle.textContent = t("research.detail");
    setMeta(el.researchMeta, t("status.no_completed_public_researches"));
    el.researchOutput.textContent = "";
    return;
  }
  state.selectedResearchId = item.run_id || runId;
  writeUiPrefs();
  const topics = (item.topics || []).map((t) => t.tag).filter(Boolean).join(", ");
  const videoCount = Array.isArray(item.videos) ? item.videos.length : 0;
  el.researchTitle.textContent = researchDisplayTitle(item);
  setMeta(
    el.researchMeta,
    `Kind: ${item.run_kind || "research"} | status: ${item.status || ""} | videos: ${videoCount} | topics: ${topics || "none"}`
  );
  el.researchOutput.innerHTML = formatResearchDetailHtml(item);
  renderResearches();
}

function jobFromMap(jobId) {
  return state.jobs.get(jobId) || null;
}

function upsertJob(job) {
  if (!job || !job.job_id) return;
  state.jobs.set(job.job_id, job);
  if (!state.selectedJobId) state.selectedJobId = job.job_id;
}

function findSavedVideoForBrewCard(video) {
  const row = video && typeof video === "object" ? video : {};
  const vid = String(row.video_id || "").trim();
  const src = String(row.url || "").trim();
  if (!Array.isArray(state.videos) || !state.videos.length) return null;
  return (
    state.videos.find((x) => String(x.video_id || "").trim() === vid)
    || state.videos.find((x) => {
      const xSrc = String(x.source_url || x.youtube_url || "").trim();
      return Boolean(src && xSrc && xSrc === src);
    })
    || null
  );
}

function brewCardSourceUrl(video, savedRow) {
  const row = video && typeof video === "object" ? video : {};
  const saved = savedRow && typeof savedRow === "object" ? savedRow : {};
  const fromVideo = String(row.url || "").trim();
  if (fromVideo) return fromVideo;
  const fromSaved = String(saved.source_url || saved.youtube_url || "").trim();
  if (fromSaved) return fromSaved;
  const vid = String(row.video_id || saved.video_id || "").trim();
  return vid ? `https://www.youtube.com/watch?v=${vid}` : "";
}

function renderVideoCards(node, videos, emptyText = t("status.no_items")) {
  if (!node) return;
  const list = Array.isArray(videos) ? videos : [];
  if (!list.length) {
    node.innerHTML = `<p class="meta">${escapeHtml(emptyText)}</p>`;
    return;
  }
  node.innerHTML = list
    .map((v) => {
      const savedRow = findSavedVideoForBrewCard(v) || {};
      const thumb = v.thumbnail_url || "";
      const sourceUrl = brewCardSourceUrl(v, savedRow);
      const href = sourceUrl || "#";
      const savedUrlRaw = String(savedRow.public_url || "").trim();
      const savedUrl = safeHref(savedUrlRaw);
      const canSave = Boolean(sourceUrl && !savedUrlRaw);
      const sourceHref = safeHref(sourceUrl || "#");
      const serverAction = savedUrlRaw
        ? `<a class="btn ghost" href="${escapeHtml(savedUrl)}" target="_blank" rel="noreferrer" download>${escapeHtml(t("btn.open_saved_file"))}</a>`
        : (canSave
          ? `<button class="btn ghost brew-save-btn" type="button" data-url="${encodeURIComponent(sourceUrl)}">${escapeHtml(t("btn.download_to_server"))}</button>`
          : "");
      const hasActionRow = Boolean(serverAction);
      const thumbBlock = thumb
        ? (
          sourceUrl
            ? `<a class="brew-video-thumb-link" href="${escapeHtml(sourceHref)}" target="_blank" rel="noreferrer"><img src="${escapeHtml(thumb)}" alt="" loading="lazy" /></a>`
            : `<img src="${escapeHtml(thumb)}" alt="" loading="lazy" />`
        )
        : "";
      return `
        <article class="video-card">
          ${thumbBlock}
          <div class="body">
            <p class="title"><a href="${escapeHtml(href)}" target="_blank" rel="noreferrer">${escapeHtml(v.title || v.video_id || "Video")}</a></p>
            <p class="meta-line">${escapeHtml(v.channel || "Unknown")} | duration: ${escapeHtml(formatDuration(v.duration_sec))}</p>
            <p class="meta-line">captions: ${v.has_captions ? "yes" : "no/unknown"}</p>
            <p class="meta-line">${escapeHtml(v.video_id || "")}</p>
            ${hasActionRow ? `<div class="row direct-action-row">
              ${serverAction}
            </div>` : ""}
            ${sourceUrl ? directSaveProgressHtml(sourceUrl) : ""}
          </div>
        </article>
      `;
    })
    .join("");
  node.querySelectorAll(".brew-save-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const rawUrl = decodeURIComponent(String(btn.dataset.url || ""));
      if (!rawUrl) return;
      if (!shouldStartDirectServerSave(rawUrl)) {
        setMeta(el.juiceMeta, "Server save already in progress or already available.");
        return;
      }
      btn.disabled = true;
      try {
        await runSaveOnServer(rawUrl);
      } finally {
        btn.disabled = false;
      }
    });
  });
}

function renderJobs() {
  const jobs = Array.from(state.jobs.values()).sort((a, b) => String(b.updated_at || "").localeCompare(String(a.updated_at || "")));
  const active = jobs.filter((j) => ["queued", "running"].includes(j.status || ""));
  if (!active.some((j) => j.job_id === state.selectedJobId)) {
    state.selectedJobId = active.length ? active[0].job_id : "";
  }
  el.activeBrewList.innerHTML = active.length
    ? active
        .map((j) => makeBrewingItem(j, state.selectedJobId === j.job_id))
        .join("")
    : "";
  el.activeBrewList.querySelectorAll(".item").forEach((btn) => btn.addEventListener("click", () => selectJob(btn.dataset.id)));
}

function renderSelectedJob() {
  const job = jobFromMap(state.selectedJobId);
  const isActive = job && ["queued", "running"].includes(String(job.status || ""));
  if (!isActive) {
    if (el.brewDetailPanel) el.brewDetailPanel.hidden = true;
    if (el.brewMeta) el.brewMeta.innerHTML = "";
    if (el.brewConfigMeta) el.brewConfigMeta.innerHTML = "";
    el.brewProgressBar.style.width = "0%";
    if (el.currentReviewWrap) el.currentReviewWrap.hidden = true;
    if (el.reviewedWrap) el.reviewedWrap.hidden = true;
    if (el.candidateWrap) el.candidateWrap.hidden = true;
    if (el.juiceResultWrap) el.juiceResultWrap.hidden = true;
    if (el.currentReview) el.currentReview.innerHTML = "";
    if (el.reviewedVideos) el.reviewedVideos.innerHTML = "";
    if (el.candidateVideos) el.candidateVideos.innerHTML = "";
    if (el.juiceOutput) el.juiceOutput.textContent = "";
    return;
  }
  if (el.brewDetailPanel) el.brewDetailPanel.hidden = false;

  const progress = job.progress || {};
  const ratio = Math.max(0, Math.min(1, Number(progress.ratio || 0)));
  const percent = Math.round(ratio * 100);
  const status = formatStatus(job.status);
  const sClass = statusClass(job.status);
  const totalSteps = progress.total_steps || 5;
  const step = Math.min(progress.step || 0, totalSteps);
  const stage = brewStageLabel(job);
  const detail = String(job.progress_detail || "").trim();
  const cfg = job.config || {};
  const captionsOnly = Boolean(cfg.captions_only);
  const targetVideos = job.total_videos || cfg.max_videos || 0;
  const reviewed = Math.max(0, Math.min(job.current_index || 0, targetVideos || 0));
  const searchSummary = searchStatsSummary(job);
  const llmLabel = formatLlmBackendLabel(job.llm_backend);
  const currentStep = Math.max(1, Math.min(totalSteps, Number(step || 1)));
  const currentVideoTitle = String((job.current_video || {}).title || "").trim();

  let infoLine = detail;
  if (!infoLine) {
    if (currentStep <= 2) infoLine = t("status.preparing_search_plan");
    else if (currentStep === 3) infoLine = searchSummary || t("status.searching_youtube_videos");
    else if (currentStep === 4) infoLine = `${t("status.reviewing_videos", { reviewed, total: targetVideos })}${currentVideoTitle ? ` · ${currentVideoTitle}` : ""}`;
    else infoLine = t("status.comparing_insights", { reviewed });
  }
  const funSeed = Math.floor(Date.now() / 3500) + (currentStep * 11) + reviewed;
  const funLine = funStatusLine("brew", funSeed);

  el.brewProgressBar.style.width = `${percent}%`;
  el.brewMeta.innerHTML = `
    <p class="brew-submeta">Step ${step}/${totalSteps}: ${escapeHtml(stage)} · ${percent}%</p>
    ${infoLine ? `<p class="brew-detail">${escapeHtml(infoLine)}</p>` : ""}
    ${funLine ? `<p class="brew-detail">${escapeHtml(funLine)}</p>` : ""}
  `;

  const pills = [
    llmLabel !== "Unknown" ? `<span class="cfg-pill">${escapeHtml(llmLabel)}</span>` : "",
    `<span class="cfg-pill"><strong>${cfg.max_videos || 0}</strong> videos</span>`,
    captionsOnly ? `<span class="cfg-pill">${escapeHtml(t("status.fast_mode"))}</span>` : "",
  ].filter(Boolean).join("");
  el.brewConfigMeta.innerHTML = pills ? `<div class="config-grid" style="margin-top:0.3rem">${pills}</div>` : "";

  const current = job.current_video && job.current_video.video_id ? [job.current_video] : [];
  const reviewedVideos = job.reviewed_videos || [];
  const candidates = job.candidate_videos || [];

  function showSection(wrap, node, videos) {
    const hasItems = videos.length > 0;
    if (wrap) wrap.hidden = !hasItems;
    if (hasItems) renderVideoCards(node, videos);
    else if (node) node.innerHTML = "";
  }

  if (currentStep <= 2) {
    showSection(el.currentReviewWrap, el.currentReview, []);
    showSection(el.reviewedWrap, el.reviewedVideos, []);
    showSection(el.candidateWrap, el.candidateVideos, []);
  } else if (currentStep === 3) {
    showSection(el.currentReviewWrap, el.currentReview, []);
    showSection(el.reviewedWrap, el.reviewedVideos, []);
    showSection(el.candidateWrap, el.candidateVideos, candidates);
  } else if (currentStep === 4) {
    showSection(el.currentReviewWrap, el.currentReview, current);
    showSection(el.reviewedWrap, el.reviewedVideos, reviewedVideos);
    showSection(el.candidateWrap, el.candidateVideos, []);
  } else {
    showSection(el.currentReviewWrap, el.currentReview, []);
    showSection(el.reviewedWrap, el.reviewedVideos, reviewedVideos);
    showSection(el.candidateWrap, el.candidateVideos, []);
  }

  const friendlyReport = stripMoneySection(
    toFriendlyResearchReport({ run_kind: "knowledge_juice", status: job.status }, job.report_text || "") ||
    (job.error ? toFriendlyJuiceError(job.error) : "")
  );
  el.juiceOutput.textContent = friendlyReport;
  if (el.juiceResultWrap) {
    el.juiceResultWrap.hidden = !friendlyReport && !job.error;
    setMeta(
      el.juiceResultMeta,
      job.error
        ? t("status.error", { error: toFriendlyJuiceError(job.error) })
        : t("status.updated_at", { time: formatTime(job.updated_at) }),
      Boolean(job.error)
    );
  }
}

function selectJob(jobId) {
  state.selectedJobId = jobId;
  writeUiPrefs();
  renderJobs();
  renderSelectedJob();
}

async function loadJobs(activeOnly = false) {
  const data = await apiGet(`/api/knowledge_juice/jobs?active_only=${activeOnly ? "1" : "0"}`);
  const items = data.items || [];
  items.forEach((j) => upsertJob(j));
  if (!state.selectedJobId && items.length) state.selectedJobId = items[0].job_id;
  writeUiPrefs();
  renderJobs();
  renderSelectedJob();
}

function setWsState(text, ok = false) {
  if (!el.wsState) return;
  el.wsState.textContent = text;
  el.wsState.dataset.ok = ok ? "1" : "0";
}

function connectWebSocket() {
  if (!state.runtime || !state.runtime.ws_enabled) {
    setWsState(t("status.live_updates_on_polling"), true);
    return;
  }
  if (state.ws && state.ws.readyState === WebSocket.OPEN) return;

  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  const isHttps = window.location.protocol === "https:";
  const host = window.location.hostname || "127.0.0.1";
  const port = state.runtime.ws_port;
  const path = state.runtime.ws_path || "/ws";
  const url = isHttps
    ? `${proto}://${window.location.host}${path}`
    : `${proto}://${host}:${port}${path}`;

  try {
    setWsState(t("status.connecting_live_updates"), false);
    const ws = new WebSocket(url);
    state.ws = ws;
    ws.onopen = () => setWsState(t("status.live_updates_on"), true);
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data || "{}");
        if (msg.type === "hello") {
          const jobs = msg.active_jobs || [];
          jobs.forEach((j) => upsertJob(j));
          const componentJobs = msg.active_component_jobs || [];
          componentJobs.forEach((j) => upsertComponentJob(j));
          renderJobs();
          renderSelectedJob();
          renderComponentJobs();
          renderSelectedComponentJob();
          return;
        }
        if ((msg.type === "juice_job_created" || msg.type === "juice_job_update") && msg.job) {
          upsertJob(msg.job);
          renderJobs();
          renderSelectedJob();
          return;
        }
        if ((msg.type === "component_job_created" || msg.type === "component_job_update") && msg.job) {
          upsertComponentJob(msg.job);
          renderComponentJobs();
          renderSelectedComponentJob();
        }
      } catch (_err) {
        // ignore malformed WS frames
      }
    };
    ws.onclose = () => {
      setWsState(t("status.live_updates_polling_fallback"), true);
      if (state.wsReconnectTimer) window.clearTimeout(state.wsReconnectTimer);
      state.wsReconnectTimer = window.setTimeout(connectWebSocket, 2500);
    };
    ws.onerror = () => setWsState(t("status.live_updates_polling_fallback"), true);
  } catch (_err) {
    setWsState(t("status.live_updates_polling_fallback"), true);
  }
}

async function initRuntime() {
  try {
    const data = await apiGet("/api/runtime");
    state.runtime = data.runtime || null;
  } catch (_err) {
    state.runtime = { ws_enabled: false };
  }
  const days = Number((state.runtime || {}).retention_days || 0);
  const msg = days > 0 ? t("status.saved_files_retention", { days }) : "";
  setMeta(el.directRetentionMeta, msg);
  setMeta(el.liveRetentionMeta, msg);
}

async function runJuice(ev) {
  ev.preventDefault();
  const topic = (el.juiceTopicInput.value || "").trim();
  if (!topic) {
    setMeta(el.juiceMeta, t("status.topic_required"), true);
    return;
  }

  persistJuicePrefs();
  const prefs = collectJuicePrefs();
  const payload = {
    topic,
    private_run: Boolean(prefs.private_run),
    max_videos: toInt(prefs.max_videos, 6),
    max_queries: toInt(prefs.max_queries, 8),
    per_query: toInt(prefs.per_query, 8),
    min_duration_sec: toInt(prefs.min_duration_sec, 0),
    max_duration_sec: toInt(prefs.max_duration_sec, 0),
    captions_only: Boolean(prefs.captions_only),
  };

  el.juiceRunBtn.disabled = true;
  setMeta(el.juiceMeta, `${t("status.starting_brewing")}\n${funStatusLine("brew", 0)}`);
  try {
    const data = await apiPost("/api/knowledge_juice/start", payload);
    const item = data.item || {};
    upsertJob(item);
    state.selectedJobId = item.job_id || state.selectedJobId;
    writeUiPrefs();
    renderJobs();
    renderSelectedJob();
    setMeta(el.juiceMeta, t("status.brewing_started"));
    switchPage("juice");
  } catch (err) {
    setMeta(el.juiceMeta, toFriendlyJuiceError(String(err.message || err)), true);
  } finally {
    el.juiceRunBtn.disabled = false;
  }
}

function renderStackColumn(node, items) {
  if (!node) return;
  const list = Array.isArray(items) ? items : [];
  node.innerHTML = list.length
    ? list
        .map((row) => {
          const name = String((row || {}).name || "").trim() || "Unknown";
          const details = String((row || {}).details || "").trim() || "No details";
          return `
            <article class="stack-item">
              <p class="line-1">${escapeHtml(name)}</p>
              <p class="line-2">${escapeHtml(details)}</p>
            </article>
          `;
        })
        .join("")
    : `<p class="meta">No stack items.</p>`;
}

function renderStackInfo() {
  const info = state.stackInfo && typeof state.stackInfo === "object" ? state.stackInfo : {};
  const uiItems = Array.isArray(info.ui_part) ? info.ui_part : (info.web || []);
  const beItems = Array.isArray(info.be_side) ? info.be_side : (info.tg_chatbot || []);
  renderStackColumn(el.stackWebList, uiItems);
  renderStackColumn(el.stackTgList, beItems);
  const generated = String(info.generated_at || "").trim();
  setMeta(el.stackMeta, generated ? `Updated: ${formatTime(generated)}` : "");
}

async function loadStackInfo() {
  const data = await apiGet("/api/advanced/stack");
  state.stackInfo = data.item || {};
  renderStackInfo();
}

function upsertComponentJob(job) {
  if (!job || !job.job_id) return;
  state.componentJobs.set(job.job_id, job);
  if (!state.selectedComponentJobId) state.selectedComponentJobId = job.job_id;
}

function componentJobStatusClass(status) {
  const raw = String(status || "").trim().toLowerCase();
  if (raw === "running") return "is-running";
  if (raw === "queued") return "is-queued";
  if (raw === "completed") return "is-done";
  if (raw === "failed") return "is-error";
  return "is-default";
}

function componentCaseStatusClass(status) {
  const raw = String(status || "").trim().toLowerCase();
  if (raw === "running") return "is-running";
  if (raw === "passed") return "is-done";
  if (raw === "failed" || raw === "error") return "is-error";
  if (raw === "skipped") return "is-queued";
  return "is-default";
}

function componentCaseStatusLabel(status) {
  const raw = String(status || "").trim().toLowerCase();
  if (raw === "running") return "Running";
  if (raw === "passed") return "Passed";
  if (raw === "failed") return "Failed";
  if (raw === "error") return "Error";
  if (raw === "skipped") return "Skipped";
  return "Pending";
}

function renderSelectedComponentJob() {
  const currentId = String(state.selectedComponentJobId || "").trim();
  const job = currentId ? state.componentJobs.get(currentId) : null;
  if (!job) {
    if (el.componentProgressBar) el.componentProgressBar.style.width = "0%";
    if (el.componentStats) el.componentStats.innerHTML = "";
    if (el.componentCaseList) el.componentCaseList.innerHTML = "";
    if (el.componentLog) el.componentLog.textContent = "";
    setMeta(el.componentMeta, "No component test runs yet.");
    return;
  }

  const metrics = job.metrics && typeof job.metrics === "object" ? job.metrics : {};
  const progress = Number(metrics.progress_pct || 0);
  const duration = Number(metrics.duration_sec || 0);
  const rate = Number(metrics.pass_rate_pct || 0);
  const failureRate = Number(metrics.failure_rate_pct || 0);

  if (el.componentProgressBar) {
    el.componentProgressBar.style.width = `${Math.max(0, Math.min(100, progress))}%`;
  }
  if (el.componentStats) {
    el.componentStats.innerHTML = `
      <span class="cfg-pill"><strong>${Number(metrics.completed || 0)}</strong> / ${Number(metrics.total || 0)} done</span>
      <span class="cfg-pill"><strong>${Number(metrics.passed || 0)}</strong> passed</span>
      <span class="cfg-pill"><strong>${Number(metrics.failed || 0)}</strong> failed</span>
      <span class="cfg-pill"><strong>${Number(metrics.errors || 0)}</strong> errors</span>
      <span class="cfg-pill"><strong>${Number(metrics.skipped || 0)}</strong> skipped</span>
      <span class="cfg-pill">pass rate <strong>${rate.toFixed(2)}%</strong></span>
      <span class="cfg-pill">failure rate <strong>${failureRate.toFixed(2)}%</strong></span>
      <span class="cfg-pill">${Number(metrics.tests_per_sec || 0).toFixed(2)} tests/s</span>
      <span class="cfg-pill">${Number(metrics.avg_test_ms || 0).toFixed(1)} ms/test</span>
      <span class="cfg-pill">duration ${duration.toFixed(2)}s</span>
    `;
  }
  if (el.componentCaseList) {
    const rows = Array.isArray(job.test_cases) ? job.test_cases : [];
    el.componentCaseList.innerHTML = rows.length
      ? rows
          .map((row) => {
            const st = String((row || {}).status || "pending");
            const stLabel = componentCaseStatusLabel(st);
            const stClass = componentCaseStatusClass(st);
            const label = String((row || {}).label || (row || {}).test_id || "test");
            const idx = Number((row || {}).index || 0);
            return `
              <article class="component-case-item">
                <p class="line-1">${idx > 0 ? `${idx}. ` : ""}${escapeHtml(label)}</p>
                <span class="status-chip ${stClass}">${escapeHtml(stLabel)}</span>
              </article>
            `;
          })
          .join("")
      : `<p class="meta">No per-test details yet.</p>`;
  }
  if (el.componentLog) {
    const lines = Array.isArray(job.log_tail) ? job.log_tail : [];
    el.componentLog.textContent = lines.join("\n");
    el.componentLog.scrollTop = el.componentLog.scrollHeight;
  }
  const summary = String(job.summary || "").trim();
  const status = String(job.status || "").trim();
  const componentLabel = String(job.component_label || job.component || "Component");
  const fallback = `${componentLabel} · ${status}`;
  setMeta(el.componentMeta, summary || fallback, Boolean(job.error));
}

function renderComponentJobs() {
  if (!el.componentJobsList) return;
  const jobs = Array.from(state.componentJobs.values()).sort((a, b) =>
    String(b.updated_at || b.created_at || "").localeCompare(String(a.updated_at || a.created_at || ""))
  );
  if (!state.selectedComponentJobId && jobs.length) state.selectedComponentJobId = jobs[0].job_id;
  if (state.selectedComponentJobId && !jobs.some((j) => j.job_id === state.selectedComponentJobId)) {
    state.selectedComponentJobId = jobs.length ? jobs[0].job_id : "";
  }

  el.componentJobsList.innerHTML = jobs.length
    ? jobs
        .slice(0, 14)
        .map((job) => {
          const metrics = job.metrics && typeof job.metrics === "object" ? job.metrics : {};
          const statusChip = formatStatus(job.status || "");
          const statusClass = componentJobStatusClass(job.status);
          const title = `${job.component_label || job.component || "Component"} · ${statusChip}`;
          const details = `${Number(metrics.completed || 0)}/${Number(metrics.total || 0)} tests · progress ${Number(metrics.progress_pct || 0).toFixed(1)}% · pass ${Number(metrics.pass_rate_pct || 0).toFixed(2)}%`;
          return `
            <button class="item component-job-item${state.selectedComponentJobId === job.job_id ? " active" : ""}" data-id="${escapeHtml(job.job_id || "")}">
              <div class="line-1">
                <span>${escapeHtml(title)}</span>
                <span class="status-chip ${statusClass}">${escapeHtml(statusChip)}</span>
              </div>
              <div class="line-2">${escapeHtml(details)}</div>
            </button>
          `;
        })
        .join("")
    : `<p class="meta">No test jobs yet.</p>`;

  el.componentJobsList.querySelectorAll(".component-job-item").forEach((node) => {
    node.addEventListener("click", () => {
      state.selectedComponentJobId = node.dataset.id || "";
      renderComponentJobs();
      renderSelectedComponentJob();
    });
  });
}

async function loadComponentJobs(activeOnly = false) {
  const data = await apiGet(`/api/component_tests/jobs?active_only=${activeOnly ? "1" : "0"}`);
  const items = Array.isArray(data.items) ? data.items : [];
  items.forEach((job) => upsertComponentJob(job));
  renderComponentJobs();
  renderSelectedComponentJob();
}

async function runComponentTests() {
  const component = String(el.componentTypeSelect?.value || "all").trim() || "all";
  if (el.componentRunBtn) el.componentRunBtn.disabled = true;
  setMeta(el.componentMeta, "Starting component tests...");
  try {
    const data = await apiPost("/api/component_tests/start", { component });
    const item = data.item || {};
    upsertComponentJob(item);
    state.selectedComponentJobId = item.job_id || state.selectedComponentJobId;
    renderComponentJobs();
    renderSelectedComponentJob();
    switchPage("advanced");
  } catch (err) {
    setMeta(el.componentMeta, String(err.message || err), true);
  } finally {
    if (el.componentRunBtn) el.componentRunBtn.disabled = false;
  }
}

async function refreshAll() {
  await Promise.all([loadVideos(true), loadResearches(true), loadJobs(false), loadStackInfo(), loadComponentJobs(false)]);
  renderRecentDirectSearches();
}

function wireEvents() {
  el.langSelect?.addEventListener("change", () => {
    applyLanguage(el.langSelect?.value || "en");
    renderRecentDirectSearches();
    renderVideoLists();
    renderResearches();
    renderJobs();
    renderSelectedJob();
    if (state.selectedVideoId) {
      loadVideoDetail(state.selectedVideoId).catch(() => {});
    } else {
      clearSelectedVideoDetail();
    }
  });
  el.themeToggle?.addEventListener("change", () => {
    applyTheme(el.themeToggle?.checked ? "night" : "day");
  });
  if (el.tabsNav) {
    el.tabsNav.querySelectorAll(".tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        switchPage(tab.dataset.page);
        if (tab.dataset.page === "juice") loadJobs(false).catch(() => {});
        if (tab.dataset.page === "advanced") {
          loadStackInfo().catch(() => {});
          loadComponentJobs(false).catch(() => {});
        }
      });
    });
  }
  if (el.refreshBtn) {
    el.refreshBtn.addEventListener("click", async () => {
      const refreshMetaNode = el.directMeta || el.ingestMeta;
      setMeta(refreshMetaNode, t("status.refreshing"));
      try {
        await refreshAll();
        setMeta(refreshMetaNode, t("status.data_refreshed"));
      } catch (err) {
        setMeta(refreshMetaNode, String(err.message || err), true);
      }
    });
  }
  el.vaultSearchInput?.addEventListener("input", renderVideoLists);
  el.notesSearchInput?.addEventListener("input", renderNotesVideoList);
  el.archiveSearchInput?.addEventListener("input", renderVideoLists);
  el.liveStartForm?.addEventListener("submit", runLiveStart);
  el.researchSearchInput?.addEventListener("input", renderResearches);
  el.juiceFiltersToggle?.addEventListener("click", () => {
    if (!el.juiceFiltersPanel) return;
    el.juiceFiltersPanel.hidden = !el.juiceFiltersPanel.hidden;
    persistJuicePrefs();
  });
  [el.juiceMaxVideos, el.juiceMaxQueries, el.juicePerQuery, el.juiceMinDuration, el.juiceMaxDuration, el.juiceFast, el.juicePrivate]
    .filter(Boolean)
    .forEach((node) => node.addEventListener("change", persistJuicePrefs));
  el.askModeSelect?.addEventListener("change", persistExecModePrefs);
  el.analyzeModeSelect?.addEventListener("change", persistExecModePrefs);
  el.ingestForm?.addEventListener("submit", runIngest);
  el.askForm?.addEventListener("submit", runAsk);
  el.analyzeBtn?.addEventListener("click", runAnalyze);
  el.directForm?.addEventListener("submit", runDirectPrepare);
  el.directUrlInput?.addEventListener("input", () => renderDirectPreview());
  el.juiceForm?.addEventListener("submit", runJuice);
  el.stackRefreshBtn?.addEventListener("click", () => loadStackInfo().catch((err) => setMeta(el.stackMeta, String(err.message || err), true)));
  el.componentRunBtn?.addEventListener("click", () => runComponentTests());
}

async function init() {
  const prefs = readUiPrefs();
  _uiLang = prefs.lang;
  _uiTheme = prefs.theme;
  state.page = _normalizePage(prefs.page || state.page);
  state.selectedResearchId = String(prefs.selected_research_id || "").trim();
  state.selectedJobId = String(prefs.selected_job_id || "").trim();
  state.selectedVideoId = String(prefs.selected_video_id || "").trim();
  applyTheme(_uiTheme, false);
  applyLanguage(_uiLang, false);
  applyJuicePrefs();
  applyExecModePrefs();
  wireEvents();
  syncNotesActionButtons();
  renderRecentDirectSearches();
  switchPage(state.page || "direct", false);
  try {
    await initRuntime();
    connectWebSocket();
    await refreshAll();
    if (state.jobsPollTimer) window.clearInterval(state.jobsPollTimer);
    state.jobsPollTimer = window.setInterval(() => loadJobs(true).catch(() => {}), 5000);
    if (state.livePollTimer) window.clearInterval(state.livePollTimer);
    state.livePollTimer = window.setInterval(() => loadVideos(true).catch(() => {}), 7000);
    if (state.componentPollTimer) window.clearInterval(state.componentPollTimer);
    state.componentPollTimer = window.setInterval(() => loadComponentJobs(false).catch(() => {}), 2500);
    setMeta(el.ingestMeta || el.directMeta, t("status.connected"));
  } catch (err) {
    setMeta(el.ingestMeta || el.directMeta, String(err.message || err), true);
  }
}

init();
