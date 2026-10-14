// TOP[top.webui.ui] 插件管理页前端：音源切换、洛雪源(每源版本号)、同步、音质设置
/* fnmusic-ext WebUI — 原生 JS，无框架无外部资产。 */
"use strict";

// 飞牛桌面用 HTTPS 打开管理窗，页面必须挂在同源路径 /app/fnmusic-ext 下。
// WebUI 自己也会剥掉这个前缀。管理接口只认飞牛网关注入的管理员头。
const APP_BASE = "/app/fnmusic-ext";

const $ = (sel) => document.querySelector(sel);
function escapeHtml(value) {
  const entities = { "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" };
  return String(value == null ? "" : value).replace(/[&<>"']/g, (ch) => entities[ch]);
}

function versionLabel(version) {
  const value = String(version || "").trim();
  return value ? (value.toLowerCase().startsWith("v") ? value : `v${value}`) : "";
}

function redactSensitive(value) {
  const text = typeof value === "string" ? value : JSON.stringify(value ?? "");
  return text
    .replace(/([?&](?:key|token|api_key|apikey)=)[^&\s"']+/gi, "$1***")
    .replace(/(CERU_KEY-)[A-Za-z0-9-]+/gi, "$1***");
}

function versionTag(version) {
  const value = versionLabel(version);
  return value
    ? `<span class="version-tag">${escapeHtml(value)}</span>`
    : '<span class="version-tag muted">版本未知</span>';
}
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const PROVIDER_LABEL = { musicdl: "musicdl 聚合音源", musicbox: "网易云音乐盒子", lxmusic: "洛雪自定义源", none: "未配置" };
const PROC_LABEL = { musicdl: "musicdl", musicbox: "musicbox", lxmusic: "lxmusic", webui: "WebUI" };

let configValues = {};   // GET /api/config 的 values
let platforms = { enabled: [], registered: [] };
let dirty = false;
let qrTimer = null;
let lxVerifiedUrl = null; // 已通过测试的 lx URL（保存时免二次校验提示用）

async function api(path, options) {
  const resp = await fetch(APP_BASE + path, options);
  let body = {};
  try { body = await resp.json(); } catch (_) { /* 非 JSON */ }
  if (!resp.ok) throw new Error(body.error || body.detail || `HTTP ${resp.status}`);
  return body;
}

function toast(message, kind) {
  const el = $("#toast");
  el.textContent = message;
  el.className = "toast " + (kind || "");
  el.hidden = false;
  clearTimeout(el._t);
  el._t = setTimeout(() => { el.hidden = true; }, 3800);
}

function markDirty(note) {
  dirty = true;
  const bar = $("#save-bar");
  if (bar) bar.classList.add("show");
  const noteEl = $("#save-note");
  if (noteEl) noteEl.textContent = note || "有未保存的修改";
}

function clearDirty() {
  dirty = false;
  const bar = $("#save-bar");
  if (bar) bar.classList.remove("show");
  const noteEl = $("#save-note");
  if (noteEl) noteEl.textContent = "";
}

/* -------------------------------------------------------------- 导航 */
function switchPage(page) {
  $$(".page").forEach((el) => el.classList.toggle("active", el.id === "page-" + page));
  $$("[data-page]").forEach((el) => el.classList.toggle("active", el.dataset.page === page));
}
$$("[data-page]").forEach((btn) => btn.addEventListener("click", () => switchPage(btn.dataset.page)));

/* -------------------------------------------------------------- 概览 */
async function loadStatus() {
  try {
    const st = await api("/api/status");
    $("#brand-version").textContent = `v${st.version}`;
    $("#sidebar-foot").textContent = `${PROVIDER_LABEL[st.current_provider] || st.current_provider}`;
    $("#ov-provider-body").innerHTML =
      `<span class="state-line"><span class="dot ok"></span>${PROVIDER_LABEL[st.current_provider] || st.current_provider}</span>`;
    $("#ov-processes").innerHTML = Object.entries(st.processes).map(([name, p]) => {
      const ok = p.state === "RUNNING";
      const cls = ok ? "ok" : p.state === "FATAL" ? "err" : "";
      return `<span class="chip"><span class="dot ${cls}"></span>${PROC_LABEL[name] || name} · ${p.state}</span>`;
    }).join("");
    $("#ov-services").innerHTML = Object.entries(st.services).map(([name, s]) => {
      if (!s.reachable && s.note) return `<span class="state-line"><span class="dot"></span>${PROC_LABEL[name]}：${s.note}</span>`;
      return `<span class="state-line"><span class="dot ${s.reachable ? "ok" : "err"}"></span>${PROC_LABEL[name]}：${s.reachable ? "正常" : "不可达"}</span>`;
    }).join("");
    const lxCard = $("#ov-lx-card");
    if (st.current_provider === "lxmusic" && st.lx_source) {
      lxCard.hidden = false;
      const s = st.lx_source.source;
      $("#lx-main-version").innerHTML = versionTag(s && s.version);
      const rows = [];
      rows.push(`<div class="kv"><b>状态</b>${st.lx_source.initialized ? "已加载" : "未加载"}</div>`);
      if (s) {
        rows.push(`<div class="kv"><b>源名称</b>${s.name || "-"} ${versionLabel(s.version)}</div>`);
        rows.push(`<div class="kv"><b>平台</b>${Object.keys(s.platforms || {}).join("、") || "-"}</div>`);
        rows.push(`<div class="kv"><b>运行</b>${s.running ? "是" : "否"}</div>`);
        if (s.update_alert) {
          const text = redactSensitive(s.update_alert);
          rows.push(`<div class="kv"><b>更新提示</b>${escapeHtml(text)}</div>`);
        }
      }
      if (st.lx_source.last_error) rows.push(`<div class="kv"><b>错误</b>${st.lx_source.last_error}</div>`);
      $("#ov-lx").innerHTML = rows.join("");
    } else {
      lxCard.hidden = true;
    }
  } catch (exc) {
    $("#ov-provider-body").textContent = "状态加载失败：" + exc.message;
  }
}

/* -------------------------------------------------------------- 配置读写 */
async function loadConfig() {
  const cfg = await api("/api/config");
  configValues = cfg.values;
  applyConfigToForm();
  clearDirty();
}

function applyConfigToForm() {
  const v = configValues;
  const provider = v.FNMUSIC_NETEASE_ENABLED === "true" ? "musicbox"
    : v.FNMUSIC_MUSICDL_ENABLED === "true" ? "musicdl"
    : v.FNMUSIC_LX_ENABLED === "true" ? "lxmusic" : "";
  $$("input[name=provider]").forEach((el) => { el.checked = el.value === provider; });
  syncProviderPanels(provider);
  if (provider === "musicbox") syncNeteaseAccount();
  const quality = v.FNMUSIC_QUALITY_MODE || "high";
  $$("input[name=quality]").forEach((el) => { el.checked = el.value === quality; });
  $("#recommend-hot").checked = v.FNMUSIC_RECOMMEND_HOT === "true";
  $("#recommend-daily").checked = v.FNMUSIC_RECOMMEND_DAILY === "true";
  $("#tee-enabled").checked = v.FNMUSIC_TEE_SAVE_ENABLED === "true";
  $("#auto-cover").checked = v.FNMUSIC_AUTO_COVER !== "false";
  $("#lyric-auto-dl").checked = v.FNMUSIC_LYRIC_AUTO_DL === "true";
  $("#fav-autobind").checked = v.FNMUSIC_FAV_AUTO_BIND === "true";
  $("#tee-dir").value = v.FNMUSIC_TEE_SAVE_DIR || "";
  $("#tee-max").value = v.FNMUSIC_TEE_CACHE_MAX || "2";
  $("#bind-timeout").value = v.FNMUSIC_OFFICIAL_BIND_TIMEOUT_S || "120";
  $("#handoff-max").value = v.FNMUSIC_TEE_HANDOFF_MAX != null ? v.FNMUSIC_TEE_HANDOFF_MAX : "3";
  $("#scan-path").value = v.FNMUSIC_LIBRARY_SCAN_PATH || "";
  updateTeeCountLabel();
  updateBindTimeoutLabel();
  $("#llm-base").value = v.FNMUSIC_LLM_BASE_URL || "";
  $("#llm-key").value = v.FNMUSIC_LLM_API_KEY || "";
  $("#llm-model").value = v.FNMUSIC_LLM_MODEL || "";
  $("#search-timeout").value = v.FNMUSIC_SEARCH_TIMEOUT || "15";
  $("#search-probe").checked = v.FNMUSIC_SEARCH_PROBE === "true";
  $("#wan-direct").checked = v.FNMUSIC_WAN_DIRECT !== "false";
  $("#netease-my-playlists").checked = v.FNMUSIC_NETEASE_MY_PLAYLISTS === "true";
  $("#ceru-sync-enabled").checked = v.FNMUSIC_CERU_SYNC_ENABLED === "true";
  $("#ceru-sync-host").value = v.FNMUSIC_CERU_SYNC_HOST || "";
  $("#ceru-sync-port").value = v.FNMUSIC_CERU_SYNC_PORT || "31231";
  $("#ceru-sync-use-https").checked = v.FNMUSIC_CERU_SYNC_USE_HTTPS === "true";
  $("#ceru-sync-pair-code").value = v.FNMUSIC_CERU_SYNC_PAIR_CODE || "";
  $("#lx-url").value = v.LX_SOURCE_URL || "";
  lxVerifiedUrl = v.LX_SOURCE_URL || null;
  renderPlatformChips();
}

function collectConfig() {
  const provider = ($$("input[name=provider]").find((el) => el.checked) || {}).value || "";
  const values = {
    FNMUSIC_MUSICDL_ENABLED: provider === "musicdl",
    FNMUSIC_NETEASE_ENABLED: provider === "musicbox",
    FNMUSIC_LX_ENABLED: provider === "lxmusic",
    FNMUSIC_QUALITY_MODE: ($$("input[name=quality]").find((el) => el.checked) || {}).value || "high",
    FNMUSIC_RECOMMEND_HOT: $("#recommend-hot").checked,
    FNMUSIC_RECOMMEND_DAILY: $("#recommend-daily").checked,
    FNMUSIC_TEE_SAVE_ENABLED: $("#tee-enabled").checked,
    FNMUSIC_AUTO_COVER: $("#auto-cover").checked,
    FNMUSIC_LYRIC_AUTO_DL: $("#lyric-auto-dl").checked,
    FNMUSIC_FAV_AUTO_BIND: $("#fav-autobind").checked,
    FNMUSIC_TEE_SAVE_DIR: $("#tee-dir").value.trim(),
    FNMUSIC_TEE_CACHE_MAX: parseInt($("#tee-max").value || "2", 10),
    FNMUSIC_OFFICIAL_BIND_TIMEOUT_S: parseInt($("#bind-timeout").value || "120", 10) || 120,
    FNMUSIC_TEE_HANDOFF_MAX: parseInt($("#handoff-max").value || "3", 10) || 0,
    FNMUSIC_LIBRARY_SCAN_PATH: $("#scan-path").value.trim(),
    FNMUSIC_LLM_BASE_URL: $("#llm-base").value.trim(),
    FNMUSIC_LLM_API_KEY: $("#llm-key").value.trim(),
    FNMUSIC_LLM_MODEL: $("#llm-model").value.trim(),
    FNMUSIC_SEARCH_TIMEOUT: parseInt($("#search-timeout").value || "15", 10) || 15,
    FNMUSIC_SEARCH_PROBE: $("#search-probe").checked,
    FNMUSIC_WAN_DIRECT: $("#wan-direct").checked,
    FNMUSIC_NETEASE_MY_PLAYLISTS: $("#netease-my-playlists").checked,
    FNMUSIC_CERU_SYNC_ENABLED: $("#ceru-sync-enabled").checked,
    FNMUSIC_CERU_SYNC_HOST: $("#ceru-sync-host").value.trim(),
    FNMUSIC_CERU_SYNC_PORT: parseInt($("#ceru-sync-port").value || "31231", 10) || 31231,
    FNMUSIC_CERU_SYNC_USE_HTTPS: $("#ceru-sync-use-https").checked,
    FNMUSIC_CERU_SYNC_PAIR_CODE: $("#ceru-sync-pair-code").value.trim(),
  };
  if (provider === "musicdl") {
    values.FNMUSIC_ONLINE_SOURCES = platforms.enabled.join(",");
    values.MUSICDL_SOURCES = platforms.enabled.join(",");
  }
  if (provider === "lxmusic") {
    const url = $("#lx-url").value.trim();
    if (!url) throw new Error("洛雪源需要填写脚本 URL");
    values.LX_SOURCE_URL = url;
  }
  return values;
}

async function saveConfig() {
  let values;
  try { values = collectConfig(); } catch (exc) { toast(exc.message, "fail"); return; }
  const btn = $("#save-btn");
  btn.disabled = true;
  btn.textContent = "保存中…";
  try {
    const result = await api("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ values }),
    });
    const failed = (result.actions || []).filter((a) => !a.ok);
    const parts = [];
    if (result.changed && result.changed.length) parts.push(`已保存 ${result.changed.length} 项`);
    (result.actions || []).forEach((a) => {
      if (a.kind === "process") parts.push(`进程 ${a.program} ${a.op} ${a.ok ? "成功" : "失败"}`);
      if (a.kind === "lx_activate") parts.push(`洛雪源激活${a.ok ? "成功" : "失败"}`);
    });
    if (failed.length) {
      toast((parts.join("；") || "") + ` —— ${failed.map((f) => f.error).join("；")}`, "fail");
    } else {
      toast(parts.join("；") || "配置无变化", "ok");
    }
    clearDirty();
    await loadConfig();
    await loadStatus();
  } catch (exc) {
    toast("保存失败：" + exc.message, "fail");
  } finally {
    btn.disabled = false;
    btn.textContent = "保存并生效";
  }
}
$("#save-btn").addEventListener("click", saveConfig);

/* -------------------------------------------------------------- Ceru 同步 */
async function ceruLogin() {
  const btn = $("#ceru-login");
  btn.disabled = true;
  try {
    const r = await api("/api/ceru/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        host: $("#ceru-sync-host").value.trim(),
        port: parseInt($("#ceru-sync-port").value || "31231", 10) || 31231,
        use_https: $("#ceru-sync-use-https").checked,
        pair_code: $("#ceru-sync-pair-code").value.trim(),
      }),
    });
    $("#ceru-status-note").textContent = r.message || "已连接";
    toast(r.message || "登录成功", "ok");
    await loadConfig();
    await ceruRefreshStatus();
  } catch (exc) {
    $("#ceru-status-note").textContent = "未连接";
    toast("登录失败：" + exc.message, "fail");
  } finally {
    btn.disabled = false;
  }
}

async function ceruRefreshStatus() {
  try {
    const r = await api("/api/ceru/status");
    const el = $("#ceru-status-note");
    if (r.connected) {
      const bits = [];
      if (r.user && r.user.username) bits.push(r.user.username);
      if (r.counts && typeof r.counts.playlists === "number") bits.push(`${r.counts.playlists} 个歌单`);
      if (r.counts && typeof r.counts.favorites === "number") bits.push(`${r.counts.favorites} 首收藏`);
      el.textContent = "已连接" + (bits.length ? " · " + bits.join(" · ") : "");
    } else if (r.configured) {
      el.textContent = r.error || "未连接";
    } else {
      el.textContent = "未配置";
    }
  } catch (_) {
    $("#ceru-status-note").textContent = "未连接";
  }
}
$("#ceru-login").addEventListener("click", ceruLogin);
$("#ceru-status").addEventListener("click", ceruRefreshStatus);

/* -------------------------------------------------------------- 音源选择 */
function savedProvider() {
  const v = configValues;
  return v.FNMUSIC_NETEASE_ENABLED === "true" ? "musicbox"
    : v.FNMUSIC_MUSICDL_ENABLED === "true" ? "musicdl"
    : v.FNMUSIC_LX_ENABLED === "true" ? "lxmusic" : "";
}

// 点选未启用的音源：临时拉起其进程供预览（不写配置；5 分钟内未保存自动停止）
async function startPreview(provider) {
  try {
    const r = await api("/api/preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ provider }),
    });
    if (r.preview) toast(`已临时启动 ${PROVIDER_LABEL[provider]}（预览）：5 分钟内未保存将自动停止`);
    return true;
  } catch (exc) {
    toast(`临时启动 ${PROVIDER_LABEL[provider]} 失败：${exc.message}`, "fail");
    return false;
  }
}

function syncProviderPanels(provider) {
  $$(".provider-card").forEach((el) => el.classList.toggle("selected", el.dataset.provider === provider));
  $("#panel-musicbox").hidden = provider !== "musicbox";
  $("#panel-musicdl").hidden = provider !== "musicdl";
  $("#panel-lxmusic").hidden = provider !== "lxmusic";
}
$$("input[name=provider]").forEach((el) =>
  el.addEventListener("change", async () => {
    syncProviderPanels(el.value);
    if (el.value === "musicbox") syncNeteaseAccount();
    markDirty("音源切换需保存后生效");
    if (el.value && el.value !== savedProvider() && await startPreview(el.value)) {
      if (el.value === "musicdl") await loadPlatforms(false);
    }
  }));

/* -------------------------------------------------------------- musicdl 平台 */
function setPlatformFallback() {
  platforms = { enabled: (configValues.FNMUSIC_ONLINE_SOURCES || "").split(",").filter(Boolean), registered: [] };
  $("#platform-note").textContent = "musicdl 进程未运行，暂无法获取平台列表（点选 musicdl 音源可临时启动预览）";
  renderPlatformChips();
}

async function fetchPlatforms() {
  const data = await api("/api/platforms");
  platforms = { enabled: data.enabled || [], registered: data.registered || [] };
  $("#platform-note").textContent = `共 ${platforms.registered.length} 个注册平台，已启用 ${platforms.enabled.length} 个`;
  renderPlatformChips();
}

async function loadPlatforms(autoPreview = true) {
  try {
    await fetchPlatforms();
  } catch (_) {
    // 进程未运行：autoPreview（用户点选/刷新触发）时临时拉起后重试；页面加载不自动拉起
    if (!autoPreview) { setPlatformFallback(); return; }
    try {
      if (await startPreview("musicdl")) await fetchPlatforms();
      else setPlatformFallback();
    } catch (_) {
      setPlatformFallback();
    }
  }
}

function renderPlatformChips() {
  const keyword = $("#platform-search").value.trim().toLowerCase();
  const container = $("#platform-list");
  const enabledSet = new Set(platforms.enabled);
  const items = platforms.registered.filter((p) => !keyword || p.toLowerCase().includes(keyword));
  container.innerHTML = items.length
    ? items.map((p) => `<span class="chip ${enabledSet.has(p) ? "on" : ""}" data-platform="${p}">${p}</span>`).join("")
    : `<span class="muted">无匹配平台</span>`;
  container.querySelectorAll(".chip[data-platform]").forEach((chip) =>
    chip.addEventListener("click", () => {
      const p = chip.dataset.platform;
      const set = new Set(platforms.enabled);
      if (set.has(p)) { set.delete(p); chip.classList.remove("on"); }
      else { set.add(p); chip.classList.add("on"); }
      platforms.enabled = Array.from(set);
      markDirty("musicdl 平台已修改");
    }));
}
$("#platform-search").addEventListener("input", renderPlatformChips);
$("#platform-reload").addEventListener("click", loadPlatforms);

/* -------------------------------------------------------------- 网易扫码 */
async function syncNeteaseAccount(retries = 0) {
  // 把网易账号态同步到 #qr-check（已登录显示昵称；未登录/失败清空）
  for (let i = 0; ; i++) {
    try {
      const st = await api("/api/netease/auth/status");
      const d = st.data || st;
      if (d.logged_in) {
        $("#qr-check").textContent = `当前登录：${d.nickname || d.user_id || "已登录用户"}`;
        return;
      }
    } catch (_) { /* status 不可达视为未登录 */ }
    if (i >= retries) { $("#qr-check").textContent = ""; return; }
    await new Promise((r) => setTimeout(r, 1500));
  }
}

async function startQrLogin() {
  stopQrPolling();
  $("#qr-status").textContent = "正在生成二维码…";
  $("#qr-img").hidden = true;
  syncNeteaseAccount(); // 生成前同步当前账号态（非致命，不阻塞出码）
  const callLogin = () => api("/api/netease/auth/login", { method: "POST" });
  try {
    let data;
    try {
      data = await callLogin();
    } catch (exc) {
      // musicbox 进程未运行（未点选/预览过期）：临时拉起后重试一次
      if (!await startPreview("musicbox")) throw exc;
      data = await callLogin();
    }
    const unikey = data.unikey || data.codekey || (data.data && (data.data.unikey || data.data.codekey)) || "";
    if (!unikey) throw new Error("未获取到 unikey");
    $("#qr-img").src = `${APP_BASE}/api/netease/qr?unikey=${encodeURIComponent(unikey)}`;
    $("#qr-img").hidden = false;
    $("#qr-status").textContent = "请用手机网易云音乐 App 扫码";
    pollQr(unikey);
  } catch (exc) {
    $("#qr-status").textContent = "生成失败：" + exc.message;
  }
}

async function checkQrStatus(unikey) {
  const st = await api(`/api/netease/auth/login/check?unikey=${encodeURIComponent(unikey)}`);
  // musicbox CLI 返回 {ok, data:{code}} 信封结构；兼容扁平 {code}
  const code = st?.data?.code ?? st?.code;
  if (code === 803) {
    stopQrPolling();
    $("#qr-status").textContent = "登录成功 ✓";
    syncNeteaseAccount(2); // 803 后 cookie 落盘需要一点时间，带重试取昵称
    toast("网易账号登录成功", "ok");
  } else if (code === 802) {
    $("#qr-status").textContent = "已扫码，请在手机上确认";
  } else if (code === 800) {
    stopQrPolling();
    $("#qr-status").textContent = "二维码已过期，请重新生成";
  } else {
    $("#qr-status").textContent = "等待扫码…";
  }
}

function pollQr(unikey, intervalMs = 2000) {
  stopQrPolling();
  qrTimer = setInterval(() => { checkQrStatus(unikey).catch(() => {}); }, intervalMs);
}

function stopQrPolling() {
  if (qrTimer) { clearInterval(qrTimer); qrTimer = null; }
}
$("#qr-btn").addEventListener("click", startQrLogin);

/* -------------------------------------------------------------- lx 源测试 */
$("#lx-test").addEventListener("click", async () => {
  const url = $("#lx-url").value.trim();
  const box = $("#lx-report");
  if (!url) { toast("请先填写源 URL", "fail"); return; }
  box.hidden = false;
  box.className = "report";
  box.textContent = "测试中（下载脚本 → 沙箱初始化 → 多首抽样搜索/解析/探活）…";
  const callVerify = () => api("/api/lx/verify", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ values: { url } }),
  });
  try {
    let r;
    try {
      r = await callVerify();
    } catch (exc) {
      // lxmusic 进程未运行（未点选/预览过期）：临时拉起后重试一次
      if (!await startPreview("lxmusic")) throw exc;
      r = await callVerify();
    }
    if (r.ok) {
      const d = r.data || {};
      const meta = d.meta || {};
      const probe = d.probe || {};
      lxVerifiedUrl = url;
      box.className = "report ok";
      box.innerHTML =
        `<div class="kv"><b>源名称</b>${meta.name || "-"} ${versionLabel(meta.version)}（${meta.author || "未知作者"}）</div>` +
        `<div class="kv"><b>可用平台</b>${(d.platforms || []).join("、")}</div>` +
        (probe.title ? `<div class="kv"><b>实测</b>${probe.title} - ${probe.artist} [${probe.platform}/${probe.quality}] ${probe.content_type || ""}</div>` : "") +
        `<div class="kv"><b>结论</b>可用 ✓（保存后激活）</div>`;
    } else {
      const d = r.data || {};
      box.className = "report fail";
      box.innerHTML = `<div class="kv"><b>不可用</b>${d.message || r.error || "校验失败"}</div>`;
    }
  } catch (exc) {
    box.className = "report fail";
    box.textContent = "测试失败：" + exc.message;
  }
});

/* -------------------------------------------------- lx 源：文件上传 / NAS 选择 */
async function lxUploadScript(filename, script) {
  // 先确保 lxmusic 进程可用（预览拉起），再转发落盘
  const callUpload = () => api("/api/lx/upload", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ filename, script }),
  });
  try {
    return await callUpload();
  } catch (exc) {
    if (!await startPreview("lxmusic")) throw exc;
    return await callUpload();
  }
}

async function lxAfterUpload(r) {
  const d = r.data || {};
  $("#lx-url").value = d.url || "";
  lxVerifiedUrl = null; // 上传地址仍需走一次"测试"
  $("#lx-upload-note").textContent = d.meta && d.meta.name ? `已上传：${d.meta.name}` : "已上传";
  markDirty("洛雪源已更新为上传脚本，测试后保存生效");
  toast("脚本已上传，请点「测试」验证后保存", "ok");
}

$("#lx-upload").addEventListener("click", () => $("#lx-file").click());
$("#lx-file").addEventListener("change", async () => {
  const file = $("#lx-file").files && $("#lx-file").files[0];
  if (!file) return;
  if (!file.name.toLowerCase().endsWith(".js")) { toast("只支持 .js 后缀文件", "fail"); return; }
  if (file.size > 9_000_000) { toast("脚本超过 9MB 上限", "fail"); return; }
  $("#lx-upload-note").textContent = "读取并上传中…";
  try {
    const script = await file.text();
    const r = await lxUploadScript(file.name, script);
    await lxAfterUpload(r);
  } catch (exc) {
    $("#lx-upload-note").textContent = "";
    toast("上传失败：" + exc.message, "fail");
  } finally {
    $("#lx-file").value = ""; // 允许重复选择同一文件
  }
});

/* -------------------------------------------------- lx 附加源池（多源启用/兜底） */
let lxPoolRetryTimer = null;
function renderLxPoolPlaceholder(text) {
  const box = $("#lx-source-list");
  box.innerHTML = `<div class="muted">${escapeHtml(text)}</div>`;
}

function lxSourceRow(it, index) {
  const meta = it.source || {};
  const version = meta.version || "";
  const platforms = meta.platforms ? Object.keys(meta.platforms).join(" / ") : "";
  const primary = !!it.primary;
  const on = primary ? true : !!it.enabled;
  let state;
  if (it.running) state = '<span class="dot ok"></span>运行中';
  else if (primary && it.configured === false) state = '<span class="dot"></span>未配置';
  else if (it.last_error) state = '<span class="dot err"></span>异常';
  else if (!on) state = '<span class="dot"></span>已停用';
  else state = '<span class="dot warn"></span>待启动';
  const err = it.last_error
    ? `<div class="sub">错误：${escapeHtml(redactSensitive(it.last_error))}</div>` : "";
  const alertInfo = it.update_alert
    ? `<div class="sub">更新提示：${escapeHtml(redactSensitive(it.update_alert))}</div>` : "";
  const badge = primary
    ? '<span class="badge primary">主源</span>'
    : `<span class="badge">附加 ${index}</span>`;
  // 主源由安装流程激活、始终参与播放，不提供停用/删除；附加源可逐个开关
  const poolSwitches = primary ? "" : `
      <label class="sub"><input type="checkbox" class="lx-pool-enabled" ${it.enabled ? "checked" : ""}> 启用</label>
      <label class="sub"><input type="checkbox" class="lx-pool-auto" ${it.auto_update ? "checked" : ""}> 检测到脚本变化自动更新</label>`;
  const removeBtn = primary ? "" : '<button class="btn lx-pool-remove">删除</button>';
  return `<div class="source-row${on ? "" : " is-off"}" data-lx-source="${escapeHtml(it.id)}"
      data-primary="${primary ? "1" : ""}" data-current-version="${escapeHtml(version)}">
    <div class="row-head">
      ${state}
      <span class="row-title">${escapeHtml(it.name || it.id)}</span>
      ${badge}${versionTag(version)}
      <span class="row-actions">
        <button class="btn lx-src-check">检查更新</button>
        <button class="btn lx-src-update">立即更新</button>
        ${removeBtn}
      </span>
    </div>
    <div class="sub">${escapeHtml(it.url || "本地上传脚本")}${platforms ? " · " + escapeHtml(platforms) : ""} · 版本：${escapeHtml(versionLabel(version) || "未知")}</div>
    ${err}${alertInfo}${poolSwitches}
  </div>`;
}

function renderLxSources(items) {
  const box = $("#lx-source-list");
  if (!items.length) {
    box.innerHTML = '<div class="muted">还没有音源。用下面的地址或上传按钮添加洛雪源脚本。</div>';
    return;
  }
  let poolIndex = 0;
  box.innerHTML = items.map((it) => {
    const index = it.primary ? 0 : (poolIndex += 1);
    return lxSourceRow(it, index);
  }).join("");

  const rowOf = (el) => el.closest("[data-lx-source]");
  const isPrimary = (row) => row.dataset.primary === "1";

  box.querySelectorAll(".lx-pool-enabled").forEach((el) => {
    el.addEventListener("change", async (ev) => {
      const id = rowOf(ev.target).dataset.lxSource;
      try {
        await api(`/api/lx/sources/${encodeURIComponent(id)}/enabled`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ values: { enabled: ev.target.checked } }),
        });
        toast(ev.target.checked ? "已启用该源" : "已停用该源", "ok");
      } catch (exc) { toast("切换失败：" + exc.message, "fail"); }
      await loadLxPool();
    });
  });
  box.querySelectorAll(".lx-pool-remove").forEach((el) => {
    el.addEventListener("click", async (ev) => {
      const id = rowOf(ev.target).dataset.lxSource;
      try {
        await api(`/api/lx/sources/${encodeURIComponent(id)}`, { method: "DELETE" });
        toast("附加源已删除", "ok");
      } catch (exc) { toast("删除失败：" + exc.message, "fail"); }
      await loadLxPool();
    });
  });
  box.querySelectorAll(".lx-pool-auto").forEach((el) => {
    el.addEventListener("change", async (ev) => {
      const id = rowOf(ev.target).dataset.lxSource;
      try {
        await api(`/api/lx/sources/${encodeURIComponent(id)}/auto-update`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ values: { auto_update: ev.target.checked } }),
        });
        toast(ev.target.checked ? "已开启自动更新" : "已关闭自动更新", "ok");
      } catch (exc) { toast("设置失败：" + exc.message, "fail"); }
      await loadLxPool();
    });
  });
  box.querySelectorAll(".lx-src-check").forEach((el) => {
    el.addEventListener("click", async (ev) => {
      const row = rowOf(ev.target);
      const primary = isPrimary(row);
      const path = primary ? "/api/lx/source/check-update"
        : `/api/lx/sources/${encodeURIComponent(row.dataset.lxSource)}/check-update`;
      ev.target.disabled = true;
      try {
        const r = await api(path, { method: "POST" });
        const d = r.data || {};
        if (d.error) throw new Error(d.error);
        const current = versionLabel(d.current_version || row.dataset.currentVersion);
        const latest = versionLabel(d.latest_version || (d.meta || {}).version);
        if (d.available && !d.auto_applied) {
          toast(`发现新版本${latest ? ` ${latest}` : ""}${current ? `，当前 ${current}` : ""}，请点击“立即更新”`, "ok");
        } else if (d.available) {
          toast(`已自动更新${latest ? `到 ${latest}` : ""}`, "ok");
        } else {
          toast(`已是最新版本${current ? ` ${current}` : ""}`, "ok");
        }
      } catch (exc) { toast("检查更新失败：" + exc.message, "fail"); }
      ev.target.disabled = false;
      await loadLxPool();
    });
  });
  box.querySelectorAll(".lx-src-update").forEach((el) => {
    el.addEventListener("click", async (ev) => {
      const row = rowOf(ev.target);
      const primary = isPrimary(row);
      const path = primary ? "/api/lx/source/update"
        : `/api/lx/sources/${encodeURIComponent(row.dataset.lxSource)}/update`;
      ev.target.disabled = true;
      try {
        const r = await api(path, { method: "POST" });
        const d = r.data || {};
        const version = versionLabel(d.source?.source?.version || d.source?.version || d.meta?.version);
        if (primary) await loadStatus();
        toast(d.updated ? `已更新并重启${version ? `到 ${version}` : ""}` : "无需更新", "ok");
      } catch (exc) { toast("更新失败：" + exc.message, "fail"); }
      ev.target.disabled = false;
      await loadLxPool();
    });
  });
}

async function loadLxPool() {
  // 页面打开时音源服务可能尚未启动：已有源不能因此显示为空。
  const load = () => api("/api/lx/sources");
  try {
    const r = await load();
    if (lxPoolRetryTimer) { clearTimeout(lxPoolRetryTimer); lxPoolRetryTimer = null; }
    renderLxSources((r.data && r.data.items) || []);
    return;
  } catch (exc) {
    const lxEnabled = configValues.FNMUSIC_LX_ENABLED === true || configValues.FNMUSIC_LX_ENABLED === "true";
    if (lxEnabled && await startPreview("lxmusic")) {
      try {
        const r = await load();
        if (lxPoolRetryTimer) { clearTimeout(lxPoolRetryTimer); lxPoolRetryTimer = null; }
        renderLxSources((r.data && r.data.items) || []);
        return;
      } catch (_) { /* fall through：下面继续等待重试 */ }
    }
    // 升级/重装后容器重启，服务可能尚未就绪：显示空列表会让人以为源丢了。
    // 改为显示等待提示并自动重试（数据一直保存在 sources-data/lxmusic/sources.json，不会丢失）。
    if (lxEnabled && !lxPoolRetryTimer) {
      renderLxPoolPlaceholder("音源服务尚未就绪：已有源不会丢失，正在自动重试…");
      let tries = 0;
      const tick = async () => {
        lxPoolRetryTimer = null;
        try {
          const r = await load();
          renderLxSources((r.data && r.data.items) || []);
          return;
        } catch (_) { /* 服务还没好，继续等 */ }
        tries += 1;
        if (lxEnabled && tries < 5) {
          renderLxPoolPlaceholder(`音源服务尚未就绪，3 秒后重试…（${tries}/5）`);
          lxPoolRetryTimer = setTimeout(tick, 3000);
        } else {
          renderLxPoolPlaceholder("音源服务未就绪：请刷新本页，或在概览页查看 lxmusic 状态。");
        }
      };
      lxPoolRetryTimer = setTimeout(tick, 3000);
    } else if (!lxEnabled) {
      renderLxPoolPlaceholder("lxmusic 未启用：请先在上方音源选择中勾选「洛雪自定义源」。");
    }
  }
}

async function lxPoolAddUrl(url, name = "") {
  const call = () => api("/api/lx/sources", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ values: { url, name } }),
  });
  try {
    const r = await call();
    return r.data || {};
  } catch (exc) {
    if (!await startPreview("lxmusic")) throw exc;
    const r = await call();
    return r.data || {};
  }
}

$("#lx-pool-add").addEventListener("click", async () => {
  const url = $("#lx-pool-url").value.trim();
  if (!url) { toast("请先填写附加洛雪源 URL", "fail"); return; }
  try {
    await lxPoolAddUrl(url);
    $("#lx-pool-url").value = "";
    toast("附加源已添加并启用", "ok");
  } catch (exc) { toast("添加失败：" + exc.message, "fail"); }
  await loadLxPool();
});

$("#lx-pool-upload").addEventListener("click", () => $("#lx-pool-file").click());
async function lxPoolUploadFiles(files) {
  const list = Array.from(files || []);
  if (!list.length) return { ok: 0, failed: [] };
  const failed = [];
  let ok = 0;
  // 串行上传，避免一次选择多个脚本时并发打爆音源服务。
  for (const file of list) {
    try {
      if (!file.name.toLowerCase().endsWith(".js")) throw new Error("只支持 .js 后缀文件");
      if (file.size > 9_000_000) throw new Error("脚本超过 9MB 上限");
      const r = await lxUploadScript(file.name, await file.text());
      const url = (r.data || {}).url || "";
      if (!url) throw new Error("上传未返回文件地址");
      await lxPoolAddUrl(url, (r.data || {}).meta?.name || file.name);
      ok += 1;
    } catch (exc) {
      failed.push(`${file.name}: ${exc.message}`);
    }
  }
  return { ok, failed };
}

$("#lx-pool-file").addEventListener("change", async () => {
  const files = $("#lx-pool-file").files;
  try {
    const result = await lxPoolUploadFiles(files);
    if (!result.ok && !result.failed.length) return;
    if (result.ok && !result.failed.length) toast(`已上传并启用 ${result.ok} 个附加洛雪源`, "ok");
    else if (result.ok) toast(`已上传 ${result.ok} 个；失败 ${result.failed.length} 个`, "ok");
    else toast(`上传失败：${result.failed[0]}`, "fail");
  } catch (exc) {
    toast("上传失败：" + exc.message, "fail");
  } finally {
    $("#lx-pool-file").value = "";
  }
  await loadLxPool();
});

/* ------------------------------------------------- 导入歌单（洛雪 JSON / 平台链接） */
// 数据落在宿主数据根的一堆 JSON 文件里，由 webui_gateway 在宿主侧直接读写
// （容器看不到那个目录），飞牛音乐「音乐」页再把这些歌单当在线歌单渲染。
const IMP_ORIGIN_LABEL = { link: "链接导入", lxjson: "洛雪歌单" };

function impTime(epoch) {
  const sec = Number(epoch || 0);
  if (!sec) return "-";
  const d = new Date(sec * 1000);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function renderImported(items) {
  const box = $("#imp-list");
  if (!box) return;
  if (!items.length) {
    box.innerHTML = '<div class="muted">还没有导入歌单。粘贴一个分享链接或洛雪导出的歌单 JSON 试试。</div>';
    return;
  }
  box.innerHTML = items.map((it) => {
    const origin = IMP_ORIGIN_LABEL[it.origin] || it.origin || "导入";
    const platform = it.platform ? ` · ${escapeHtml(String(it.platform).toUpperCase())}` : "";
    const refreshBtn = it.refreshable ? '<button class="btn imp-refresh">刷新</button>' : "";
    return `<div class="import-row" data-imp-id="${escapeHtml(it.id)}">
      <span class="row-title">${escapeHtml(it.name || "未命名歌单")}</span>
      <span class="badge">${Number(it.count || 0)} 首</span>
      <span class="row-actions">${refreshBtn}<button class="btn imp-delete">删除</button></span>
      <div class="sub">${escapeHtml(origin)}${platform} · 导入于 ${impTime(it.created_at)} · 更新于 ${impTime(it.updated_at)}</div>
    </div>`;
  }).join("");

  const idOf = (el) => el.closest("[data-imp-id]").dataset.impId;
  box.querySelectorAll(".imp-refresh").forEach((el) => {
    el.addEventListener("click", async (ev) => {
      const target = ev.target;
      target.disabled = true;
      target.textContent = "刷新中…";
      try {
        const r = await api("/api/playlists/imported/refresh", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ id: idOf(target) }),
        });
        const d = r.data || {};
        toast(`已刷新：${d.name || "歌单"}（${Number(d.count || 0)} 首）`, "ok");
      } catch (exc) { toast("刷新失败：" + exc.message, "fail"); }
      await loadImported();
    });
  });
  box.querySelectorAll(".imp-delete").forEach((el) => {
    el.addEventListener("click", async (ev) => {
      const row = ev.target.closest("[data-imp-id]");
      if (!window.confirm(`删除歌单「${row.querySelector(".row-title").textContent}」？`)) return;
      try {
        await api("/api/playlists/imported/delete", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ id: row.dataset.impId }),
        });
        toast("已删除", "ok");
      } catch (exc) { toast("删除失败：" + exc.message, "fail"); }
      await loadImported();
    });
  });
}

async function loadImported() {
  const box = $("#imp-list");
  if (!box) return;
  try {
    const r = await api("/api/playlists/imported");
    renderImported(r.data || []);
  } catch (exc) {
    box.innerHTML = `<div class="muted">导入歌单暂不可用：${escapeHtml(exc.message)}</div>`;
  }
}

async function importPlaylistText(text, name) {
  const r = await api("/api/playlists/import", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text, name: name || "" }),
  });
  return (r.data || []).map((it) => `${it.name || "未命名歌单"}（${Number(it.count || 0)} 首）`).join("、");
}

$("#imp-submit").addEventListener("click", async () => {
  const text = $("#imp-text").value.trim();
  if (!text) { toast("请先粘贴歌单链接、口令或 JSON 内容", "fail"); return; }
  const btn = $("#imp-submit");
  btn.disabled = true;
  btn.textContent = "导入中…";
  try {
    const label = await importPlaylistText(text, $("#imp-name").value.trim());
    if (!label) throw new Error("没有识别到歌单：请确认链接是歌单（不是单曲/歌手），或 JSON 是洛雪导出的歌单");
    toast(`已导入 ${label}`, "ok");
    $("#imp-text").value = "";
    $("#imp-name").value = "";
  } catch (exc) {
    toast("导入失败：" + exc.message, "fail");
  } finally {
    btn.disabled = false;
    btn.textContent = "导入";
  }
  await loadImported();
});

$("#imp-upload").addEventListener("click", () => $("#imp-file").click());
$("#imp-file").addEventListener("change", async () => {
  const file = $("#imp-file").files && $("#imp-file").files[0];
  if (!file) return;
  if (file.size > 8_000_000) { toast("文件超过 8MB 上限", "fail"); $("#imp-file").value = ""; return; }
  const btn = $("#imp-submit");
  btn.disabled = true;
  btn.textContent = "导入中…";
  try {
    const text = await file.text();
    const fallback = file.name.replace(/\.(json|txt|js)$/i, "");
    const label = await importPlaylistText(text, $("#imp-name").value.trim() || fallback);
    if (!label) throw new Error("文件里没有识别到歌单内容");
    toast(`已导入 ${label}`, "ok");
    $("#imp-name").value = "";
  } catch (exc) {
    toast("导入失败：" + exc.message, "fail");
  } finally {
    btn.disabled = false;
    btn.textContent = "导入";
    $("#imp-file").value = "";
  }
  await loadImported();
});

/* NAS 文件选择：仅桌面环境（统一网关 /app/fnmusic-ext 内）可用。
   选中的主机路径经 /api/host-file 代读（webui_gateway 本地处理），再走上传落盘。 */
let lxTrimSdk = null;
async function lxLoadTrimSdk() {
  if (lxTrimSdk !== null) return lxTrimSdk;
  try {
    const mod = await import("/app/fnmusic-ext/static/vendor/trim-web-app.js");
    lxTrimSdk = new mod.TrimApp();
  } catch (_) {
    lxTrimSdk = false;
  }
  return lxTrimSdk;
}

async function lxPickFromNas() {
  const sdk = await lxLoadTrimSdk();
  if (!sdk) { toast("当前环境不支持 NAS 文件选择（直连 8774 时请用上传或 URL）", "fail"); return; }
  try {
    const result = await sdk.pickUserFile({
      directory: false,
      accept: [".js"],
      title: "选择洛雪源脚本",
      okText: "选择",
      sidebarGroup: ["myFiles", "otherShare", "favorites"],
    });
    const paths = (result && result.data) || [];
    if (!paths.length) return;
    const hostPath = paths[0];
    $("#lx-upload-note").textContent = "读取 NAS 文件中…";
    const resp = await fetch(APP_BASE + "/api/host-file", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: hostPath }),
    });
    let body = {};
    try { body = await resp.json(); } catch (_) { /* 非 JSON */ }
    if (!resp.ok) throw new Error(body.error || body.detail || `HTTP ${resp.status}`);
    const filename = hostPath.split("/").pop() || "source.js";
    const r = await lxUploadScript(filename, body.script || "");
    await lxAfterUpload(r);
  } catch (exc) {
    $("#lx-upload-note").textContent = "";
    toast("NAS 选择失败：" + exc.message, "fail");
  }
}
$("#lx-pick").addEventListener("click", lxPickFromNas);

(async function detectNasPicker() {
  // 桌面网关路径下才尝试加载 SDK；探测失败（直连 8774）保持隐藏
  const pathname = (typeof window !== "undefined" && window.location && window.location.pathname) || "";
  if (pathname.startsWith("/app/")) {
    const sdk = await lxLoadTrimSdk();
    if (sdk) $("#lx-pick").hidden = false;
  }
})();

/* -------------------------------------------------------------- 表单脏标记 */
["#tee-dir", "#tee-max", "#llm-base", "#llm-key", "#llm-model", "#lx-url", "#search-timeout", "#bind-timeout", "#handoff-max", "#scan-path", "#ceru-sync-host", "#ceru-sync-port", "#ceru-sync-pair-code"].forEach((sel) =>
  $(sel).addEventListener("input", () => markDirty()));
$$("input[name=quality]").forEach((el) => el.addEventListener("change", () => markDirty("音质偏好需保存后生效")));
["#recommend-hot", "#recommend-daily", "#search-probe", "#tee-enabled", "#fav-autobind", "#auto-cover", "#lyric-auto-dl", "#netease-my-playlists", "#ceru-sync-enabled", "#ceru-sync-use-https"].forEach((sel) =>
  $(sel).addEventListener("change", () => markDirty()));

function updateTeeCountLabel() {
  const n = $("#tee-max").value || configValues.FNMUSIC_TEE_CACHE_MAX || "2";
  $("#tee-count-label").textContent = `（缓存数 ${n} 首）`;
}
$("#tee-max").addEventListener("input", updateTeeCountLabel);

function updateBindTimeoutLabel() {
  const n = $("#bind-timeout").value || configValues.FNMUSIC_OFFICIAL_BIND_TIMEOUT_S || "120";
  $("#bind-timeout-label").textContent = `（${n} 秒）`;
}
$("#bind-timeout").addEventListener("input", updateBindTimeoutLabel);

window.addEventListener("beforeunload", (ev) => {
  if (dirty) ev.preventDefault();
});

/* -------------------------------------------------------------- 启动 */
(async function boot() {
  await loadConfig();
  await loadStatus();
  await ceruRefreshStatus();
  await loadLxPool();
  await loadImported();
  await loadPlatforms(false);  // 页面加载不自动拉起预览进程，等用户点选音源
  setInterval(loadStatus, 15000);
  setInterval(ceruRefreshStatus, 15000);
})();
