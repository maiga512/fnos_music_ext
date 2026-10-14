/* static/app.js 的 Node 行为测试 —— 零 npm 依赖，node 原生 assert。
 *
 * 背景：musicbox CLI 的 auth 接口返回 {ok, data:{...}} 信封结构（与
 * netease_login.sh / proxy/tests/test_musicbox_service.py 的契约一致），
 * 前端 pollQr/checkQrStatus 必须从 data.code 取扫码状态码。
 * 本文件用最小 DOM/fetch 桩直接加载 app.js 驱动真实函数，防止信封解析回归。
 */
"use strict";

const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

/* ------------------------------------------------ 最小 DOM/浏览器桩 -------- */
function stubEl() {
  const classes = new Set();
  return {
    hidden: false, textContent: "", innerHTML: "", className: "", value: "",
    checked: false, disabled: false, src: "", dataset: {}, _t: null,
    classList: {
      add(...c) { c.forEach((x) => classes.add(x)); },
      remove(...c) { c.forEach((x) => classes.delete(x)); },
      toggle(c, force) {
        if (force === undefined) { classes.has(c) ? classes.delete(c) : classes.add(c); }
        else if (force) { classes.add(c); }
        else { classes.delete(c); }
      },
      contains(c) { return classes.has(c); },
    },
    addEventListener() {},
    querySelectorAll() { return []; },
  };
}

const els = new Map();

global.document = {
  querySelector(sel) {
    if (!els.has(sel)) els.set(sel, stubEl());
    return els.get(sel);
  },
  querySelectorAll() { return []; },
};
global.window = { addEventListener() {} };

// 定时器：unref 避免挡住进程退出；记录句柄开关状态供停表断言
let openIntervals = 0;
const realSetInterval = global.setInterval;
global.setInterval = (fn, ms, ...a) => {
  const h = realSetInterval(fn, ms, ...a);
  h.unref && h.unref();
  h._open = true;
  openIntervals += 1;
  return h;
};
const realClearInterval = global.clearInterval;
global.clearInterval = (h) => {
  if (h && h._open) { h._open = false; openIntervals -= 1; }
  return realClearInterval(h);
};
const realSetTimeout = global.setTimeout;
global.setTimeout = (fn, ms, ...a) => {
  const h = realSetTimeout(fn, ms, ...a);
  h.unref && h.unref();
  return h;
};

/* ------------------------------------------------ fetch 桩（可编排队列）---- */
const defaults = new Map([
  ["/app/fnmusic-ext/api/config", { values: {} }],
  ["/app/fnmusic-ext/api/status", { version: "t", current_provider: "none", processes: {}, services: {}, lx_source: null }],
  ["/app/fnmusic-ext/api/platforms", { ok: true, enabled: [], registered: [] }],
]);
let routes = new Map(); // path -> bodies 队列（Error 表示 HTTP 非 2xx）

function enqueue(p, ...bodies) {
  if (!routes.has(p)) routes.set(p, []);
  routes.get(p).push(...bodies);
}

global.fetch = async (url) => {
  const p = String(url).split("?")[0];
  if (routes.has(p) && routes.get(p).length) {
    const body = routes.get(p).shift();
    if (body instanceof Error) {
      return { ok: false, status: 502, json: async () => ({ detail: body.message }) };
    }
    return { ok: true, status: 200, json: async () => body };
  }
  return { ok: true, status: 200, json: async () => (defaults.get(p) || { ok: true }) };
};

function reset() {
  els.clear();
  routes = new Map();
}

const tick = (ms) => new Promise((r) => realSetTimeout(r, ms));

/* ------------------------------------------------ 加载 app.js -------------- */
const src = fs.readFileSync(path.join(__dirname, "static", "app.js"), "utf8");
vm.runInThisContext(src, { filename: "app.js" });

for (const fn of ["pollQr", "checkQrStatus", "startQrLogin", "syncNeteaseAccount", "stopQrPolling"]) {
  assert.strictEqual(typeof global[fn], "function", `app.js 应在全局暴露 ${fn}`);
}

/* ------------------------------------------------ 用例 --------------------- */
const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("checkQrStatus：信封 data.code 各状态分支文案", async () => {
  const cases = [
    [801, "等待扫码…"],
    [802, "已扫码，请在手机上确认"],
    [800, "二维码已过期，请重新生成"],
  ];
  for (const [code, text] of cases) {
    reset();
    enqueue("/app/fnmusic-ext/api/netease/auth/login/check", { ok: true, data: { code, message: "x" } });
    await global.checkQrStatus("K1");
    assert.strictEqual(els.get("#qr-status").textContent, text);
  }
});

test("checkQrStatus：803 信封 → 登录成功并同步账号昵称", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/netease/auth/login/check", { ok: true, data: { code: 803, cookie: "c" } });
  enqueue("/app/fnmusic-ext/api/netease/auth/status", { ok: true, data: { logged_in: true, nickname: "小明", user_id: 1 } });
  await global.checkQrStatus("K1");
  await tick(5); // 等 syncNeteaseAccount 的异步 fetch 落定
  assert.ok(els.get("#qr-status").textContent.includes("登录成功"));
  assert.strictEqual(els.get("#qr-check").textContent, "当前登录：小明");
});

test("checkQrStatus：803 扁平 {code} 结构兼容", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/netease/auth/login/check", { code: 803 });
  enqueue("/app/fnmusic-ext/api/netease/auth/status", { ok: true, data: { logged_in: true, user_id: 42 } });
  await global.checkQrStatus("K1");
  await tick(5);
  assert.ok(els.get("#qr-status").textContent.includes("登录成功"));
  assert.strictEqual(els.get("#qr-check").textContent, "当前登录：42"); // 无昵称回退 user_id
});

test("checkQrStatus：803 停止轮询定时器", async () => {
  reset();
  const before = openIntervals;
  global.pollQr("K1"); // 默认 2000ms，测试期内不会自然触发
  assert.strictEqual(openIntervals, before + 1);
  enqueue("/app/fnmusic-ext/api/netease/auth/login/check", { ok: true, data: { code: 803 } });
  enqueue("/app/fnmusic-ext/api/netease/auth/status", { ok: true, data: { logged_in: false } });
  await global.checkQrStatus("K1");
  await tick(5);
  assert.strictEqual(openIntervals, before); // 803 后停表
});

test("startQrLogin：信封 data.unikey 出码并启动轮询", async () => {
  reset();
  const before = openIntervals;
  enqueue("/app/fnmusic-ext/api/netease/auth/status", { ok: true, data: { logged_in: false } });
  enqueue("/app/fnmusic-ext/api/netease/auth/login", { ok: true, data: { unikey: "KEY9", qr_url: "u" } });
  await global.startQrLogin();
  assert.strictEqual(els.get("#qr-img").src, "/app/fnmusic-ext/api/netease/qr?unikey=KEY9");
  assert.strictEqual(els.get("#qr-img").hidden, false);
  assert.strictEqual(els.get("#qr-status").textContent, "请用手机网易云音乐 App 扫码");
  assert.strictEqual(openIntervals, before + 1); // 轮询已启动
  global.stopQrPolling();
});

test("startQrLogin：上游不可达 → 生成失败文案", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/netease/auth/login", new Error("musicbox 服务不可达"), new Error("musicbox 服务不可达"));
  await global.startQrLogin();
  assert.ok(els.get("#qr-status").textContent.startsWith("生成失败："));
  assert.strictEqual(els.get("#qr-img").hidden, true);
});

test("syncNeteaseAccount：未登录清空 / 请求失败清空", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/netease/auth/status", { ok: true, data: { logged_in: false } });
  await global.syncNeteaseAccount();
  assert.strictEqual(els.get("#qr-check").textContent, "");
  reset();
  enqueue("/app/fnmusic-ext/api/netease/auth/status", new Error("down"));
  await global.syncNeteaseAccount();
  assert.strictEqual(els.get("#qr-check").textContent, "");
});

test("markDirty / clearDirty：save-bar 的 show 类显隐与文案同步", () => {
  reset();
  global.markDirty("已修改配置");
  assert.strictEqual(els.get("#save-note").textContent, "已修改配置");
  assert.strictEqual(els.get("#save-bar").classList.contains("show"), true);
  global.clearDirty();
  assert.strictEqual(els.get("#save-note").textContent, "");
  assert.strictEqual(els.get("#save-bar").classList.contains("show"), false);
});

test("lxUploadScript→lxAfterUpload：上传成功后 file:// URL 填入输入框并标脏", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/lx/upload", {
    ok: true,
    data: { path: "/data/lxmusic/uploads/mine.js", url: "file:///data/lxmusic/uploads/mine.js", meta: { name: "上传源" } },
  });
  const r = await global.lxUploadScript("mine.js", "/*stub*/");
  assert.strictEqual(r.data.url, "file:///data/lxmusic/uploads/mine.js");
  await global.lxAfterUpload(r);
  assert.strictEqual(els.get("#lx-url").value, "file:///data/lxmusic/uploads/mine.js");
  assert.strictEqual(els.get("#lx-upload-note").textContent, "已上传：上传源");
  assert.strictEqual(els.get("#save-bar").classList.contains("show"), true);
});

test("lxUploadScript：lxmusic 未运行时先拉预览再重试", async () => {
  reset();
  // 第一次 upload 502（进程未起），预览成功后重试成功
  enqueue("/app/fnmusic-ext/api/lx/upload", new Error("lxmusic 服务不可达"));
  enqueue("/app/fnmusic-ext/api/preview", { ok: true, preview: true });
  enqueue("/app/fnmusic-ext/api/lx/upload", {
    ok: true,
    data: { path: "/p", url: "file:///p", meta: { name: "n" } },
  });
  const r = await global.lxUploadScript("a.js", "/*s*/");
  assert.strictEqual(r.ok, true);
});

test("网易账号歌单开关：loadConfig 回填 + collectConfig 收集", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/config", { values: { FNMUSIC_NETEASE_MY_PLAYLISTS: "true" } });
  await global.loadConfig();
  assert.strictEqual(els.get("#netease-my-playlists").checked, true);
  els.get("#netease-my-playlists").checked = false;
  assert.strictEqual(global.collectConfig().FNMUSIC_NETEASE_MY_PLAYLISTS, false);
  // 回填 false：缺省/关闭都表现为未勾选
  reset();
  enqueue("/app/fnmusic-ext/api/config", { values: { FNMUSIC_NETEASE_MY_PLAYLISTS: "false" } });
  await global.loadConfig();
  assert.strictEqual(els.get("#netease-my-playlists").checked, false);
});

test("自动下载歌词开关：loadConfig 回填 + collectConfig 收集", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/config", { values: { FNMUSIC_LYRIC_AUTO_DL: "true" } });
  await global.loadConfig();
  assert.strictEqual(els.get("#lyric-auto-dl").checked, true);
  els.get("#lyric-auto-dl").checked = false;
  assert.strictEqual(global.collectConfig().FNMUSIC_LYRIC_AUTO_DL, false);
  // 缺省/关闭都表现为未勾选（默认关）
  reset();
  enqueue("/app/fnmusic-ext/api/config", { values: {} });
  await global.loadConfig();
  assert.strictEqual(els.get("#lyric-auto-dl").checked, false);
});

/* ------------------------------------------------ 运行 --------------------- */
test("附加洛雪源批量上传：多文件串行处理，单个失败不影响其他文件", async () => {
  reset();
  const uploads = [];
  const originalUpload = global.lxUploadScript;
  const originalAdd = global.lxPoolAddUrl;
  global.lxUploadScript = async (name) => {
    uploads.push(name);
    if (name === "bad.js") throw new Error("坏文件");
    return { data: { url: `file:///data/${name}`, meta: { name } } };
  };
  global.lxPoolAddUrl = async () => ({});
  try {
    const result = await global.lxPoolUploadFiles([
      { name: "one.js", size: 1, text: async () => "one" },
      { name: "bad.js", size: 1, text: async () => "bad" },
      { name: "two.js", size: 1, text: async () => "two" },
    ]);
    assert.strictEqual(result.ok, 2);
    assert.strictEqual(result.failed.length, 1);
    assert.ok(result.failed[0].startsWith("bad.js:"));
    assert.deepStrictEqual(uploads, ["one.js", "bad.js", "two.js"]);
  } finally {
    global.lxUploadScript = originalUpload;
    global.lxPoolAddUrl = originalAdd;
  }
});


/* ------------------------------------------------ 运行 --------------------- */
test("洛雪源列表：版本号和未知版本都能明确显示", () => {
  reset();
  global.renderLxSources([
    { id: "source-a", name: "附加源 A", enabled: true, source: { version: "2.3.4" } },
    { id: "source-b", name: "附加源 B", enabled: false, source: null },
    { id: "source-c", name: "附加源 C", enabled: true, source: { version: "v3.4.5" } },
  ]);
  const html = els.get("#lx-source-list").innerHTML;
  assert.ok(html.includes("v2.3.4"));
  assert.ok(html.includes("版本未知"));
  assert.ok(html.includes("v3.4.5"));
  assert.ok(!html.includes("vv3.4.5"));
});


test("洛雪源列表：主源与附加源并成一行一源，主源不给停用/删除", () => {
  reset();
  global.renderLxSources([
    { id: "__primary__", primary: true, name: "主源脚本", configured: true, running: true,
      enabled: true, source: { version: "3.2.1", platforms: { tx: [], kw: [] } } },
    { id: "s1", name: "附加源一", enabled: true, auto_update: true,
      source: { version: "1.0.0" } },
    { id: "s2", name: "附加源二", enabled: false, source: { version: "1.1.0" } },
  ]);
  const html = els.get("#lx-source-list").innerHTML;
  assert.ok(html.indexOf("__primary__") < html.indexOf("s1"), "主源必须在第一行");
  assert.ok(html.includes("badge primary") && html.includes("主源"));
  assert.ok(html.includes("附加 1") && html.includes("附加 2"), "附加源要有顺位");
  assert.ok(html.includes("v3.2.1") && html.includes("v1.0.0"), "每行都要有版本");
  assert.ok(html.includes("lx-pool-remove"), "删除按钮只给附加源");
  assert.strictEqual((html.match(/lx-pool-remove/g) || []).length, 2);
  assert.strictEqual((html.match(/lx-pool-enabled/g) || []).length, 2, "停用开关只给附加源");
  assert.strictEqual((html.match(/lx-src-check/g) || []).length, 3, "三行都要有检查更新");
  assert.ok(html.includes("is-off"), "停用的源要有置灰态");
});


test("附加洛雪源：服务未就绪不得显示空列表，应显示等待提示并自动重试", async () => {
  reset();
  enqueue("/app/fnmusic-ext/api/config", { values: { FNMUSIC_LX_ENABLED: "true" } });
  await global.loadConfig();
  // 服务未就绪：首次请求与预览拉起后的立即重试都失败
  enqueue("/app/fnmusic-ext/api/lx/sources",
          new Error("lxmusic 服务不可达"), new Error("lxmusic 服务不可达"));
  enqueue("/app/fnmusic-ext/api/preview", { ok: true, preview: true, seconds_left: 300 });
  await global.loadLxPool();
  let html = els.get("#lx-source-list").innerHTML;
  assert.ok(html.includes("音源服务尚未就绪"), "服务未就绪应显示等待提示");
  assert.ok(!html.includes("还没有音源"), "不能让用户误以为已有源全部丢失");
  // 3 秒后台重试成功：已有源应全部自动恢复显示
  enqueue("/app/fnmusic-ext/api/lx/sources", { ok: true, data: { items: [
    { id: "src_old1", name: "旧源 A", enabled: true, source: { version: "1.0.0" } },
    { id: "src_old2", name: "旧源 B", enabled: false, source: { version: "2.1.0" } },
  ] } });
  await tick(3200); // 等待 3 秒后台重试
  html = els.get("#lx-source-list").innerHTML;
  assert.ok(html.includes("旧源 A"), "重试成功后旧源应自动显示");
  assert.ok(html.includes("旧源 B"), "重试成功后旧源应自动显示");
  assert.ok(!html.includes("音源服务尚未就绪"), "重试成功后等待提示应被清除");
});


test("导入歌单：列表渲染数量/来源/时间，不可刷新的不给刷新按钮", () => {
  reset();
  global.renderImported([
    { id: "imp1", guid: "online:playlist:imp:imp1", name: "老歌收藏", count: 128,
      origin: "link", platform: "tx", created_at: 1760000000, updated_at: 1760003600,
      refreshable: true },
    { id: "imp2", guid: "online:playlist:imp:imp2", name: "本地快照", count: 3,
      origin: "lxjson", platform: "", created_at: 1760000000, updated_at: 1760000000,
      refreshable: false },
  ]);
  const html = els.get("#imp-list").innerHTML;
  assert.ok(html.includes("老歌收藏") && html.includes("128 首"));
  assert.ok(html.includes("链接导入") && html.includes("TX"));
  assert.ok(html.includes("2025-10-09"), "导入时间要能看懂");
  assert.strictEqual((html.match(/imp-refresh/g) || []).length, 1, "JSON 导入没有可刷新的上游");
  assert.strictEqual((html.match(/imp-delete/g) || []).length, 2);
});


test("导入歌单：请求带原文与可选歌单名，空结果按失败提示", async () => {
  reset();
  const seen = [];
  const original = global.fetch;
  global.fetch = async (url, options) => {
    seen.push({ url: String(url), body: JSON.parse((options || {}).body || "{}") });
    return { ok: true, status: 200, json: async () => ({ ok: true, data: [
      { name: "深夜电台", count: 42 }, { name: "老歌", count: 8 }] }) };
  };
  try {
    const label = await global.importPlaylistText("https://kugou.com/xxx?id=1", "我的名字");
    assert.ok(label.includes("深夜电台（42 首）") && label.includes("老歌（8 首）"));
    assert.strictEqual(seen[0].url, "/app/fnmusic-ext/api/playlists/import");
    assert.strictEqual(seen[0].body.text, "https://kugou.com/xxx?id=1");
    assert.strictEqual(seen[0].body.name, "我的名字");
  } finally {
    global.fetch = original;
  }
  enqueue("/app/fnmusic-ext/api/playlists/imported", { ok: true, data: [] });
  await global.loadImported();
  assert.ok(els.get("#imp-list").innerHTML.includes("还没有导入歌单"));
});


/* ------------------------------------------------ 运行 --------------------- */
(async () => {
  let failed = 0;
  for (const [name, fn] of tests) {
    try {
      reset();
      await fn();
      console.log(`ok - ${name}`);
    } catch (exc) {
      failed += 1;
      console.error(`not ok - ${name}\n  ${exc && exc.stack ? exc.stack : exc}`);
    }
  }
  console.log(`${tests.length - failed}/${tests.length} passed, ${failed} failed`);
  process.exitCode = failed ? 1 : 0;
})();
