/* 迁移台前端逻辑：轮询/SSE 刷新、三视图渲染、迁移控制、并发夹击、教学剧本。 */
"use strict";

const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let state = null;
let activeTab = {};          // 每个对象当前查看的 tab
let lastChangePaths = {};    // 对象 -> 本次写入触及的路径（高亮用）

// ------------------------------------------------------------ HTTP
async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = new Error(data.message || `${resp.status} ${resp.statusText}`);
    err.payload = data;
    err.status = resp.status;
    throw err;
  }
  return data;
}

// ------------------------------------------------------------ 提示
function toast(message, kind = "info", ms = 4200) {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = message;
  $("toasts").appendChild(el);
  setTimeout(() => el.remove(), ms);
}

// ------------------------------------------------------------ JSON 渲染（带路径高亮）
function jsonLines(obj) {
  return JSON.stringify(obj, null, 2).split("\n");
}

function classifyPath(path, ctx) {
  if (!path) return "";
  const classes = [];
  if (ctx.onlyPaths && ctx.onlyPaths.includes(path)) classes.push("hl-only");
  if (ctx.stashPaths && ctx.stashPaths.includes(path)) classes.push("hl-stash");
  if (ctx.changed && ctx.changed.includes(path)) classes.push("hl-changed");
  if (ctx.annotationPath && path.startsWith(ctx.annotationPath)) classes.push("hl-stash");
  return classes.join(" ");
}

function renderJson(obj, ctx = {}) {
  const raw = JSON.stringify(obj, null, 2);
  // 先转义再按 token 上色
  let escaped = raw
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  escaped = escaped.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
    (match) => {
      let cls = "json-number";
      if (/^"/.test(match)) cls = /:$/.test(match) ? "json-key" : "json-string";
      else if (/true|false/.test(match)) cls = "json-bool";
      else if (/null/.test(match)) cls = "json-null";
      return `<span class="${cls}">${match}</span>`;
    }
  );

  // 行级高亮：根据缩进 key 重建点路径
  const stack = [];
  const out = escaped.split("\n").map((line) => {
    const indent = line.search(/\S|$/);
    const level = indent / 2;
    while (stack.length > level) stack.pop();
    const keyMatch = line.match(/"([^"]+)"\s*:/);
    let path = null;
    if (keyMatch) {
      stack[level] = keyMatch[1];
      path = stack.slice(0, level + 1).join(".");
    }
    const hl = classifyPath(path, ctx);
    return hl ? `<span class="${hl}">${line}</span>` : line;
  });
  return out.join("\n");
}

// ------------------------------------------------------------ 总览渲染
function renderHeader() {
  const mig = state.migrator;
  const crdDef = state.crd;
  $("phase-text").textContent = mig.phase;
  const dot = $("phase-dot");
  dot.className = `dot ${mig.phase}`;
  const storage = crdDef.versions.find((v) => v.storage);
  $("storage-ver").textContent = storage ? storage.name : "?";
  $("served-ver").textContent = crdDef.versions.filter((v) => v.served).map((v) => v.name).join(", ");
  $("mig-count").textContent = `对象 ${mig.migrated}/${mig.total} 已迁移 · 待处理 ${mig.pending}`;
  const pct = mig.total ? Math.round((mig.migrated / mig.total) * 100) : 0;
  $("mig-progress").style.width = `${pct}%`;

  $("mig-kv").innerHTML = [
    ["阶段", mig.phase],
    ["storage 版本", mig.storageVersion],
    ["对象总数", mig.total],
    ["待迁移", mig.pending],
    ["已迁移", mig.migrated],
    ["worker", mig.workerRunning ? "运行中" : "空闲"],
    ["失败注入", mig.config.failOnNames.length ? `对象: ${mig.config.failOnNames.join(",")}` : "关"],
    ["随机失败率", mig.config.failRate],
  ].map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("");

  // 按钮可用性
  const finalized = mig.phase === "finalized";
  $("btn-flip").disabled =
    finalized || ["in_progress", "succeeded", "rolling_back"].includes(mig.phase);
  $("btn-start").disabled = mig.phase !== "failed";
  $("btn-retry").disabled = mig.phase !== "failed";
  $("btn-rollback").disabled = finalized || ["idle", "rolled_back"].includes(mig.phase);
  $("btn-finalize").disabled = mig.phase !== "succeeded";
  $("btn-old-write").disabled = finalized;
  $("btn-new-write").disabled = finalized;
}

function renderObjects() {
  const onlyAlpha = new Set(state.crd.v1alpha1OnlyPaths);
  const onlyV1 = new Set(state.crd.v1OnlyPaths);
  const wrap = $("objects");
  wrap.innerHTML = "";

  for (const obj of state.objects) {
    const card = document.createElement("div");
    card.className = "obj-card";

    const tabs = [
      ["stored", `磁盘存储（${obj.stored.apiVersion.split("/")[1]} 格式）`],
      ["v1alpha1", "旧客户端视图 v1alpha1"],
      ["v1", "新客户端视图 v1"],
    ];
    if (!activeTab[obj.name]) activeTab[obj.name] = "stored";
    const current = activeTab[obj.name];

    const stateColor = {
      migrated: "var(--green)", pending: "var(--red)",
      failed: "var(--red)", migrating: "var(--amber)", retrying: "var(--amber)",
    }[obj.migrationState] || "var(--muted)";

    card.innerHTML = `
      <div class="obj-head">
        <span class="obj-name">${obj.name}</span>
        <span class="badge"><span class="dot ${obj.migrationState}"></span>${obj.migrationState}</span>
        <span class="badge">rv=${obj.resourceVersion}</span>
        <span class="badge">stash 字段: ${Object.keys(obj.stash).length}</span>
        <div class="spacer"></div>
        <button class="warn" data-blast="1">⚡ 并发夹击</button>
      </div>
      <div class="obj-body">
        <div class="tabs">
          ${tabs.map(([key, label]) =>
            `<span class="tab ${current === key ? "active" : ""}" data-tab="${key}">${label}</span>`
          ).join("")}
        </div>
        <pre class="view" id="view-${obj.name}"></pre>
        <div class="legend" id="legend-${obj.name}"></div>
      </div>`;
    wrap.appendChild(card);

    card.querySelectorAll("[data-tab]").forEach((t) =>
      t.addEventListener("click", () => {
        activeTab[obj.name] = t.dataset.tab;
        renderObjects();
      })
    );
    card.querySelector("[data-blast]").addEventListener("click", () => runBlast(obj.name));

    const changed = lastChangePaths[obj.name] || [];
    let viewObj, onlyPaths;
    if (current === "stored") {
      viewObj = obj.stored;
      onlyPaths = [...onlyAlpha, ...onlyV1];
    } else {
      viewObj = obj.views[current];
      onlyPaths = current === "v1alpha1" ? [...onlyAlpha] : [...onlyV1];
    }
    const viewEl = $(`view-${obj.name}`);
    if (!viewObj) {
      viewEl.textContent = "该版本已下线（finalize），不再提供服务。";
      continue;
    }
    viewEl.innerHTML = renderJson(viewObj, {
      onlyPaths,
      stashPaths: Object.keys(obj.stash),
      changed,
      annotationPath: "metadata.annotations.sim.crd.k8s.io/converted-object",
    });
    $(`legend-${obj.name}`).innerHTML = changed.length
      ? `<span>本次写入触及：${changed.map((p) => `<code class="inline">${p}</code>`).join(" ")}</span>`
      : "";
  }
}

async function refresh() {
  try {
    state = await api("/api/overview");
    renderHeader();
    renderObjects();
  } catch (err) {
    toast(`刷新失败: ${err.message}`, "error");
  }
}

// ------------------------------------------------------------ 事件流
function connectSSE() {
  const es = new EventSource("/api/events");
  const log = $("log");
  es.onmessage = () => {};
  // 订阅全部命名事件：后端发布类型集中在这里维护，避免新增事件时漏接
  const interesting = new Set([
    "store.create", "store.update", "store.conflict", "store.rewrite", "store.delete",
    "client.update", "migrator.flip", "migrator.start", "migrator.finish",
    "migrator.object.start", "migrator.object.done", "migrator.object.conflict",
    "migrator.object.retry", "migrator.object.failed", "migrator.rollback.start",
    "migrator.retry", "migrator.finalize.done", "migrator.finalize.blocked",
    "migrator.config", "blast.start", "blast.done", "sim.reset",
  ]);
  for (const type of interesting) {
    es.addEventListener(type, (evt) => {
      const data = JSON.parse(evt.data);
      appendLog(data);
      // 记录本次写入触及的业务路径，供三视图高亮（独占路径优先，更精确）
      const meta = data.message?.meta;
      if (meta && data.message.name) {
        const paths = meta.changedPaths || [];
        if (meta.touchedOnlyPaths?.length) paths.push(...meta.touchedOnlyPaths);
        lastChangePaths[data.message.name] = [...new Set(paths)];
      }
      refresh();
    });
  }
  es.onerror = () => {
    // 浏览器会自动重连，这里无需处理
  };
}

function appendLog(event) {
  const log = $("log");
  const entry = document.createElement("div");
  entry.className = `log-entry ${event.type}`;
  const time = new Date(event.ts * 1000).toLocaleTimeString("zh-CN", { hour12: false });
  const detail = summarizeDetail(event);
  entry.innerHTML = `<span class="t">${time}</span><span class="type">${event.type}</span>` +
    (detail ? `<span class="detail">${detail}</span>` : "");
  log.prepend(entry);
  while (log.children.length > 120) log.lastChild.remove();
}

function summarizeDetail(event) {
  const m = event.message || {};
  switch (event.type) {
    case "store.conflict":
      return `409 Conflict: ${m.name} 期望 rv=${m.expected}，实际 rv=${m.current}`;
    case "client.update":
      return `${m.client}（${m.version}）写 ${m.name}，第 ${m.attempt} 次尝试 → ${
        m.result === "ok" ? `成功 rv=${m.current || ""}` : "冲突，退避重试"
      }`;
    case "migrator.object.conflict":
      return `${m.name} 第 ${m.try} 次 CAS 冲突，${m.backoff}s 后重读重转`;
    case "migrator.object.failed":
      return `${m.name} 迁移失败：${m.error}（共 ${m.attempts} 次）`;
    case "migrator.object.done":
      return `${m.name} 重写完成，尝试 ${m.attempts} 次`;
    case "migrator.finalize.blocked":
      return "拦截原因：" + m.blocking.map((b) => `${b.name}（${b.reason}）`).join("; ");
    case "blast.start":
      return `对 ${m.name} 同时发起旧/新客户端写入`;
    case "blast.done":
      return m.results.map((r) =>
        `${r.client}: ${r.ok ? `成功（${r.attempts} 次尝试，rv=${r.final_rv}）` : `失败：${r.error}`}`
      ).join("\n");
    default:
      return Object.keys(m).length ? JSON.stringify(m) : "";
  }
}

// ------------------------------------------------------------ 操作
async function guard(fn, okMsg) {
  try {
    await fn();
    if (okMsg) toast(okMsg, "ok");
    await refresh();
  } catch (err) {
    toast(err.message, "error", 6000);
  }
}

async function waitPhase(phases, timeoutMs = 15000) {
  const wanted = new Set(Array.isArray(phases) ? phases : [phases]);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const mig = await api("/api/migrator");
    if (wanted.has(mig.phase)) return mig.phase;
    await sleep(200);
  }
  throw new Error(`等待阶段 ${[...wanted].join("/")} 超时`);
}

async function runBlast(name) {
  await guard(async () => {
    const payload = {
      oldPatch: { replicas: Math.floor(Math.random() * 9) + 1 },
      newPatch: { timeoutSeconds: 10 + Math.floor(Math.random() * 50) },
      oldStatus: { legacyAck: `ACK-${Date.now() % 10000}` },
      newStatus: { scheduledNode: `node-${Math.floor(Math.random() * 5) + 1}` },
    };
    const data = await api(`/api/objects/${name}/blast`, { method: "POST", body: payload });
    const conflicts = data.results.filter((r) => r.attempts > 1).length;
    toast(
      `夹击完成：${data.results.map((r) => `${r.client} ${r.ok ? "✓" : "✗"}/${r.attempts}次`).join("，")}` +
      (conflicts ? `，观察到 ${conflicts} 路 409 重试` : ""),
      conflicts ? "info" : "ok", 5500
    );
  });
}

async function applyConfig() {
  await guard(async () => {
    await api("/api/migrator/config", {
      method: "POST",
      body: {
        failOnNames: $("cfg-names").value.split(",").map((s) => s.trim()).filter(Boolean),
        failRate: parseFloat($("cfg-rate").value) || 0,
        stepDelay: parseFloat($("cfg-delay").value) || 0,
        maxRetries: parseInt($("cfg-retries").value, 10) || 0,
      },
    });
  }, "配置已应用");
}

// ------------------------------------------------------------ 教学剧本
async function runDemo() {
  const setBtns = (disabled) =>
    document.querySelectorAll("button").forEach((b) => { b.disabled = disabled; });
  setBtns(true);
  try {
    toast("剧本 0/8：重置环境…", "info");
    await api("/api/reset", { method: "POST" });
    await refresh();

    toast("剧本 1/8：新客户端先写 v1 独占字段，再由旧客户端更新 —— 验证字段不被擦掉", "info", 5200);
    await api("/api/objects/edge-delivery-a/patch", {
      method: "POST",
      body: { version: "v1", specPatch: { timeoutSeconds: 30, nodeSelector: { zone: "edge-a" } } },
    });
    await api("/api/objects/edge-delivery-a/patch", {
      method: "POST",
      body: { version: "v1alpha1", specPatch: { replicas: 7 } },
    });
    await refresh();
    const stillThere = state.objects.find((o) => o.name === "edge-delivery-a");
    const protectedOk = stillThere.stash["spec.timeoutSeconds"] === 30;
    toast(protectedOk
      ? "✓ 旧客户端 PUT 后，timeoutSeconds/nodeSelector 仍存活在 convertedObject 注解里"
      : "✗ 独占字段丢失，请检查 write_merge 逻辑", protectedOk ? "ok" : "error", 6000);
    await sleep(1200);

    toast("剧本 2/8：并发夹击 edge-delivery-b，制造 409 冲突与退避重试", "info", 5000);
    await runBlast("edge-delivery-b");

    toast("剧本 3/8：注入 edge-delivery-c 的转换故障，翻转 storage → v1，迁移器开始逐对象重写", "info", 5000);
    await api("/api/migrator/config", {
      method: "POST",
      body: { failOnNames: ["edge-delivery-c"], stepDelay: 0.6, maxRetries: 0 },
    });
    await api("/api/migrate/flip", { method: "POST", body: { version: "v1" } });

    toast("剧本 4/8：迁移进行中夹击 edge-delivery-a —— 迁移器与客户端抢同一对象", "info", 5000);
    await sleep(700); // 等 worker 开始处理第一个对象
    await runBlast("edge-delivery-a");

    const phase = await waitPhase(["failed", "succeeded"]);
    if (phase === "failed") {
      toast("剧本 5/8：edge-delivery-c 如预期失败。演示回滚 —— 回到旧控制器", "info", 5500);
      await api("/api/migrate/rollback", { method: "POST" });
      await waitPhase("rolled_back");
      toast("✓ 已回滚：storage 恢复 v1alpha1，对象逐批写回旧格式，旧控制器重新接管", "ok", 5500);
      await sleep(800);

      toast("剧本 6/8：清除故障注入，重新翻转并完成迁移", "info", 5000);
      await api("/api/migrator/config", {
        method: "POST",
        body: { failOnNames: [], stepDelay: 0.35, maxRetries: 3 },
      });
      await api("/api/migrate/flip", { method: "POST", body: { version: "v1" } });
      await waitPhase("succeeded");
    } else {
      toast("剧本 5/8：未出现失败态，跳过回滚演示，直接收尾", "info");
    }

    toast("剧本 7/8：全部对象迁移成功，回滚窗口仍开放；执行 finalize 下线 v1alpha1", "info", 5500);
    await api("/api/migrate/finalize", { method: "POST" });
    await waitPhase("finalized");
    toast("✓ finalize 完成：旧版本不再 served，回滚窗口关闭；新字段已转正，旧字段仍由注解兜底", "ok", 6500);

    toast("剧本 8/8：演示结束，可点「重置环境」自行实验", "ok");
  } catch (err) {
    toast(`演示中断: ${err.message}`, "error", 7000);
  } finally {
    setBtns(false);
    await refresh();
  }
}

// ------------------------------------------------------------ 绑定
function bind() {
  $("btn-refresh").addEventListener("click", refresh);
  $("btn-reset").addEventListener("click", () =>
    guard(async () => {
      lastChangePaths = {};
      await api("/api/reset", { method: "POST" });
    }, "环境已重置"));
  $("btn-flip").addEventListener("click", () =>
    guard(() => api("/api/migrate/flip", { method: "POST", body: { version: "v1" } }),
      "存储版本已翻转，迁移器开始工作"));
  $("btn-start").addEventListener("click", () =>
    guard(() => api("/api/migrate/start", { method: "POST" }), "迁移器已启动"));
  $("btn-retry").addEventListener("click", () =>
    guard(() => api("/api/migrate/retry", { method: "POST" }), "已重试失败对象"));
  $("btn-rollback").addEventListener("click", () =>
    guard(() => api("/api/migrate/rollback", { method: "POST" }),
      "回滚已开始：storage 切回 v1alpha1，对象逐批写回"));
  $("btn-finalize").addEventListener("click", () =>
    guard(() => api("/api/migrate/finalize", { method: "POST" }),
      "finalize 完成，旧版本已下线"));
  $("btn-cfg").addEventListener("click", applyConfig);
  $("btn-old-write").addEventListener("click", () =>
    guard(async () => {
      const name = $("w-name").value.trim();
      await api(`/api/objects/${name}/patch`, {
        method: "POST",
        body: { version: "v1alpha1", specPatch: { replicas: Math.floor(Math.random() * 9) + 1 } },
      });
      toast("旧客户端写入完成，检查 v1 独占字段是否仍在", "ok");
    }));
  $("btn-new-write").addEventListener("click", () =>
    guard(async () => {
      const name = $("w-name").value.trim();
      await api(`/api/objects/${name}/patch`, {
        method: "POST",
        body: { version: "v1", specPatch: { timeoutSeconds: 20 + Math.floor(Math.random() * 40) } },
      });
      toast("新客户端写入完成", "ok");
    }));
  $("btn-demo").addEventListener("click", runDemo);
}

bind();
refresh();
connectSSE();
