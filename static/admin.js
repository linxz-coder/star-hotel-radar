const metricGrid = document.getElementById("metric-grid");
const activeJobsNode = document.getElementById("active-jobs");
const completedJobsNode = document.getElementById("completed-jobs");
const failedJobsNode = document.getElementById("failed-jobs");
const activityListNode = document.getElementById("activity-list");
const cacheBoxNode = document.getElementById("cache-box");
const hotTargetsNode = document.getElementById("hot-targets");
const refreshStateNode = document.getElementById("refresh-state");
const refreshNowButton = document.getElementById("refresh-now");

const adminToken = window.__ADMIN_TOKEN__ || "";
let polling = false;
let lastPayload = null;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number.toLocaleString("zh-CN") : "0";
}

function formatTime(value) {
  if (!value) return "-";
  const date = new Date(String(value).replace(" ", "T"));
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDuration(ms) {
  const totalSeconds = Math.max(0, Math.round(Number(ms || 0) / 1000));
  if (totalSeconds < 60) return `${totalSeconds}秒`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return `${minutes}分${seconds}秒`;
  const hours = Math.floor(minutes / 60);
  return `${hours}小时${minutes % 60}分`;
}

function statusUrl() {
  const params = new URLSearchParams();
  if (adminToken) params.set("token", adminToken);
  const query = params.toString();
  return query ? `/api/admin/status?${query}` : "/api/admin/status";
}

function queryTitle(query) {
  const city = query?.city || "-";
  const target = query?.targetHotel || "-";
  return `${city}｜${target}`;
}

function queryMeta(query) {
  const parts = [];
  if (query?.selectedDate) parts.push(`入住 ${query.selectedDate}`);
  if (query?.radiusKm) parts.push(`半径 ${query.radiusKm}km`);
  if (query?.minStar) parts.push(`${query.minStar}星以上`);
  if (query?.minPrice || query?.maxPrice) {
    parts.push(`价格 ${query.minPrice || "不限"}-${query.maxPrice || "不限"}`);
  }
  if (query?.forceRefresh) parts.push("重新实时搜索");
  return parts.join("｜") || "-";
}

function badgeClass(status) {
  if (status === "running") return "badge-running";
  if (status === "complete") return "badge-complete";
  if (status === "cache-hit") return "badge-cache-hit";
  if (status === "error") return "badge-error";
  return "badge-default";
}

function statusText(status) {
  const labels = {
    running: "运行中",
    complete: "已完成",
    error: "失败",
    "cache-hit": "缓存命中",
    queued: "排队中",
  };
  return labels[status] || status || "未知";
}

function metricCard(label, value, note, className = "") {
  return `
    <article class="metric-card ${className}">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
      <small>${escapeHtml(note)}</small>
    </article>
  `;
}

function renderMetrics(data) {
  const metrics = data.metrics || {};
  const busy = data.busy || {};
  const busyClass = Number(busy.score || 0) >= 80
    ? "busy-error"
    : Number(busy.score || 0) >= 45
      ? "busy-hot"
      : "busy-normal";
  metricGrid.innerHTML = [
    metricCard("繁忙程度", `${busy.label || "空闲"} ${busy.score || 0}%`, `更新时间 ${data.generatedAt || "-"}`, busyClass),
    metricCard("运行中搜索", formatNumber(metrics.runningJobs), `排队 ${formatNumber(metrics.queuedJobs)} 个`),
    metricCard("中文名任务", formatNumber(metrics.nameVerificationJobs), "正在核验或等待兜底"),
    metricCard("待补价酒店", formatNumber(metrics.unpricedActiveHotels), "运行中任务内仍缺当天含税价"),
    metricCard("已完成搜索", formatNumber(metrics.completedJobs), `异常 ${formatNumber(metrics.errorJobs)} 个`),
    metricCard("缓存条目", `${formatNumber(metrics.memoryCacheItems)}/${formatNumber(metrics.diskCacheItems)}`, "内存 / 本地磁盘"),
  ].join("");
}

function summaryChips(summary = {}) {
  const chips = [
    `<span>候选 ${formatNumber(summary.candidateCount)} 家</span>`,
    `<span class="good">有价 ${formatNumber(summary.pricedHotelCount)} 家</span>`,
    `<span class="${Number(summary.unpricedCandidateCount || 0) > 0 ? "warn" : ""}">待补价 ${formatNumber(summary.unpricedCandidateCount)} 家</span>`,
    `<span>捡漏 ${formatNumber(summary.dealCount)} 家</span>`,
    `<span>高端 ${formatNumber(summary.recommendedCount)} 家</span>`,
  ];
  if (summary.totalCompareDateCount) {
    chips.push(`<span>对比日期 ${formatNumber(summary.completedCompareDateCount)}/${formatNumber(summary.totalCompareDateCount)}</span>`);
  }
  if (summary.nameVerificationTotal) {
    chips.push(`<span>中文名 ${formatNumber(summary.nameVerificationResolvedCount)}/${formatNumber(summary.nameVerificationTotal)}</span>`);
  }
  return chips.join("");
}

function progressHtml(percent) {
  const value = Math.max(0, Math.min(100, Number(percent || 0)));
  return `
    <div class="progress-row">
      <div class="progress-track"><div class="progress-bar" style="width:${value}%"></div></div>
      <div class="progress-value">${value}%</div>
    </div>
  `;
}

function priceProgressText(summary = {}) {
  const progress = summary.priceProgress || {};
  if (!Object.keys(progress).length) return "";
  const date = progress.date ? `${progress.date} ` : "";
  const priced = progress.pricedHotelCount ?? summary.pricedHotelCount ?? 0;
  const total = progress.totalHotels ?? summary.candidateCount ?? 0;
  const missing = progress.missingHotelCount ?? summary.unpricedCandidateCount ?? 0;
  return `补价进度：${date}已匹配 ${priced}/${total} 家，待补 ${missing} 家`;
}

function pipelineHtml(stages = []) {
  if (!Array.isArray(stages) || !stages.length) return "";
  return `
    <div class="pipeline-line">
      ${stages.map((stage) => `
        <span class="pipeline-step pipeline-step--${escapeHtml(stage.status || "pending")}">
          ${escapeHtml(stage.label || stage.key || "")}
        </span>
      `).join("")}
    </div>
  `;
}

function jobCard(job) {
  const summary = job.summary || {};
  const progress = job.progress || {};
  const message = progress.message || job.error || "等待后台更新状态";
  const priceText = priceProgressText(summary);
  return `
    <article class="job-card">
      <div class="job-top">
        <div class="job-title">
          <strong>${escapeHtml(queryTitle(job.query))}</strong>
          <span>${escapeHtml(queryMeta(job.query))}</span>
        </div>
        <span class="badge ${badgeClass(job.status)}">${escapeHtml(job.taskLabel || statusText(job.status))}</span>
      </div>
      ${progressHtml(job.progressPercent)}
      ${pipelineHtml(job.pipelineStages)}
      <div class="job-message">${escapeHtml(message)}</div>
      <div class="summary-line">${summaryChips(summary)}</div>
      <div class="job-meta">
        <span>已用时 ${escapeHtml(formatDuration(job.elapsedMs))}</span>
        <span>更新 ${escapeHtml(formatTime(job.updatedAtText))}</span>
        <span>Job ${escapeHtml(String(job.jobId || "").slice(0, 10))}</span>
        ${priceText ? `<span>${escapeHtml(priceText)}</span>` : ""}
      </div>
      ${job.error ? `<div class="job-message">${escapeHtml(job.error)}</div>` : ""}
    </article>
  `;
}

function renderActiveJobs(jobs = []) {
  if (!jobs.length) {
    activeJobsNode.innerHTML = `<div class="empty-state">当前没有运行中的搜索任务。</div>`;
    return;
  }
  activeJobsNode.innerHTML = jobs.map(jobCard).join("");
}

function compactJobItem(job, fallbackStatus = "complete") {
  const summary = job.summary || {};
  return `
    <article class="compact-item">
      <div class="compact-top">
        <div class="compact-main">
          <strong>${escapeHtml(queryTitle(job.query))}</strong>
          <span>${escapeHtml(queryMeta(job.query))}</span>
        </div>
        <span class="badge ${badgeClass(job.status || fallbackStatus)}">${escapeHtml(statusText(job.status || fallbackStatus))}</span>
      </div>
      <div class="summary-line">${summaryChips(summary)}</div>
      <div class="activity-meta">
        ${escapeHtml(formatTime(job.updatedAtText))}｜用时 ${escapeHtml(formatDuration(job.elapsedMs))}
        ${job.error ? `｜${escapeHtml(job.error)}` : ""}
      </div>
    </article>
  `;
}

function renderCompletedJobs(jobs = []) {
  completedJobsNode.innerHTML = jobs.length
    ? jobs.map(job => compactJobItem(job, "complete")).join("")
    : `<div class="empty-state">还没有完成的搜索任务。</div>`;
}

function renderFailedJobs(jobs = []) {
  failedJobsNode.innerHTML = jobs.length
    ? jobs.map(job => compactJobItem(job, "error")).join("")
    : `<div class="empty-state">当前没有失败任务。</div>`;
}

function eventText(event) {
  const labels = {
    "job-started": "任务开始",
    "job-complete": "任务完成",
    "job-error": "任务失败",
    "cache-hit": "缓存命中",
    "async-response": "异步返回",
    "sync-complete": "同步完成",
    "search-error": "搜索报错",
  };
  return labels[event] || event || "活动";
}

function renderActivities(activities = []) {
  if (!activities.length) {
    activityListNode.innerHTML = `<div class="empty-state">暂无搜索活动。</div>`;
    return;
  }
  activityListNode.innerHTML = activities.slice(0, 30).map(item => `
    <article class="activity-item">
      <div class="activity-top">
        <div>
          <div class="event-name">${escapeHtml(eventText(item.event))}</div>
          <div class="activity-meta">${escapeHtml(queryTitle(item.query))}</div>
        </div>
        <span class="badge ${badgeClass(item.status)}">${escapeHtml(statusText(item.status))}</span>
      </div>
      <div class="activity-meta">
        ${escapeHtml(queryMeta(item.query))}｜${escapeHtml(formatTime(item.createdAtText))}
      </div>
      <div class="summary-line">${summaryChips(item.summary || {})}</div>
      ${item.error ? `<div class="activity-meta">${escapeHtml(item.error)}</div>` : ""}
    </article>
  `).join("");
}

function renderCache(data) {
  const metrics = data.metrics || {};
  const cache = data.cache || {};
  cacheBoxNode.innerHTML = `
    <div class="cache-line"><span>内存搜索缓存</span><strong>${formatNumber(metrics.memoryCacheItems)} 条</strong></div>
    <div class="cache-line"><span>磁盘搜索缓存</span><strong>${formatNumber(metrics.diskCacheItems)} 条</strong></div>
    <div class="cache-line"><span>MySQL 搜索缓存</span><strong>${cache.searchCacheEnabled ? "已启用" : "未启用"}</strong></div>
    <div class="cache-line"><span>MySQL 中文名缓存</span><strong>${cache.hotelNameCacheEnabled ? "已启用" : "未启用"}</strong></div>
    <div class="cache-line"><span>MySQL 候选元数据缓存</span><strong>${cache.hotelCandidateCacheEnabled ? "已启用" : "未启用"}</strong></div>
    <div class="cache-line"><span>MySQL 酒店日期价格缓存</span><strong>${cache.hotelPriceCacheEnabled ? "已启用" : "未启用"}</strong></div>
    ${cache.searchCacheLastError ? `<div class="cache-line"><span>搜索缓存错误</span><strong>${escapeHtml(cache.searchCacheLastError)}</strong></div>` : ""}
    ${cache.hotelNameCacheLastError ? `<div class="cache-line"><span>中文名缓存错误</span><strong>${escapeHtml(cache.hotelNameCacheLastError)}</strong></div>` : ""}
    ${cache.hotelCandidateCacheLastError ? `<div class="cache-line"><span>候选缓存错误</span><strong>${escapeHtml(cache.hotelCandidateCacheLastError)}</strong></div>` : ""}
    ${cache.hotelPriceCacheLastError ? `<div class="cache-line"><span>价格缓存错误</span><strong>${escapeHtml(cache.hotelPriceCacheLastError)}</strong></div>` : ""}
  `;
}

function renderHotTargets(targets = []) {
  hotTargetsNode.innerHTML = targets.length
    ? targets.map(item => `
      <span class="hot-chip">${escapeHtml(item.city)}｜${escapeHtml(item.targetHotel)}｜${escapeHtml(item.heatLabel || item.targetType || "")}</span>
    `).join("")
    : `<div class="empty-state">暂无热搜记录。</div>`;
}

function renderAll(data) {
  lastPayload = data;
  renderMetrics(data);
  renderActiveJobs(data.activeJobs || []);
  renderCompletedJobs(data.completedJobs || []);
  renderFailedJobs(data.failedJobs || []);
  renderActivities(data.activities || []);
  renderCache(data);
  renderHotTargets(data.hotTargets || []);
  refreshStateNode.textContent = `已刷新 ${formatTime(data.generatedAt)}`;
}

async function refreshStatus() {
  if (polling) return;
  polling = true;
  refreshNowButton.disabled = true;
  try {
    const response = await fetch(statusUrl(), { headers: { Accept: "application/json" } });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || `后台状态请求失败：${response.status}`);
    }
    renderAll(data);
  } catch (error) {
    refreshStateNode.textContent = error.message || "刷新失败";
    if (!lastPayload) {
      metricGrid.innerHTML = metricCard("后台状态", "异常", refreshStateNode.textContent, "busy-error");
      activeJobsNode.innerHTML = `<div class="empty-state">${escapeHtml(refreshStateNode.textContent)}</div>`;
    }
  } finally {
    polling = false;
    refreshNowButton.disabled = false;
  }
}

refreshNowButton.addEventListener("click", refreshStatus);
refreshStatus();
window.setInterval(refreshStatus, 2000);
