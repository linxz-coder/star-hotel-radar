const form = document.getElementById("search-form");
const submitBtn = document.getElementById("submit-btn");
const errorBox = document.getElementById("error-box");
const loadingBox = document.getElementById("loading-box");
const priceStatusBox = document.getElementById("price-status-box");
const exportPdfBtn = document.getElementById("export-pdf-btn");
const exportPdfHint = document.getElementById("export-pdf-hint");
const compareNoticeBox = document.getElementById("compare-notice");
const sourcePill = document.getElementById("source-pill");
const candidateCount = document.getElementById("candidate-count");
const dealCount = document.getElementById("deal-count");
const brandCount = document.getElementById("brand-count");
const resultJumpWidget = document.getElementById("result-jump-widget");
const resultJumpToggle = document.getElementById("result-jump-toggle");
const resultJumpPanel = document.getElementById("result-jump-panel");
const jumpDealCount = document.getElementById("jump-deal-count");
const jumpCandidateCount = document.getElementById("jump-candidate-count");
const jumpBrandCount = document.getElementById("jump-brand-count");
const compareDatesNode = document.getElementById("compare-dates");
const dealList = document.getElementById("deal-list");
const candidateList = document.getElementById("candidate-list");
const recommendList = document.getElementById("recommend-list");
const hotelFilters = {
  deal: {
    keyword: document.getElementById("deal-filter-keyword"),
    sort: document.getElementById("deal-filter-sort"),
    minPrice: document.getElementById("deal-filter-min-price"),
    maxPrice: document.getElementById("deal-filter-max-price"),
    reset: document.getElementById("deal-filter-reset"),
    count: document.getElementById("deal-filter-count"),
  },
  candidate: {
    keyword: document.getElementById("candidate-filter-keyword"),
    sort: document.getElementById("candidate-filter-sort"),
    minPrice: document.getElementById("candidate-filter-min-price"),
    maxPrice: document.getElementById("candidate-filter-max-price"),
    reset: document.getElementById("candidate-filter-reset"),
    count: document.getElementById("candidate-filter-count"),
  },
  recommend: {
    keyword: document.getElementById("recommend-filter-keyword"),
    sort: document.getElementById("recommend-filter-sort"),
    minPrice: document.getElementById("recommend-filter-min-price"),
    maxPrice: document.getElementById("recommend-filter-max-price"),
    reset: document.getElementById("recommend-filter-reset"),
    count: document.getElementById("recommend-filter-count"),
  },
};
const targetSuggestions = document.getElementById("target-suggestions");
const hotSearchList = document.getElementById("hot-search-list");
let latestData = null;
let suggestionTimer = null;
let searchPollTimer = null;
let searchEventSource = null;
let latestSuggestions = [];
let latestHotTargets = [];
let suggestionRequestId = 0;
let searchRequestId = 0;
let pdfExporting = false;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

const t2sPhrases = [
  ["希爾頓花園酒店", "希尔顿花园酒店"],
  ["皇冠假日酒店", "皇冠假日酒店"],
  ["洲際酒店", "洲际酒店"],
  ["凱悅酒店", "凯悦酒店"],
  ["格蘭雲天", "格兰云天"],
  ["維也納酒店", "维也纳酒店"],
  ["萬豪酒店", "万豪酒店"],
  ["喜來登酒店", "喜来登酒店"],
  ["朗廷酒店", "朗廷酒店"],
  ["文華東方", "文华东方"],
  ["嶺南東方", "岭南东方"],
  ["深圳國際會展中心", "深圳国际会展中心"],
  ["光明虹橋", "光明虹桥"],
];

const t2sChars = {
  "廣": "广", "東": "东", "門": "门", "雲": "云", "國": "国", "際": "际", "會": "会",
  "灣": "湾", "橋": "桥", "園": "园", "華": "华", "凱": "凯", "悅": "悦", "爾": "尔",
  "頓": "顿", "維": "维", "納": "纳", "蘭": "兰", "瀾": "澜", "麗": "丽", "濱": "滨",
  "樓": "楼", "閣": "阁", "館": "馆", "莊": "庄", "龍": "龙", "寧": "宁", "蘇": "苏",
  "縣": "县", "區": "区", "內": "内", "陽": "阳", "長": "长", "慶": "庆", "達": "达",
  "連": "连", "濟": "济", "鄭": "郑", "漢": "汉", "貴": "贵", "樂": "乐", "兒": "儿",
  "親": "亲", "雙": "双", "張": "张", "萬": "万", "與": "与", "裏": "里", "裡": "里",
  "臺": "台", "島": "岛", "飯": "饭", "體": "体", "號": "号", "潔": "洁", "寶": "宝",
  "緣": "缘", "錦": "锦", "匯": "汇", "恆": "恒", "榮": "荣", "業": "业", "廈": "厦",
  "廳": "厅", "營": "营", "適": "适", "選": "选", "鄰": "邻", "韓": "韩", "歐": "欧",
  "羅": "罗", "倫": "伦", "紐": "纽", "舊": "旧", "聖": "圣", "侶": "侣", "鬆": "松", "豐": "丰",
  "齋": "斋", "齊": "齐", "淺": "浅", "澀": "涩", "環": "环", "購": "购", "碼": "码",
  "櫚": "榈", "壯": "壮", "劇": "剧", "鐵": "铁", "盧": "卢", "奧": "奥", "馬": "马",
};

function simplifyChineseText(text) {
  let value = String(text || "");
  for (const [from, to] of t2sPhrases) value = value.split(from).join(to);
  return Array.from(value).map((char) => t2sChars[char] || char).join("");
}

function containsChineseText(text) {
  return /[\u3400-\u9fff]/.test(String(text || ""));
}

function hasNameVerificationMarker(text) {
  return /中文名(?:待核验|正在核验中)/.test(String(text || ""));
}

function normalizeNameVerificationText(text) {
  return String(text || "")
    .replace(/（\s*中文名待核验\s*）/g, "（中文名正在核验中...）")
    .replace(/（\s*中文名正在核验中\.*\s*）/g, "（中文名正在核验中...）");
}

function fallbackChineseHotelName(hotel) {
  if (!hotel) return "";
  const city = simplifyChineseText(hotel.city || latestData?.query?.city || "").trim();
  const cityPrefix = containsChineseText(city) ? city : "";
  const brand = simplifyChineseText(hotel.brandLabel || hotel.brand || hotel.groupLabel || hotel.group || "").trim();
  const brandText = containsChineseText(brand) && !["独立酒店", "酒店", "集团"].includes(brand)
    ? brand.replace(/集团$/, "")
    : "";
  if (brandText && brandText.endsWith("酒店")) {
    return `${cityPrefix && !brandText.startsWith(cityPrefix) ? cityPrefix : ""}${brandText}（中文名正在核验中...）`;
  }
  if (brandText) return `${cityPrefix}${brandText}酒店（中文名正在核验中...）`;
  if (cityPrefix) return `${cityPrefix}星级酒店（中文名正在核验中...）`;
  if (hotel.hotelId) return "星级酒店（中文名正在核验中...）";
  return "";
}

function displayHotelName(hotel) {
  const candidates = [
    hotel?.hotelName,
    hotel?.hotelNameSimplified,
    hotel?.hotelOriginalName,
  ];
  for (const candidate of candidates) {
    const name = simplifyChineseText(candidate || "").trim();
    if (name && containsChineseText(name)) return normalizeNameVerificationText(name);
  }
  return fallbackChineseHotelName(hotel);
}

function currency(value) {
  if (value === null || value === undefined || value === "") return "-";
  return `¥${Number(value).toLocaleString("zh-CN")}`;
}

function percent(value) {
  if (value === null || value === undefined || value === "") return "-";
  return `${Number(value).toFixed(1).replace(".0", "")}%`;
}

function starText(value) {
  const count = Math.max(0, Math.min(5, Math.round(Number(value || 0))));
  return `${"★".repeat(count)}${count ? " " : ""}${Number(value || 0).toFixed(0)}星级`;
}

function reviewText(hotel) {
  const rating = hotel.rating ? `${Number(hotel.rating).toFixed(1)}分` : "";
  const count = hotel.reviewCount ? `${Number(hotel.reviewCount).toLocaleString("zh-CN")}条点评` : "";
  return [rating, count].filter(Boolean).join("｜");
}

function hotelMeta(hotel) {
  const brand = hotel.groupLabel || hotel.brandLabel || hotel.group || hotel.brand || "独立酒店";
  return [
    hotel.nameProcessing ? "中文名处理中" : "",
    starText(hotel.starRating),
    brand,
    `距离目标酒店 ${Number(hotel.distanceKm || 0).toFixed(1)}km`,
    reviewText(hotel),
  ].filter(Boolean).map(escapeHtml).join("｜");
}

function imageTag(hotel) {
  const src = hotel.imageUrl || "";
  return `<img class="hotel-image" src="${escapeHtml(src)}" alt="${escapeHtml(displayHotelName(hotel))}酒店图片" onerror="this.removeAttribute('src')" />`;
}

function tripLink(hotel) {
  if (!hotel.tripUrl) return "";
  return `
    <div class="card-actions">
      <a class="trip-link" href="${escapeHtml(hotel.tripUrl)}" target="_blank" rel="noreferrer">打开 Trip.com</a>
    </div>
  `;
}

function nameVerificationBar(hotel) {
  if (!hotel.nameProcessing && !hasNameVerificationMarker(hotel.hotelName)) return "";
  return `
    <div class="name-verification">
      <span>正在核验酒店中文名，核验完成后会自动刷新标题</span>
      <i aria-hidden="true"></i>
    </div>
  `;
}

function pricePendingBar(hotel) {
  if (!hotel.pricePending) return "";
  const stillSearching = Boolean(latestData?.summary?.partial || latestData?.summary?.refreshing);
  const text = stillSearching
    ? "正在继续查询该酒店目标日期含税价，查到后会自动刷新"
    : "本轮未拿到该酒店目标日期含税价，已保留为待补价候选";
  return `
    <div class="price-pending-bar">
      <span>${escapeHtml(text)}</span>
      <i aria-hidden="true"></i>
    </div>
  `;
}

function priceDeltaTone(discountValue, hasDiscount) {
  if (!hasDiscount || !Number.isFinite(discountValue)) return "neutral";
  if (discountValue > 0) return "positive";
  if (discountValue < 0) return "negative";
  return "neutral";
}

function hotelCard(hotel, mode) {
  const hasDiscount = hotel.discountAmount !== null && hotel.discountAmount !== undefined && hotel.discountAmount !== "";
  const discountValue = Number(hotel.discountAmount || 0);
  const deltaTone = priceDeltaTone(discountValue, hasDiscount);
  const badgeText = mode === "deal"
    ? `便宜 ${currency(hotel.discountAmount)}`
    : escapeHtml(hotel.pricePending ? "待补价" : (hotel.brandLabel || hotel.brand || "连锁品牌"));
  const badgeClass = mode === "deal" ? `deal-badge deal-badge--${deltaTone}` : "deal-badge";
  const hotelName = displayHotelName(hotel);
  const referenceLabel = hotel.referencePriceLabel || "平时参考价";
  const referencePrice = hotel.referencePrice ?? hotel.averageComparePrice;
  const hasPercent = hotel.discountPercent !== null && hotel.discountPercent !== undefined && hotel.discountPercent !== "";
  const percentValue = Number(hotel.discountPercent || 0);
  const discountLabel = deltaTone === "negative"
    ? "比平时高"
    : (deltaTone === "positive"
      ? (hotel.dealBasis === "single_day" ? "较高对比日便宜" : "比平时便宜")
      : "与平时持平");
  const percentLabel = deltaTone === "negative" ? "高出比例" : (deltaTone === "positive" ? "优惠力度" : "变化比例");
  if (!hotelName) return "";
  return `
    <article class="hotel-card">
      ${imageTag(hotel)}
      <div class="hotel-body">
        <div class="hotel-title-row">
          <h3>${escapeHtml(hotelName)}</h3>
          <span class="${badgeClass}">${badgeText}</span>
        </div>
        <div class="hotel-meta">${hotelMeta(hotel)}</div>
        ${nameVerificationBar(hotel)}
        ${pricePendingBar(hotel)}
        <div class="price-grid">
          <div class="price-cell ${hotel.pricePending ? "price-cell--pending" : ""}">
            <span>目标日期价格</span>
            <strong>${hotel.pricePending ? "待补价" : currency(hotel.currentPrice)}</strong>
          </div>
          <div class="price-cell">
            <span>${escapeHtml(referenceLabel)}</span>
            <strong>${currency(referencePrice)}</strong>
          </div>
          <div class="price-cell price-delta price-delta--${deltaTone}">
            <span>${escapeHtml(discountLabel)}</span>
            <strong>${hasDiscount ? currency(Math.abs(discountValue)) : "-"}</strong>
          </div>
          <div class="price-cell price-delta price-delta--${deltaTone}">
            <span>${escapeHtml(percentLabel)}</span>
            <strong>${hasPercent ? percent(Math.abs(percentValue)) : "-"}</strong>
          </div>
        </div>
        <p class="reason">${escapeHtml(hotel.recommendationReason)}</p>
        ${tripLink(hotel)}
      </div>
    </article>
  `;
}

function chineseNamedHotels(items) {
  return (items || []).filter(Boolean);
}

function normalizedFilterText(value) {
  return simplifyChineseText(value || "")
    .toLowerCase()
    .replace(/[\s·・,，.。()（）\-_/｜|]+/g, "");
}

function hotelSearchText(hotel) {
  return normalizedFilterText([
    displayHotelName(hotel),
    hotel.hotelName,
    hotel.hotelNameSimplified,
    hotel.hotelOriginalName,
    hotel.brandLabel,
    hotel.brand,
    hotel.groupLabel,
    hotel.group,
    hotel.hotelNameSource,
  ].filter(Boolean).join(" "));
}

function parseFilterPrice(input) {
  const value = String(input?.value || "").trim();
  if (!value) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function activeHotelFilters(mode) {
  const controls = hotelFilters[mode] || {};
  return {
    keyword: normalizedFilterText(controls.keyword?.value || ""),
    minPrice: parseFilterPrice(controls.minPrice),
    maxPrice: parseFilterPrice(controls.maxPrice),
  };
}

function filterHotels(items, mode) {
  const filters = activeHotelFilters(mode);
  return (items || []).filter((hotel) => {
    if (filters.keyword && !hotelSearchText(hotel).includes(filters.keyword)) {
      return false;
    }
    if (filters.minPrice === null && filters.maxPrice === null) {
      return true;
    }
    const price = Number(hotel.currentPrice);
    if (!Number.isFinite(price)) return false;
    if (filters.minPrice !== null && price < filters.minPrice) return false;
    if (filters.maxPrice !== null && price > filters.maxPrice) return false;
    return true;
  });
}

function updateHotelFilterCount(mode, visibleCount, totalCount) {
  const countNode = hotelFilters[mode]?.count;
  if (!countNode) return;
  if (!totalCount) {
    countNode.textContent = "等待结果";
    return;
  }
  countNode.textContent = visibleCount === totalCount
    ? `${totalCount} 家`
    : `${visibleCount}/${totalCount} 家`;
}

function emptyState(text) {
  return `<div class="empty-state">${escapeHtml(text)}</div>`;
}

function holidayCompareNoticeFromSummary(summary) {
  if (summary?.compareMode !== "holiday") return "";
  const holidayName = summary.holidayName || summary.holiday?.name || "公众假期";
  return `你选择的是${holidayName}公众假期，对比日已改为公众假期对比。`;
}

function setCompareNotice(text) {
  if (!compareNoticeBox) return;
  compareNoticeBox.textContent = text || "";
  compareNoticeBox.classList.toggle("hidden", !text);
}

function priceStatusText(summary = {}) {
  const priceProgress = summary.priceProgress || {};
  const missing = Number(priceProgress.missingHotelCount ?? summary.unpricedCandidateCount ?? 0);
  const priced = Number(priceProgress.pricedHotelCount ?? summary.pricedHotelCount ?? 0);
  const total = Number(priceProgress.totalHotels ?? summary.candidateCount ?? 0);
  const dateValue = priceProgress.date || "";
  if (summary.partial || summary.refreshing) {
    if (total && Number.isFinite(missing) && missing > 0) {
      const dateLabel = dateValue ? `${dateValue} ` : "";
      return `正在补齐${dateLabel}酒店含税价：已匹配 ${Number.isFinite(priced) ? priced : 0}/${total} 家，仍有 ${missing} 家待补价。`;
    }
    if (total && Number.isFinite(priced) && priced > 0) {
      return `正在补齐酒店含税价：已匹配 ${priced}/${total} 家，结果会继续刷新。`;
    }
  }
  if (!summary.partial && Number.isFinite(missing) && missing > 0) {
    return `本轮搜索完成后仍有 ${missing}/${total || "若干"} 家酒店未拿到目标日期含税价，已保留为“待补价”候选；Trip.com 后续返回价格或重新实时搜索时会继续补齐。`;
  }
  return "";
}

function setPriceStatus(summary = {}) {
  if (!priceStatusBox) return;
  const text = priceStatusText(summary);
  priceStatusBox.textContent = text;
  priceStatusBox.classList.toggle("hidden", !text);
}

function sortedHotels(items, mode, summary = {}) {
  const sectionSort = hotelFilters[mode]?.sort?.value || "distance";
  const sortBy = sectionSort || (form.elements.sortBy.value || "discount");
  const hotels = [...(items || [])];
  const price = (hotel) => {
    if (hotel.currentPrice === null || hotel.currentPrice === undefined || hotel.currentPrice === "") {
      return 1000000000;
    }
    const value = Number(hotel.currentPrice);
    return Number.isFinite(value) ? value : 1000000000;
  };
  const distance = (hotel) => {
    const value = Number(hotel.distanceKm);
    return Number.isFinite(value) ? value : 999;
  };
  const star = (hotel) => Number(hotel.starRating || 0);
  const discount = (hotel) => Number(hotel.discountAmount || 0);
  const brandRank = (hotel) => Number(hotel.brandRank || 99);

  if (sortBy === "price") {
    return hotels.sort((a, b) => price(a) - price(b) || distance(a) - distance(b));
  }
  if (sortBy === "distance") {
    return hotels.sort((a, b) => distance(a) - distance(b) || star(b) - star(a) || price(a) - price(b));
  }
  if (sortBy === "star") {
    return hotels.sort((a, b) => star(b) - star(a) || price(a) - price(b));
  }
  if (mode === "recommend") {
    return hotels.sort((a, b) => distance(a) - distance(b) || star(b) - star(a) || brandRank(a) - brandRank(b) || price(a) - price(b));
  }
  return hotels.sort((a, b) => discount(b) - discount(a) || price(a) - price(b));
}

function setLoading(isLoading, message = "") {
  submitBtn.disabled = isLoading;
  submitBtn.textContent = isLoading ? "搜索中..." : "开始捡漏";
  if (message) loadingBox.textContent = message;
  else if (isLoading) loadingBox.textContent = "正在查询价格和计算优惠...";
  loadingBox.classList.toggle("hidden", !isLoading);
}

function formatElapsed(ms) {
  const seconds = Math.max(0, Math.round(Number(ms || 0) / 1000));
  if (!seconds) return "";
  if (seconds < 60) return `${seconds}秒`;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return rest ? `${minutes}分${rest}秒` : `${minutes}分钟`;
}

function runningMessage(data) {
  const result = data.result || {};
  const summary = result.summary || {};
  const progress = data.progress || summary.progress || {};
  const message = progress.message || "后台仍在搜索 Trip.com，找到候选后会自动刷新。";
  const parts = [message];
  const candidateCount = Number(summary.candidateCount || 0);
  if (candidateCount) parts.push(`已找到 ${candidateCount} 家候选`);
  const elapsed = formatElapsed(data.elapsedMs || progress.elapsedMs);
  if (elapsed) parts.push(`已用时 ${elapsed}`);
  return parts.join("｜");
}

function nextPollDelay(data) {
  const result = data?.result || {};
  const progress = data?.progress || result.summary?.progress || {};
  const elapsedMs = Number(data?.elapsedMs || progress.elapsedMs || 0);
  if (elapsedMs < 60_000) return 1000;
  if (elapsedMs < 240_000) return 1500;
  return 2500;
}

function setError(message) {
  errorBox.textContent = message || "";
  errorBox.classList.toggle("hidden", !message);
}

function exportableHotelCount(data) {
  if (!data) return 0;
  if (Array.isArray(data.allHotels) && data.allHotels.length) return data.allHotels.length;
  const seen = new Set();
  for (const hotel of [...(data.dealHotels || []), ...(data.recommendedHotels || [])]) {
    if (!hotel) continue;
    const key = hotel.hotelId || hotel.hotelName || hotel.hotelOriginalName || JSON.stringify(hotel);
    seen.add(key);
  }
  return seen.size;
}

function setExportState() {
  if (!exportPdfBtn || !exportPdfHint) return;
  const count = exportableHotelCount(latestData);
  exportPdfBtn.disabled = !count || pdfExporting;
  exportPdfBtn.textContent = pdfExporting ? "正在生成 PDF..." : "导出 PDF";
  if (!count) {
    exportPdfHint.textContent = "搜索出酒店后可导出本次完整记录";
    return;
  }
  const stillSearching = Boolean(latestData?.summary?.partial || latestData?.summary?.refreshing);
  exportPdfHint.textContent = stillSearching
    ? `可先导出当前 ${count} 家酒店，后台刷新后可再次导出`
    : `导出本次搜索记录，包含 ${count} 家酒店`;
}

function filenameFromDisposition(disposition) {
  const header = String(disposition || "");
  const encoded = header.match(/filename\*=UTF-8''([^;]+)/i);
  if (encoded?.[1]) {
    try {
      return decodeURIComponent(encoded[1].replaceAll("+", "%20"));
    } catch (error) {
      return encoded[1];
    }
  }
  const plain = header.match(/filename="?([^";]+)"?/i);
  return plain?.[1] || "";
}

async function exportCurrentPdf() {
  if (!latestData || !exportableHotelCount(latestData) || pdfExporting) return;
  pdfExporting = true;
  setError("");
  setExportState();
  let successMessage = "";
  try {
    const response = await fetch("/api/export/pdf", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({result: latestData}),
    });
    if (!response.ok) {
      let errorMessage = "PDF 导出失败";
      try {
        const data = await response.json();
        errorMessage = data.error || errorMessage;
      } catch (error) {
        errorMessage = await response.text() || errorMessage;
      }
      throw new Error(errorMessage);
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filenameFromDisposition(response.headers.get("Content-Disposition")) || "星级酒店捡漏雷达搜索记录.pdf";
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    successMessage = "PDF 已生成，后台结果刷新后可再次导出";
  } catch (error) {
    setError(error.message || String(error));
  } finally {
    pdfExporting = false;
    setExportState();
    if (successMessage && exportPdfHint) exportPdfHint.textContent = successMessage;
  }
}

function formPayload() {
  const data = new FormData(form);
  const payload = {};
  for (const [key, value] of data.entries()) {
    payload[key] = String(value).trim();
  }
  payload.forceRefresh = form.elements.forceRefresh?.checked ? "1" : "0";
  if (payload.provider !== "tripcom" || !payload.targetHint) {
    delete payload.targetHint;
  }
  return payload;
}

function tripcomActive() {
  return form.elements.provider.value === "tripcom";
}

function clearTargetHint() {
  form.elements.targetHint.value = "";
}

function hideSuggestions() {
  latestSuggestions = [];
  targetSuggestions.classList.add("hidden");
  targetSuggestions.innerHTML = "";
}

function suggestionMeta(item) {
  const parts = [
    item.resultTypeLabel || item.searchType || "位置",
    item.city,
    item.hotelOriginalName && item.hotelOriginalName !== item.hotelName ? item.hotelOriginalName : "",
  ].filter(Boolean);
  return parts.join("｜");
}

function renderSuggestions(items, statusText = "") {
  latestSuggestions = items || [];
  if (statusText) {
    targetSuggestions.innerHTML = `<div class="suggestion-status">${escapeHtml(statusText)}</div>`;
    targetSuggestions.classList.remove("hidden");
    return;
  }
  if (!latestSuggestions.length) {
    targetSuggestions.innerHTML = `<div class="suggestion-status">没有匹配到 Trip.com 目标，可换成地标、商圈或英文名再试。</div>`;
    targetSuggestions.classList.remove("hidden");
    return;
  }
  targetSuggestions.innerHTML = latestSuggestions.map((item, index) => `
    <button class="suggestion-item" type="button" data-index="${index}">
      <span class="suggestion-title">${escapeHtml(item.hotelName)}</span>
      <span class="suggestion-meta">${escapeHtml(suggestionMeta(item))}</span>
    </button>
  `).join("");
  targetSuggestions.classList.remove("hidden");
}

async function fetchTargetSuggestions() {
  if (!tripcomActive()) {
    hideSuggestions();
    return;
  }
  const city = form.elements.city.value.trim();
  const query = form.elements.targetHotel.value.trim();
  if (!city || query.length < 2) {
    hideSuggestions();
    return;
  }

  const requestId = ++suggestionRequestId;
  renderSuggestions([], "正在匹配 Trip.com 名称...");
  try {
    const params = new URLSearchParams({city, q: query, limit: "8"});
    const response = await fetch(`/api/tripcom/suggest?${params.toString()}`);
    const data = await response.json();
    if (requestId !== suggestionRequestId) return;
    if (!response.ok) throw new Error(data.error || "Trip.com 目标匹配失败");
    renderSuggestions(data.suggestions || []);
  } catch (error) {
    if (requestId !== suggestionRequestId) return;
    renderSuggestions([], error.message || "Trip.com 目标匹配失败");
  }
}

function scheduleTargetSuggestions() {
  clearTimeout(suggestionTimer);
  suggestionTimer = setTimeout(fetchTargetSuggestions, 420);
}

function hotTargetMeta(item) {
  return [item.targetType || "酒店/地区", item.heatLabel || ""].filter(Boolean).join("｜");
}

function renderHotTargets(items) {
  latestHotTargets = Array.isArray(items) ? items : [];
  if (!hotSearchList) return;
  if (!latestHotTargets.length) {
    hotSearchList.innerHTML = "";
    return;
  }
  hotSearchList.innerHTML = latestHotTargets.map((item, index) => `
    <button class="hot-chip" type="button" data-index="${index}">
      <span class="hot-city">${escapeHtml(item.city)}</span>
      <span class="hot-target">${escapeHtml(item.targetHotel)}</span>
      <span class="hot-type">${escapeHtml(hotTargetMeta(item))}</span>
    </button>
  `).join("");
}

async function fetchHotTargets() {
  if (!hotSearchList) return;
  try {
    const response = await fetch("/api/hot-targets?limit=10");
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "热搜读取失败");
    renderHotTargets(data.targets || []);
  } catch (error) {
    renderHotTargets(window.__HOT_TARGETS__ || []);
  }
}

function applyHotTarget(item) {
  if (!item) return;
  form.elements.city.value = item.city || "";
  form.elements.targetHotel.value = item.targetHotel || "";
  if (item.targetHint) {
    form.elements.targetHint.value = JSON.stringify(item.targetHint);
    hideSuggestions();
  } else {
    clearTargetHint();
    scheduleTargetSuggestions();
  }
}

async function updateCompareDates() {
  const selectedDate = form.elements.selectedDate.value;
  if (!selectedDate) return;
  try {
    const response = await fetch(`/api/compare-dates?date=${encodeURIComponent(selectedDate)}`);
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "对比日期生成失败");
    const notice = data.compareNotice || holidayCompareNoticeFromSummary(data);
    compareDatesNode.textContent = data.compareMode === "holiday"
      ? `公众假期对比：${data.compareDates.join("、")}`
      : data.compareDates.join("、");
    setCompareNotice(notice);
  } catch (error) {
    compareDatesNode.textContent = "-";
    setCompareNotice("");
  }
}

function renderResult(data) {
  const summary = data.summary || {};
  const progress = summary.progress || {};
  candidateCount.textContent = summary.candidateCount ?? 0;
  dealCount.textContent = summary.dealCount ?? 0;
  brandCount.textContent = summary.recommendedCount ?? 0;
  if (jumpCandidateCount) jumpCandidateCount.textContent = summary.candidateCount ?? 0;
  if (jumpDealCount) jumpDealCount.textContent = summary.dealCount ?? 0;
  if (jumpBrandCount) jumpBrandCount.textContent = summary.recommendedCount ?? 0;
  const holidayNotice = holidayCompareNoticeFromSummary(summary);
  compareDatesNode.textContent = summary.compareMode === "holiday"
    ? `公众假期对比：${(data.compareDates || []).join("、") || "-"}`
    : ((data.compareDates || []).join("、") || "-");
  setCompareNotice(holidayNotice);
  setPriceStatus(summary);
  const expanded = summary.radiusExpanded
    ? `｜已扩展至 ${Number(summary.effectiveRadiusKm).toFixed(0)}km`
    : "";
  const compareLabel = summary.compareMode === "holiday" && summary.holidayName
    ? `｜${summary.holidayName}假期内对比`
    : "";
  const cacheLabel = summary.cacheHit ? "｜缓存命中" : "";
  const mergeLabel = summary.mergedFromCache
    ? `｜已合并旧缓存${summary.cacheCarriedHotelCount ? `，补回 ${summary.cacheCarriedHotelCount} 家` : ""}`
    : "";
  const partialLabel = summary.partial
    ? (progress.stage === "name-verification" ? "｜中文名核验中" : "｜已先返回候选，后台计算优惠中")
    : "";
  const nameLabel = summary.nameVerificationActive ? "｜中文名正在同步更新" : "";
  const progressLabel = progress.message ? `｜${progress.message}` : "";
  if (sourcePill) {
    sourcePill.textContent = `${summary.source || "Trip.com 实时价格"}${expanded}${compareLabel}${cacheLabel}${mergeLabel}${partialLabel}${nameLabel}${progressLabel}`;
  }

  const deals = chineseNamedHotels(sortedHotels(data.dealHotels, "deal", summary));
  const filteredDeals = filterHotels(deals, "deal");
  const allCandidates = chineseNamedHotels(sortedHotels(data.allHotels, "candidate", summary));
  const candidates = filterHotels(allCandidates, "candidate");
  const allRecommended = chineseNamedHotels(sortedHotels(data.recommendedHotels, "recommend", summary));
  const recommended = filterHotels(allRecommended, "recommend");
  updateHotelFilterCount("deal", filteredDeals.length, deals.length);
  updateHotelFilterCount("candidate", candidates.length, allCandidates.length);
  updateHotelFilterCount("recommend", recommended.length, allRecommended.length);
  dealList.innerHTML = filteredDeals.length
    ? filteredDeals.map((hotel) => hotelCard(hotel, "deal")).join("")
    : emptyState(deals.length ? "没有符合当前筛选的捡漏酒店，可重置关键词或价格范围。" : (summary.partial ? (progress.message || "已先展示目标日期候选酒店，后台正在计算优惠力度。") : "当前筛选条件下没有便宜 100 元以上的酒店。"));
  candidateList.innerHTML = candidates.length
    ? candidates.map((hotel) => hotelCard(hotel, "candidate")).join("")
    : emptyState(allCandidates.length ? "没有符合当前筛选的候选酒店，可重置关键词或价格范围。" : (summary.partial ? (progress.message || "正在抓取附近四星级以上酒店，请稍候。") : "当前筛选条件下没有抓到四星级以上附近酒店。"));
  recommendList.innerHTML = recommended.length
    ? recommended.map((hotel) => hotelCard(hotel, "brand")).join("")
    : emptyState(allRecommended.length ? "没有符合当前筛选的高端连锁酒店，可重置关键词或价格范围。" : (summary.partial ? "连锁品牌推荐会在候选酒店返回后自动刷新。" : "当前筛选条件下没有命中指定高端连锁品牌。"));
  setExportState();
}

function clearSearchPoll() {
  if (searchPollTimer) {
    clearTimeout(searchPollTimer);
    searchPollTimer = null;
  }
  if (searchEventSource) {
    searchEventSource.close();
    searchEventSource = null;
  }
}

function handleSearchJobUpdate(data, requestId) {
  if (requestId !== searchRequestId) return true;
  if (data.status === "complete" && data.result) {
    latestData = data.result;
    renderResult(latestData);
    setLoading(false);
    fetchHotTargets();
    return true;
  }
  if (data.result) {
    latestData = data.result;
    renderResult(latestData);
  }
  if (data.status === "error") {
    setLoading(false);
    setError(data.error || "后台搜索失败");
    return true;
  }
  setLoading(true, runningMessage(data));
  return false;
}

async function pollSearchJob(jobId, requestId) {
  if (!jobId || requestId !== searchRequestId) return;
  let pollData = null;
  try {
    const params = new URLSearchParams({sortBy: form.elements.sortBy.value || "discount"});
    const response = await fetch(`/api/search/status/${encodeURIComponent(jobId)}?${params.toString()}`);
    const data = await response.json();
    pollData = data;
    if (handleSearchJobUpdate(data, requestId)) return;
  } catch (error) {
    if (requestId !== searchRequestId) return;
  }
  searchPollTimer = setTimeout(() => pollSearchJob(jobId, requestId), nextPollDelay(pollData || (latestData ? {result: latestData} : {})));
}

function startSearchUpdates(jobId, requestId) {
  if (!jobId || requestId !== searchRequestId) return;
  if (typeof EventSource === "undefined") {
    searchPollTimer = setTimeout(() => pollSearchJob(jobId, requestId), 500);
    return;
  }
  const params = new URLSearchParams({sortBy: form.elements.sortBy.value || "discount"});
  const source = new EventSource(`/api/search/events/${encodeURIComponent(jobId)}?${params.toString()}`);
  searchEventSource = source;
  let receivedEvent = false;
  const fallbackTimer = setTimeout(() => {
    if (!receivedEvent && requestId === searchRequestId && searchEventSource === source) {
      source.close();
      searchEventSource = null;
      pollSearchJob(jobId, requestId);
    }
  }, 3500);

  source.onmessage = (event) => {
    if (requestId !== searchRequestId) {
      source.close();
      return;
    }
    receivedEvent = true;
    clearTimeout(fallbackTimer);
    try {
      const data = JSON.parse(event.data);
      if (handleSearchJobUpdate(data, requestId) && searchEventSource === source) {
        source.close();
        searchEventSource = null;
      }
    } catch (error) {
      source.close();
      if (searchEventSource === source) searchEventSource = null;
      pollSearchJob(jobId, requestId);
    }
  };

  source.onerror = () => {
    clearTimeout(fallbackTimer);
    if (searchEventSource !== source) return;
    source.close();
    searchEventSource = null;
    if (requestId === searchRequestId) {
      searchPollTimer = setTimeout(() => pollSearchJob(jobId, requestId), 500);
    }
  };
}

async function runSearch() {
  const requestId = ++searchRequestId;
  clearSearchPoll();
  setError("");
  latestData = null;
  setExportState();
  setPriceStatus({});
  setLoading(true, "搜索任务准备中...");
  try {
    const response = await fetch("/api/search", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(formPayload()),
    });
    const data = await response.json();
    if (requestId !== searchRequestId) return;
    if (!response.ok) throw new Error(data.error || "搜索失败");
    latestData = data;
    renderResult(latestData);
    fetchHotTargets();
    const pollingJobId = data.summary?.jobId || data.summary?.refreshJobId;
    if ((data.summary?.partial || data.summary?.refreshing) && pollingJobId) {
      setLoading(true, data.summary.progress?.message || "搜索任务已启动，后台会逐步刷新结果。");
      startSearchUpdates(pollingJobId, requestId);
    }
  } catch (error) {
    if (requestId !== searchRequestId) return;
    setError(error.message || String(error));
  } finally {
    const stillPolling = latestData?.summary
      && (latestData.summary.partial || latestData.summary.refreshing)
      && (latestData.summary.jobId || latestData.summary.refreshJobId);
    if (requestId === searchRequestId && !stillPolling) {
      setLoading(false);
    }
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  runSearch();
});

exportPdfBtn?.addEventListener("click", exportCurrentPdf);

form.elements.selectedDate.addEventListener("change", updateCompareDates);
form.elements.city.addEventListener("input", () => {
  clearTargetHint();
  scheduleTargetSuggestions();
});
form.elements.targetHotel.addEventListener("input", () => {
  clearTargetHint();
  scheduleTargetSuggestions();
});
form.elements.targetHotel.addEventListener("focus", () => {
  if (tripcomActive() && form.elements.targetHotel.value.trim().length >= 2) {
    scheduleTargetSuggestions();
  }
});
form.elements.provider.addEventListener("change", () => {
  clearTargetHint();
  if (tripcomActive()) {
    scheduleTargetSuggestions();
  } else {
    hideSuggestions();
  }
});
form.elements.sortBy.addEventListener("change", () => {
  if (latestData) {
    renderResult(latestData);
  } else {
    runSearch();
  }
});

for (const [mode, controls] of Object.entries(hotelFilters)) {
  [controls.keyword, controls.minPrice, controls.maxPrice].forEach((input) => {
    input?.addEventListener("input", () => {
      if (latestData) renderResult(latestData);
      else updateHotelFilterCount(mode, 0, 0);
    });
  });
  controls.sort?.addEventListener("change", () => {
    if (latestData) renderResult(latestData);
  });
  controls.reset?.addEventListener("click", () => {
    if (controls.keyword) controls.keyword.value = "";
    if (controls.sort) controls.sort.value = mode === "deal" ? "discount" : "distance";
    if (controls.minPrice) controls.minPrice.value = "";
    if (controls.maxPrice) controls.maxPrice.value = "";
    if (latestData) renderResult(latestData);
    else updateHotelFilterCount(mode, 0, 0);
  });
}

function setResultJumpOpen(open) {
  resultJumpWidget?.classList.toggle("is-open", open);
  resultJumpToggle?.setAttribute("aria-expanded", open ? "true" : "false");
}

resultJumpToggle?.addEventListener("click", (event) => {
  event.stopPropagation();
  setResultJumpOpen(!resultJumpWidget?.classList.contains("is-open"));
});

resultJumpPanel?.addEventListener("click", (event) => {
  if (event.target.closest("a")) setResultJumpOpen(false);
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") setResultJumpOpen(false);
});

targetSuggestions?.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-index]");
  if (!button) return;
  const item = latestSuggestions[Number(button.dataset.index)];
  if (!item) return;
  form.elements.targetHotel.value = item.hotelName;
  form.elements.targetHint.value = JSON.stringify(item);
  hideSuggestions();
});

hotSearchList?.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-index]");
  if (!button) return;
  applyHotTarget(latestHotTargets[Number(button.dataset.index)]);
});

document.addEventListener("click", (event) => {
  if (resultJumpWidget && !resultJumpWidget.contains(event.target)) {
    setResultJumpOpen(false);
  }
  if (event.target.closest(".suggest-field")) return;
  hideSuggestions();
});

renderHotTargets(window.__HOT_TARGETS__ || []);
fetchHotTargets();
updateCompareDates();
setExportState();
