from __future__ import annotations

import datetime as dt
import copy
import hashlib
import html
import inspect
import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from werkzeug.exceptions import HTTPException

from hotel_deals import (
    HotelDealError,
    apply_verified_hotel_name_payload,
    calculateDealScore,
    finalize_pending_hotel_name,
    get_compare_date_info,
    hotel_brand_payload,
    hotel_name_needs_verification,
    normalize_result_hotel_name,
    search_current_prices,
    search_deals,
    sort_hotels,
    sort_recommended_hotels,
)
from localization import contains_chinese_text, domestic_hotel_name_key, simplify_chinese_text
from mysql_cache import MySQLHotelNameCache, MySQLSearchCache
from providers import LocalJsonProvider, ProviderError, TripComProvider, provider_from_name


APP_DIR = Path(__file__).resolve().parent
app = Flask(__name__)
LOCAL_PROVIDER = LocalJsonProvider(APP_DIR)
SEARCH_CACHE: dict[str, tuple[float, dict[str, Any], float]] = {}
SEARCH_JOBS: dict[str, dict[str, Any]] = {}
SEARCH_JOB_LOCK = threading.Lock()
SEARCH_CACHE_DIR = APP_DIR / ".cache" / "search_cache"
HOTEL_NAME_CACHE_PATH = APP_DIR / ".cache" / "hotel_name_cache.json"
HOT_SEARCH_PATH = APP_DIR / ".cache" / "hot_searches.json"
LOCAL_CACHE_TTL_SECONDS = int(os.environ.get("HOTEL_DEAL_LOCAL_CACHE_TTL_SECONDS", str(7 * 24 * 60 * 60)))
TRIPCOM_CACHE_TTL_SECONDS = int(os.environ.get("HOTEL_DEAL_TRIPCOM_CACHE_TTL_SECONDS", str(7 * 24 * 60 * 60)))
TRIPCOM_REFRESH_AFTER_SECONDS = int(os.environ.get("HOTEL_DEAL_TRIPCOM_REFRESH_AFTER_SECONDS", str(12 * 60 * 60)))
CACHE_RETRY_DELAY_SECONDS = int(os.environ.get("HOTEL_DEAL_SEARCH_RETRY_DELAY_SECONDS", "60"))
MAX_SEARCH_CACHE_ITEMS = 256
MAX_HOT_SEARCH_RECORDS = 80
HOT_SEARCH_TTL_SECONDS = 30 * 24 * 60 * 60
HOTEL_NAME_CACHE_TTL_SECONDS = int(os.environ.get("HOTEL_DEAL_NAME_CACHE_TTL_SECONDS", str(365 * 24 * 60 * 60)))
CACHE_LOGIC_VERSION = "search_v37_pending_price_detail_backfill"
MYSQL_SEARCH_CACHE = MySQLSearchCache.from_env()
MYSQL_HOTEL_NAME_CACHE = MySQLHotelNameCache.from_env()
HOTEL_NAME_CACHE_LOCK = threading.RLock()
HOTEL_NAME_MEMORY_CACHE: dict[str, dict[str, Any]] = {"byHotelId": {}, "byNameKey": {}}
HOTEL_NAME_CACHE_LOADED = False
TARGET_TYPE_LABELS = {"H": "酒店", "LM": "地标", "D": "地区", "CT": "城市", "Z": "商圈"}
DEFAULT_HOT_TARGETS = [
    {"city": "深圳", "targetHotel": "深圳国际会展中心希尔顿酒店", "targetType": "酒店", "heatLabel": "热搜"},
    {"city": "深圳", "targetHotel": "深圳国际会展中心", "targetType": "地区", "heatLabel": "热搜"},
    {"city": "上海", "targetHotel": "上海迪士尼度假区", "targetType": "地区", "heatLabel": "热搜"},
    {"city": "上海", "targetHotel": "上海虹桥国家会展中心", "targetType": "地区", "heatLabel": "热搜"},
    {"city": "广州", "targetHotel": "广州塔", "targetType": "地标", "heatLabel": "热搜"},
    {"city": "广州", "targetHotel": "珠江新城", "targetType": "地区", "heatLabel": "热搜"},
    {"city": "北京", "targetHotel": "北京国贸", "targetType": "地区", "heatLabel": "热搜"},
    {"city": "成都", "targetHotel": "春熙路太古里", "targetType": "地区", "heatLabel": "热搜"},
]


def parse_optional_int(value: Any, field_name: str) -> int | None:
    if value in ("", None):
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise HotelDealError(f"{field_name}必须是整数") from exc


def parse_float(value: Any, field_name: str, default: float) -> float:
    if value in ("", None):
        return default
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise HotelDealError(f"{field_name}必须是数字") from exc


def parse_bool(value: Any, default: bool = True) -> bool:
    if value in ("", None):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off", "disabled"}


def export_text(value: Any, default: str = "-") -> str:
    text = simplify_chinese_text(str(value or "")).strip()
    return text or default


def export_money(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"¥{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return "-"


def export_percent(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number:.1f}".rstrip("0").rstrip(".") + "%"


def export_number(value: Any, suffix: str = "", digits: int = 1) -> str:
    if value in (None, ""):
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    formatted = f"{number:.{digits}f}".rstrip("0").rstrip(".")
    return f"{formatted}{suffix}"


def export_hotel_name(hotel: dict[str, Any]) -> str:
    return export_text(
        hotel.get("hotelName")
        or hotel.get("hotelNameSimplified")
        or hotel.get("hotelOriginalName")
        or "未命名酒店"
    )


def export_hotel_brand(hotel: dict[str, Any]) -> str:
    return export_text(
        hotel.get("groupLabel")
        or hotel.get("brandLabel")
        or hotel.get("group")
        or hotel.get("brand")
        or "独立酒店"
    )


def html_escape(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def export_price_delta(hotel: dict[str, Any]) -> tuple[str, str]:
    amount = hotel.get("discountAmount")
    percent = hotel.get("discountPercent")
    if amount in (None, ""):
        return "-", "neutral"
    try:
        numeric = float(amount)
    except (TypeError, ValueError):
        return "-", "neutral"
    try:
        percent_value = abs(float(percent))
    except (TypeError, ValueError):
        percent_value = None
    percent_text = export_percent(percent_value)
    if numeric > 0:
        return f"便宜 {export_money(abs(numeric))} / {percent_text}", "positive"
    if numeric < 0:
        return f"高出 {export_money(abs(numeric))} / {percent_text}", "negative"
    return "持平", "neutral"


def export_sort_number(value: Any, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def export_hotels_by_distance(hotels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        hotels,
        key=lambda hotel: (
            export_sort_number(hotel.get("distanceKm"), 999999),
            export_sort_number(hotel.get("currentPrice"), 999999999),
            -export_sort_number(hotel.get("starRating"), 0),
            export_hotel_name(hotel),
        ),
    )


def export_hotel_table(hotels: list[dict[str, Any]], *, title: str, empty_text: str) -> str:
    rows: list[str] = []
    for index, hotel in enumerate(hotels, start=1):
        delta_text, delta_class = export_price_delta(hotel)
        trip_url = str(hotel.get("tripUrl") or "").strip()
        trip_link = f'<a href="{html_escape(trip_url)}">Trip.com</a>' if trip_url else "-"
        rows.append(
            "<tr>"
            f"<td class=\"idx\">{index}</td>"
            f"<td class=\"name\">{html_escape(export_hotel_name(hotel))}</td>"
            f"<td>{html_escape(export_number(hotel.get('starRating'), '星', 0))}</td>"
            f"<td>{html_escape(export_hotel_brand(hotel))}</td>"
            f"<td>{html_escape(export_number(hotel.get('distanceKm'), 'km', 1))}</td>"
            f"<td>{html_escape(export_money(hotel.get('currentPrice')))}</td>"
            f"<td>{html_escape(export_money(hotel.get('referencePrice') or hotel.get('averageComparePrice')))}</td>"
            f"<td class=\"delta {delta_class}\">{html_escape(delta_text)}</td>"
            f"<td>{html_escape(export_number(hotel.get('rating'), '分', 1))}</td>"
            f"<td>{html_escape(export_number(hotel.get('reviewCount'), '', 0))}</td>"
            f"<td>{trip_link}</td>"
            "</tr>"
        )
    body = "\n".join(rows) if rows else f"<tr><td colspan=\"11\" class=\"empty\">{html_escape(empty_text)}</td></tr>"
    return f"""
      <section class="pdf-section">
        <h2>{html_escape(title)} <small>{len(hotels)} 家</small></h2>
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>酒店名称</th>
              <th>星级</th>
              <th>品牌/集团</th>
              <th>距离</th>
              <th>目标价</th>
              <th>参考价</th>
              <th>价格差</th>
              <th>评分</th>
              <th>点评</th>
              <th>链接</th>
            </tr>
          </thead>
          <tbody>{body}</tbody>
        </table>
      </section>
    """


def export_pdf_filename(result: dict[str, Any]) -> str:
    query = result.get("query") if isinstance(result.get("query"), dict) else {}
    city = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", export_text(query.get("city"), "搜索"))
    target = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", export_text(query.get("targetHotel"), "酒店"))
    selected_date = re.sub(r"[^0-9-]+", "", export_text(query.get("selectedDate"), dt.date.today().isoformat()))
    return f"星级酒店捡漏雷达-{city}-{target}-{selected_date}.pdf"


def build_search_result_pdf_html(result: dict[str, Any]) -> str:
    query = result.get("query") if isinstance(result.get("query"), dict) else {}
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    compare_dates = result.get("compareDates") or []
    all_hotels = export_hotels_by_distance(
        [hotel for hotel in (result.get("allHotels") or []) if isinstance(hotel, dict)]
    )
    deal_hotels = export_hotels_by_distance(
        [hotel for hotel in (result.get("dealHotels") or []) if isinstance(hotel, dict)]
    )
    recommended_hotels = export_hotels_by_distance(
        [hotel for hotel in (result.get("recommendedHotels") or []) if isinstance(hotel, dict)]
    )
    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    city = export_text(query.get("city"))
    target = export_text(query.get("targetHotel"))
    selected_date = export_text(query.get("selectedDate"))
    provider = export_text(summary.get("source"), "Trip.com 实时价格")
    holiday_payload = summary.get("holiday") if isinstance(summary.get("holiday"), dict) else {}
    holiday = export_text(summary.get("holidayName") or holiday_payload.get("name"), "")
    compare_label = "、".join(export_text(date) for date in compare_dates) or "-"
    holiday_note = f"<p class=\"notice\">本次为{html_escape(holiday)}公众假期对比。</p>" if holiday else ""
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>星级酒店捡漏雷达搜索记录</title>
  <style>
    @page {{ size: A4; margin: 14mm 10mm; }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: #162033;
      font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei", Arial, sans-serif;
      font-size: 11px;
      line-height: 1.45;
    }}
    h1 {{ margin: 0 0 6px; font-size: 24px; }}
    h2 {{ margin: 18px 0 8px; font-size: 15px; }}
    h2 small {{ color: #64748b; font-size: 11px; font-weight: 600; }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 8px;
      margin: 12px 0;
    }}
    .meta div {{
      border: 1px solid #d7e0ea;
      border-radius: 6px;
      padding: 7px 8px;
      background: #f8fafc;
    }}
    .meta span {{ display: block; color: #64748b; font-size: 10px; }}
    .meta strong {{ display: block; margin-top: 2px; font-size: 12px; }}
    .caption, .notice {{ color: #526174; margin: 0; }}
    .notice {{ color: #9a5a00; }}
    table {{ width: 100%; border-collapse: collapse; page-break-inside: auto; }}
    thead {{ display: table-header-group; }}
    tr {{ page-break-inside: avoid; page-break-after: auto; }}
    th, td {{ border: 1px solid #d9e2ee; padding: 5px 6px; vertical-align: top; }}
    th {{ background: #edf3f8; color: #44546a; font-size: 10px; text-align: left; }}
    td {{ word-break: break-word; }}
    .idx {{ width: 24px; text-align: center; color: #64748b; }}
    .name {{ width: 190px; font-weight: 700; }}
    .delta.positive {{ color: #08704f; font-weight: 700; }}
    .delta.negative {{ color: #b42318; font-weight: 700; }}
    .delta.neutral {{ color: #64748b; }}
    .empty {{ text-align: center; color: #64748b; padding: 14px; }}
    a {{ color: #1c5fc7; text-decoration: none; }}
  </style>
</head>
<body>
  <header>
    <h1>星级酒店捡漏雷达搜索记录</h1>
    <p class="caption">导出时间：{html_escape(generated_at)}｜数据源：{html_escape(provider)}｜价格为 Trip.com 已返回的含税价/参考价。</p>
    {holiday_note}
    <div class="meta">
      <div><span>目标城市</span><strong>{html_escape(city)}</strong></div>
      <div><span>目标酒店/位置</span><strong>{html_escape(target)}</strong></div>
      <div><span>入住日期</span><strong>{html_escape(selected_date)}</strong></div>
      <div><span>搜索半径</span><strong>{html_escape(export_number(query.get('effectiveRadiusKm') or query.get('radiusKm'), 'km', 0))}</strong></div>
      <div><span>对比日期</span><strong>{html_escape(compare_label)}</strong></div>
      <div><span>全部候选</span><strong>{len(all_hotels)} 家</strong></div>
      <div><span>捡漏酒店</span><strong>{len(deal_hotels)} 家</strong></div>
      <div><span>连锁推荐</span><strong>{len(recommended_hotels)} 家</strong></div>
    </div>
  </header>
  {export_hotel_table(deal_hotels, title="适合捡漏的酒店", empty_text="本次搜索暂无符合捡漏标准的酒店。")}
  {export_hotel_table(recommended_hotels, title="知名高端连锁推荐酒店", empty_text="本次搜索暂无知名高端连锁推荐酒店。")}
  {export_hotel_table(all_hotels, title="全部附近星级候选酒店", empty_text="本次搜索暂无附近星级候选酒店。")}
</body>
</html>"""


def render_search_result_pdf(result: dict[str, Any]) -> bytes:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise HotelDealError("当前 Python 环境没有安装 Playwright，无法导出 PDF") from exc

    html_text = build_search_result_pdf_html(result)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1240, "height": 1754})
        page.set_content(html_text, wait_until="load")
        pdf_bytes = page.pdf(
            format="A4",
            print_background=True,
            margin={"top": "14mm", "right": "10mm", "bottom": "14mm", "left": "10mm"},
        )
        browser.close()
    return pdf_bytes


def default_check_in() -> str:
    demo_date = dt.date(2026, 6, 1)
    today = dt.date.today()
    if today <= demo_date:
        return demo_date.isoformat()
    return (today + dt.timedelta(days=14)).isoformat()


def local_provider() -> LocalJsonProvider:
    return LOCAL_PROVIDER


def reload_local_provider() -> None:
    global LOCAL_PROVIDER
    LOCAL_PROVIDER = LocalJsonProvider(APP_DIR)
    clear_search_cache(provider="local")


def canonical_provider_name(value: Any) -> str:
    provider_name = str(value or "tripcom").strip().lower()
    if provider_name in {"", "trip", "tripcom", "trip.com", "live"}:
        return "tripcom"
    if provider_name == "local":
        return "local"
    return provider_name


def local_data_version() -> str:
    parts: list[str] = []
    for path in (APP_DIR / "data" / "sample_hotels.json", APP_DIR / ".cache" / "imported_hotels.json"):
        try:
            stat = path.stat()
        except OSError:
            parts.append(f"{path.name}:missing")
            continue
        parts.append(f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}")
    return "|".join(parts)


def parse_target_hint(value: Any) -> dict[str, Any] | None:
    if not value:
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def normalized_target_hint(value: Any) -> dict[str, str] | None:
    hint = parse_target_hint(value)
    if not hint:
        return None
    normalized = {
        "hotelId": str(hint.get("hotelId") or hint.get("code") or "").strip(),
        "searchType": str(hint.get("searchType") or hint.get("resultType") or "").strip().upper(),
        "hotelName": str(hint.get("hotelName") or hint.get("name") or "").strip(),
    }
    if not normalized["hotelId"] or not normalized["searchType"]:
        return None
    return normalized


def public_target_hint(value: Any) -> dict[str, Any] | None:
    hint = parse_target_hint(value)
    if not hint:
        return None
    hotel_id = str(hint.get("hotelId") or hint.get("code") or "").strip()
    hotel_name = str(hint.get("hotelName") or hint.get("name") or "").strip()
    search_type = str(hint.get("searchType") or hint.get("resultType") or "").strip().upper()
    if not hotel_id or not hotel_name or search_type not in TARGET_TYPE_LABELS:
        return None
    allowed_fields = {
        "hotelId",
        "hotelName",
        "hotelOriginalName",
        "hotelNameSimplified",
        "hotelNameSource",
        "city",
        "cityId",
        "provinceId",
        "countryId",
        "latitude",
        "longitude",
        "searchType",
        "searchValue",
        "searchCoordinate",
        "resultTypeLabel",
        "matchedQuery",
    }
    cleaned = {key: hint.get(key) for key in allowed_fields if hint.get(key) not in ("", None)}
    cleaned["hotelId"] = hotel_id
    cleaned["hotelName"] = hotel_name
    cleaned["searchType"] = search_type
    cleaned.setdefault("resultTypeLabel", TARGET_TYPE_LABELS[search_type])
    return cleaned


def infer_target_type(target_hint: dict[str, Any] | None, fallback: Any = None) -> str:
    if fallback:
        return str(fallback).strip()
    if target_hint:
        label = str(target_hint.get("resultTypeLabel") or "").strip()
        if label:
            return label
        search_type = str(target_hint.get("searchType") or "").strip().upper()
        if search_type in TARGET_TYPE_LABELS:
            return TARGET_TYPE_LABELS[search_type]
    return "酒店/地区"


def hot_target_key(item: dict[str, Any]) -> str:
    city = str(item.get("city") or "").strip().casefold()
    target_hint = item.get("targetHint") if isinstance(item.get("targetHint"), dict) else None
    if target_hint and target_hint.get("hotelId"):
        search_type = str(target_hint.get("searchType") or "").strip().upper()
        return f"{city}|{search_type}:{target_hint['hotelId']}"
    target_hotel = str(item.get("targetHotel") or "").strip().casefold()
    return f"{city}|{target_hotel}"


def normalize_hot_target(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    city = str(item.get("city") or "").strip()
    target_hotel = str(item.get("targetHotel") or item.get("hotelName") or item.get("target") or "").strip()
    if not city or not target_hotel:
        return None
    target_hint = public_target_hint(item.get("targetHint"))
    try:
        count = max(0, int(item.get("count") or 0))
    except (TypeError, ValueError):
        count = 0
    try:
        updated_at = float(item.get("updatedAt") or 0)
    except (TypeError, ValueError):
        updated_at = 0
    return {
        "city": city,
        "targetHotel": target_hotel,
        "targetType": infer_target_type(target_hint, item.get("targetType")),
        "targetHint": target_hint,
        "count": count,
        "updatedAt": updated_at,
        "heatLabel": str(item.get("heatLabel") or "").strip(),
    }


def hot_target_from_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    city = str(payload.get("city") or "").strip()
    target_hotel = str(payload.get("targetHotel") or payload.get("hotel") or "").strip()
    if not city or not target_hotel:
        return None
    target_hint = public_target_hint(payload.get("targetHint"))
    return {
        "city": city,
        "targetHotel": target_hotel,
        "targetType": infer_target_type(target_hint),
        "targetHint": target_hint,
    }


def load_hot_search_records() -> list[dict[str, Any]]:
    try:
        raw = json.loads(HOT_SEARCH_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = raw.get("targets") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return []
    cutoff = time.time() - HOT_SEARCH_TTL_SECONDS
    records: list[dict[str, Any]] = []
    for row in rows:
        record = normalize_hot_target(row)
        if not record:
            continue
        if record["updatedAt"] and record["updatedAt"] < cutoff:
            continue
        records.append(record)
    return records


def public_hot_target(item: dict[str, Any]) -> dict[str, Any]:
    count = int(item.get("count") or 0)
    heat_label = f"{count}次搜索" if count > 0 else str(item.get("heatLabel") or "热搜")
    return {
        "city": item["city"],
        "targetHotel": item["targetHotel"],
        "targetType": item.get("targetType") or "酒店/地区",
        "heatLabel": heat_label,
        "count": count,
        "targetHint": item.get("targetHint"),
    }


def current_hot_targets(limit: int = 10) -> list[dict[str, Any]]:
    records = load_hot_search_records()
    records.sort(key=lambda item: (item.get("count") or 0, item.get("updatedAt") or 0), reverse=True)
    defaults = [item for item in (normalize_hot_target(row) for row in DEFAULT_HOT_TARGETS) if item]
    combined: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in [*records, *defaults]:
        key = hot_target_key(item)
        if not key or key in seen:
            continue
        seen.add(key)
        combined.append(public_hot_target(item))
        if len(combined) >= limit:
            break
    return combined


def record_hot_search(payload: dict[str, Any]) -> None:
    target = hot_target_from_payload(payload)
    if not target:
        return
    now = time.time()
    records = load_hot_search_records()
    target_key = hot_target_key(target)
    updated = False
    for index, record in enumerate(records):
        if hot_target_key(record) != target_key:
            continue
        merged = {**record, **target}
        merged["count"] = int(record.get("count") or 0) + 1
        merged["updatedAt"] = now
        records[index] = merged
        updated = True
        break
    if not updated:
        target["count"] = 1
        target["updatedAt"] = now
        records.append(target)
    records.sort(key=lambda item: (item.get("count") or 0, item.get("updatedAt") or 0), reverse=True)
    try:
        HOT_SEARCH_PATH.parent.mkdir(parents=True, exist_ok=True)
        HOT_SEARCH_PATH.write_text(
            json.dumps({"targets": records[:MAX_HOT_SEARCH_RECORDS]}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass


def normalized_cache_payload(payload: dict[str, Any]) -> dict[str, Any]:
    provider_name = canonical_provider_name(payload.get("provider"))
    normalized = {
        "logicVersion": CACHE_LOGIC_VERSION,
        "provider": provider_name,
        "city": str(payload.get("city") or "").strip(),
        "targetHotel": str(payload.get("targetHotel") or payload.get("hotel") or "").strip(),
        "selectedDate": str(payload.get("selectedDate") or payload.get("checkIn") or "").strip(),
        "radiusKm": str(payload.get("radiusKm") or "3").strip(),
        "minStar": str(payload.get("minStar") or "4").strip(),
        "minPrice": str(payload.get("minPrice") or "").strip(),
        "maxPrice": str(payload.get("maxPrice") or "").strip(),
    }
    target_hint = normalized_target_hint(payload.get("targetHint"))
    if target_hint:
        normalized["targetHint"] = target_hint
    if provider_name == "local":
        normalized["dataVersion"] = local_data_version()
    return normalized


def cache_key(payload: dict[str, Any]) -> str:
    return json.dumps(normalized_cache_payload(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def cache_path(key: str) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return SEARCH_CACHE_DIR / f"{digest}.json"


def cache_ttl_seconds(provider_name: str) -> int:
    if provider_name == "local":
        return LOCAL_CACHE_TTL_SECONDS
    if provider_name == "tripcom":
        return TRIPCOM_CACHE_TTL_SECONDS
    return 0


def apply_sort_to_result(result: dict[str, Any], sort_by: str) -> dict[str, Any]:
    data = copy.deepcopy(result)
    data.setdefault("query", {})["sortBy"] = sort_by
    summary = data.setdefault("summary", {})
    query_city = str((data.get("query") or {}).get("city") or "").strip()

    def normalize_hotel(hotel: dict[str, Any]) -> dict[str, Any]:
        item = normalize_result_hotel_name(hotel, city=query_city)
        brand_payload = hotel_brand_payload(item)
        if brand_payload:
            item.update(
                {
                    "brand": brand_payload.get("brand") or item.get("brand") or "独立酒店",
                    "brandLabel": brand_payload.get("brandLabel") or item.get("brandLabel") or "独立酒店",
                    "group": brand_payload.get("group") or item.get("group") or "",
                    "groupLabel": brand_payload.get("groupLabel") or item.get("groupLabel") or "",
                    "brandRank": brand_payload.get("brandRank") or 99,
                    "brandTier": brand_payload.get("brandTier") or item.get("brandTier") or "",
                    "isRecommendedBrand": bool(brand_payload.get("brandRank") and brand_payload.get("brandRank") != 99),
                }
            )
        return item

    all_hotels = [
        normalize_hotel(hotel)
        for hotel in list(data.get("allHotels") or [])
    ]
    if all_hotels:
        deal_hotels = [hotel for hotel in all_hotels if hotel.get("isDeal")]
        recommended_hotels = [hotel for hotel in all_hotels if hotel.get("isRecommendedBrand")]
    else:
        deal_hotels = [normalize_hotel(hotel) for hotel in list(data.get("dealHotels") or [])]
        recommended_hotels = [normalize_hotel(hotel) for hotel in list(data.get("recommendedHotels") or [])]
    preserve_order = bool(summary.get("sortDeferred"))
    data["allHotels"] = all_hotels if preserve_order else sort_hotels(all_hotels, sort_by)
    data["dealHotels"] = deal_hotels if preserve_order else sort_hotels(deal_hotels, sort_by)
    recommended_sort = sort_by if sort_by != "discount" else "recommendation"
    data["recommendedHotels"] = (
        recommended_hotels
        if preserve_order
        else sort_recommended_hotels(recommended_hotels, recommended_sort)
    )
    summary["candidateCount"] = len(data["allHotels"])
    summary["pricedHotelCount"] = sum(1 for hotel in data["allHotels"] if hotel.get("currentPrice") not in (None, ""))
    summary["unpricedCandidateCount"] = sum(1 for hotel in data["allHotels"] if hotel.get("currentPrice") in (None, ""))
    summary["dealCount"] = len(data["dealHotels"])
    summary["recommendedCount"] = len(data["recommendedHotels"])
    return data


def cached_search_result(key: str, provider_name: str, sort_by: str) -> dict[str, Any] | None:
    ttl_seconds = cache_ttl_seconds(provider_name)
    if ttl_seconds <= 0:
        return None
    cached = SEARCH_CACHE.get(key)
    expires_at: float
    updated_at: float
    result: dict[str, Any]
    cache_source = "memory"
    if cached:
        expires_at, result, updated_at = cached
    else:
        record: dict[str, Any] | None = None
        if MYSQL_SEARCH_CACHE is not None:
            record = MYSQL_SEARCH_CACHE.get(key, provider_name)
            if record:
                cache_source = "mysql"
        if record is None:
            try:
                record = json.loads(cache_path(key).read_text(encoding="utf-8"))
                cache_source = "disk"
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                return None
        try:
            expires_at = float(record.get("expiresAt") or 0)
        except (TypeError, ValueError):
            return None
        try:
            updated_at = float(record.get("updatedAt") or record.get("createdAt") or 0)
        except (TypeError, ValueError):
            updated_at = 0
        result = record.get("result") if isinstance(record.get("result"), dict) else {}
        if not result:
            return None
        if not cacheable_search_result(result):
            return None
        SEARCH_CACHE[key] = (expires_at, copy.deepcopy(result), updated_at)
    if expires_at < time.time():
        SEARCH_CACHE.pop(key, None)
        try:
            cache_path(key).unlink()
        except OSError:
            pass
        return None
    cache_age_seconds = max(0, round(time.time() - updated_at, 1)) if updated_at else None
    data = apply_cached_hotel_names_to_result(result, provider_name)
    data = apply_sort_to_result(data, sort_by)
    data.setdefault("summary", {})["cacheHit"] = True
    data["summary"]["cacheSource"] = cache_source
    data["summary"]["elapsedMs"] = 0
    data["summary"]["cacheUpdatedAt"] = updated_at or None
    data["summary"]["cacheAgeSeconds"] = cache_age_seconds
    return data


def result_candidate_count(result: dict[str, Any]) -> int:
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    try:
        summary_count = int(summary.get("candidateCount") or 0)
    except (TypeError, ValueError):
        summary_count = 0
    list_count = len(result.get("allHotels") or []) if isinstance(result.get("allHotels"), list) else 0
    return max(summary_count, list_count)


def result_has_resumable_target(result: dict[str, Any]) -> bool:
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    if summary.get("partial") is not True:
        return False
    target = result.get("targetHotel") if isinstance(result.get("targetHotel"), dict) else {}
    return bool(target.get("hotelId") and target.get("searchType"))


def cacheable_search_result(result: dict[str, Any]) -> bool:
    if result_candidate_count(result) <= 0:
        return result_has_resumable_target(result)
    return True


def cached_target_hint(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    return public_target_hint(result.get("targetHotel"))


def payload_with_cached_target_hint(payload: dict[str, Any], cached_result: dict[str, Any] | None) -> dict[str, Any]:
    if payload.get("targetHint"):
        return payload
    hint = cached_target_hint(cached_result)
    if not hint:
        return payload
    return {**payload, "targetHint": hint}


def retriable_provider_error(exc: Exception) -> bool:
    message = str(exc)
    non_retriable_markers = (
        "没有匹配到目标酒店",
        "请从输入框下方建议里选择",
        "必须是",
        "不能为空",
    )
    return not any(marker in message for marker in non_retriable_markers)


def cached_result_needs_refresh(data: dict[str, Any], provider_name: str) -> bool:
    if provider_name != "tripcom" or TRIPCOM_REFRESH_AFTER_SECONDS <= 0:
        return False
    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    if unique_hotels_needing_name_verification(data):
        return True
    if summary.get("partial"):
        return True
    age = summary.get("cacheAgeSeconds")
    try:
        return float(age) >= TRIPCOM_REFRESH_AFTER_SECONDS
    except (TypeError, ValueError):
        return True


def force_refresh_requested(payload: dict[str, Any]) -> bool:
    for key in ("forceRefresh", "bypassCache", "noCache", "refresh"):
        if key in payload:
            return parse_bool(payload.get(key), default=False)
    return False


def load_hotel_name_cache() -> dict[str, dict[str, Any]]:
    global HOTEL_NAME_CACHE_LOADED
    with HOTEL_NAME_CACHE_LOCK:
        if HOTEL_NAME_CACHE_LOADED:
            return copy.deepcopy(HOTEL_NAME_MEMORY_CACHE)
        try:
            payload = json.loads(HOTEL_NAME_CACHE_PATH.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if isinstance(payload, dict):
            HOTEL_NAME_MEMORY_CACHE["byHotelId"] = dict(payload.get("byHotelId") or {})
            HOTEL_NAME_MEMORY_CACHE["byNameKey"] = dict(payload.get("byNameKey") or {})
        HOTEL_NAME_CACHE_LOADED = True
        return copy.deepcopy(HOTEL_NAME_MEMORY_CACHE)


def persist_hotel_name_cache_unlocked() -> None:
    HOTEL_NAME_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    HOTEL_NAME_CACHE_PATH.write_text(
        json.dumps(HOTEL_NAME_MEMORY_CACHE, ensure_ascii=False),
        encoding="utf-8",
    )


def hotel_name_cache_provider(provider_name: str) -> str:
    return canonical_provider_name(provider_name or "tripcom")


def strip_name_status_text(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[（(]\s*中文名(?:正在核验中|待核验)\.*\s*[）)]", "", text)
    return text.strip()


def hotel_original_name_key(hotel: dict[str, Any], payload: dict[str, Any] | None = None) -> str:
    candidates = [
        (payload or {}).get("hotelOriginalName"),
        hotel.get("hotelOriginalName"),
        (payload or {}).get("hotelName"),
        hotel.get("hotelNameSimplified"),
        hotel.get("hotelName"),
    ]
    for value in candidates:
        text = strip_name_status_text(value)
        if not text:
            continue
        key = domestic_hotel_name_key(text)
        if key and "中文名正在核验中" not in key and "中文名待核验" not in key:
            return hashlib.sha256(key.encode("utf-8")).hexdigest()
    return ""


def hotel_name_payload_is_cacheable(payload: dict[str, Any]) -> bool:
    name = simplify_chinese_text(payload.get("hotelNameSimplified") or payload.get("hotelName") or "").strip()
    source = str(payload.get("hotelNameSource") or payload.get("source") or "")
    if not name or not contains_chinese_text(name):
        return False
    if "中文名正在核验中" in name or "中文名待核验" in name:
        return False
    blocked_source_markers = ("本地中文名兜底", "正在核验", "待核验", "未匹配到标准中文名")
    if any(marker in source for marker in blocked_source_markers):
        return False
    if re.fullmatch(r".{0,12}(星级酒店|携程酒店\d*)", name):
        return False
    return True


def normalized_name_cache_payload(payload: dict[str, Any]) -> dict[str, str]:
    name = simplify_chinese_text(payload.get("hotelNameSimplified") or payload.get("hotelName") or "").strip()
    original = str(payload.get("hotelOriginalName") or "").strip()
    return {
        "hotelName": name,
        "hotelOriginalName": original if original and original != name else "",
        "hotelNameSimplified": name,
        "hotelNameSource": str(payload.get("hotelNameSource") or payload.get("source") or "中文名缓存").strip(),
    }


def hotel_name_cache_record_payload(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None
    try:
        expires_at = float(record.get("expiresAt") or 0)
    except (TypeError, ValueError):
        return None
    if expires_at < time.time():
        return None
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else None
    if payload and hotel_name_payload_is_cacheable(payload):
        return payload
    return None


def cached_hotel_name_payload(hotel: dict[str, Any], provider_name: str) -> dict[str, Any] | None:
    provider = hotel_name_cache_provider(provider_name)
    hotel_id = str(hotel.get("hotelId") or "").strip()
    name_key = hotel_original_name_key(hotel)
    cache = load_hotel_name_cache()
    now = time.time()
    keys = []
    if hotel_id:
        keys.append(("byHotelId", f"{provider}:{hotel_id}"))
    if name_key:
        keys.append(("byNameKey", f"{provider}:{name_key}"))
    for group, key in keys:
        payload = hotel_name_cache_record_payload(cache.get(group, {}).get(key))
        if payload:
            return payload

    mysql_payload = None
    if MYSQL_HOTEL_NAME_CACHE is not None:
        mysql_payload = MYSQL_HOTEL_NAME_CACHE.get(provider, hotel_id=hotel_id, original_name_key=name_key)
    if mysql_payload and hotel_name_payload_is_cacheable(mysql_payload):
        cache_hotel_name_payload(hotel, mysql_payload, provider)
        return mysql_payload

    with HOTEL_NAME_CACHE_LOCK:
        changed = False
        for group, key in keys:
            record = HOTEL_NAME_MEMORY_CACHE.get(group, {}).get(key)
            if isinstance(record, dict) and float(record.get("expiresAt") or 0) < now:
                HOTEL_NAME_MEMORY_CACHE[group].pop(key, None)
                changed = True
        if changed:
            try:
                persist_hotel_name_cache_unlocked()
            except OSError:
                pass
    return None


def cache_hotel_name_payload(hotel: dict[str, Any], payload: dict[str, Any], provider_name: str) -> None:
    normalized = normalized_name_cache_payload(payload)
    if not hotel_name_payload_is_cacheable(normalized):
        return
    provider = hotel_name_cache_provider(provider_name)
    hotel_id = str(hotel.get("hotelId") or "").strip()
    name_key = hotel_original_name_key(hotel, normalized)
    if not hotel_id and not name_key:
        return
    original_name = (
        strip_name_status_text(normalized.get("hotelOriginalName"))
        or strip_name_status_text(hotel.get("hotelOriginalName"))
        or strip_name_status_text(hotel.get("hotelName"))
    )
    now = time.time()
    expires_at = now + HOTEL_NAME_CACHE_TTL_SECONDS
    record = {
        "provider": provider,
        "hotelId": hotel_id,
        "originalNameKey": name_key,
        "originalName": original_name,
        "payload": normalized,
        "updatedAt": now,
        "expiresAt": expires_at,
    }
    with HOTEL_NAME_CACHE_LOCK:
        load_hotel_name_cache()
        if hotel_id:
            HOTEL_NAME_MEMORY_CACHE.setdefault("byHotelId", {})[f"{provider}:{hotel_id}"] = record
        if name_key:
            HOTEL_NAME_MEMORY_CACHE.setdefault("byNameKey", {})[f"{provider}:{name_key}"] = record
        try:
            persist_hotel_name_cache_unlocked()
        except OSError:
            pass
    if MYSQL_HOTEL_NAME_CACHE is not None:
        MYSQL_HOTEL_NAME_CACHE.store(
            provider,
            hotel_id=hotel_id,
            original_name=original_name,
            original_name_key=name_key,
            payload=normalized,
            expires_at=expires_at,
        )


def remember_result_hotel_names(result: dict[str, Any], provider_name: str) -> None:
    for list_name in ("allHotels", "dealHotels", "recommendedHotels"):
        for hotel in result.get(list_name) or []:
            if not isinstance(hotel, dict):
                continue
            payload = {
                "hotelName": hotel.get("hotelName"),
                "hotelOriginalName": hotel.get("hotelOriginalName"),
                "hotelNameSimplified": hotel.get("hotelNameSimplified"),
                "hotelNameSource": hotel.get("hotelNameSource"),
            }
            cache_hotel_name_payload(hotel, payload, provider_name)


def apply_cached_hotel_names_to_result(result: dict[str, Any], provider_name: str) -> dict[str, Any]:
    data = copy.deepcopy(result)
    query_city = str((data.get("query") or {}).get("city") or "").strip()
    payloads: dict[str, dict[str, Any]] = {}
    for list_name in ("allHotels", "dealHotels", "recommendedHotels"):
        for hotel in data.get(list_name) or []:
            if not isinstance(hotel, dict):
                continue
            payload = cached_hotel_name_payload(hotel, provider_name)
            if not payload:
                continue
            hotel_id = str(hotel.get("hotelId") or "").strip()
            key = hotel_merge_key(hotel)
            if hotel_id:
                payloads[hotel_id] = payload
            if key:
                payloads[key] = payload
    if not payloads:
        return data
    updated = apply_name_payloads_to_result(data, payloads)
    updated.setdefault("summary", {})["nameCacheHit"] = True
    updated["summary"]["nameCacheAppliedCount"] = len(payloads)
    for list_name in ("allHotels", "dealHotels", "recommendedHotels"):
        updated[list_name] = [
            normalize_result_hotel_name(hotel, city=query_city)
            for hotel in list(updated.get(list_name) or [])
            if isinstance(hotel, dict)
        ]
    return updated


def hotel_merge_key(hotel: dict[str, Any]) -> str:
    hotel_id = str(hotel.get("hotelId") or "").strip()
    if hotel_id:
        return f"id:{hotel_id}"
    name = str(hotel.get("hotelNameSimplified") or hotel.get("hotelName") or hotel.get("hotelOriginalName") or "").strip()
    return f"name:{name.casefold()}" if name else ""


def value_missing(value: Any) -> bool:
    return value in (None, "")


def merge_compare_prices_with_cached(item: dict[str, Any], cached: dict[str, Any]) -> bool:
    fresh_dates = [str(date) for date in (item.get("compareDates") or [])]
    cached_dates = [str(date) for date in (cached.get("compareDates") or [])]
    fresh_prices = list(item.get("comparePrices") or [])
    cached_prices = list(cached.get("comparePrices") or [])
    if not fresh_dates or not cached_dates or not cached_prices:
        return False
    cached_by_date = {
        date: cached_prices[index]
        for index, date in enumerate(cached_dates)
        if index < len(cached_prices) and cached_prices[index] not in (None, "")
    }
    changed = False
    merged_prices: list[Any] = []
    for index, date in enumerate(fresh_dates):
        price = fresh_prices[index] if index < len(fresh_prices) else None
        cached_price = cached_by_date.get(date)
        if value_missing(price) and cached_price not in (None, ""):
            merged_prices.append(cached_price)
            changed = True
        else:
            merged_prices.append(price)
    if changed:
        item["comparePrices"] = merged_prices
    return changed


def refresh_score_after_cached_price_merge(item: dict[str, Any]) -> None:
    current_price = item.get("currentPrice")
    compare_prices = item.get("comparePrices") or []
    if value_missing(current_price) or not isinstance(compare_prices, list):
        return
    item.update(calculateDealScore(current_price, compare_prices))
    item["pricePending"] = False


def merge_fresh_hotel_with_cached_price_fields(fresh: dict[str, Any], cached: dict[str, Any]) -> dict[str, Any]:
    item = copy.deepcopy(fresh)
    carried_price = False
    if value_missing(item.get("currentPrice")) and not value_missing(cached.get("currentPrice")):
        for key in (
            "currentPrice",
            "priceIncludesTax",
            "priceSource",
            "averageComparePrice",
            "referencePrice",
            "referencePriceLabel",
            "averageDiscountAmount",
            "maxComparePrice",
            "maxSingleDayDiscountAmount",
            "dealBasis",
            "discountAmount",
            "discountPercent",
            "isDeal",
        ):
            if key in cached:
                item[key] = copy.deepcopy(cached.get(key))
        if value_missing(item.get("tripUrl")) and not value_missing(cached.get("tripUrl")):
            item["tripUrl"] = cached.get("tripUrl")
        item["pricePending"] = False
        carried_price = True
    if merge_compare_prices_with_cached(item, cached):
        carried_price = True
        refresh_score_after_cached_price_merge(item)
    if carried_price:
        item["priceCarriedFromCache"] = True
    return item


def merge_hotel_lists_with_fresh_priority(
    fresh_hotels: list[dict[str, Any]],
    cached_hotels: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    fresh_keys: set[str] = set()
    corrected_count = 0

    cached_by_key = {hotel_merge_key(hotel): hotel for hotel in cached_hotels if hotel_merge_key(hotel)}
    cached_keys = set(cached_by_key)
    for hotel in fresh_hotels:
        item = copy.deepcopy(hotel)
        key = hotel_merge_key(item)
        if key and key in seen:
            continue
        if key and key in cached_keys:
            corrected_count += 1
            item = merge_fresh_hotel_with_cached_price_fields(item, cached_by_key[key])
        if key:
            seen.add(key)
            fresh_keys.add(key)
        merged.append(item)

    carried_count = 0
    for hotel in cached_hotels:
        key = hotel_merge_key(hotel)
        if not key or key in seen:
            continue
        item = copy.deepcopy(hotel)
        item["carriedFromCache"] = True
        seen.add(key)
        merged.append(item)
        carried_count += 1

    return merged, carried_count, corrected_count


def merge_search_result_with_cached(fresh_result: dict[str, Any], cached_result: dict[str, Any] | None) -> dict[str, Any]:
    if not cached_result or not cacheable_search_result(cached_result):
        return fresh_result
    data = copy.deepcopy(fresh_result)
    cached_hotels = list(cached_result.get("allHotels") or [])
    if not cached_hotels:
        return data
    all_hotels, carried_count, corrected_count = merge_hotel_lists_with_fresh_priority(
        list(data.get("allHotels") or []),
        cached_hotels,
    )
    sort_by = str((data.get("query") or {}).get("sortBy") or "discount")
    summary = data.setdefault("summary", {})
    preserve_order = bool(summary.get("sortDeferred"))
    data["allHotels"] = all_hotels if preserve_order else sort_hotels(all_hotels, sort_by)
    deal_hotels = [hotel for hotel in all_hotels if hotel.get("isDeal")]
    data["dealHotels"] = deal_hotels if preserve_order else sort_hotels(deal_hotels, sort_by)
    recommended_sort = sort_by if sort_by != "discount" else "recommendation"
    recommended_hotels = [hotel for hotel in all_hotels if hotel.get("isRecommendedBrand")]
    data["recommendedHotels"] = (
        recommended_hotels
        if preserve_order
        else sort_recommended_hotels(recommended_hotels, recommended_sort)
    )
    summary["candidateCount"] = len(data["allHotels"])
    summary["pricedHotelCount"] = sum(1 for hotel in data["allHotels"] if hotel.get("currentPrice") not in (None, ""))
    summary["unpricedCandidateCount"] = sum(1 for hotel in data["allHotels"] if hotel.get("currentPrice") in (None, ""))
    summary["dealCount"] = len(data["dealHotels"])
    summary["recommendedCount"] = len(data["recommendedHotels"])
    summary["mergedFromCache"] = True
    summary["cacheCarriedHotelCount"] = carried_count
    summary["cacheCorrectedHotelCount"] = corrected_count
    summary["cacheMergedHotelCount"] = carried_count + corrected_count
    return data


def unique_hotels_needing_name_verification(result: dict[str, Any]) -> list[dict[str, Any]]:
    hotels: list[dict[str, Any]] = []
    seen: set[str] = set()
    for list_name in ("allHotels", "dealHotels", "recommendedHotels"):
        for hotel in result.get(list_name) or []:
            if not isinstance(hotel, dict) or not hotel_name_needs_verification(hotel):
                continue
            key = hotel_merge_key(hotel) or f"object:{id(hotel)}"
            if key in seen:
                continue
            seen.add(key)
            hotels.append(copy.deepcopy(hotel))
    return hotels


def apply_name_payloads_to_result(
    result: dict[str, Any],
    payloads: dict[str, dict[str, Any]],
    *,
    finalize_remaining: bool = False,
) -> dict[str, Any]:
    data = copy.deepcopy(result)
    query_city = str((data.get("query") or {}).get("city") or "").strip()

    def update_hotel(hotel: dict[str, Any]) -> dict[str, Any]:
        key = hotel_merge_key(hotel)
        hotel_id = str(hotel.get("hotelId") or "").strip()
        payload = payloads.get(hotel_id) or payloads.get(key)
        if payload:
            return apply_verified_hotel_name_payload(hotel, payload, city=query_city)
        if finalize_remaining and hotel_name_needs_verification(hotel):
            return finalize_pending_hotel_name(hotel, city=query_city)
        return normalize_result_hotel_name(hotel, city=query_city)

    for list_name in ("allHotels", "dealHotels", "recommendedHotels"):
        data[list_name] = [update_hotel(hotel) for hotel in list(data.get(list_name) or []) if isinstance(hotel, dict)]

    sort_by = str((data.get("query") or {}).get("sortBy") or "discount")
    return apply_sort_to_result(data, sort_by)


def verify_result_hotel_names(
    provider: Any,
    result: dict[str, Any],
    selected_date: str,
    *,
    progress_callback: Any | None = None,
    provider_name: str = "tripcom",
) -> dict[str, Any]:
    result = apply_cached_hotel_names_to_result(result, provider_name)
    pending_hotels = unique_hotels_needing_name_verification(result)
    if not pending_hotels:
        return apply_name_payloads_to_result(result, {}, finalize_remaining=True)

    verified_payloads: dict[str, dict[str, Any]] = {}
    pending_by_id = {str(hotel.get("hotelId") or ""): hotel for hotel in pending_hotels if hotel.get("hotelId")}
    verifier = getattr(provider, "verify_hotel_names", None)
    if callable(verifier):
        def handle_progress(progress_info: dict[str, Any]) -> None:
            hotel_id = str(progress_info.get("hotelId") or "").strip()
            payload = progress_info.get("payload") if isinstance(progress_info.get("payload"), dict) else None
            if hotel_id and payload:
                cache_hotel_name_payload(pending_by_id.get(hotel_id, {}), payload, provider_name)
                verified_payloads[hotel_id] = payload
            if progress_callback:
                progress_callback(
                    apply_name_payloads_to_result(result, verified_payloads),
                    {
                        **progress_info,
                        "resolvedCount": len(verified_payloads),
                        "total": len(pending_hotels),
                    },
                )

        try:
            resolved = verifier(pending_hotels, selected_date, progress_callback=handle_progress)
            if isinstance(resolved, dict):
                for key, payload in resolved.items():
                    if isinstance(payload, dict):
                        cache_hotel_name_payload(pending_by_id.get(str(key), {}), payload, provider_name)
                        verified_payloads[str(key)] = payload
        except Exception as exc:
            if progress_callback:
                progress_callback(
                    apply_name_payloads_to_result(result, verified_payloads),
                    {
                        "phase": "error",
                        "error": str(exc),
                        "resolvedCount": len(verified_payloads),
                        "total": len(pending_hotels),
                    },
                )

    final_result = apply_name_payloads_to_result(result, verified_payloads, finalize_remaining=True)
    final_summary = final_result.setdefault("summary", {})
    remaining = unique_hotels_needing_name_verification(final_result)
    final_summary["nameVerificationComplete"] = True
    final_summary["nameVerificationTotal"] = len(pending_hotels)
    final_summary["nameVerificationResolvedCount"] = len(verified_payloads)
    final_summary["nameVerificationFallbackCount"] = max(len(pending_hotels) - len(verified_payloads), 0)
    final_summary["nameVerificationRemainingCount"] = len(remaining)
    return final_result


class ProgressiveNameVerifier:
    def __init__(
        self,
        *,
        provider: Any,
        selected_date: str,
        job_id: str,
        cache_key_value: str,
        provider_name: str,
        sort_by: str,
    ) -> None:
        self.provider = provider
        self.selected_date = selected_date
        self.job_id = job_id
        self.cache_key_value = cache_key_value
        self.provider_name = provider_name
        self.sort_by = sort_by
        self._lock = threading.Lock()
        self._payloads: dict[str, dict[str, Any]] = {}
        self._queue: list[dict[str, Any]] = []
        self._seen_keys: set[str] = set()
        self._attempted_keys: set[str] = set()
        self._thread: threading.Thread | None = None
        self._verifier = getattr(provider, "verify_hotel_names", None)

    def enabled(self) -> bool:
        return callable(self._verifier)

    def payloads(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._payloads)

    def total_seen(self) -> int:
        with self._lock:
            return len(self._seen_keys)

    def resolved_count(self) -> int:
        with self._lock:
            return len(self._payloads)

    def apply_to_result(self, result: dict[str, Any], *, finalize_remaining: bool = False) -> dict[str, Any]:
        result = apply_cached_hotel_names_to_result(result, self.provider_name)
        payloads = self.payloads()
        if not payloads and not finalize_remaining:
            return result
        return apply_name_payloads_to_result(result, payloads, finalize_remaining=finalize_remaining)

    def enqueue_from_result(self, result: dict[str, Any]) -> None:
        if not self.enabled():
            return
        result = apply_cached_hotel_names_to_result(result, self.provider_name)
        pending_hotels = unique_hotels_needing_name_verification(result)
        if not pending_hotels:
            return
        should_start = False
        with self._lock:
            for hotel in pending_hotels:
                hotel_id = str(hotel.get("hotelId") or "").strip()
                if not hotel_id:
                    continue
                key = hotel_merge_key(hotel)
                if not key or key in self._seen_keys:
                    continue
                self._seen_keys.add(key)
                self._queue.append(copy.deepcopy(hotel))
                should_start = True
            if should_start and (self._thread is None or not self._thread.is_alive()):
                self._thread = threading.Thread(
                    target=self._worker,
                    name=f"hotel-name-verifier-{self.job_id}",
                    daemon=True,
                )
                self._thread.start()
        if should_start:
            self.publish_current_partial(phase="queued")

    def wait_until_idle(self) -> None:
        while True:
            with self._lock:
                thread = self._thread
            if thread is None:
                return
            thread.join(timeout=0.2)

    def publish_current_partial(self, *, phase: str = "progress") -> None:
        payloads = self.payloads()
        with self._lock:
            total = len(self._seen_keys)
            completed = len(self._attempted_keys)
            resolved = len(self._payloads)
            active = bool(self._queue) or (self._thread is not None and self._thread.is_alive())
        with SEARCH_JOB_LOCK:
            job = SEARCH_JOBS.get(self.job_id)
            if not job:
                return
            partial_result = copy.deepcopy(job.get("partialResult"))
            progress = copy.deepcopy(job.get("progress"))
        if not isinstance(partial_result, dict):
            return
        updated = apply_name_payloads_to_result(partial_result, payloads)
        summary = updated.setdefault("summary", {})
        summary["nameVerificationActive"] = active
        summary["nameVerificationTotal"] = total
        summary["nameVerificationCompletedCount"] = completed
        summary["nameVerificationResolvedCount"] = resolved
        summary["nameVerificationPhase"] = phase
        if progress:
            summary["progress"] = progress
            summary["jobStatus"] = progress.get("stage") or summary.get("jobStatus") or "pricing"
        update_search_job(self.job_id, partialResult=updated)
        if self.cache_key_value and cacheable_search_result(updated):
            remember_search_result(self.cache_key_value, self.provider_name, updated)

    def _worker(self) -> None:
        while True:
            with self._lock:
                if not self._queue:
                    self._thread = None
                    return
                hotel = self._queue.pop(0)
            hotel_key = hotel_merge_key(hotel)
            self.publish_current_partial(phase="start")

            def handle_progress(progress_info: dict[str, Any]) -> None:
                hotel_id = str(progress_info.get("hotelId") or "").strip()
                payload = progress_info.get("payload") if isinstance(progress_info.get("payload"), dict) else None
                if hotel_id and payload:
                    cache_hotel_name_payload(hotel, payload, self.provider_name)
                    with self._lock:
                        self._payloads[hotel_id] = payload
                self.publish_current_partial(phase=str(progress_info.get("phase") or "progress"))

            try:
                kwargs: dict[str, Any] = {"progress_callback": handle_progress}
                signature = inspect.signature(self._verifier) if callable(self._verifier) else None
                if signature and "lightweight_only" in signature.parameters:
                    kwargs["lightweight_only"] = True
                resolved = self._verifier([hotel], self.selected_date, **kwargs) if callable(self._verifier) else {}
                if isinstance(resolved, dict):
                    with self._lock:
                        for key, payload in resolved.items():
                            if isinstance(payload, dict):
                                cache_hotel_name_payload(hotel, payload, self.provider_name)
                                self._payloads[str(key)] = payload
            except Exception:
                pass
            finally:
                with self._lock:
                    if hotel_key:
                        self._attempted_keys.add(hotel_key)
                self.publish_current_partial(phase="complete")


def remember_search_result(key: str, provider_name: str, result: dict[str, Any]) -> None:
    ttl_seconds = cache_ttl_seconds(provider_name)
    if ttl_seconds <= 0:
        return
    if not cacheable_search_result(result):
        return
    remember_result_hotel_names(result, provider_name)
    now = time.time()
    expires_at = now + ttl_seconds
    if len(SEARCH_CACHE) >= MAX_SEARCH_CACHE_ITEMS:
        oldest_key = min(SEARCH_CACHE, key=lambda item: SEARCH_CACHE[item][0])
        SEARCH_CACHE.pop(oldest_key, None)
    stored = copy.deepcopy(result)
    stored = apply_sort_to_result(stored, str((stored.get("query") or {}).get("sortBy") or "discount"))
    stored.setdefault("summary", {}).pop("elapsedMs", None)
    stored["summary"].pop("cacheHit", None)
    stored["summary"].pop("cacheSource", None)
    stored["summary"].pop("cacheAgeSeconds", None)
    stored["summary"].pop("cacheUpdatedAt", None)
    stored["summary"].pop("refreshing", None)
    stored["summary"].pop("refreshJobId", None)
    stored["summary"].pop("jobId", None)
    stored["summary"].pop("jobStatus", None)
    stored["summary"].pop("progress", None)
    SEARCH_CACHE[key] = (expires_at, stored, now)
    SEARCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "version": 1,
        "key": key,
        "provider": provider_name,
        "createdAt": now,
        "updatedAt": now,
        "expiresAt": expires_at,
        "result": stored,
    }
    cache_path(key).write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    if MYSQL_SEARCH_CACHE is not None:
        MYSQL_SEARCH_CACHE.store(key, provider_name, stored, expires_at)


def search_parameters(payload: dict[str, Any], provider_name: str | None = None) -> dict[str, Any]:
    provider_name = provider_name or canonical_provider_name(payload.get("provider"))
    target_hint = parse_target_hint(payload.get("targetHint")) if provider_name == "tripcom" else None
    return {
        "city": str(payload.get("city") or "").strip(),
        "target_hotel_name": str(payload.get("targetHotel") or payload.get("hotel") or "").strip(),
        "selected_date": str(payload.get("selectedDate") or payload.get("checkIn") or "").strip(),
        "radius_km": parse_float(payload.get("radiusKm"), "搜索半径", 3),
        "min_star": parse_float(payload.get("minStar"), "酒店星级", 4),
        "min_price": parse_optional_int(payload.get("minPrice"), "最低价格"),
        "max_price": parse_optional_int(payload.get("maxPrice"), "最高价格"),
        "sort_by": str(payload.get("sortBy") or "discount"),
        "target_hint": target_hint,
    }


def run_search_payload(payload: dict[str, Any], provider_name: str, *, quick: bool = False) -> dict[str, Any]:
    params = search_parameters(payload, provider_name)
    provider = local_provider() if provider_name == "local" else provider_from_name(APP_DIR, provider_name)
    search_fn = search_current_prices if quick else search_deals
    return search_fn(provider=provider, **params)


def job_progress(stage: str, message: str, started_at: float | None = None) -> dict[str, Any]:
    now = time.time()
    return {
        "stage": stage,
        "message": message,
        "updatedAt": now,
        "elapsedMs": round((now - float(started_at or now)) * 1000, 1),
    }


def update_search_job(job_id: str, **updates: Any) -> None:
    with SEARCH_JOB_LOCK:
        job = SEARCH_JOBS.get(job_id)
        if job is not None:
            job.update(updates)


def queued_search_result(payload: dict[str, Any], provider_name: str, job_id: str, started_at: float) -> dict[str, Any]:
    params = search_parameters(payload, provider_name)
    compare_info = get_compare_date_info(params["selected_date"])
    target_hint = public_target_hint(payload.get("targetHint")) if provider_name == "tripcom" else None
    progress = job_progress(
        "queued",
        "搜索任务已启动，正在连接 Trip.com 并匹配目标酒店/位置。",
        started_at,
    )
    return {
        "query": {
            "city": params["city"],
            "targetHotel": params["target_hotel_name"],
            "selectedDate": params["selected_date"],
            "radiusKm": params["radius_km"],
            "effectiveRadiusKm": params["radius_km"],
            "minStar": params["min_star"],
            "minPrice": params["min_price"],
            "maxPrice": params["max_price"],
            "sortBy": params["sort_by"],
            "compareMode": compare_info["compareMode"],
            "holiday": compare_info["holiday"],
        },
        "targetHotel": target_hint
        or {
            "city": params["city"],
            "hotelName": params["target_hotel_name"],
            "matchedBy": "等待 Trip.com 匹配",
        },
        "compareDates": compare_info["compareDates"],
        "allHotels": [],
        "dealHotels": [],
        "recommendedHotels": [],
        "summary": {
            "candidateCount": 0,
            "pricedHotelCount": 0,
            "dealCount": 0,
            "recommendedCount": 0,
            "source": TripComProvider.source_name if provider_name == "tripcom" else provider_name,
            "compareMode": compare_info["compareMode"],
            "holiday": compare_info["holiday"],
            "holidayName": (compare_info["holiday"] or {}).get("name"),
            "requestedRadiusKm": params["radius_km"],
            "effectiveRadiusKm": params["radius_km"],
            "attemptedRadii": [params["radius_km"]],
            "radiusExpanded": False,
            "partial": True,
            "jobId": job_id,
            "jobStatus": "queued",
            "cacheHit": False,
            "cacheSource": "live-progress",
            "elapsedMs": 0,
            "progress": progress,
        },
    }


def job_id_for_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def start_background_search_job(
    payload: dict[str, Any],
    key: str,
    provider_name: str,
    *,
    force_refresh: bool = False,
    base_cached_result: dict[str, Any] | None = None,
) -> str:
    job_id = job_id_for_key(key)
    effective_payload = payload_with_cached_target_hint(payload, base_cached_result)
    with SEARCH_JOB_LOCK:
        existing = SEARCH_JOBS.get(job_id)
        if existing and existing.get("status") == "running":
            return job_id
        if existing and existing.get("status") == "complete" and not force_refresh:
            return job_id
        started_at = time.time()
        SEARCH_JOBS[job_id] = {
            "id": job_id,
            "status": "running",
            "key": key,
            "provider": provider_name,
            "payload": copy.deepcopy(payload),
            "effectivePayload": copy.deepcopy(effective_payload),
            "startedAt": started_at,
            "progress": job_progress(
                "queued",
                "搜索任务已启动，正在排队连接 Trip.com。",
                started_at,
            ),
        }

    def worker() -> None:
        try:
            params = search_parameters(effective_payload, provider_name)
            provider = local_provider() if provider_name == "local" else provider_from_name(APP_DIR, provider_name)
            name_verifier = ProgressiveNameVerifier(
                provider=provider,
                selected_date=params["selected_date"],
                job_id=job_id,
                cache_key_value=key,
                provider_name=provider_name,
                sort_by=params["sort_by"],
            )

            def publish_partial_result(partial_result: dict[str, Any], *, cache_source: str, stage: str, message: str) -> None:
                with SEARCH_JOB_LOCK:
                    previous_partial = copy.deepcopy((SEARCH_JOBS.get(job_id) or {}).get("partialResult"))
                partial_result = merge_search_result_with_cached(partial_result, previous_partial)
                partial_result = merge_search_result_with_cached(partial_result, base_cached_result)
                partial_result = name_verifier.apply_to_result(partial_result)
                partial_summary = partial_result.setdefault("summary", {})
                partial_summary["cacheHit"] = False
                partial_summary["cacheSource"] = cache_source
                partial_summary["partial"] = True
                partial_summary["jobId"] = job_id
                partial_summary["jobStatus"] = "pricing"
                progress = job_progress(stage, message, SEARCH_JOBS[job_id]["startedAt"])
                partial_summary["progress"] = progress
                update_search_job(job_id, partialResult=partial_result, progress=progress)
                if cacheable_search_result(partial_result):
                    remember_search_result(key, provider_name, partial_result)
                name_verifier.enqueue_from_result(partial_result)

            def publish_progress_only(stage: str, message: str, *, price_progress: dict[str, Any] | None = None) -> None:
                progress = job_progress(stage, message, SEARCH_JOBS[job_id]["startedAt"])
                with SEARCH_JOB_LOCK:
                    job = SEARCH_JOBS.get(job_id)
                    if job is None:
                        return
                    job["progress"] = progress
                    partial_result = job.get("partialResult")
                    if isinstance(partial_result, dict):
                        partial_summary = partial_result.setdefault("summary", {})
                        partial_summary["progress"] = progress
                        partial_summary["jobStatus"] = "pricing"
                        if price_progress is not None:
                            partial_summary["priceProgress"] = price_progress

            update_search_job(
                job_id,
                progress=job_progress(
                    "current-price",
                    "正在抓取目标日期附近星级酒店，找到候选后会先展示。",
                    SEARCH_JOBS[job_id]["startedAt"],
                ),
            )
            if provider_name == "tripcom":
                quick_result = search_current_prices(
                    provider=provider,
                    **params,
                    fast_mode=True,
                    include_provisional_names=True,
                    defer_price_filter=True,
                    preserve_order=True,
                )
                quick_summary = quick_result.setdefault("summary", {})
                candidate_count = int(quick_summary.get("candidateCount") or 0)
                if candidate_count:
                    progress_message = f"已先展示 Trip.com 首屏 {candidate_count} 家候选，后台正在继续深度搜索。"
                else:
                    progress_message = "目标日期首屏还没有候选，正在后台扩大半径并继续深度搜索。"
                publish_partial_result(
                    quick_result,
                    cache_source="live-first-screen",
                    stage="first-screen",
                    message=progress_message,
                )

                def publish_deep_candidates(partial_result: dict[str, Any]) -> None:
                    deep_summary = partial_result.get("summary") or {}
                    deep_count = int(deep_summary.get("candidateCount") or 0)
                    if deep_summary.get("priceCompareComplete") is False:
                        completed = int(deep_summary.get("completedCompareDateCount") or 0)
                        total = int(deep_summary.get("totalCompareDateCount") or 0)
                        deal_count = int(deep_summary.get("dealCount") or 0)
                        message = f"已补齐 {completed}/{total} 个对比日期，先展示当前可判断的 {deep_count} 家候选和 {deal_count} 家捡漏结果。"
                        stage = "partial-deals"
                    elif deep_count:
                        message = f"深度搜索已找到 {deep_count} 家目标日期候选酒店，正在逐家补齐对比日期含税价。"
                        stage = "deep-current-price"
                    else:
                        message = "深度搜索暂未找到候选，正在继续补抓 Trip.com 数据。"
                        stage = "deep-current-price"
                    publish_partial_result(
                        partial_result,
                        cache_source="live-deep-partial",
                        stage=stage,
                        message=message,
                    )

                def publish_price_progress(progress_info: dict[str, Any]) -> None:
                    date_value = str(progress_info.get("date") or "")
                    phase = str(progress_info.get("phase") or "")
                    date_index = int(progress_info.get("dateIndex") or 0)
                    completed = int(progress_info.get("completedDates") or 0)
                    total = int(progress_info.get("totalDates") or 0)
                    priced = int(progress_info.get("pricedHotelCount") or 0)
                    total_hotels = int(progress_info.get("totalHotels") or 0)
                    missing = int(progress_info.get("missingHotelCount") or 0)
                    backfill_mode = str(progress_info.get("backfillMode") or "")
                    if backfill_mode == "final" and phase == "detail":
                        message = f"搜索完成前正在总检查 {date_value} 的待补价酒店，逐家打开 Trip.com 详情页补齐含税价。"
                    elif backfill_mode == "pending" and phase == "detail":
                        message = f"正在异步检查 {date_value} 仍待补价的酒店，逐家打开 Trip.com 详情页补齐含税价。"
                    elif phase == "start":
                        message = f"正在补齐对比日期含税价：{date_value}（第 {date_index}/{total} 个日期），当前已匹配 {priced}/{total_hotels} 家。"
                    elif phase == "list":
                        message = f"{date_value} 列表价已匹配 {priced}/{total_hotels} 家，正在用详情页补齐剩余含税价。"
                    elif phase == "detail":
                        message = f"正在打开 {date_value} 的酒店详情页补齐含税价，当前已匹配 {priced}/{total_hotels} 家。"
                    elif phase == "deep":
                        message = f"{date_value} 仍有 {missing} 家未匹配，正在做深度列表兜底搜索。"
                    else:
                        message = f"已补齐 {date_value} 含税价（{completed}/{total}），本日已匹配 {priced}/{total_hotels} 家。"
                    if phase != "start" and missing:
                        message += f" 未匹配的 {missing} 家会继续用其它日期结果综合判断。"
                    publish_progress_only("compare-price", message, price_progress=progress_info)

                result = search_deals(
                    provider=provider,
                    **params,
                    progress_callback=publish_deep_candidates,
                    price_progress_callback=publish_price_progress,
                )
            else:
                update_search_job(
                    job_id,
                    progress=job_progress(
                        "compare-price",
                        "正在计算完整优惠结果。",
                        SEARCH_JOBS[job_id]["startedAt"],
                    ),
                )
                result = search_deals(provider=provider, **params)
            result = merge_search_result_with_cached(result, base_cached_result)
            result = name_verifier.apply_to_result(result)
            name_verifier.enqueue_from_result(result)
            pending_name_count = len(unique_hotels_needing_name_verification(result))

            if pending_name_count or name_verifier.total_seen():
                def publish_name_progress(partial_result: dict[str, Any], progress_info: dict[str, Any]) -> None:
                    total = int(progress_info.get("total") or pending_name_count)
                    completed = int(progress_info.get("completed") or progress_info.get("completedCount") or 0)
                    resolved = name_verifier.resolved_count() + int(progress_info.get("resolvedCount") or 0)
                    total = max(total, resolved, name_verifier.total_seen())
                    completed = min(total, completed + name_verifier.resolved_count())
                    phase = str(progress_info.get("phase") or "")
                    if phase == "error":
                        message = f"完整比价已完成，中文名核验遇到阻塞，已先保留 {resolved}/{total} 个已确认名称，其余会用本地中文名兜底。"
                    elif phase == "complete":
                        message = f"完整比价已完成，正在核验酒店中文名：{completed}/{total}，已更新 {resolved} 家。"
                    else:
                        message = f"完整比价已完成，正在继续核验酒店中文名：{completed}/{total}，已更新 {resolved} 家。"
                    progress = job_progress("name-verification", message, SEARCH_JOBS[job_id]["startedAt"])
                    partial_result = apply_sort_to_result(partial_result, params["sort_by"])
                    partial_summary = partial_result.setdefault("summary", {})
                    partial_summary["cacheHit"] = False
                    partial_summary["cacheSource"] = "live-name-verification"
                    partial_summary["partial"] = True
                    partial_summary["jobId"] = job_id
                    partial_summary["jobStatus"] = "name-verification"
                    partial_summary["priceCompareComplete"] = True
                    partial_summary["nameVerificationTotal"] = total
                    partial_summary["nameVerificationResolvedCount"] = resolved
                    partial_summary["nameVerificationActive"] = True
                    partial_summary["progress"] = progress
                    update_search_job(job_id, partialResult=partial_result, progress=progress)
                    if cacheable_search_result(partial_result):
                        remember_search_result(key, provider_name, partial_result)

                initial_name_progress = job_progress(
                    "name-verification",
                    f"完整比价已完成，正在收尾核验酒店中文名；前面已并行更新 {name_verifier.resolved_count()} 家。",
                    SEARCH_JOBS[job_id]["startedAt"],
                )
                name_partial = apply_sort_to_result(result, params["sort_by"])
                name_partial.setdefault("summary", {})["partial"] = True
                name_partial["summary"]["jobStatus"] = "name-verification"
                name_partial["summary"]["priceCompareComplete"] = True
                name_partial["summary"]["nameVerificationTotal"] = max(name_verifier.total_seen(), pending_name_count)
                name_partial["summary"]["nameVerificationResolvedCount"] = name_verifier.resolved_count()
                name_partial["summary"]["nameVerificationActive"] = True
                name_partial["summary"]["progress"] = initial_name_progress
                update_search_job(job_id, partialResult=name_partial, progress=initial_name_progress)
                name_verifier.wait_until_idle()
                result = name_verifier.apply_to_result(result)
                if unique_hotels_needing_name_verification(result):
                    result = verify_result_hotel_names(
                        provider,
                        result,
                        params["selected_date"],
                        progress_callback=publish_name_progress,
                        provider_name=provider_name,
                    )
                    final_name_summary = result.setdefault("summary", {})
                    full_total = int(final_name_summary.get("nameVerificationTotal") or 0)
                    full_resolved = int(final_name_summary.get("nameVerificationResolvedCount") or 0)
                    combined_total = max(name_verifier.total_seen(), full_total)
                    combined_resolved = min(combined_total, name_verifier.resolved_count() + full_resolved)
                    final_name_summary["nameVerificationTotal"] = combined_total
                    final_name_summary["nameVerificationResolvedCount"] = combined_resolved
                    final_name_summary["nameVerificationFallbackCount"] = max(combined_total - combined_resolved, 0)
                    final_name_summary["nameVerificationRemainingCount"] = len(unique_hotels_needing_name_verification(result))
                else:
                    result = apply_name_payloads_to_result(result, name_verifier.payloads(), finalize_remaining=True)
                    final_name_summary = result.setdefault("summary", {})
                    final_name_summary["nameVerificationComplete"] = True
                    final_name_summary["nameVerificationTotal"] = name_verifier.total_seen()
                    final_name_summary["nameVerificationResolvedCount"] = name_verifier.resolved_count()
                    final_name_summary["nameVerificationFallbackCount"] = 0
                    final_name_summary["nameVerificationRemainingCount"] = 0
                result.setdefault("summary", {})["nameVerificationActive"] = False

            result.setdefault("summary", {})["cacheHit"] = False
            result["summary"]["cacheSource"] = "live-complete"
            result["summary"]["elapsedMs"] = round((time.time() - SEARCH_JOBS[job_id]["startedAt"]) * 1000, 1)
            result["summary"]["partial"] = False
            result["summary"]["jobStatus"] = "complete"
            result["summary"]["jobId"] = job_id
            result["summary"]["progress"] = job_progress(
                "complete",
                "完整比价已完成。",
                SEARCH_JOBS[job_id]["startedAt"],
            )
            remember_search_result(key, provider_name, result)
            record_hot_search(payload)
            with SEARCH_JOB_LOCK:
                SEARCH_JOBS[job_id].update(
                    {
                        "status": "complete",
                        "result": result,
                        "finishedAt": time.time(),
                        "elapsedMs": result["summary"]["elapsedMs"],
                        "progress": result["summary"]["progress"],
                    }
                )
        except Exception as exc:  # pragma: no cover - exercised through integration
            if provider_name == "tripcom" and isinstance(exc, ProviderError) and retriable_provider_error(exc):
                with SEARCH_JOB_LOCK:
                    job = SEARCH_JOBS.get(job_id)
                    partial_result = job.get("partialResult") if job else None
                    retry_count = int((job or {}).get("retryCount") or 0) + 1
                    progress_message = (
                        f"Trip.com 本轮抓取暂时失败，已保存当前进度，"
                        f"{CACHE_RETRY_DELAY_SECONDS} 秒后继续补抓。失败原因：{exc}"
                    )
                    if not partial_result:
                        progress_message = (
                            f"Trip.com 本轮抓取暂时失败，暂未拿到候选，"
                            f"{CACHE_RETRY_DELAY_SECONDS} 秒后继续尝试。失败原因：{exc}"
                        )
                    progress = job_progress(
                        "waiting-retry",
                        progress_message,
                        SEARCH_JOBS[job_id]["startedAt"],
                    )
                    if isinstance(partial_result, dict):
                        partial_summary = partial_result.setdefault("summary", {})
                        partial_summary["progress"] = progress
                        partial_summary["jobStatus"] = "waiting-retry"
                        partial_summary["partial"] = True
                        if cacheable_search_result(partial_result):
                            remember_search_result(key, provider_name, partial_result)
                    SEARCH_JOBS[job_id].update(
                        {
                            "status": "running",
                            "lastError": str(exc),
                            "retryCount": retry_count,
                            "progress": progress,
                        }
                    )

                def retry_worker() -> None:
                    time.sleep(max(1, CACHE_RETRY_DELAY_SECONDS))
                    with SEARCH_JOB_LOCK:
                        job = SEARCH_JOBS.get(job_id)
                        if not job or job.get("status") != "running":
                            return
                    worker()

                threading.Thread(target=retry_worker, name=f"hotel-search-retry-{job_id}", daemon=True).start()
                return

            with SEARCH_JOB_LOCK:
                partial_result = SEARCH_JOBS.get(job_id, {}).get("partialResult")
                progress_message = "后台完整比价失败"
                if partial_result and result_candidate_count(partial_result) > 0:
                    progress_message = "已展示目标日期候选，但后台完整比价失败，可稍后重试。"
                elif partial_result:
                    progress_message = "后台完整比价失败，当前还没有可展示候选，可稍后重试。"
                progress = job_progress(
                    "error",
                    f"{progress_message}：{exc}",
                    SEARCH_JOBS[job_id]["startedAt"],
                )
                if isinstance(partial_result, dict):
                    partial_summary = partial_result.setdefault("summary", {})
                    partial_summary["progress"] = progress
                    partial_summary["jobStatus"] = "error"
                SEARCH_JOBS[job_id].update(
                    {
                        "status": "error",
                        "error": str(exc),
                        "finishedAt": time.time(),
                        "progress": progress,
                    }
                )

    threading.Thread(target=worker, name=f"hotel-search-{job_id}", daemon=True).start()
    return job_id


def clear_search_cache(provider: str | None = None) -> None:
    provider_name = canonical_provider_name(provider) if provider else None
    SEARCH_CACHE.clear()
    if MYSQL_SEARCH_CACHE is not None:
        MYSQL_SEARCH_CACHE.clear(provider_name)
    try:
        paths = list(SEARCH_CACHE_DIR.glob("*.json"))
    except OSError:
        return
    for path in paths:
        if provider_name:
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if record.get("provider") != provider_name:
                continue
        try:
            path.unlink()
        except OSError:
            pass


def search_job_payload(job_id: str, job: dict[str, Any], sort_by: str) -> tuple[dict[str, Any], int]:
    if job.get("status") == "complete":
        result = apply_sort_to_result(job.get("result") or {}, sort_by)
        result.setdefault("summary", {})["jobId"] = job_id
        result["summary"]["jobStatus"] = "complete"
        result["summary"]["partial"] = False
        return {"status": "complete", "result": result}, 200

    if job.get("status") == "error":
        partial_result = job.get("partialResult")
        payload: dict[str, Any] = {
            "status": "error",
            "error": job.get("error") or "后台搜索失败",
            "progress": job.get("progress"),
        }
        if partial_result:
            result = apply_sort_to_result(partial_result, sort_by)
            result.setdefault("summary", {})["jobId"] = job_id
            result["summary"]["jobStatus"] = "error"
            result["summary"]["partial"] = True
            result["summary"]["progress"] = job.get("progress")
            payload["result"] = result
        return payload, 200 if partial_result else 500

    partial_result = job.get("partialResult")
    elapsed_ms = round((time.time() - float(job.get("startedAt") or time.time())) * 1000, 1)
    progress = copy.deepcopy(job.get("progress") or {})
    if progress:
        progress["elapsedMs"] = elapsed_ms
    payload = {
        "status": "running",
        "jobId": job_id,
        "progress": progress or None,
        "elapsedMs": elapsed_ms,
    }
    if partial_result:
        result = apply_sort_to_result(partial_result, sort_by)
        result.setdefault("summary", {})["jobId"] = job_id
        progress_stage = str((progress or job.get("progress") or {}).get("stage") or "")
        result["summary"]["jobStatus"] = progress_stage if progress_stage in {"waiting-retry", "name-verification"} else "pricing"
        result["summary"]["partial"] = True
        result["summary"]["progress"] = progress or job.get("progress")
        payload["result"] = result
    return payload, 200


def search_job_snapshot(job_id: str, sort_by: str) -> tuple[dict[str, Any], int]:
    with SEARCH_JOB_LOCK:
        job = copy.deepcopy(SEARCH_JOBS.get(job_id))
    if not job:
        return {"status": "missing", "error": "搜索任务不存在或已过期"}, 404
    return search_job_payload(job_id, job, sort_by)


def sse_message(payload: dict[str, Any], event: str = "message") -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


@app.get("/")
def index():
    hot_targets = current_hot_targets()
    initial_target = hot_targets[0] if hot_targets else {"city": "深圳", "targetHotel": "深圳国际会展中心希尔顿酒店"}
    return render_template(
        "index.html",
        default_check_in=default_check_in(),
        hot_targets=hot_targets,
        initial_target=initial_target,
    )


@app.get("/api/health")
def health():
    return jsonify({"ok": True, "app": "star-hotel-deal-app"})


@app.get("/api/compare-dates")
def compare_dates():
    selected_date = (request.args.get("date") or "").strip()
    if not selected_date:
        return jsonify({"error": "date 不能为空"}), 400
    try:
        return jsonify(get_compare_date_info(selected_date))
    except HotelDealError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/sample-targets")
def sample_targets():
    return jsonify({"targets": local_provider().sample_targets()})


@app.get("/api/hot-targets")
def hot_targets():
    try:
        limit = parse_optional_int(request.args.get("limit"), "limit") or 10
    except HotelDealError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"targets": current_hot_targets(limit=max(1, min(limit, 20)))})


@app.get("/api/tripcom/suggest")
def tripcom_suggest():
    city = (request.args.get("city") or "").strip()
    query = (request.args.get("q") or request.args.get("query") or "").strip()
    limit = parse_optional_int(request.args.get("limit"), "limit") or 8
    if not city or not query:
        return jsonify({"suggestions": []})
    try:
        suggestions = TripComProvider().suggest_targets(city=city, query=query, limit=max(1, min(limit, 12)))
    except ProviderError as exc:
        return jsonify({"error": str(exc), "suggestions": []}), 400
    return jsonify({"suggestions": suggestions})


@app.post("/api/search")
def search():
    payload = request.get_json(silent=True) or {}
    started_at = time.perf_counter()
    try:
        provider_name = canonical_provider_name(payload.get("provider"))
        sort_by = str(payload.get("sortBy") or "discount")
        force_refresh = force_refresh_requested(payload)
        key = cache_key(payload)
        cached = cached_search_result(key, provider_name, sort_by) if key else None
        if key and not force_refresh:
            if cached is not None:
                if (
                    provider_name == "tripcom"
                    and parse_bool(payload.get("asyncMode"), default=True)
                    and parse_bool(payload.get("backgroundMode"), default=True)
                    and cached_result_needs_refresh(cached, provider_name)
                ):
                    refresh_job_id = start_background_search_job(
                        payload,
                        key,
                        provider_name,
                        force_refresh=True,
                        base_cached_result=cached,
                    )
                    cached.setdefault("summary", {})["refreshing"] = True
                    cached["summary"]["refreshJobId"] = refresh_job_id
                    cached["summary"]["progress"] = job_progress(
                        "refresh",
                        "已秒出缓存结果，正在后台刷新 Trip.com 实时价格。",
                        time.time(),
                    )
                record_hot_search(payload)
                return jsonify(cached)

        async_mode = provider_name == "tripcom" and parse_bool(payload.get("asyncMode"), default=True)
        if async_mode:
            if parse_bool(payload.get("backgroundMode"), default=True):
                job_kwargs: dict[str, Any] = {}
                if force_refresh:
                    job_kwargs["force_refresh"] = True
                if cached is not None:
                    job_kwargs["base_cached_result"] = cached
                job_id = start_background_search_job(payload, key, provider_name, **job_kwargs)
                with SEARCH_JOB_LOCK:
                    job = copy.deepcopy(SEARCH_JOBS.get(job_id) or {})
                if job.get("partialResult"):
                    result = apply_sort_to_result(job["partialResult"], sort_by)
                    result.setdefault("summary", {})["jobId"] = job_id
                    result["summary"]["jobStatus"] = "pricing"
                    result["summary"]["partial"] = True
                elif job.get("result"):
                    result = apply_sort_to_result(job["result"], sort_by)
                    result.setdefault("summary", {})["jobId"] = job_id
                    result["summary"]["jobStatus"] = "complete"
                    result["summary"]["partial"] = False
                else:
                    result = queued_search_result(payload, provider_name, job_id, float(job.get("startedAt") or time.time()))
                    if job.get("progress"):
                        result["summary"]["progress"] = job["progress"]
                        result["summary"]["jobStatus"] = job["progress"].get("stage") or result["summary"]["jobStatus"]
            else:
                result = run_search_payload(payload, provider_name, quick=True)
                result.setdefault("summary", {})["jobStatus"] = "quick-only"
                result["summary"]["cacheSource"] = "live-partial"
                result["summary"]["partial"] = True
            result["summary"]["cacheHit"] = False
            result["summary"]["elapsedMs"] = round((time.perf_counter() - started_at) * 1000, 1)
            return jsonify(result)

        result = run_search_payload(payload, provider_name, quick=False)
        if force_refresh:
            result = merge_search_result_with_cached(result, cached)
        result.setdefault("summary", {})["cacheHit"] = False
        result["summary"]["cacheSource"] = "live-refresh" if force_refresh else "live"
        result["summary"]["elapsedMs"] = round((time.perf_counter() - started_at) * 1000, 1)
        result["summary"]["partial"] = False
        if key:
            remember_search_result(key, provider_name, result)
        record_hot_search(payload)
    except (HotelDealError, ProviderError) as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # pragma: no cover
        return jsonify({"error": f"搜索失败：{exc}"}), 500
    return jsonify(result)


@app.get("/api/search/status/<job_id>")
def search_status(job_id: str):
    sort_by = str(request.args.get("sortBy") or "discount")
    payload, status_code = search_job_snapshot(job_id, sort_by)
    return jsonify(payload), status_code


@app.get("/api/search/events/<job_id>")
def search_events(job_id: str):
    sort_by = str(request.args.get("sortBy") or "discount")

    @stream_with_context
    def event_stream():
        last_signature = ""
        last_keepalive = 0.0
        yield "retry: 1000\n\n"
        while True:
            payload, _status_code = search_job_snapshot(job_id, sort_by)
            signature = hashlib.sha256(
                json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            if signature != last_signature:
                yield sse_message(payload)
                last_signature = signature
                last_keepalive = time.time()
            if payload.get("status") in {"complete", "error", "missing"}:
                break
            if time.time() - last_keepalive >= 15:
                yield ": keep-alive\n\n"
                last_keepalive = time.time()
            time.sleep(0.6)

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/export/pdf")
def export_pdf():
    payload = request.get_json(silent=True) or {}
    result = payload.get("result") if isinstance(payload.get("result"), dict) else None
    if result is None and payload.get("jobId"):
        sort_by = str(payload.get("sortBy") or "discount")
        snapshot, status_code = search_job_snapshot(str(payload.get("jobId")), sort_by)
        if status_code != 200 or not isinstance(snapshot.get("result"), dict):
            return jsonify({"error": snapshot.get("error") or "搜索记录不存在，无法导出 PDF"}), status_code
        result = snapshot["result"]
    if result is None and any(key in payload for key in ("allHotels", "dealHotels", "recommendedHotels")):
        result = payload
    if not isinstance(result, dict):
        return jsonify({"error": "没有可导出的搜索记录"}), 400
    hotel_count = sum(
        len(result.get(list_name) or [])
        for list_name in ("allHotels", "dealHotels", "recommendedHotels")
        if isinstance(result.get(list_name), list)
    )
    if hotel_count <= 0:
        return jsonify({"error": "当前搜索记录还没有酒店结果，暂时无法导出 PDF"}), 400
    try:
        pdf_bytes = render_search_result_pdf(result)
    except HotelDealError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # pragma: no cover
        return jsonify({"error": f"PDF 导出失败：{exc}"}), 500
    filename = export_pdf_filename(result)
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"},
    )


@app.post("/api/import")
def import_hotels():
    payload = request.get_json(silent=True) or {}
    hotels = payload.get("hotels")
    if not isinstance(hotels, list) or not hotels:
        return jsonify({"error": "JSON 必须包含非空 hotels 数组"}), 400

    normalized: list[dict[str, Any]] = []
    required = {"hotelName", "city", "starRating", "latitude", "longitude"}
    for index, hotel in enumerate(hotels, start=1):
        if not isinstance(hotel, dict):
            return jsonify({"error": f"第 {index} 条酒店不是对象"}), 400
        missing = [field for field in required if hotel.get(field) in ("", None)]
        if missing:
            return jsonify({"error": f"第 {index} 条酒店缺少字段：{', '.join(missing)}"}), 400
        item = dict(hotel)
        item["hotelId"] = str(item.get("hotelId") or f"import-{index}")
        normalized.append(item)

    target_path = APP_DIR / ".cache" / "imported_hotels.json"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(json.dumps({"hotels": normalized}, ensure_ascii=False, indent=2), encoding="utf-8")
    reload_local_provider()
    return jsonify({"ok": True, "importedCount": len(normalized)})


@app.errorhandler(Exception)
def handle_error(exc: Exception):
    if isinstance(exc, HTTPException):
        return jsonify({"error": exc.description}), exc.code
    return jsonify({"error": f"服务异常：{exc}"}), 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5013, debug=False, threaded=True)
