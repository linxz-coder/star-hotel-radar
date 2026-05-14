from __future__ import annotations

import datetime as dt
import copy
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from werkzeug.exceptions import HTTPException

from hotel_deals import (
    HotelDealError,
    get_compare_date_info,
    normalize_result_hotel_name,
    search_current_prices,
    search_deals,
    sort_hotels,
    sort_recommended_hotels,
)
from mysql_cache import MySQLSearchCache
from providers import LocalJsonProvider, ProviderError, TripComProvider, provider_from_name


APP_DIR = Path(__file__).resolve().parent
app = Flask(__name__)
LOCAL_PROVIDER = LocalJsonProvider(APP_DIR)
SEARCH_CACHE: dict[str, tuple[float, dict[str, Any], float]] = {}
SEARCH_JOBS: dict[str, dict[str, Any]] = {}
SEARCH_JOB_LOCK = threading.Lock()
SEARCH_CACHE_DIR = APP_DIR / ".cache" / "search_cache"
HOT_SEARCH_PATH = APP_DIR / ".cache" / "hot_searches.json"
LOCAL_CACHE_TTL_SECONDS = int(os.environ.get("HOTEL_DEAL_LOCAL_CACHE_TTL_SECONDS", str(7 * 24 * 60 * 60)))
TRIPCOM_CACHE_TTL_SECONDS = int(os.environ.get("HOTEL_DEAL_TRIPCOM_CACHE_TTL_SECONDS", str(7 * 24 * 60 * 60)))
TRIPCOM_REFRESH_AFTER_SECONDS = int(os.environ.get("HOTEL_DEAL_TRIPCOM_REFRESH_AFTER_SECONDS", str(12 * 60 * 60)))
CACHE_RETRY_DELAY_SECONDS = int(os.environ.get("HOTEL_DEAL_SEARCH_RETRY_DELAY_SECONDS", "60"))
MAX_SEARCH_CACHE_ITEMS = 256
MAX_HOT_SEARCH_RECORDS = 80
HOT_SEARCH_TTL_SECONDS = 30 * 24 * 60 * 60
CACHE_LOGIC_VERSION = "search_v33_preserve_and_localize_names"
MYSQL_SEARCH_CACHE = MySQLSearchCache.from_env()
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

    all_hotels = [
        normalize_result_hotel_name(hotel, city=query_city)
        for hotel in list(data.get("allHotels") or [])
    ]
    deal_hotels = [
        normalize_result_hotel_name(hotel, city=query_city)
        for hotel in list(data.get("dealHotels") or [])
    ]
    recommended_hotels = [
        normalize_result_hotel_name(hotel, city=query_city)
        for hotel in list(data.get("recommendedHotels") or [])
    ]
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
    data = apply_sort_to_result(result, sort_by)
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


def hotel_merge_key(hotel: dict[str, Any]) -> str:
    hotel_id = str(hotel.get("hotelId") or "").strip()
    if hotel_id:
        return f"id:{hotel_id}"
    name = str(hotel.get("hotelNameSimplified") or hotel.get("hotelName") or hotel.get("hotelOriginalName") or "").strip()
    return f"name:{name.casefold()}" if name else ""


def merge_hotel_lists_with_fresh_priority(
    fresh_hotels: list[dict[str, Any]],
    cached_hotels: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    fresh_keys: set[str] = set()
    corrected_count = 0

    cached_keys = {hotel_merge_key(hotel) for hotel in cached_hotels if hotel_merge_key(hotel)}
    for hotel in fresh_hotels:
        item = copy.deepcopy(hotel)
        key = hotel_merge_key(item)
        if key and key in seen:
            continue
        if key and key in cached_keys:
            corrected_count += 1
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


def remember_search_result(key: str, provider_name: str, result: dict[str, Any]) -> None:
    ttl_seconds = cache_ttl_seconds(provider_name)
    if ttl_seconds <= 0:
        return
    if not cacheable_search_result(result):
        return
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

            def publish_partial_result(partial_result: dict[str, Any], *, cache_source: str, stage: str, message: str) -> None:
                with SEARCH_JOB_LOCK:
                    previous_partial = copy.deepcopy((SEARCH_JOBS.get(job_id) or {}).get("partialResult"))
                partial_result = merge_search_result_with_cached(partial_result, previous_partial)
                partial_result = merge_search_result_with_cached(partial_result, base_cached_result)
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
                    if phase == "start":
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
        result["summary"]["jobStatus"] = "waiting-retry" if progress_stage == "waiting-retry" else "pricing"
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
