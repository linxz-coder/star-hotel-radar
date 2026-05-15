from __future__ import annotations

import datetime as dt
import asyncio
import hashlib
import html as html_lib
import inspect
import json
import os
import random
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen

from hotel_deals import HotelDealError, detectHotelBrand, haversine_km, parse_date
from localization import contains_chinese_text, domestic_hotel_name_payload, hotel_name_payload_from_sources, simplify_chinese_text


UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)
KEYWORD_LOCALE = "en-XX"
LIST_LOCALE = "zh-CN"
RESULT_TYPE_LABELS = {
    "H": "酒店",
    "LM": "地标",
    "D": "区域",
    "CT": "城市",
    "Z": "商圈",
}
NAME_VERIFICATION_MARKERS = ("中文名待核验", "中文名正在核验中")


class ProviderError(HotelDealError):
    pass


def normalize_name(value: str) -> str:
    return re.sub(r"[\s·・,，.。()（）\-_/]+", "", simplify_chinese_text(value).lower())


def has_name_verification_marker(value: Any) -> bool:
    text = str(value or "")
    return any(marker in text for marker in NAME_VERIFICATION_MARKERS)


def read_json_file(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        raise ProviderError(f"{path.name} 不是合法 JSON") from exc


class LocalJsonProvider:
    source_name = "本地样例/导入数据"

    def __init__(self, app_dir: Path) -> None:
        self.app_dir = app_dir
        self.sample_path = app_dir / "data" / "sample_hotels.json"
        self.imported_path = app_dir / ".cache" / "imported_hotels.json"
        self.hotels = self._load_hotels()

    def _load_hotels(self) -> list[dict[str, Any]]:
        hotels: list[dict[str, Any]] = []
        for path in (self.sample_path, self.imported_path):
            data = read_json_file(path)
            rows = data.get("hotels") if isinstance(data, dict) else None
            if isinstance(rows, list):
                hotels.extend(self._normalize_hotel(row, source=path.name) for row in rows if isinstance(row, dict))
        return hotels

    def _normalize_hotel(self, row: dict[str, Any], source: str) -> dict[str, Any]:
        hotel = dict(row)
        hotel["hotelId"] = str(hotel.get("hotelId") or hotel.get("id") or hotel.get("hotelName") or "")
        hotel.update(domestic_hotel_name_payload(hotel.get("hotelName") or hotel.get("name") or "", hotel_id=hotel["hotelId"], source=source))
        hotel["city"] = simplify_chinese_text(hotel.get("city") or hotel.get("destination") or "")
        if hotel.get("brand"):
            hotel["brand"] = simplify_chinese_text(hotel.get("brand"))
        if hotel.get("group"):
            hotel["group"] = simplify_chinese_text(hotel.get("group"))
        hotel["starRating"] = float(hotel.get("starRating") or hotel.get("star") or 0)
        hotel["latitude"] = float(hotel.get("latitude") or hotel.get("lat") or 0)
        hotel["longitude"] = float(hotel.get("longitude") or hotel.get("lon") or hotel.get("lng") or 0)
        hotel["basePrice"] = int(hotel.get("basePrice") or hotel.get("currentPrice") or 600)
        hotel["source"] = source
        return hotel

    def sample_targets(self) -> list[dict[str, str]]:
        targets = [
            {"city": hotel["city"], "hotelName": hotel["hotelName"]}
            for hotel in self.hotels
            if hotel.get("isTarget")
        ]
        return targets[:10]

    def resolve_target_hotel(
        self,
        city: str,
        hotel_name: str,
        target_hint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        city_norm = normalize_name(city)
        target_norm = normalize_name(hotel_name)
        if not city_norm or not target_norm:
            raise ProviderError("城市和目标酒店不能为空")

        city_hotels = [hotel for hotel in self.hotels if city_norm in normalize_name(hotel.get("city", ""))]
        if not city_hotels:
            raise ProviderError(f"本地数据中没有找到城市：{city}")

        exact = [
            hotel
            for hotel in city_hotels
            if target_norm == normalize_name(hotel.get("hotelName", ""))
            or target_norm in normalize_name(hotel.get("hotelName", ""))
            or normalize_name(hotel.get("hotelName", "")) in target_norm
        ]
        if not exact:
            suggestions = "、".join(hotel["hotelName"] for hotel in city_hotels[:5])
            raise ProviderError(f"本地数据中没有找到目标酒店，可先试：{suggestions}")

        target = dict(exact[0])
        target["distanceKm"] = 0
        target["matchedBy"] = self.source_name
        return target

    def get_nearby_hotels(
        self,
        target_hotel: dict[str, Any],
        radius_km: float = 3,
        min_star: float = 4,
        selected_date: str | None = None,
        fast_mode: bool = False,
        progress_callback: Any | None = None,
    ) -> list[dict[str, Any]]:
        target_lat = float(target_hotel.get("latitude") or 0)
        target_lon = float(target_hotel.get("longitude") or 0)
        target_id = str(target_hotel.get("hotelId") or "")
        target_city = normalize_name(target_hotel.get("city", ""))
        hotels: list[dict[str, Any]] = []

        for hotel in self.hotels:
            if str(hotel.get("hotelId") or "") == target_id:
                continue
            if normalize_name(hotel.get("city", "")) != target_city:
                continue
            if float(hotel.get("starRating") or 0) < float(min_star):
                continue
            if not hotel.get("latitude") or not hotel.get("longitude"):
                continue
            distance = haversine_km(target_lat, target_lon, float(hotel["latitude"]), float(hotel["longitude"]))
            if distance <= radius_km:
                item = dict(hotel)
                item["distanceKm"] = round(distance, 2)
                hotels.append(item)

        hotels.sort(key=lambda item: (float(item.get("distanceKm") or 999), -float(item.get("starRating") or 0)))
        if progress_callback and hotels:
            progress_callback(list(hotels))
        return hotels

    def get_hotel_prices(
        self,
        hotel_ids: list[str],
        dates: list[str],
        progress_callback: Any | None = None,
    ) -> dict[str, dict[str, int | None]]:
        hotel_map = {str(hotel["hotelId"]): hotel for hotel in self.hotels}
        result: dict[str, dict[str, int | None]] = {str(hotel_id): {} for hotel_id in hotel_ids}
        total_dates = len(dates)
        total_hotels = len(hotel_ids)
        for date_index, date_value in enumerate(dates, start=1):
            for hotel_id in hotel_ids:
                hotel_id_str = str(hotel_id)
                hotel = hotel_map.get(hotel_id_str)
                result.setdefault(hotel_id_str, {})[date_value] = self._price_for_date(hotel, date_value) if hotel else None
            if progress_callback:
                priced_count = sum(
                    1
                    for hotel_id in hotel_ids
                    if result.get(str(hotel_id), {}).get(date_value) not in (None, "")
                )
                progress_callback(
                    {
                        "stage": "compare-price",
                        "phase": "complete",
                        "date": date_value,
                        "dateIndex": date_index,
                        "completedDates": date_index,
                        "totalDates": total_dates,
                        "pricedHotelCount": priced_count,
                        "missingHotelCount": max(total_hotels - priced_count, 0),
                        "totalHotels": total_hotels,
                    }
                )
        for hotel_id in hotel_ids:
            result.setdefault(str(hotel_id), {date: None for date in dates})
        return result

    def _price_for_date(self, hotel: dict[str, Any], date_value: str) -> int:
        overrides = hotel.get("priceOverrides") or {}
        if date_value in overrides:
            return int(overrides[date_value])

        day = parse_date(date_value)
        base = int(hotel.get("basePrice") or 600)
        weekend_factor = 1.22 if day.weekday() in (4, 5) else 1.0
        season_seed = int(hashlib.sha256(f"{hotel['hotelId']}:{date_value}".encode()).hexdigest()[:6], 16)
        noise = ((season_seed % 17) - 8) / 100
        deal_days = {int(value) for value in hotel.get("dealDays", [])}
        deal_factor = float(hotel.get("dealFactor") or 0.82)

        price = base * weekend_factor * (1 + noise)
        if day.day in deal_days:
            price *= deal_factor
        elif (day.toordinal() + season_seed) % 31 == 0:
            price *= 0.78
        return max(180, int(round(price / 10) * 10))


class TripComProvider:
    source_name = "Trip.com 实时抓取"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_target: dict[str, Any] | None = None
        self._last_search_targets: list[dict[str, Any]] = []
        self._candidate_cache: dict[str, dict[str, Any]] = {}
        self._price_cache: dict[str, dict[str, int | None]] = {}
        self._cache_stats: dict[str, dict[str, Any]] = {}
        self._persistent_price_cache: Any | None = None
        self._persistent_candidate_cache: Any | None = None
        self._bypass_persistent_price_cache = False
        self._bypass_persistent_candidate_cache = False
        self._persistent_price_cache_ttl_seconds = float(
            os.environ.get("HOTEL_DEAL_HOTEL_PRICE_CACHE_TTL_SECONDS", str(24 * 60 * 60))
        )
        self._persistent_candidate_cache_ttl_seconds = float(
            os.environ.get("HOTEL_DEAL_HOTEL_CANDIDATE_CACHE_TTL_SECONDS", str(30 * 24 * 60 * 60))
        )
        self.reset_cache_stats()

    def reset_cache_stats(self) -> None:
        self._cache_stats = {
            "candidateMetadata": {
                "label": "候选酒店元数据",
                "type": "candidateMetadata",
                "enabled": False,
                "bypassed": False,
                "hit": False,
                "hitCount": 0,
                "storedCount": 0,
                "source": "MySQL",
            },
            "hotelPrice": {
                "label": "酒店日期含税价",
                "type": "hotelPrice",
                "enabled": False,
                "bypassed": False,
                "hit": False,
                "hitCount": 0,
                "requestedCount": 0,
                "storedCount": 0,
                "source": "MySQL",
            },
        }

    def cache_layers(self) -> list[dict[str, Any]]:
        layers: list[dict[str, Any]] = []
        for key in ("candidateMetadata", "hotelPrice"):
            layer = dict(self._cache_stats.get(key) or {})
            if not layer:
                continue
            layer["status"] = "hit" if layer.get("hit") else ("bypassed" if layer.get("bypassed") else "miss")
            layers.append(layer)
        return layers

    def set_persistent_price_cache(
        self,
        price_cache: Any | None,
        *,
        bypass: bool = False,
        ttl_seconds: float | None = None,
    ) -> None:
        self._persistent_price_cache = price_cache
        self._bypass_persistent_price_cache = bool(bypass)
        if ttl_seconds is not None:
            self._persistent_price_cache_ttl_seconds = float(ttl_seconds)
        layer = self._cache_stats.setdefault("hotelPrice", {})
        layer["enabled"] = price_cache is not None and self._persistent_price_cache_ttl_seconds > 0
        layer["bypassed"] = bool(bypass)

    def set_persistent_candidate_cache(
        self,
        candidate_cache: Any | None,
        *,
        bypass: bool = False,
        ttl_seconds: float | None = None,
    ) -> None:
        self._persistent_candidate_cache = candidate_cache
        self._bypass_persistent_candidate_cache = bool(bypass)
        if ttl_seconds is not None:
            self._persistent_candidate_cache_ttl_seconds = float(ttl_seconds)
        layer = self._cache_stats.setdefault("candidateMetadata", {})
        layer["enabled"] = candidate_cache is not None and self._persistent_candidate_cache_ttl_seconds > 0
        layer["bypassed"] = bool(bypass)

    def _persistent_candidate_cache_available(self) -> bool:
        return bool(
            self._persistent_candidate_cache is not None
            and not self._bypass_persistent_candidate_cache
            and self._persistent_candidate_cache_ttl_seconds > 0
        )

    def _candidate_metadata_target_key(self, target_hotel: dict[str, Any], radius_km: float, min_star: float) -> str:
        payload = {
            "cityId": int(target_hotel.get("cityId") or 0),
            "hotelId": str(target_hotel.get("hotelId") or ""),
            "searchType": str(target_hotel.get("searchType") or ""),
            "searchValue": str(target_hotel.get("searchValue") or ""),
            "lat": round(float(target_hotel.get("latitude") or 0), 5),
            "lon": round(float(target_hotel.get("longitude") or 0), 5),
            "name": normalize_name(str(target_hotel.get("hotelName") or "")),
            "radiusKm": round(float(radius_km), 1),
            "minStar": round(float(min_star), 1),
        }
        return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    def _candidate_metadata_payload(self, hotel: dict[str, Any]) -> dict[str, Any]:
        allowed_keys = (
            "hotelId",
            "hotelName",
            "hotelOriginalName",
            "hotelNameSimplified",
            "hotelNameSource",
            "city",
            "cityId",
            "brand",
            "group",
            "starRating",
            "latitude",
            "longitude",
            "distanceKm",
            "imageUrl",
            "rating",
            "reviewCount",
            "source",
        )
        item = {key: copy_value for key in allowed_keys if (copy_value := hotel.get(key)) not in (None, "")}
        item["currentPrice"] = None
        item["priceDate"] = ""
        item["priceIncludesTax"] = False
        item["priceSource"] = ""
        return item

    def _candidate_metadata_for_date(self, hotel: dict[str, Any], selected_date: str) -> dict[str, Any]:
        item = dict(hotel)
        item["currentPrice"] = None
        item["priceDate"] = ""
        item["priceIncludesTax"] = False
        item["priceSource"] = ""
        item.pop("visiblePrice", None)
        item.pop("visiblePriceDate", None)
        item.pop("currentPricePreview", None)
        hotel_id = str(item.get("hotelId") or "")
        city_id = self._hotel_city_id(item, self._last_target)
        if hotel_id and city_id:
            checkin_date = parse_date(selected_date)
            item["tripUrl"] = self._detail_url(hotel_id, city_id, checkin_date, checkin_date + dt.timedelta(days=1))
        item.setdefault("source", self.source_name)
        return item

    def _load_persistent_candidate_hotels(
        self,
        target_hotel: dict[str, Any],
        *,
        radius_km: float,
        min_star: float,
        selected_date: str,
    ) -> list[dict[str, Any]]:
        if not self._persistent_candidate_cache_available():
            return []
        target_key = self._candidate_metadata_target_key(target_hotel, radius_km, min_star)
        try:
            rows = self._persistent_candidate_cache.get(
                self.source_name,
                target_key=target_key,
                radius_km=float(radius_km),
                min_star=float(min_star),
                max_age_seconds=float(self._persistent_candidate_cache_ttl_seconds),
            )
        except Exception:
            return []
        hotels = [self._candidate_metadata_for_date(row, selected_date) for row in rows if isinstance(row, dict)]
        if hotels:
            layer = self._cache_stats.setdefault("candidateMetadata", {})
            layer["hit"] = True
            layer["hitCount"] = int(layer.get("hitCount") or 0) + len(hotels)
            layer["targetName"] = str(target_hotel.get("hotelName") or "")
        for hotel in hotels:
            hotel_id = str(hotel.get("hotelId") or "")
            hotel["candidateMetadataCacheHit"] = True
            if hotel_id:
                self._candidate_cache[hotel_id] = hotel
        return hotels

    def _store_persistent_candidate_hotels(
        self,
        target_hotel: dict[str, Any],
        *,
        radius_km: float,
        min_star: float,
        hotels: list[dict[str, Any]],
    ) -> None:
        if not self._persistent_candidate_cache_available() or not hotels:
            return
        rows = [self._candidate_metadata_payload(hotel) for hotel in hotels if hotel.get("hotelId")]
        if not rows:
            return
        layer = self._cache_stats.setdefault("candidateMetadata", {})
        layer["storedCount"] = max(int(layer.get("storedCount") or 0), len(rows))
        try:
            self._persistent_candidate_cache.store(
                self.source_name,
                target_key=self._candidate_metadata_target_key(target_hotel, radius_km, min_star),
                target_name=str(target_hotel.get("hotelName") or ""),
                radius_km=float(radius_km),
                min_star=float(min_star),
                hotels=rows,
                expires_at=time.time() + float(self._persistent_candidate_cache_ttl_seconds),
            )
        except Exception:
            return

    def resolve_target_hotel(
        self,
        city: str,
        hotel_name: str,
        target_hint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        target = self._target_from_hint(target_hint)
        if target is None:
            suggestions = self.suggest_targets(city=city, query=hotel_name, limit=10)
            if not suggestions:
                raise ProviderError("Trip.com 没有匹配到目标酒店或位置，请从输入框下方建议里选择一个结果")
            target = self._default_target_from_suggestions(suggestions, hotel_name)
        target = dict(target)
        target["matchedBy"] = self.source_name
        self._last_target = target
        self._last_search_targets = [target]
        return target

    def _default_target_from_suggestions(self, suggestions: list[dict[str, Any]], query: str) -> dict[str, Any]:
        query_norm = normalize_name(query)
        hotel_intent = bool(
            re.search(
                (
                    r"酒店|大酒店|宾馆|公寓|度假村|客栈|民宿|hotel|inn|resort|"
                    r"hilton|marriott|accor|wyndham|ihg|hyatt|shangri|kempinski|"
                    r"希尔顿|万豪|雅高|温德姆|洲际|皇冠假日|凯悦|香格里拉|凯宾斯基"
                ),
                simplify_chinese_text(query).lower(),
            )
        )
        if not hotel_intent and query_norm:
            for item in suggestions:
                if str(item.get("searchType") or "").upper() == "H":
                    continue
                name_norm = normalize_name(item.get("hotelName") or "")
                if name_norm and (query_norm == name_norm or query_norm in name_norm or name_norm in query_norm):
                    return item
        return suggestions[0]

    def suggest_targets(self, city: str, query: str, limit: int = 8) -> list[dict[str, Any]]:
        city = str(city or "").strip()
        query = str(query or "").strip()
        if not city or not query:
            return []

        query_variants = self._suggestion_queries(city, query)
        targets: list[dict[str, Any]] = []
        seen: set[str] = set()
        for keyword in query_variants:
            rows = self._keyword_search(keyword)
            for row in rows:
                target = self._keyword_result_to_target(row, fallback_city=city)
                if target is None:
                    continue
                key = f"{target.get('searchType')}:{target.get('hotelId')}"
                if key in seen:
                    continue
                seen.add(key)
                target["matchScore"] = self._suggestion_score(target, city=city, query=query, matched_query=keyword)
                target["matchedQuery"] = keyword
                target["resultTypeLabel"] = RESULT_TYPE_LABELS.get(str(target.get("searchType") or ""), "位置")
                targets.append(target)

            if self._has_confident_suggestion(targets, query=query, limit=limit):
                break

        targets.sort(key=lambda item: item.get("matchScore") or 0, reverse=True)
        return [self._public_target_payload(item) for item in targets[:limit]]

    def _has_confident_suggestion(self, targets: list[dict[str, Any]], *, query: str, limit: int) -> bool:
        if len(targets) >= limit:
            return True
        query_norm = normalize_name(query)
        if not query_norm:
            return False
        for target in targets:
            names = [
                normalize_name(target.get("hotelName") or ""),
                normalize_name(target.get("hotelOriginalName") or ""),
                normalize_name(target.get("hotelNameSimplified") or ""),
            ]
            if query_norm in names:
                return True
        return False

    def _suggestion_queries(self, city: str, query: str) -> list[str]:
        values = [
            f"{city} {query}",
            f"{city}{query}" if not query.startswith(city) else query,
            query,
        ]
        simplified_query = simplify_chinese_text(query)
        if simplified_query != query:
            values.extend([f"{city} {simplified_query}", simplified_query])

        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            text = re.sub(r"\s+", " ", str(value or "").strip())
            if text and text not in seen:
                seen.add(text)
                result.append(text)
        return result

    def _suggestion_score(self, target: dict[str, Any], *, city: str, query: str, matched_query: str) -> int:
        search_type = str(target.get("searchType") or "")
        type_score = {"H": 50, "LM": 38, "D": 30, "Z": 24, "CT": 16}.get(search_type, 0)
        title = normalize_name(target.get("hotelName") or "")
        original = normalize_name(target.get("hotelOriginalName") or "")
        haystack = f"{title} {original}"
        query_norm = normalize_name(query)
        city_norm = normalize_name(city)
        item_city = normalize_name(target.get("city") or "")
        score = type_score
        if city_norm and city_norm in item_city:
            score += 18
        if query_norm and query_norm == title:
            score += 28
        elif query_norm and (query_norm in haystack or title in query_norm):
            score += 18
        if normalize_name(matched_query) == normalize_name(f"{city}{query}"):
            score += 4
        return score - min(len(title), 80) // 8

    def _public_target_payload(self, target: dict[str, Any]) -> dict[str, Any]:
        return {
            "hotelId": str(target.get("hotelId") or ""),
            "hotelName": target.get("hotelName") or "",
            "hotelOriginalName": target.get("hotelOriginalName") or "",
            "hotelNameSimplified": target.get("hotelNameSimplified") or "",
            "hotelNameSource": target.get("hotelNameSource") or "",
            "city": target.get("city") or "",
            "cityId": int(target.get("cityId") or 0),
            "provinceId": int(target.get("provinceId") or 0),
            "countryId": int(target.get("countryId") or 0),
            "latitude": float(target.get("latitude") or 0),
            "longitude": float(target.get("longitude") or 0),
            "searchType": target.get("searchType") or "H",
            "searchValue": target.get("searchValue") or "",
            "searchCoordinate": target.get("searchCoordinate") or "",
            "resultTypeLabel": target.get("resultTypeLabel") or RESULT_TYPE_LABELS.get(str(target.get("searchType") or ""), "位置"),
            "matchedQuery": target.get("matchedQuery") or "",
        }

    def _target_from_hint(self, target_hint: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(target_hint, dict):
            return None
        hotel_id = str(target_hint.get("hotelId") or target_hint.get("code") or "").strip()
        hotel_name = str(target_hint.get("hotelName") or target_hint.get("name") or "").strip()
        search_type = str(target_hint.get("searchType") or target_hint.get("resultType") or "").strip().upper()
        if not hotel_id or not hotel_name or search_type not in RESULT_TYPE_LABELS:
            return None
        payload = self._public_target_payload({**target_hint, "hotelId": hotel_id, "hotelName": hotel_name, "searchType": search_type})
        return payload

    def get_nearby_hotels(
        self,
        target_hotel: dict[str, Any],
        radius_km: float = 3,
        min_star: float = 4,
        selected_date: str | None = None,
        fast_mode: bool = False,
        progress_callback: Any | None = None,
    ) -> list[dict[str, Any]]:
        if not selected_date:
            selected_date = dt.date.today().isoformat()
        supplemental_targets = self._supplemental_search_targets(target_hotel)
        fallback_targets: list[dict[str, Any]] = []
        target_type = str(target_hotel.get("searchType") or "H").upper()
        if target_type == "H":
            fallback_targets = self._fallback_search_targets(target_hotel)
            search_targets = supplemental_targets or fallback_targets or [target_hotel]
        else:
            search_targets = [target_hotel, *supplemental_targets]

        search_targets = self._dedupe_search_targets(search_targets)[:3]
        cached_hotels = self._load_persistent_candidate_hotels(
            target_hotel,
            radius_km=radius_km,
            min_star=min_star,
            selected_date=selected_date,
        )
        if cached_hotels and progress_callback:
            cached_hotels.sort(key=lambda item: (float(item.get("distanceKm") or 999), -float(item.get("starRating") or 0)))
            progress_callback(list(cached_hotels))
        if (
            cached_hotels
            and fast_mode
            and not self._needs_detail_seed_fallback(cached_hotels, fast_mode=True)
        ):
            self._last_search_targets = search_targets[:3]
            return cached_hotels

        hotels, used_search_targets = self._fetch_search_targets_for_date(
            search_targets,
            selected_date,
            target_hotel=target_hotel,
            radius_km=radius_km,
            min_star=min_star,
            fast_mode=fast_mode,
            progress_callback=progress_callback,
        )
        if cached_hotels:
            hotels = self._merge_hotel_lists(cached_hotels, hotels)

        filtered = self._filter_nearby_hotels(
            hotels=hotels,
            target_hotel=target_hotel,
            radius_km=radius_km,
            min_star=min_star,
            selected_date=selected_date,
        )

        if not filtered:
            retry_targets = self._retry_search_targets(
                target_hotel,
                fallback_targets=fallback_targets,
                fast_mode=fast_mode,
            )
            tried = {self._search_target_key(item) for item in search_targets}
            retry_targets = [item for item in retry_targets if self._search_target_key(item) not in tried]
            if retry_targets:
                retry_hotels, retry_used_targets = self._fetch_search_targets_for_date(
                    self._dedupe_search_targets(retry_targets)[: (1 if fast_mode else 2)],
                    selected_date,
                    target_hotel=target_hotel,
                    radius_km=radius_km,
                    min_star=min_star,
                    fast_mode=fast_mode,
                    progress_callback=progress_callback,
                )
                hotels = self._merge_hotel_lists(hotels, retry_hotels)
                used_search_targets.extend(retry_used_targets)
                filtered = self._filter_nearby_hotels(
                    hotels=hotels,
                    target_hotel=target_hotel,
                    radius_km=radius_km,
                    min_star=min_star,
                    selected_date=selected_date,
                )

        can_use_detail_seed = str(target_hotel.get("matchedBy") or "") == self.source_name
        if selected_date and can_use_detail_seed and self._needs_detail_seed_fallback(filtered, fast_mode=fast_mode):
            detail_hotels = self._fetch_detail_seed_candidates(
                target_hotel,
                selected_date,
                radius_km=max(radius_km, 10 if fast_mode else radius_km),
                min_star=min_star,
                fast_mode=fast_mode,
                progress_callback=progress_callback,
            )
            if detail_hotels:
                hotels = self._merge_hotel_lists(hotels, detail_hotels)
                filtered = self._filter_nearby_hotels(
                    hotels=hotels,
                    target_hotel=target_hotel,
                    radius_km=radius_km,
                    min_star=min_star,
                    selected_date=selected_date,
                )
                if not filtered and fast_mode:
                    filtered = self._filter_nearby_hotels(
                        hotels=hotels,
                        target_hotel=target_hotel,
                        radius_km=10,
                        min_star=min_star,
                        selected_date=selected_date,
                    )

        self._last_search_targets = (used_search_targets or search_targets[:1])[:3]
        filtered.sort(key=lambda item: (float(item.get("distanceKm") or 999), -float(item.get("starRating") or 0)))
        self._store_persistent_candidate_hotels(
            target_hotel,
            radius_km=radius_km,
            min_star=min_star,
            hotels=filtered,
        )
        return filtered

    def _needs_detail_seed_fallback(self, filtered: list[dict[str, Any]], *, fast_mode: bool) -> bool:
        desired_count = 5 if fast_mode else 6
        if len(filtered) >= desired_count:
            return False
        recommended_count = sum(1 for hotel in filtered if detectHotelBrand(str(hotel.get("hotelName") or "")))
        return recommended_count < 3

    def _fetch_detail_seed_candidates(
        self,
        target_hotel: dict[str, Any],
        selected_date: str,
        *,
        radius_km: float,
        min_star: float,
        fast_mode: bool,
        progress_callback: Any | None = None,
    ) -> list[dict[str, Any]]:
        seeds = self._hotel_seed_targets_for_target(target_hotel, radius_km=radius_km)
        detail_hotels: list[dict[str, Any]] = []
        for seed in seeds[: (2 if fast_mode else 3)]:
            try:
                fetched_hotels = self._fetch_hotel_detail_context_for_date(seed, selected_date)
            except ProviderError:
                continue
            detail_hotels = self._merge_hotel_lists(detail_hotels, fetched_hotels)
            filtered = self._filter_nearby_hotels(
                hotels=detail_hotels,
                target_hotel=target_hotel,
                radius_km=radius_km,
                min_star=min_star,
                selected_date=selected_date,
            )
            if progress_callback and filtered:
                filtered.sort(key=lambda item: (float(item.get("distanceKm") or 999), -float(item.get("starRating") or 0)))
                progress_callback(list(filtered))
            if (fast_mode and len(filtered) >= 4) or (not fast_mode and len(filtered) >= 6):
                break
        return detail_hotels

    def _hotel_seed_targets_for_target(self, target: dict[str, Any], *, radius_km: float) -> list[dict[str, Any]]:
        city = str(target.get("city") or "").strip()
        target_name = str(target.get("hotelName") or "").strip()
        if not city or not target_name:
            return []
        keywords = self._suggestion_queries(city, f"{target_name}酒店")
        seeds: list[dict[str, Any]] = []
        seen: set[str] = set()
        for keyword in keywords:
            try:
                rows = self._keyword_search(keyword)
            except ProviderError:
                continue
            for row in rows:
                seed = self._keyword_result_to_target(row, fallback_city=city)
                if seed is None or str(seed.get("searchType") or "").upper() != "H":
                    continue
                key = str(seed.get("hotelId") or "")
                if not key or key in seen:
                    continue
                if not self._target_within_radius(seed, target, radius_km=radius_km):
                    continue
                seen.add(key)
                seed["distanceKm"] = round(
                    haversine_km(
                        float(target.get("latitude") or 0),
                        float(target.get("longitude") or 0),
                        float(seed.get("latitude") or 0),
                        float(seed.get("longitude") or 0),
                    ),
                    2,
                )
                seeds.append(seed)
            if len(seeds) >= 4:
                break
        seeds.sort(key=lambda item: (float(item.get("distanceKm") or 999), int((detectHotelBrand(item.get("hotelName") or "") or {}).get("brandRank") or 99)))
        return seeds

    def _target_within_radius(self, seed: dict[str, Any], target: dict[str, Any], *, radius_km: float) -> bool:
        if not seed.get("latitude") or not seed.get("longitude") or not target.get("latitude") or not target.get("longitude"):
            return True
        distance = haversine_km(
            float(target.get("latitude") or 0),
            float(target.get("longitude") or 0),
            float(seed.get("latitude") or 0),
            float(seed.get("longitude") or 0),
        )
        return distance <= radius_km

    def _fetch_search_targets_for_date(
        self,
        search_targets: list[dict[str, Any]],
        selected_date: str,
        *,
        target_hotel: dict[str, Any] | None = None,
        radius_km: float | None = None,
        min_star: float | None = None,
        fast_mode: bool = False,
        progress_callback: Any | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        used_search_targets: list[dict[str, Any]] = []
        hotels: list[dict[str, Any]] = []
        first_error: ProviderError | None = None
        last_progress_signature: tuple[str, ...] = ()

        def publish_candidate_progress(candidate_hotels: list[dict[str, Any]]) -> None:
            nonlocal last_progress_signature
            if (
                not progress_callback
                or target_hotel is None
                or radius_km is None
                or min_star is None
            ):
                return
            filtered = self._filter_nearby_hotels(
                hotels=candidate_hotels,
                target_hotel=target_hotel,
                radius_km=radius_km,
                min_star=min_star,
                selected_date=selected_date,
            )
            if not filtered:
                return
            filtered.sort(key=lambda item: (float(item.get("distanceKm") or 999), -float(item.get("starRating") or 0)))
            signature = tuple(str(item.get("hotelId") or item.get("hotelName") or "") for item in filtered)
            if signature == last_progress_signature:
                return
            last_progress_signature = signature
            progress_callback(list(filtered))

        def absorb_fetched_hotels(fetched_items: list[dict[str, Any]]) -> None:
            nonlocal hotels
            if not fetched_items:
                return
            hotels = self._merge_hotel_lists(hotels, fetched_items)
            publish_candidate_progress(hotels)

        try:
            fetch_signature = inspect.signature(self._fetch_hotel_list_for_date)
            fetch_supports_callback = "result_callback" in fetch_signature.parameters or any(
                param.kind == inspect.Parameter.VAR_KEYWORD
                for param in fetch_signature.parameters.values()
            )
        except (TypeError, ValueError):
            fetch_supports_callback = False

        for search_target in search_targets:
            try:
                fetch_kwargs: dict[str, Any] = {"fast_mode": fast_mode}
                if fetch_supports_callback:
                    fetch_kwargs["result_callback"] = absorb_fetched_hotels
                fetched_hotels = self._fetch_hotel_list_for_date(search_target, selected_date, **fetch_kwargs)
            except ProviderError as exc:
                if first_error is None:
                    first_error = exc
                if fast_mode and not hotels:
                    break
                continue
            if fetched_hotels:
                used_search_targets.append(search_target)
            hotels = self._merge_hotel_lists(hotels, fetched_hotels)
            publish_candidate_progress(hotels)
            if fast_mode and not fetched_hotels and not hotels:
                break
            if fast_mode and target_hotel is not None and radius_km is not None and min_star is not None:
                filtered = self._filter_nearby_hotels(
                    hotels=hotels,
                    target_hotel=target_hotel,
                    radius_km=radius_km,
                    min_star=min_star,
                    selected_date=selected_date,
                )
                if filtered and not self._needs_detail_seed_fallback(filtered, fast_mode=True):
                    break
        if first_error is not None and not hotels and not fast_mode:
            raise first_error
        return hotels, used_search_targets

    def _filter_nearby_hotels(
        self,
        *,
        hotels: list[dict[str, Any]],
        target_hotel: dict[str, Any],
        radius_km: float,
        min_star: float,
        selected_date: str,
    ) -> list[dict[str, Any]]:
        target_lat = float(target_hotel.get("latitude") or 0)
        target_lon = float(target_hotel.get("longitude") or 0)
        target_id = str(target_hotel.get("hotelId") or "")
        filtered: list[dict[str, Any]] = []

        for hotel in hotels:
            hotel_id = str(hotel.get("hotelId") or "")
            if hotel_id and hotel_id == target_id:
                continue
            if float(hotel.get("starRating") or 0) < min_star:
                continue
            lat = hotel.get("latitude")
            lon = hotel.get("longitude")
            if lat is None or lon is None:
                if hotel.get("distanceKm") in (None, ""):
                    continue
                distance = float(hotel.get("distanceKm") or 999)
            else:
                distance = haversine_km(target_lat, target_lon, float(lat), float(lon))
            if distance <= radius_km:
                item = dict(hotel)
                item["distanceKm"] = round(distance, 2)
                filtered.append(item)
                self._candidate_cache[hotel_id] = item
                if self._hotel_price_matches_date(hotel, selected_date):
                    self._price_cache.setdefault(hotel_id, {})[selected_date] = hotel.get("currentPrice")
                    if hotel.get("priceIncludesTax") is True:
                        self._store_price_if_available(hotel_id, selected_date, hotel.get("currentPrice"))
        return filtered

    def _hotel_price_matches_date(self, hotel: dict[str, Any], selected_date: str) -> bool:
        if hotel.get("currentPrice") in (None, ""):
            return False
        if hotel.get("priceIncludesTax") is False:
            return False
        price_date = str(hotel.get("priceDate") or hotel.get("selectedDate") or "").strip()
        return not price_date or price_date == selected_date

    def get_cached_nearby_hotels(
        self,
        target_hotel: dict[str, Any],
        radius_km: float = 3,
        min_star: float = 4,
        selected_date: str | None = None,
    ) -> list[dict[str, Any]]:
        target_lat = float(target_hotel.get("latitude") or 0)
        target_lon = float(target_hotel.get("longitude") or 0)
        target_id = str(target_hotel.get("hotelId") or "")
        filtered: list[dict[str, Any]] = []
        for hotel in self._candidate_cache.values():
            hotel_id = str(hotel.get("hotelId") or "")
            if hotel_id and hotel_id == target_id:
                continue
            if float(hotel.get("starRating") or 0) < float(min_star):
                continue
            lat = hotel.get("latitude")
            lon = hotel.get("longitude")
            if lat is None or lon is None:
                if hotel.get("distanceKm") in (None, ""):
                    continue
                distance = float(hotel.get("distanceKm") or 999)
            else:
                distance = haversine_km(target_lat, target_lon, float(lat), float(lon))
            if distance <= float(radius_km):
                item = dict(hotel)
                item["distanceKm"] = round(distance, 2)
                filtered.append(item)
        filtered.sort(key=lambda item: (float(item.get("distanceKm") or 999), -float(item.get("starRating") or 0)))
        return filtered

    def get_cached_hotel_prices(self, hotel_ids: list[str], dates: list[str]) -> dict[str, dict[str, int | None]]:
        self._hydrate_persistent_price_cache(hotel_ids, dates)
        return {
            str(hotel_id): {
                date: self._price_cache.get(str(hotel_id), {}).get(date)
                for date in dates
                if self._cached_price_available(str(hotel_id), date)
            }
            for hotel_id in hotel_ids
        }

    def _cached_price_available(self, hotel_id: str, date_value: str) -> bool:
        return self._price_cache.get(str(hotel_id), {}).get(date_value) not in (None, "")

    def _store_price_if_available(self, hotel_id: str, date_value: str, price: Any) -> None:
        if price not in (None, ""):
            hotel_id_str = str(hotel_id)
            self._price_cache.setdefault(hotel_id_str, {})[date_value] = price
            layer = self._cache_stats.setdefault("hotelPrice", {})
            layer["storedCount"] = int(layer.get("storedCount") or 0) + 1
            if self._persistent_price_cache_available(write=True):
                try:
                    self._persistent_price_cache.store_price(
                        self.source_name,
                        hotel_id=hotel_id_str,
                        price_date=str(date_value),
                        current_price=price,
                        expires_at=time.time() + float(self._persistent_price_cache_ttl_seconds),
                    )
                except Exception:
                    pass

    def _persistent_price_cache_available(self, *, write: bool = False) -> bool:
        cache = self._persistent_price_cache
        if cache is None:
            return False
        if self._persistent_price_cache_ttl_seconds <= 0:
            return False
        if self._bypass_persistent_price_cache and not write:
            return False
        return True

    def _hydrate_persistent_price_cache(self, hotel_ids: list[str], dates: list[str]) -> None:
        if not self._persistent_price_cache_available():
            return
        missing_hotel_ids = [
            str(hotel_id)
            for hotel_id in hotel_ids
            if str(hotel_id or "").strip()
            and any(not self._cached_price_available(str(hotel_id), date_value) for date_value in dates)
        ]
        if not missing_hotel_ids:
            return
        requested_count = len(missing_hotel_ids) * len([date for date in dates if str(date or "").strip()])
        layer = self._cache_stats.setdefault("hotelPrice", {})
        layer["requestedCount"] = int(layer.get("requestedCount") or 0) + requested_count
        try:
            cached_prices = self._persistent_price_cache.get_many(
                self.source_name,
                missing_hotel_ids,
                [str(date_value) for date_value in dates],
                max_age_seconds=float(self._persistent_price_cache_ttl_seconds),
            )
        except Exception:
            return
        if not isinstance(cached_prices, dict):
            return
        hit_count = 0
        for hotel_id, prices in cached_prices.items():
            if not isinstance(prices, dict):
                continue
            for date_value, price in prices.items():
                if price not in (None, ""):
                    self._price_cache.setdefault(str(hotel_id), {})[str(date_value)] = int(price)
                    hit_count += 1
        if hit_count:
            layer["hit"] = True
            layer["hitCount"] = int(layer.get("hitCount") or 0) + hit_count

    def _resolved_name_payload_from_sources(
        self,
        sources: list[Any],
        *,
        hotel_id: str,
        source: str,
    ) -> dict[str, str] | None:
        cleaned = [
            str(value or "").strip()
            for value in sources
            if str(value or "").strip() and not has_name_verification_marker(value)
        ]
        if not cleaned:
            return None
        payload = hotel_name_payload_from_sources(cleaned, hotel_id=hotel_id, source=source)
        name = simplify_chinese_text(payload.get("hotelNameSimplified") or payload.get("hotelName") or "")
        if name and contains_chinese_text(name) and not has_name_verification_marker(name):
            return payload
        return None

    def verify_hotel_names(
        self,
        hotels: list[dict[str, Any]],
        selected_date: str,
        progress_callback: Any | None = None,
        lightweight_only: bool = False,
    ) -> dict[str, dict[str, str]]:
        resolved: dict[str, dict[str, str]] = {}
        total = len(hotels)
        for index, hotel in enumerate(hotels, start=1):
            hotel_id = str(hotel.get("hotelId") or "").strip()
            if not hotel_id:
                continue
            if progress_callback:
                progress_callback(
                    {
                        "phase": "start",
                        "hotelId": hotel_id,
                        "completed": index - 1,
                        "total": total,
                        "resolvedCount": len(resolved),
                    }
                )

            cached = self._candidate_cache.get(hotel_id) or {}
            payload = self._resolved_name_payload_from_sources(
                [
                    cached.get("hotelName"),
                    cached.get("hotelNameSimplified"),
                    cached.get("hotelOriginalName"),
                    hotel.get("hotelOriginalName"),
                    hotel.get("hotelNameSimplified"),
                    hotel.get("hotelName"),
                ],
                hotel_id=hotel_id,
                source=str(cached.get("hotelNameSource") or hotel.get("hotelNameSource") or "Trip.com 中文名核验"),
            )

            if payload is None and not lightweight_only:
                payload = self._fetch_detail_page_name_payload(hotel, selected_date)

            if payload is None:
                payload = self._fetch_ctrip_name_payload(hotel, selected_date)

            if payload is None:
                payload = self._fetch_elong_name_payload(hotel, selected_date)

            if payload is None:
                payload = self._fetch_map_poi_name_payload(hotel)

            if payload is None:
                payload = self._fetch_search_engine_name_payload(hotel)

            if payload:
                resolved[hotel_id] = payload
                if progress_callback:
                    progress_callback(
                        {
                            "phase": "resolved",
                            "hotelId": hotel_id,
                            "completed": index,
                            "total": total,
                            "resolvedCount": len(resolved),
                            "payload": payload,
                        }
                    )
            elif progress_callback:
                progress_callback(
                    {
                        "phase": "unresolved",
                        "hotelId": hotel_id,
                        "completed": index,
                        "total": total,
                        "resolvedCount": len(resolved),
                    }
                )
        if progress_callback:
            progress_callback(
                {
                    "phase": "complete",
                    "completed": total,
                    "total": total,
                    "resolvedCount": len(resolved),
                }
            )
        return resolved

    def get_hotel_prices(
        self,
        hotel_ids: list[str],
        dates: list[str],
        progress_callback: Any | None = None,
    ) -> dict[str, dict[str, int | None]]:
        if not self._last_target:
            raise ProviderError("Trip.com Provider 还没有目标酒店上下文")

        self._hydrate_persistent_price_cache(hotel_ids, dates)
        search_targets = self._last_search_targets or [self._last_target]
        for date_index, date_value in enumerate(dates, start=1):
            if progress_callback:
                self._publish_price_progress(
                    progress_callback,
                    hotel_ids=hotel_ids,
                    dates=dates,
                    date_value=date_value,
                    date_index=date_index,
                    completed_dates=date_index - 1,
                    phase="start",
                )
            date_missing = any(not self._cached_price_available(str(hotel_id), date_value) for hotel_id in hotel_ids)
            if not date_missing:
                if progress_callback:
                    self._publish_price_progress(
                        progress_callback,
                        hotel_ids=hotel_ids,
                        dates=dates,
                        date_value=date_value,
                        date_index=date_index,
                        completed_dates=date_index,
                        phase="complete",
                    )
                continue

            hotels_for_date: list[dict[str, Any]] = []
            for search_target in search_targets:
                if not search_target:
                    continue
                try:
                    fetched_hotels = self._fetch_hotel_list_for_date(search_target, date_value, fast_mode=True)
                except ProviderError:
                    continue
                hotels_for_date = self._merge_hotel_lists(hotels_for_date, fetched_hotels)
            for hotel in hotels_for_date:
                hotel_id = str(hotel.get("hotelId") or "")
                if not hotel_id:
                    continue
                self._candidate_cache[hotel_id] = hotel
                if self._hotel_price_matches_date(hotel, date_value):
                    self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))
            still_missing = [
                str(hotel_id)
                for hotel_id in hotel_ids
                if not self._cached_price_available(str(hotel_id), date_value)
            ]
            if progress_callback:
                self._publish_price_progress(
                    progress_callback,
                    hotel_ids=hotel_ids,
                    dates=dates,
                    date_value=date_value,
                    date_index=date_index,
                    completed_dates=date_index - 1,
                    phase="list",
                )
            if still_missing:
                if self._should_run_list_dom_backfill():
                    if progress_callback:
                        self._publish_price_progress(
                            progress_callback,
                            hotel_ids=hotel_ids,
                            dates=dates,
                            date_value=date_value,
                            date_index=date_index,
                            completed_dates=date_index - 1,
                            phase="dom-list",
                        )
                    dom_hotels = self._fetch_list_dom_prices_for_missing(date_value, still_missing)
                    for hotel in dom_hotels:
                        hotel_id = str(hotel.get("hotelId") or "")
                        if not hotel_id:
                            continue
                        self._candidate_cache[hotel_id] = hotel
                        if self._hotel_price_matches_date(hotel, date_value):
                            self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))
                    still_missing = [
                        str(hotel_id)
                        for hotel_id in hotel_ids
                        if not self._cached_price_available(str(hotel_id), date_value)
                    ]
            if still_missing:
                if progress_callback:
                    self._publish_price_progress(
                        progress_callback,
                        hotel_ids=hotel_ids,
                        dates=dates,
                        date_value=date_value,
                        date_index=date_index,
                        completed_dates=date_index - 1,
                        phase="detail",
                    )
                detail_hotels = self._fetch_detail_prices_for_missing(date_value, still_missing)
                for hotel in detail_hotels:
                    hotel_id = str(hotel.get("hotelId") or "")
                    if not hotel_id:
                        continue
                    self._candidate_cache[hotel_id] = hotel
                    self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))

            still_missing = [
                str(hotel_id)
                for hotel_id in hotel_ids
                if not self._cached_price_available(str(hotel_id), date_value)
            ]
            if still_missing and self._deep_list_fallback_enabled():
                if progress_callback:
                    self._publish_price_progress(
                        progress_callback,
                        hotel_ids=hotel_ids,
                        dates=dates,
                        date_value=date_value,
                        date_index=date_index,
                        completed_dates=date_index - 1,
                        phase="deep",
                    )
                deep_hotels: list[dict[str, Any]] = []
                for search_target in search_targets[:1]:
                    if not search_target:
                        continue
                    try:
                        fetched_hotels = self._fetch_hotel_list_for_date(search_target, date_value, deep_mode=True)
                    except ProviderError:
                        continue
                    deep_hotels = self._merge_hotel_lists(deep_hotels, fetched_hotels)
                for hotel in deep_hotels:
                    hotel_id = str(hotel.get("hotelId") or "")
                    if not hotel_id:
                        continue
                    self._candidate_cache[hotel_id] = hotel
                    if self._hotel_price_matches_date(hotel, date_value):
                        self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))

            if progress_callback:
                self._publish_price_progress(
                    progress_callback,
                    hotel_ids=hotel_ids,
                    dates=dates,
                    date_value=date_value,
                    date_index=date_index,
                    completed_dates=date_index,
                    phase="complete",
                )

        return {
            str(hotel_id): {
                date: self._price_cache.get(str(hotel_id), {}).get(date)
                for date in dates
            }
            for hotel_id in hotel_ids
        }

    def get_hotel_prices_for_dates_parallel(
        self,
        hotel_ids: list[str],
        dates: list[str],
        progress_callback: Any | None = None,
    ) -> dict[str, dict[str, int | None]]:
        if len(dates) <= 1 or not self._parallel_date_price_enabled():
            return self.get_hotel_prices(hotel_ids, dates, progress_callback=progress_callback)
        if not self._last_target:
            raise ProviderError("Trip.com Provider 还没有目标酒店上下文")

        hotel_ids = [str(hotel_id) for hotel_id in hotel_ids if str(hotel_id or "").strip()]
        dates = [str(date_value) for date_value in dates if str(date_value or "").strip()]
        self._hydrate_persistent_price_cache(hotel_ids, dates)
        search_targets = self._last_search_targets or [self._last_target]

        for date_index, date_value in enumerate(dates, start=1):
            if progress_callback:
                self._publish_price_progress(
                    progress_callback,
                    hotel_ids=hotel_ids,
                    dates=dates,
                    date_value=date_value,
                    date_index=date_index,
                    completed_dates=date_index - 1,
                    phase="start",
                )
            if all(self._cached_price_available(str(hotel_id), date_value) for hotel_id in hotel_ids):
                if progress_callback:
                    self._publish_price_progress(
                        progress_callback,
                        hotel_ids=hotel_ids,
                        dates=dates,
                        date_value=date_value,
                        date_index=date_index,
                        completed_dates=date_index,
                        phase="complete",
                    )
                continue

            hotels_for_date: list[dict[str, Any]] = []
            for search_target in search_targets:
                if not search_target:
                    continue
                try:
                    fetched_hotels = self._fetch_hotel_list_for_date(search_target, date_value, fast_mode=True)
                except ProviderError:
                    continue
                hotels_for_date = self._merge_hotel_lists(hotels_for_date, fetched_hotels)
            for hotel in hotels_for_date:
                hotel_id = str(hotel.get("hotelId") or "")
                if not hotel_id:
                    continue
                self._candidate_cache[hotel_id] = hotel
                if self._hotel_price_matches_date(hotel, date_value):
                    self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))
            if progress_callback:
                self._publish_price_progress(
                    progress_callback,
                    hotel_ids=hotel_ids,
                    dates=dates,
                    date_value=date_value,
                    date_index=date_index,
                    completed_dates=date_index - 1,
                    phase="list",
                )

        missing_by_date = {
            date_value: [
                str(hotel_id)
                for hotel_id in hotel_ids
                if not self._cached_price_available(str(hotel_id), date_value)
            ]
            for date_value in dates
        }
        missing_by_date = {date_value: missing for date_value, missing in missing_by_date.items() if missing}
        if missing_by_date and self._should_run_list_dom_backfill():
            if progress_callback:
                for date_index, date_value in enumerate(dates, start=1):
                    if date_value in missing_by_date:
                        self._publish_price_progress(
                            progress_callback,
                            hotel_ids=hotel_ids,
                            dates=dates,
                            date_value=date_value,
                            date_index=date_index,
                            completed_dates=date_index - 1,
                            phase="dom-list",
                        )
            dom_results = self._fetch_list_dom_prices_for_missing_dates(missing_by_date)
            for date_value, dom_hotels in dom_results.items():
                for hotel in dom_hotels:
                    hotel_id = str(hotel.get("hotelId") or "")
                    if not hotel_id:
                        continue
                    self._candidate_cache[hotel_id] = hotel
                    if self._hotel_price_matches_date(hotel, date_value):
                        self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))
                if progress_callback:
                    date_index = dates.index(date_value) + 1
                    self._publish_price_progress(
                        progress_callback,
                        hotel_ids=hotel_ids,
                        dates=dates,
                        date_value=date_value,
                        date_index=date_index,
                        completed_dates=date_index - 1,
                        phase="dom-list",
                    )

        for date_index, date_value in enumerate(dates, start=1):
            still_missing = [
                str(hotel_id)
                for hotel_id in hotel_ids
                if not self._cached_price_available(str(hotel_id), date_value)
            ]
            if still_missing:
                if progress_callback:
                    self._publish_price_progress(
                        progress_callback,
                        hotel_ids=hotel_ids,
                        dates=dates,
                        date_value=date_value,
                        date_index=date_index,
                        completed_dates=date_index - 1,
                        phase="detail",
                    )
                detail_hotels = self._fetch_detail_prices_for_missing(date_value, still_missing)
                for hotel in detail_hotels:
                    hotel_id = str(hotel.get("hotelId") or "")
                    if not hotel_id:
                        continue
                    self._candidate_cache[hotel_id] = hotel
                    self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))

            still_missing = [
                str(hotel_id)
                for hotel_id in hotel_ids
                if not self._cached_price_available(str(hotel_id), date_value)
            ]
            if still_missing and self._deep_list_fallback_enabled():
                if progress_callback:
                    self._publish_price_progress(
                        progress_callback,
                        hotel_ids=hotel_ids,
                        dates=dates,
                        date_value=date_value,
                        date_index=date_index,
                        completed_dates=date_index - 1,
                        phase="deep",
                    )
                deep_hotels: list[dict[str, Any]] = []
                for search_target in search_targets[:1]:
                    if not search_target:
                        continue
                    try:
                        fetched_hotels = self._fetch_hotel_list_for_date(search_target, date_value, deep_mode=True)
                    except ProviderError:
                        continue
                    deep_hotels = self._merge_hotel_lists(deep_hotels, fetched_hotels)
                for hotel in deep_hotels:
                    hotel_id = str(hotel.get("hotelId") or "")
                    if not hotel_id:
                        continue
                    self._candidate_cache[hotel_id] = hotel
                    if self._hotel_price_matches_date(hotel, date_value):
                        self._store_price_if_available(hotel_id, date_value, hotel.get("currentPrice"))

            if progress_callback:
                self._publish_price_progress(
                    progress_callback,
                    hotel_ids=hotel_ids,
                    dates=dates,
                    date_value=date_value,
                    date_index=date_index,
                    completed_dates=date_index,
                    phase="complete",
                )

        return {
            str(hotel_id): {
                date: self._price_cache.get(str(hotel_id), {}).get(date)
                for date in dates
            }
            for hotel_id in hotel_ids
        }

    def _publish_price_progress(
        self,
        progress_callback: Any,
        *,
        hotel_ids: list[str],
        dates: list[str],
        date_value: str,
        date_index: int,
        completed_dates: int,
        phase: str,
    ) -> None:
        total_hotels = len(hotel_ids)
        priced_count = sum(
            1
            for hotel_id in hotel_ids
            if self._price_cache.get(str(hotel_id), {}).get(date_value) not in (None, "")
        )
        total_known_price_count = sum(
            1
            for hotel_id in hotel_ids
            for item_date in dates
            if self._price_cache.get(str(hotel_id), {}).get(item_date) not in (None, "")
        )
        progress_callback(
            {
                "stage": "compare-price",
                "phase": phase,
                "date": date_value,
                "dateIndex": date_index,
                "completedDates": completed_dates,
                "totalDates": len(dates),
                "pricedHotelCount": priced_count,
                "missingHotelCount": max(total_hotels - priced_count, 0),
                "totalHotels": total_hotels,
                "totalKnownPriceCount": total_known_price_count,
                "totalExpectedPriceCount": total_hotels * len(dates),
            }
        )

    def _deep_list_fallback_enabled(self) -> bool:
        return str(os.environ.get("HOTEL_DEAL_ENABLE_DEEP_LIST_FALLBACK") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def _parallel_date_price_enabled(self) -> bool:
        return str(os.environ.get("HOTEL_DEAL_ENABLE_PARALLEL_DATE_PRICES") or "1").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }

    def _list_dom_backfill_enabled(self) -> bool:
        value = str(os.environ.get("HOTEL_DEAL_ENABLE_LIST_DOM_BACKFILL") or "1").strip().lower()
        return value not in {"0", "false", "no", "off"}

    def _hotel_list_fetcher_overridden(self) -> bool:
        return "_fetch_hotel_list_for_date" in self.__dict__

    def _should_run_list_dom_backfill(self) -> bool:
        if not self._list_dom_backfill_enabled():
            return False
        if (
            self._hotel_list_fetcher_overridden()
            and "_fetch_list_dom_prices_for_missing" not in self.__dict__
            and "_fetch_list_dom_prices_for_missing_dates" not in self.__dict__
        ):
            return False
        return True

    def _fetch_list_dom_prices_for_missing(self, date_value: str, missing_hotel_ids: list[str]) -> list[dict[str, Any]]:
        missing = {str(hotel_id) for hotel_id in missing_hotel_ids if str(hotel_id or "").strip()}
        if not missing:
            return []
        fetched: list[dict[str, Any]] = []
        search_targets = self._last_search_targets or ([self._last_target] if self._last_target else [])
        for search_target in search_targets[:2]:
            if not search_target:
                continue
            try:
                target_hotels = self._fetch_hotel_list_for_date(
                    search_target,
                    date_value,
                    deep_mode=True,
                    price_backfill_mode=True,
                    wanted_hotel_ids=missing,
                )
            except ProviderError:
                continue
            fetched = self._merge_hotel_lists(fetched, target_hotels)
            fetched_ids = {
                str(hotel.get("hotelId") or "")
                for hotel in fetched
                if self._hotel_price_matches_date(hotel, date_value)
            }
            if missing.issubset(fetched_ids):
                break
        return fetched

    def _fetch_list_dom_prices_for_missing_dates(
        self,
        missing_by_date: dict[str, list[str]],
    ) -> dict[str, list[dict[str, Any]]]:
        normalized = {
            str(date_value): {str(hotel_id) for hotel_id in hotel_ids if str(hotel_id or "").strip()}
            for date_value, hotel_ids in missing_by_date.items()
            if str(date_value or "").strip()
        }
        normalized = {date_value: hotel_ids for date_value, hotel_ids in normalized.items() if hotel_ids}
        if not normalized:
            return {}
        if len(normalized) == 1:
            date_value, hotel_ids = next(iter(normalized.items()))
            return {date_value: self._fetch_list_dom_prices_for_missing(date_value, list(hotel_ids))}
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
        except Exception:
            return {
                date_value: self._fetch_list_dom_prices_for_missing(date_value, list(hotel_ids))
                for date_value, hotel_ids in normalized.items()
            }
        try:
            with self._lock:
                return asyncio.run(self._fetch_list_dom_prices_for_missing_dates_async(normalized))
        except Exception:
            return {
                date_value: self._fetch_list_dom_prices_for_missing(date_value, list(hotel_ids))
                for date_value, hotel_ids in normalized.items()
            }

    async def _fetch_list_dom_prices_for_missing_dates_async(
        self,
        missing_by_date: dict[str, set[str]],
    ) -> dict[str, list[dict[str, Any]]]:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright

        results: dict[str, list[dict[str, Any]]] = {date_value: [] for date_value in missing_by_date}
        search_targets = self._last_search_targets or ([self._last_target] if self._last_target else [])
        search_targets = [target for target in search_targets[:2] if target]
        if not search_targets:
            return results
        max_pages = max(1, int(os.environ.get("HOTEL_DEAL_DATE_PRICE_FETCH_CONCURRENCY", "3")))
        semaphore = asyncio.Semaphore(max_pages)

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                user_agent=UA,
                locale="zh-CN",
                timezone_id="Asia/Shanghai",
                viewport={"width": 1440, "height": 1400},
            )

            async def route_handler(route: Any) -> None:
                if route.request.resource_type in {"image", "media", "font"}:
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", route_handler)
            await context.add_init_script(
                """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                """
            )

            async def fetch_date(date_value: str, wanted_ids: set[str]) -> tuple[str, list[dict[str, Any]]]:
                async with semaphore:
                    checkin_date = parse_date(date_value)
                    checkout_date = checkin_date + dt.timedelta(days=1)
                    response_items: list[dict[str, Any]] = []

                    def wanted_tax_prices_found(items: list[dict[str, Any]]) -> bool:
                        found_ids = {
                            str(item.get("hotelId") or "")
                            for item in self._dedupe_hotel_items(items)
                            if str(item.get("hotelId") or "") in wanted_ids
                            and item.get("currentPrice") not in (None, "")
                            and item.get("priceIncludesTax") is not False
                        }
                        return wanted_ids.issubset(found_ids)

                    async def collect_response_items(response: Any, target: dict[str, Any]) -> None:
                        response_url = str(response.url or "")
                        is_hotel_list_response = (
                            "/htls/getHotelList" in response_url
                            or "/restapi/soa2/34951/" in response_url
                            or "fetchHotelList" in response_url
                        )
                        if not is_hotel_list_response:
                            return
                        try:
                            data = await asyncio.wait_for(response.json(), timeout=3)
                        except Exception:
                            return
                        rows = self._hotel_list_rows(data)
                        for row in rows:
                            item = self._normalize_trip_hotel(row, checkin_date, checkout_date, target)
                            if item:
                                response_items.append(item)

                    for target in search_targets:
                        page = await context.new_page()
                        response_tasks: list[asyncio.Task[Any]] = []

                        def on_response(response: Any, target: dict[str, Any] = target) -> None:
                            response_tasks.append(asyncio.create_task(collect_response_items(response, target)))

                        page.on("response", on_response)
                        try:
                            url = self._build_list_url(target, checkin_date, checkout_date)
                            try:
                                await page.goto(url, wait_until="domcontentloaded", timeout=35000)
                            except PlaywrightTimeoutError:
                                continue
                            await page.wait_for_timeout(1800)
                            response_items = self._merge_hotel_lists(
                                response_items,
                                self._hotel_cards_from_html(await page.content(), checkin_date, checkout_date, target),
                            )
                            last_count = len(self._dedupe_hotel_items(response_items))
                            stale_rounds = 0
                            for scroll_index in range(18):
                                if wanted_tax_prices_found(response_items):
                                    break
                                await page.mouse.wheel(0, 1800)
                                await page.wait_for_timeout(650)
                                response_items = self._merge_hotel_lists(
                                    response_items,
                                    self._hotel_cards_from_html(await page.content(), checkin_date, checkout_date, target),
                                )
                                current_count = len(self._dedupe_hotel_items(response_items))
                                if current_count > last_count:
                                    last_count = current_count
                                    stale_rounds = 0
                                elif scroll_index >= 5:
                                    stale_rounds += 1
                                if scroll_index >= 5 and stale_rounds >= 3:
                                    break
                            if response_tasks:
                                _done, pending = await asyncio.wait(response_tasks, timeout=2)
                                for task in pending:
                                    task.cancel()
                            if wanted_tax_prices_found(response_items):
                                break
                        finally:
                            page.remove_listener("response", on_response)
                            await page.close()
                    return date_value, self._dedupe_hotel_items(response_items)

            pairs = await asyncio.gather(
                *(
                    asyncio.wait_for(fetch_date(date_value, wanted_ids), timeout=90)
                    for date_value, wanted_ids in missing_by_date.items()
                ),
                return_exceptions=True,
            )
            await browser.close()

        for pair in pairs:
            if isinstance(pair, Exception):
                continue
            date_value, items = pair
            results[date_value] = items
        return results

    def _fetch_detail_prices_for_missing(self, date_value: str, missing_hotel_ids: list[str]) -> list[dict[str, Any]]:
        fetched: list[dict[str, Any]] = []
        missing = {str(hotel_id) for hotel_id in missing_hotel_ids}
        seeds = self._detail_seed_hotels(missing_hotel_ids)
        if self._detail_fetcher_overridden():
            for seed_hotel in seeds[:2]:
                detail_hotels = self._fetch_detail_context_with_seed_retry(seed_hotel, date_value, missing)
                fetched = self._merge_hotel_lists(fetched, detail_hotels)
                fetched_ids = {str(hotel.get("hotelId") or "") for hotel in fetched}
                if missing.issubset(fetched_ids):
                    break
        else:
            detail_hotels = self._fetch_detail_contexts_with_seed_retry(seeds[:2], date_value, missing)
            fetched = self._merge_hotel_lists(fetched, detail_hotels)
        fetched_ids = {str(hotel.get("hotelId") or "") for hotel in fetched}
        remaining = missing - fetched_ids
        own_seeds = [
            seed
            for seed in seeds
            if str(seed.get("hotelId") or "") in remaining and self._should_fetch_own_detail_seed(seed)
        ]
        own_seeds.sort(
            key=lambda item: (
                int((detectHotelBrand(str(item.get("hotelName") or "")) or {}).get("brandRank") or 99),
                -float(item.get("starRating") or 0),
                float(item.get("currentPrice") or 10**9),
                float(item.get("distanceKm") or 999),
            )
        )
        if own_seeds:
            if self._detail_fetcher_overridden():
                for seed_hotel in own_seeds:
                    detail_hotels = self._fetch_detail_context_with_seed_retry(seed_hotel, date_value, missing)
                    fetched = self._merge_hotel_lists(fetched, detail_hotels)
                    fetched_ids = {str(hotel.get("hotelId") or "") for hotel in fetched}
                    if missing.issubset(fetched_ids):
                        break
            else:
                detail_hotels = self._fetch_detail_contexts_with_seed_retry(own_seeds, date_value, missing)
                fetched = self._merge_hotel_lists(fetched, detail_hotels)
        return fetched

    def _hotel_city_id(self, hotel: dict[str, Any], target: dict[str, Any] | None = None) -> int:
        for value in (hotel.get("cityId"), hotel.get("hotelCityId")):
            try:
                city_id = int(value or 0)
            except (TypeError, ValueError):
                city_id = 0
            if city_id:
                return city_id
        trip_url = str(hotel.get("tripUrl") or "")
        if trip_url:
            try:
                city_id = int((parse_qs(urlparse(trip_url).query).get("cityId") or ["0"])[0] or 0)
            except (TypeError, ValueError):
                city_id = 0
            if city_id:
                return city_id
        try:
            return int((target or {}).get("cityId") or 0)
        except (TypeError, ValueError):
            return 0

    def _fetch_detail_context_with_seed_retry(
        self,
        seed_hotel: dict[str, Any],
        date_value: str,
        missing: set[str],
    ) -> list[dict[str, Any]]:
        seed_id = str(seed_hotel.get("hotelId") or "")
        attempts = 2 if seed_id in missing else 1
        fetched: list[dict[str, Any]] = []
        for _ in range(attempts):
            try:
                detail_hotels = self._fetch_hotel_detail_context_for_date(seed_hotel, date_value)
            except ProviderError:
                detail_hotels = []
            fetched = self._merge_hotel_lists(fetched, detail_hotels)
            if seed_id not in missing:
                break
            if any(str(hotel.get("hotelId") or "") == seed_id for hotel in fetched):
                break
        return fetched

    def _detail_fetcher_overridden(self) -> bool:
        return "_fetch_hotel_detail_context_for_date" in self.__dict__

    def _fetch_detail_contexts_with_seed_retry(
        self,
        seed_hotels: list[dict[str, Any]],
        date_value: str,
        missing: set[str],
    ) -> list[dict[str, Any]]:
        fetched: list[dict[str, Any]] = []
        first_batch = self._fetch_hotel_detail_contexts_for_date(seed_hotels, date_value)
        for detail_hotels in first_batch.values():
            fetched = self._merge_hotel_lists(fetched, detail_hotels)

        retry_seeds: list[dict[str, Any]] = []
        fetched_ids = {str(hotel.get("hotelId") or "") for hotel in fetched}
        for seed in seed_hotels:
            seed_id = str(seed.get("hotelId") or "")
            if seed_id in missing and seed_id not in fetched_ids:
                retry_seeds.append(seed)
        if retry_seeds:
            retry_batch = self._fetch_hotel_detail_contexts_for_date(retry_seeds, date_value)
            for detail_hotels in retry_batch.values():
                fetched = self._merge_hotel_lists(fetched, detail_hotels)
        return fetched

    def _should_fetch_own_detail_seed(self, seed_hotel: dict[str, Any]) -> bool:
        if str(seed_hotel.get("hotelId") or ""):
            return True
        if detectHotelBrand(str(seed_hotel.get("hotelName") or "")):
            return True
        try:
            return float(seed_hotel.get("starRating") or 0) >= 5
        except (TypeError, ValueError):
            return False

    def _detail_seed_hotels(self, hotel_ids: list[str]) -> list[dict[str, Any]]:
        seeds = [
            self._candidate_cache.get(str(hotel_id))
            for hotel_id in hotel_ids
            if self._candidate_cache.get(str(hotel_id))
        ]
        seeds = [dict(seed) for seed in seeds if seed]
        seeds.sort(
            key=lambda item: (
                float(item.get("distanceKm") or 999),
                -float(item.get("starRating") or 0),
                int((detectHotelBrand(str(item.get("hotelName") or "")) or {}).get("brandRank") or 99),
            )
        )
        return seeds

    def _source_name_terms(self, hotel: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for value in (
            hotel.get("hotelOriginalName"),
            hotel.get("hotelNameSimplified"),
            hotel.get("hotelName"),
            hotel.get("brandLabel"),
            hotel.get("brand"),
        ):
            text = str(value or "").strip()
            if text and not has_name_verification_marker(text):
                terms.append(text)
        seen: set[str] = set()
        result: list[str] = []
        for term in terms:
            key = normalize_name(term)
            if key and key not in seen:
                seen.add(key)
                result.append(term)
        return result

    def _name_payload_from_text_candidates(
        self,
        candidates: list[Any],
        *,
        hotel_id: str,
        source: str,
        expected_city: str = "",
    ) -> dict[str, str] | None:
        cleaned: list[str] = []
        city = simplify_chinese_text(expected_city or "")
        for candidate in candidates:
            text = html_lib.unescape(str(candidate or ""))
            text = simplify_chinese_text(text)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            text = re.sub(r"\s*[-_｜|]\s*(Trip\.com|携程|Ctrip|Agoda|Booking).*$", "", text, flags=re.IGNORECASE)
            text = re.sub(r"(预订|价格|地址|电话|点评|查询).*$", "", text).strip()
            if not (2 <= len(text) <= 90) or not contains_chinese_text(text):
                continue
            if has_name_verification_marker(text):
                continue
            has_hotel_word = bool(re.search(r"酒店|大酒店|饭店|宾馆|公寓|度假|民宿|客栈|旅店|希尔顿|万豪|洲际|凯悦|亚朵|全季|维也纳|格兰云天|温德姆|华美达", text))
            if not has_hotel_word and city and city not in text:
                continue
            cleaned.append(text)
        return self._resolved_name_payload_from_sources(cleaned, hotel_id=hotel_id, source=source)

    def _http_text(self, url: str, *, timeout: int = 8) -> str:
        request = Request(
            url,
            headers={
                "User-Agent": UA,
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        with urlopen(request, timeout=timeout) as response:
            raw = response.read(1_200_000)
        return raw.decode("utf-8", errors="ignore")

    def _http_json(self, url: str, *, timeout: int = 8) -> dict[str, Any]:
        request = Request(
            url,
            headers={
                "User-Agent": UA,
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6",
                "Accept": "application/json,text/plain,*/*",
            },
        )
        with urlopen(request, timeout=timeout) as response:
            raw = response.read(1_200_000)
        data = json.loads(raw.decode("utf-8", errors="ignore"))
        return data if isinstance(data, dict) else {}

    def _extract_html_name_candidates(self, html: str) -> list[str]:
        candidates: list[str] = []
        patterns = [
            r"<title[^>]*>(.*?)</title>",
            r"<meta[^>]+(?:property|name)=[\"'](?:og:title|title)[\"'][^>]+content=[\"']([^\"']+)[\"']",
            r"<meta[^>]+content=[\"']([^\"']+)[\"'][^>]+(?:property|name)=[\"'](?:og:title|title)[\"']",
            r'"hotelName"\s*:\s*"([^"]+)"',
            r'"name"\s*:\s*"([^"]{2,100})"',
            r'"cnName"\s*:\s*"([^"]+)"',
            r'"chineseName"\s*:\s*"([^"]+)"',
        ]
        for pattern in patterns:
            candidates.extend(re.findall(pattern, html, flags=re.IGNORECASE | re.DOTALL))
        return candidates

    def _fetch_ctrip_name_payload(self, hotel: dict[str, Any], selected_date: str) -> dict[str, str] | None:
        hotel_id = str(hotel.get("hotelId") or "").strip()
        if not hotel_id.isdigit():
            return None
        checkin_date = parse_date(selected_date)
        checkout_date = checkin_date + dt.timedelta(days=1)
        urls = [
            f"https://hotels.ctrip.com/hotels/{hotel_id}.html",
            "https://hotels.ctrip.com/hotels/detail/?" + urlencode(
                {
                    "hotelId": hotel_id,
                    "checkin": checkin_date.isoformat(),
                    "checkout": checkout_date.isoformat(),
                }
            ),
        ]
        city = simplify_chinese_text(hotel.get("city") or (self._last_target or {}).get("city") or "")
        for url in urls:
            try:
                html = self._http_text(url, timeout=8)
            except Exception:
                continue
            payload = self._name_payload_from_text_candidates(
                self._extract_html_name_candidates(html),
                hotel_id=hotel_id,
                source="携程中文页",
                expected_city=city,
            )
            if payload:
                return payload
        return None

    def _fetch_elong_name_payload(self, hotel: dict[str, Any], selected_date: str) -> dict[str, str] | None:
        config = self._elong_config()
        if not config:
            return None
        hotel_id = str(hotel.get("hotelId") or "").strip()
        city = simplify_chinese_text(hotel.get("city") or (self._last_target or {}).get("city") or "")
        region_id = self._elong_region_id(city, hotel)
        if not region_id:
            return None

        checkin_date = parse_date(selected_date)
        checkout_date = checkin_date + dt.timedelta(days=1)
        request_payload: dict[str, Any] = {
            "regionId": int(region_id) if str(region_id).isdigit() else region_id,
            "checkInDate": checkin_date.isoformat(),
            "checkOutDate": checkout_date.isoformat(),
            "pageIndex": 1,
            "pageSize": int(os.environ.get("HOTEL_DEAL_ELONG_PAGE_SIZE", "10") or "10"),
        }
        latitude = self._coerce_float_value(hotel.get("latitude"))
        longitude = self._coerce_float_value(hotel.get("longitude"))
        if latitude is not None and longitude is not None:
            request_payload.update(
                {
                    "latitude": latitude,
                    "longitude": longitude,
                    "searchRadius": int(os.environ.get("HOTEL_DEAL_ELONG_SEARCH_RADIUS_METERS", "1200") or "1200"),
                }
            )

        terms = self._source_name_terms(hotel)
        if terms:
            request_payload["propertyName"] = terms[0]

        try:
            data = self._elong_request(config, request_payload)
        except Exception:
            return None

        rows = self._extract_elong_hotel_rows(data)
        if not rows:
            return None
        row = self._best_elong_hotel_row(rows, hotel, city=city)
        if not row:
            return None

        candidates = self._elong_name_candidates(row)
        return self._name_payload_from_text_candidates(
            candidates,
            hotel_id=hotel_id,
            source="艺龙中文酒店列表",
            expected_city=city,
        )

    def _elong_config(self) -> dict[str, str] | None:
        user = os.environ.get("HOTEL_DEAL_ELONG_USER") or os.environ.get("ELONG_USER")
        app_key = (
            os.environ.get("HOTEL_DEAL_ELONG_APP_KEY")
            or os.environ.get("HOTEL_DEAL_ELONG_APPKEY")
            or os.environ.get("ELONG_APP_KEY")
        )
        secret_key = (
            os.environ.get("HOTEL_DEAL_ELONG_SECRET_KEY")
            or os.environ.get("HOTEL_DEAL_ELONG_SECRET")
            or os.environ.get("ELONG_SECRET_KEY")
        )
        if not user or not app_key or not secret_key:
            return None
        return {
            "user": user,
            "appKey": app_key,
            "secretKey": secret_key,
            "host": (os.environ.get("HOTEL_DEAL_ELONG_API_HOST") or "https://api.elong.com/rest").rstrip("?"),
            "method": os.environ.get("HOTEL_DEAL_ELONG_METHOD") or "ihotel.list",
        }

    def _elong_region_id(self, city: str, hotel: dict[str, Any]) -> str:
        for key in ("elongRegionId", "regionId", "elongCityId"):
            value = str(hotel.get(key) or "").strip()
            if value:
                return value
        mapping_text = os.environ.get("HOTEL_DEAL_ELONG_REGION_ID_MAP", "").strip()
        mapping: dict[str, Any] = {}
        if mapping_text:
            try:
                parsed = json.loads(mapping_text)
                if isinstance(parsed, dict):
                    mapping = parsed
            except json.JSONDecodeError:
                for part in re.split(r"[,;，；]\s*", mapping_text):
                    if "=" not in part:
                        continue
                    name, value = part.split("=", 1)
                    mapping[name.strip()] = value.strip()
        city_key = normalize_name(city)
        for name, value in mapping.items():
            if normalize_name(name) == city_key and str(value or "").strip():
                return str(value).strip()
        return str(os.environ.get("HOTEL_DEAL_ELONG_DEFAULT_REGION_ID") or "").strip()

    def _elong_request(self, config: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
        data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        timestamp = str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000))
        data_digest = hashlib.md5((data_json + config["appKey"]).encode("utf-8")).hexdigest()
        signature = hashlib.md5((timestamp + data_digest + config["secretKey"]).encode("utf-8")).hexdigest()
        params = {
            "timestamp": timestamp,
            "format": "json",
            "method": config["method"],
            "signature": signature,
            "user": config["user"],
            "data": data_json,
        }
        return self._http_json(config["host"] + "?" + urlencode(params), timeout=8)

    def _extract_elong_hotel_rows(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        def visit(node: Any) -> None:
            if isinstance(node, list):
                if node and all(isinstance(row, dict) for row in node):
                    hotelish = [
                        row
                        for row in node
                        if isinstance(row, dict)
                        and any(key in row for key in ("HotelNameCn", "HotelName", "HotelNameEn", "HotelId"))
                    ]
                    if hotelish:
                        rows.extend(hotelish)
                        return
                for child in node:
                    visit(child)
            elif isinstance(node, dict):
                for key in ("Hotels", "HotelList", "Hotel", "hotelList", "hotels"):
                    child = node.get(key)
                    if isinstance(child, list):
                        visit(child)
                for child in node.values():
                    if isinstance(child, (dict, list)):
                        visit(child)

        visit(data)
        seen: set[str] = set()
        unique_rows: list[dict[str, Any]] = []
        for row in rows:
            key = str(row.get("HotelId") or row.get("HotelID") or row.get("HotelNameCn") or row.get("HotelName") or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            unique_rows.append(row)
        return unique_rows

    def _elong_name_candidates(self, row: dict[str, Any]) -> list[str]:
        candidates: list[str] = []
        for key in (
            "HotelNameCn",
            "HotelNameCN",
            "HotelName",
            "NameCn",
            "Name",
            "AddressCn",
            "Address",
        ):
            value = str(row.get(key) or "").strip()
            if value:
                candidates.append(value)
        return candidates

    def _best_elong_hotel_row(
        self,
        rows: list[dict[str, Any]],
        hotel: dict[str, Any],
        *,
        city: str,
    ) -> dict[str, Any] | None:
        source_terms = self._source_name_terms(hotel)
        hotel_lat = self._coerce_float_value(hotel.get("latitude"))
        hotel_lon = self._coerce_float_value(hotel.get("longitude"))
        best_score = 0
        best_row: dict[str, Any] | None = None
        for row in rows:
            score = self._elong_row_match_score(
                row,
                source_terms=source_terms,
                hotel_lat=hotel_lat,
                hotel_lon=hotel_lon,
                city=city,
                brand=str(hotel.get("brandLabel") or hotel.get("brand") or ""),
            )
            if score > best_score:
                best_score = score
                best_row = row
        return best_row if best_score >= 60 else None

    def _elong_row_match_score(
        self,
        row: dict[str, Any],
        *,
        source_terms: list[str],
        hotel_lat: float | None,
        hotel_lon: float | None,
        city: str,
        brand: str,
    ) -> int:
        score = 0
        row_names = [
            str(row.get(key) or "").strip()
            for key in ("HotelNameCn", "HotelNameCN", "HotelName", "NameCn", "Name", "HotelNameEn")
            if str(row.get(key) or "").strip()
        ]
        for row_name in row_names:
            row_key = normalize_name(row_name)
            for term in source_terms:
                term_key = normalize_name(term)
                if not row_key or not term_key:
                    continue
                if row_key == term_key:
                    score = max(score, 100)
                elif row_key in term_key or term_key in row_key:
                    score = max(score, 72)

        row_coordinates = self._extract_coordinates(row)
        if row_coordinates and hotel_lat is not None and hotel_lon is not None:
            distance = haversine_km(hotel_lat, hotel_lon, row_coordinates[0], row_coordinates[1])
            if distance <= 0.15:
                score += 50
            elif distance <= 0.35:
                score += 40
            elif distance <= 1:
                score += 22

        row_distance = self._coerce_float_value(row.get("Distance") or row.get("distance"))
        if row_distance is not None:
            distance_km = row_distance / 1000 if row_distance > 50 else row_distance
            if distance_km <= 0.35:
                score += 35
            elif distance_km <= 1:
                score += 20

        combined_text = simplify_chinese_text(json.dumps(row, ensure_ascii=False))
        city_text = simplify_chinese_text(city)
        if city_text and city_text in combined_text:
            score += 5
        brand_text = simplify_chinese_text(brand)
        if brand_text and brand_text not in {"独立酒店", "酒店", "集团"} and brand_text in combined_text:
            score += 10
        return score

    def _fetch_map_poi_name_payload(self, hotel: dict[str, Any]) -> dict[str, str] | None:
        amap = self._fetch_amap_poi_name_payload(hotel)
        if amap:
            return amap
        return self._fetch_baidu_poi_name_payload(hotel)

    def _fetch_amap_poi_name_payload(self, hotel: dict[str, Any]) -> dict[str, str] | None:
        key = os.environ.get("HOTEL_DEAL_AMAP_KEY") or os.environ.get("AMAP_WEB_SERVICE_KEY") or os.environ.get("AMAP_KEY")
        if not key:
            return None
        hotel_id = str(hotel.get("hotelId") or "").strip()
        city = simplify_chinese_text(hotel.get("city") or (self._last_target or {}).get("city") or "")
        candidates: list[str] = []
        for term in self._source_name_terms(hotel)[:3]:
            params = {
                "key": key,
                "keywords": term,
                "city": city,
                "types": "100000",
                "offset": 8,
                "page": 1,
                "extensions": "base",
            }
            try:
                data = self._http_json("https://restapi.amap.com/v3/place/text?" + urlencode(params), timeout=8)
            except Exception:
                continue
            pois = data.get("pois") if isinstance(data.get("pois"), list) else []
            candidates.extend(str(poi.get("name") or "") for poi in pois if isinstance(poi, dict))
            payload = self._name_payload_from_text_candidates(candidates, hotel_id=hotel_id, source="高德地图POI", expected_city=city)
            if payload:
                return payload
        return None

    def _fetch_baidu_poi_name_payload(self, hotel: dict[str, Any]) -> dict[str, str] | None:
        ak = os.environ.get("HOTEL_DEAL_BAIDU_MAP_AK") or os.environ.get("BAIDU_MAP_AK")
        if not ak:
            return None
        hotel_id = str(hotel.get("hotelId") or "").strip()
        city = simplify_chinese_text(hotel.get("city") or (self._last_target or {}).get("city") or "")
        candidates: list[str] = []
        for term in self._source_name_terms(hotel)[:3]:
            params = {
                "ak": ak,
                "query": term,
                "region": city or "全国",
                "output": "json",
                "scope": 1,
                "page_size": 8,
            }
            try:
                data = self._http_json("https://api.map.baidu.com/place/v2/search?" + urlencode(params), timeout=8)
            except Exception:
                continue
            results = data.get("results") if isinstance(data.get("results"), list) else []
            candidates.extend(str(row.get("name") or "") for row in results if isinstance(row, dict))
            payload = self._name_payload_from_text_candidates(candidates, hotel_id=hotel_id, source="百度地图POI", expected_city=city)
            if payload:
                return payload
        return None

    def _fetch_search_engine_name_payload(self, hotel: dict[str, Any]) -> dict[str, str] | None:
        if os.environ.get("HOTEL_DEAL_ENABLE_SEARCH_NAME_VERIFY", "").lower() not in {"1", "true", "yes", "on"}:
            return None
        hotel_id = str(hotel.get("hotelId") or "").strip()
        city = simplify_chinese_text(hotel.get("city") or (self._last_target or {}).get("city") or "")
        candidates: list[str] = []
        terms = self._source_name_terms(hotel)[:2] or [str(hotel.get("hotelOriginalName") or "")]
        for term in terms:
            query = " ".join(value for value in (city, term, "酒店 中文名 携程") if value)
            urls = [
                "https://www.bing.com/search?q=" + quote(query),
                "https://duckduckgo.com/html/?q=" + quote(query),
            ]
            for url in urls:
                try:
                    html = self._http_text(url, timeout=8)
                except Exception:
                    continue
                candidates.extend(self._extract_html_name_candidates(html))
                candidates.extend(re.findall(r"<a[^>]*>([^<]{2,100})</a>", html, flags=re.IGNORECASE))
                payload = self._name_payload_from_text_candidates(candidates, hotel_id=hotel_id, source="搜索引擎结果", expected_city=city)
                if payload:
                    return payload
        return None

    def _fetch_detail_page_name_payload(self, seed_hotel: dict[str, Any], check_in: str) -> dict[str, str] | None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            return None

        checkin_date = parse_date(check_in)
        checkout_date = checkin_date + dt.timedelta(days=1)
        target = self._last_target or {}
        hotel_id = str(seed_hotel.get("hotelId") or "")
        city_id = self._hotel_city_id(seed_hotel, target)
        if not hotel_id or not city_id:
            return None

        url = self._detail_v2_url(hotel_id, city_id, checkin_date, checkout_date)
        candidates: list[str] = []
        with self._lock:
            try:
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(
                        headless=True,
                        args=["--disable-blink-features=AutomationControlled"],
                    )
                    context = browser.new_context(
                        user_agent=UA,
                        locale="zh-CN",
                        timezone_id="Asia/Shanghai",
                        viewport={"width": 1280, "height": 1000},
                    )
                    context.route(
                        "**/*",
                        lambda route: route.abort()
                        if route.request.resource_type in {"image", "media", "font"}
                        else route.continue_(),
                    )
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=22000)
                    page.wait_for_timeout(1200)
                    candidates.append(page.title())
                    for selector in ("h1", "[data-testid*='hotel']", "meta[property='og:title']"):
                        try:
                            if selector.startswith("meta"):
                                values = page.locator(selector).evaluate_all("(els) => els.map((el) => el.content || '')")
                            else:
                                values = page.locator(selector).all_text_contents()
                        except Exception:
                            values = []
                        candidates.extend(str(value or "").strip() for value in values)
                    browser.close()
            except Exception:
                return None

        cleaned: list[str] = []
        for candidate in candidates:
            text = simplify_chinese_text(candidate)
            text = re.sub(r"\s*[-_｜|]\s*Trip\.com.*$", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\s*预订.*$", "", text)
            text = text.strip()
            if 2 <= len(text) <= 100:
                cleaned.append(text)
        return self._resolved_name_payload_from_sources(
            cleaned,
            hotel_id=hotel_id,
            source="Trip.com 详情页中文名",
        )

    def _fetch_hotel_detail_contexts_for_date(
        self,
        seed_hotels: list[dict[str, Any]],
        check_in: str,
    ) -> dict[str, list[dict[str, Any]]]:
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise ProviderError("当前 Python 环境没有安装 Playwright，无法实时抓取 Trip.com") from exc

        checkin_date = parse_date(check_in)
        checkout_date = checkin_date + dt.timedelta(days=1)
        target = self._last_target or {}
        prepared: list[tuple[str, dict[str, Any], int, list[str]]] = []
        for seed_hotel in seed_hotels:
            hotel_id = str(seed_hotel.get("hotelId") or "")
            city_id = self._hotel_city_id(seed_hotel, target)
            if not hotel_id or not city_id:
                continue
            urls = list(
                dict.fromkeys(
                    [
                        self._detail_url(hotel_id, city_id, checkin_date, checkout_date),
                        self._detail_v2_url(hotel_id, city_id, checkin_date, checkout_date),
                    ]
                )
            )
            prepared.append((hotel_id, seed_hotel, city_id, urls))
        if not prepared:
            return {}

        with self._lock:
            try:
                return asyncio.run(
                    self._fetch_hotel_detail_contexts_for_date_async(
                        prepared,
                        checkin_date,
                        checkout_date,
                        target,
                    )
                )
            except PlaywrightTimeoutError as exc:
                raise ProviderError("Trip.com 酒店详情加载超时，请稍后重试") from exc
            except Exception as exc:
                raise ProviderError(f"Trip.com 酒店详情抓取失败：{exc}") from exc

    async def _fetch_hotel_detail_contexts_for_date_async(
        self,
        prepared: list[tuple[str, dict[str, Any], int, list[str]]],
        checkin_date: dt.date,
        checkout_date: dt.date,
        target: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright

        results: dict[str, list[dict[str, Any]]] = {hotel_id: [] for hotel_id, *_rest in prepared}
        max_pages = max(1, int(os.environ.get("HOTEL_DEAL_DETAIL_FETCH_CONCURRENCY", "3")))
        semaphore = asyncio.Semaphore(max_pages)

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                user_agent=UA,
                locale="zh-CN",
                timezone_id="Asia/Shanghai",
                viewport={"width": 1440, "height": 1400},
            )

            async def route_handler(route: Any) -> None:
                if route.request.resource_type in {"image", "media", "font"}:
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", route_handler)
            await context.add_init_script(
                """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                """
            )

            async def fetch_seed(
                hotel_id: str,
                seed_hotel: dict[str, Any],
                city_id: int,
                urls: list[str],
            ) -> tuple[str, list[dict[str, Any]]]:
                async with semaphore:
                    detail_items: list[dict[str, Any]] = []
                    room_price_seen = False
                    nearby_seen = False
                    response_tasks: list[asyncio.Task[Any]] = []

                    def seed_item_from_room_price(price: int) -> dict[str, Any]:
                        item = dict(seed_hotel)
                        item.setdefault("city", simplify_chinese_text(target.get("city") or ""))
                        item["cityId"] = city_id
                        item["currentPrice"] = price
                        item["priceDate"] = checkin_date.isoformat()
                        item["priceIncludesTax"] = True
                        item["priceSource"] = "Trip.com detail room total incl. taxes & fees"
                        item["tripUrl"] = self._detail_url(hotel_id, city_id, checkin_date, checkout_date)
                        return item

                    async def collect_response_items(response: Any) -> None:
                        nonlocal room_price_seen, nearby_seen
                        response_url = str(response.url or "")
                        is_room_response = "getHotelRoomList" in response_url
                        is_nearby_response = "ctGetNearbyHotelList" in response_url
                        if not is_room_response and not is_nearby_response:
                            return
                        try:
                            data = await asyncio.wait_for(response.json(), timeout=3)
                        except Exception:
                            return
                        if is_room_response:
                            price = self._extract_detail_room_tax_price(data)
                            if price is not None:
                                detail_items.append(seed_item_from_room_price(price))
                                room_price_seen = True
                        elif is_nearby_response:
                            rows = (((data or {}).get("data") or {}).get("hotelList") or []) if isinstance(data, dict) else []
                            for row in rows:
                                if isinstance(row, dict):
                                    item = self._normalize_trip_detail_nearby_hotel(row, checkin_date, checkout_date, target)
                                    if item:
                                        detail_items.append(item)
                            nearby_seen = bool(rows)

                    page = await context.new_page()

                    def on_response(response: Any) -> None:
                        response_tasks.append(asyncio.create_task(collect_response_items(response)))

                    page.on("response", on_response)
                    try:
                        for url in urls:
                            try:
                                await page.goto(url, wait_until="domcontentloaded", timeout=35000)
                            except PlaywrightTimeoutError:
                                if url == urls[-1]:
                                    break
                                continue
                            for _ in range(12):
                                if room_price_seen and nearby_seen:
                                    break
                                await page.wait_for_timeout(800)
                            if room_price_seen:
                                break
                        if response_tasks:
                            _done, pending = await asyncio.wait(response_tasks, timeout=2)
                            for task in pending:
                                task.cancel()
                    finally:
                        page.remove_listener("response", on_response)
                        await page.close()
                    return hotel_id, self._dedupe_hotel_items(detail_items)

            pairs = await asyncio.gather(
                *(
                    asyncio.wait_for(fetch_seed(hotel_id, seed_hotel, city_id, urls), timeout=95)
                    for hotel_id, seed_hotel, city_id, urls in prepared
                ),
                return_exceptions=True,
            )
            await browser.close()
        for pair in pairs:
            if isinstance(pair, Exception):
                continue
            hotel_id, items = pair
            results[hotel_id] = items
        return results

    def _fetch_hotel_detail_context_for_date(self, seed_hotel: dict[str, Any], check_in: str) -> list[dict[str, Any]]:
        hotel_id = str(seed_hotel.get("hotelId") or "")
        if not hotel_id:
            return []
        return self._fetch_hotel_detail_contexts_for_date([seed_hotel], check_in).get(hotel_id, [])

    def _detail_v2_url(self, hotel_id: str, city_id: int | str, check_in: dt.date, check_out: dt.date) -> str:
        params = {
            "hotelId": hotel_id,
            "cityId": city_id,
            "checkin": check_in.strftime("%Y/%m/%d"),
            "checkout": check_out.strftime("%Y/%m/%d"),
            "curr": "CNY",
            "locale": "zh-CN",
            "crn": 1,
            "adult": 2,
            "children": 0,
        }
        return "https://www.trip.com/hotels/v2/detail/?" + urlencode(params)

    def _extract_detail_room_tax_price(self, data: dict[str, Any]) -> int | None:
        payload = data.get("data") if isinstance(data.get("data"), dict) else {}
        sale_room_map = payload.get("saleRoomMap") if isinstance(payload.get("saleRoomMap"), dict) else {}
        prices: list[int] = []
        for room in sale_room_map.values():
            if not isinstance(room, dict):
                continue
            total_price_info = room.get("totalPriceInfo") if isinstance(room.get("totalPriceInfo"), dict) else {}
            price_detail = room.get("priceDetail") if isinstance(room.get("priceDetail"), dict) else {}
            price_detail_info = price_detail.get("priceInfo") if isinstance(price_detail.get("priceInfo"), dict) else {}
            price_info = room.get("priceInfo") if isinstance(room.get("priceInfo"), dict) else {}
            candidates = [
                ((total_price_info.get("totalNoApprox") or {}).get("content") if isinstance(total_price_info.get("totalNoApprox"), dict) else None),
                ((total_price_info.get("total") or {}).get("content") if isinstance(total_price_info.get("total"), dict) else None),
                ((price_detail_info.get("totalPrice") or {}).get("content") if isinstance(price_detail_info.get("totalPrice"), dict) else None),
                price_info.get("priceExplanation"),
                price_info.get("priceExplanationHighlight"),
                room.get("comparingAmount"),
            ]
            for candidate in candidates:
                price = self._coerce_price_value(candidate)
                if price is not None:
                    prices.append(price)
                    break
        return min(prices) if prices else None

    def _normalize_trip_detail_nearby_hotel(
        self,
        row: dict[str, Any],
        check_in: dt.date,
        check_out: dt.date,
        target: dict[str, Any],
    ) -> dict[str, Any] | None:
        basic = row.get("base") if isinstance(row.get("base"), dict) else {}
        money = row.get("money") if isinstance(row.get("money"), dict) else {}
        position = row.get("position") if isinstance(row.get("position"), dict) else {}
        comment = row.get("comment") if isinstance(row.get("comment"), dict) else {}
        hotel_id = str(basic.get("hotelId") or "").strip()
        raw_names = [basic.get("hotelName"), basic.get("hotelEnName")]
        if isinstance(basic.get("hotelNames"), list):
            raw_names.extend(basic.get("hotelNames") or [])
        price = self._extract_detail_nearby_tax_price(money)
        if not hotel_id:
            return None

        name_payload = hotel_name_payload_from_sources(
            raw_names,
            hotel_id=hotel_id,
            source="Trip.com 繁体转简体",
        )
        hotel_name = name_payload["hotelName"]
        if not hotel_name:
            return None
        latitude = self._coerce_float_value(position.get("lat") or position.get("latitude"))
        longitude = self._coerce_float_value(position.get("lng") or position.get("lon") or position.get("longitude"))
        distance = None
        if latitude is not None and longitude is not None and target.get("latitude") and target.get("longitude"):
            distance = round(
                haversine_km(
                    float(target.get("latitude") or 0),
                    float(target.get("longitude") or 0),
                    latitude,
                    longitude,
                ),
                2,
            )
        detected = detectHotelBrand(hotel_name) or {}
        hotel_level = basic.get("hotelLevel") if isinstance(basic.get("hotelLevel"), dict) else {}
        star = self._coerce_float_value(hotel_level.get("star") or hotel_level.get("dStar")) or 0
        rating = self._coerce_float_value(comment.get("score"))
        score_max = self._coerce_float_value(comment.get("scoreMax")) or 10
        city_id = position.get("cityId") or basic.get("cityId") or target.get("cityId") or 0
        return {
            "hotelId": hotel_id,
            "hotelName": hotel_name,
            "hotelOriginalName": name_payload["hotelOriginalName"],
            "hotelNameSimplified": name_payload["hotelNameSimplified"],
            "hotelNameSource": name_payload["hotelNameSource"],
            "city": simplify_chinese_text(position.get("cityName") or target.get("city") or ""),
            "cityId": int(city_id or 0),
            "brand": detected.get("brand") or "",
            "group": detected.get("group") or "",
            "starRating": min(5.0, float(star or 0)),
            "latitude": latitude,
            "longitude": longitude,
            "distanceKm": distance,
            "currentPrice": price,
            "priceDate": check_in.isoformat() if price is not None else "",
            "priceIncludesTax": price is not None,
            "priceSource": "Trip.com detail nearby after-tax price" if price is not None else "",
            "imageUrl": basic.get("imageUrl") or basic.get("imageUrlOfCtrip") or "",
            "tripUrl": self._detail_url(hotel_id, city_id, check_in, check_out),
            "rating": round(float(rating) / float(score_max) * 5, 1) if rating and score_max and score_max > 5 else rating,
            "reviewCount": self._extract_detail_review_count(comment),
            "source": self.source_name,
        }

    def _extract_detail_nearby_tax_price(self, money: dict[str, Any]) -> int | None:
        price_float_info = money.get("priceFloatInfo") if isinstance(money.get("priceFloatInfo"), dict) else {}
        price_sum = price_float_info.get("priceSum") if isinstance(price_float_info.get("priceSum"), dict) else {}
        for value in (price_sum.get("price"), money.get("priceNote")):
            price = self._coerce_price_value(value)
            if price is not None:
                return price
        return None

    def _extract_detail_review_count(self, comment: dict[str, Any]) -> int | None:
        for key in ("totalReviews", "totalReview", "reviewCount"):
            value = comment.get(key)
            if value in (None, ""):
                continue
            match = re.search(r"\d[\d,]*", str(value))
            if match:
                return int(match.group(0).replace(",", ""))
        return None

    def _keyword_search(self, query: str) -> list[dict[str, Any]]:
        trace_id = self._trace_id()
        client_id = trace_id.split("-")[0]
        pid = str(uuid.uuid4())
        payload = {
            "code": 0,
            "codeType": "",
            "keyWord": query,
            "searchType": "D",
            "scenicCode": 0,
            "cityCodeOfUser": 0,
            "searchConditions": [
                {"type": "D_PROVINCE", "value": "T"},
                {"type": "SupportNormalSearch", "value": "T"},
                {"type": "DisplayTagIcon", "value": "F"},
            ],
            "head": self._trip_head(client_id=client_id, pid=pid, trace_id=trace_id, locale=KEYWORD_LOCALE),
        }
        url = (
            "https://www.trip.com/htls/getKeyWordSearch?"
            + urlencode({"htl_customtraceid": uuid.uuid4().hex, "x-traceID": trace_id})
        )
        req = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "currency": "CNY",
                "locale": KEYWORD_LOCALE,
                "p": payload["head"]["p"],
                "pid": pid,
                "referer": "https://www.trip.com/hotels",
                "trip-trace-id": trace_id,
                "user-agent": UA,
                "x-traceid": trace_id,
            },
            method="POST",
        )
        try:
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            raise ProviderError(f"Trip.com 关键词搜索失败：{exc}") from exc
        return data.get("keyWordSearchResults") or []

    def _supplemental_search_targets(self, target: dict[str, Any]) -> list[dict[str, Any]]:
        city = str(target.get("city") or "").strip()
        target_name = str(target.get("hotelName") or "").strip()
        keywords = self._supplemental_keywords(city, target_name)
        targets: list[dict[str, Any]] = []
        seen: set[str] = {f"{target.get('searchType') or 'H'}:{target.get('hotelId')}"}
        for keyword in keywords:
            try:
                results = self._keyword_search(keyword)
            except ProviderError:
                continue
            for item in results:
                supplemental = self._keyword_result_to_target(item, fallback_city=city)
                if supplemental is None:
                    continue
                if supplemental.get("searchType") == "H":
                    continue
                key = f"{supplemental['searchType']}:{supplemental['hotelId']}"
                if key in seen:
                    continue
                seen.add(key)
                targets.append(supplemental)
                break
        return targets[:3]

    def _fallback_search_targets(self, target: dict[str, Any]) -> list[dict[str, Any]]:
        city_target = self._city_fallback_target(target)
        return [city_target] if city_target else []

    def _retry_search_targets(
        self,
        target: dict[str, Any],
        *,
        fallback_targets: list[dict[str, Any]],
        fast_mode: bool = False,
    ) -> list[dict[str, Any]]:
        if str(target.get("searchType") or "H").upper() == "H":
            if fast_mode:
                return fallback_targets
            return [*fallback_targets, target]
        return fallback_targets

    def _city_fallback_target(self, target: dict[str, Any]) -> dict[str, Any] | None:
        city = simplify_chinese_text(str(target.get("city") or "").strip())
        try:
            city_id = int(target.get("cityId") or 0)
        except (TypeError, ValueError):
            city_id = 0
        if not city or not city_id:
            return None
        return {
            "hotelId": str(city_id),
            "hotelName": f"{city}酒店",
            "hotelOriginalName": f"{city}酒店",
            "hotelNameSimplified": f"{city}酒店",
            "hotelNameSource": "Trip.com 城市兜底",
            "city": city,
            "cityId": city_id,
            "provinceId": int(target.get("provinceId") or 0),
            "countryId": int(target.get("countryId") or 0),
            "latitude": float(target.get("latitude") or 0),
            "longitude": float(target.get("longitude") or 0),
            "searchType": "CT",
            "searchValue": f"19~{city_id}*19*{city_id}*1",
            "searchCoordinate": "",
            "resultTypeLabel": RESULT_TYPE_LABELS["CT"],
            "matchedQuery": city,
        }

    def _search_target_key(self, target: dict[str, Any]) -> str:
        return f"{target.get('searchType') or ''}:{target.get('hotelId') or ''}:{target.get('searchValue') or ''}"

    def _dedupe_search_targets(self, targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for target in targets:
            key = self._search_target_key(target)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(target)
        return deduped

    def _supplemental_keywords(self, city: str, target_name: str) -> list[str]:
        compact = normalize_name(target_name)
        seeds: list[str] = []
        for token in ("国际会展中心", "会展中心", "前海华发冰雪世界", "冰雪世界"):
            if normalize_name(token) in compact:
                seeds.append(f"{token}酒店")
                seeds.append(token)
        if not seeds and target_name:
            simplified_city = simplify_chinese_text(city)
            simplified_name = simplify_chinese_text(target_name)
            if simplified_city and simplified_name.startswith(simplified_city):
                simplified_name = simplified_name[len(simplified_city) :]
            stripped = re.sub(
                (
                    r"(丽思卡尔顿|瑞吉|威斯汀|喜来登|万丽|艾美|W酒店|\bW\b|"
                    r"铂尔曼|鉑尔曼|索菲特|诺富特|费尔蒙|莱佛士|"
                    r"希尔顿|逸林|康莱德|华尔道夫|万豪|洲际|皇冠假日|假日酒店|"
                    r"凯悦|君悦|柏悦|安达仕|雅高|温德姆|华美达|豪生|"
                    r"香格里拉|凯宾斯基|酒店|大酒店|宾馆|公寓|度假村)"
                ),
                "",
                simplified_name,
                flags=re.IGNORECASE,
            ).strip(" （()）-_/·・")
            if stripped and stripped != simplified_name:
                seeds.append(f"{stripped}酒店")
                seeds.append(stripped)
        keywords: list[str] = []
        seen: set[str] = set()
        for seed in seeds:
            seed = str(seed or "").strip()
            if not seed:
                continue
            for keyword in (f"{city}{seed}" if city and not seed.startswith(city) else seed, seed):
                if keyword and keyword not in seen:
                    seen.add(keyword)
                    keywords.append(keyword)
        return keywords

    def _keyword_result_to_target(self, item: dict[str, Any], fallback_city: str = "") -> dict[str, Any] | None:
        result_type = str(item.get("resultType") or "").strip().upper()
        if result_type not in {"H", "LM", "D", "CT", "Z"}:
            return None
        code = str(item.get("code") or "").strip()
        title = str(
            ((item.get("item") or {}).get("data") or {}).get("title")
            or item.get("resultWord")
            or item.get("word")
            or item.get("name")
            or ""
        ).strip()
        if not code or not title:
            return None
        city_payload = item.get("city") or {}
        result_city = simplify_chinese_text(city_payload.get("currentLocaleName") or city_payload.get("enusName") or "")
        if fallback_city and result_city and normalize_name(fallback_city) not in normalize_name(result_city):
            return None
        coordinates = self._keyword_coordinates(item)
        filter_id = self._keyword_filter_id(item, default_type=self._default_filter_type(result_type))
        name_payload = domestic_hotel_name_payload(title, hotel_id=code, source="Trip.com 简体")
        return {
            "hotelId": code,
            "hotelName": name_payload["hotelName"],
            "hotelOriginalName": name_payload["hotelOriginalName"],
            "hotelNameSimplified": name_payload["hotelNameSimplified"],
            "hotelNameSource": name_payload["hotelNameSource"],
            "city": simplify_chinese_text(city_payload.get("currentLocaleName") or fallback_city),
            "cityId": int(city_payload.get("geoCode") or 0),
            "provinceId": int((item.get("province") or {}).get("geoCode") or 0),
            "countryId": int((item.get("country") or {}).get("geoCode") or 0),
            "latitude": coordinates["latitude"],
            "longitude": coordinates["longitude"],
            "searchType": result_type,
            "searchValue": f"{filter_id}*{self._default_filter_type(result_type)}*{code}*1".replace("|", "~"),
            "searchCoordinate": coordinates["searchCoordinate"].replace("|", "~"),
            "raw": item,
        }

    def _keyword_filter_id(self, item: dict[str, Any], default_type: str) -> str:
        return str(((item.get("item") or {}).get("data") or {}).get("filterID") or f"{default_type}|{item.get('code')}")

    def _default_filter_type(self, result_type: str) -> str:
        return {
            "CT": "19",
            "D": "9",
            "LM": "13",
            "H": "31",
            "Z": "3",
        }.get(str(result_type or "").upper(), "31")

    def _keyword_coordinates(self, item: dict[str, Any]) -> dict[str, Any]:
        coords = item.get("coordinateInfos") or []
        preferred = (
            next((coord for coord in coords if coord.get("coordinateType") in {"GAODE", "NORMAL"}), None)
            or (coords[0] if coords else {})
        )
        search_coordinate = "|".join(
            f"{coord.get('coordinateType')}_{coord.get('latitude')}_{coord.get('longitude')}_{coord.get('accuracy', 0)}"
            for coord in coords
            if coord.get("latitude") and coord.get("longitude")
        )
        return {
            "latitude": float(preferred.get("latitude") or 0),
            "longitude": float(preferred.get("longitude") or 0),
            "searchCoordinate": search_coordinate,
        }

    def _merge_hotel_lists(self, left: list[dict[str, Any]], right: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for item in [*left, *right]:
            key = str(item.get("hotelId") or item.get("hotelName") or "")
            if not key:
                continue
            if key not in merged or self._prefer_hotel_item(item, merged[key]):
                merged[key] = item
        return list(merged.values())

    def _prefer_hotel_item(self, candidate: dict[str, Any], current: dict[str, Any]) -> bool:
        candidate_has_tax = bool(candidate.get("priceIncludesTax"))
        current_has_tax = bool(current.get("priceIncludesTax"))
        if candidate_has_tax != current_has_tax:
            return candidate_has_tax
        candidate_price = int(candidate.get("currentPrice") or 10**9)
        current_price = int(current.get("currentPrice") or 10**9)
        if candidate_price != current_price:
            return candidate_price < current_price
        candidate_visible_price = int(candidate.get("visiblePrice") or 10**9)
        current_visible_price = int(current.get("visiblePrice") or 10**9)
        if candidate_visible_price != current_visible_price:
            return candidate_visible_price < current_visible_price
        return bool(candidate.get("imageUrl")) and not bool(current.get("imageUrl"))

    def _dedupe_hotel_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self._merge_hotel_lists([], items)

    def _fetch_hotel_list_for_date(
        self,
        target: dict[str, Any],
        check_in: str,
        fast_mode: bool = False,
        deep_mode: bool = False,
        result_callback: Any | None = None,
        price_backfill_mode: bool = False,
        wanted_hotel_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        html_items = self._fetch_hotel_list_from_html(target, check_in, fast_mode=fast_mode)
        wanted_ids = {str(hotel_id) for hotel_id in (wanted_hotel_ids or set()) if str(hotel_id or "").strip()}
        last_published_signature: tuple[tuple[str, str, str, str], ...] = ()

        def publish_response_items(items: list[dict[str, Any]]) -> None:
            nonlocal last_published_signature
            if not result_callback:
                return
            deduped = self._dedupe_hotel_items(items)
            signature = tuple(
                (
                    str(item.get("hotelId") or item.get("hotelName") or ""),
                    str(item.get("currentPrice") or ""),
                    str(item.get("visiblePrice") or ""),
                    str(item.get("priceIncludesTax") or ""),
                )
                for item in deduped
            )
            if not signature or signature == last_published_signature:
                return
            last_published_signature = signature
            try:
                result_callback(deduped)
            except Exception:
                pass

        def wanted_tax_prices_found(items: list[dict[str, Any]]) -> bool:
            if not wanted_ids:
                return False
            found_ids = {
                str(item.get("hotelId") or "")
                for item in self._dedupe_hotel_items(items)
                if str(item.get("hotelId") or "") in wanted_ids
                and item.get("currentPrice") not in (None, "")
                and item.get("priceIncludesTax") is not False
            }
            return wanted_ids.issubset(found_ids)

        if html_items:
            publish_response_items(html_items)
        if html_items and fast_mode and not deep_mode:
            return html_items
        if fast_mode and not deep_mode:
            return []

        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise ProviderError("当前 Python 环境没有安装 Playwright，无法实时抓取 Trip.com") from exc

        checkin_date = parse_date(check_in)
        checkout_date = checkin_date + dt.timedelta(days=1)
        url = self._build_list_url(target, checkin_date, checkout_date)
        response_items: list[dict[str, Any]] = list(html_items)
        goto_timeout_ms = 35000 if price_backfill_mode else 45000
        homepage_wait_ms = 500 if price_backfill_mode else 800
        attempt_waits_ms = (1800,) if price_backfill_mode else ((3500,) if fast_mode else (3500, 6500))
        scroll_rounds = 18 if price_backfill_mode else (30 if deep_mode else (18 if not fast_mode else 10))
        min_scroll_rounds = 5 if price_backfill_mode else (8 if deep_mode else (5 if not fast_mode else 3))
        scroll_wait_ms = 650 if price_backfill_mode else 1000

        def collect_response_items(response: Any) -> None:
            response_url = str(response.url or "")
            is_hotel_list_response = (
                "/htls/getHotelList" in response_url
                or "/restapi/soa2/34951/" in response_url
                or "fetchHotelList" in response_url
            )
            if not is_hotel_list_response:
                return
            try:
                data = response.json()
            except Exception:
                return
            rows = self._hotel_list_rows(data)
            for row in rows:
                item = self._normalize_trip_hotel(row, checkin_date, checkout_date, target)
                if item:
                    response_items.append(item)
            publish_response_items(response_items)

        with self._lock:
            try:
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(
                        headless=True,
                        args=["--disable-blink-features=AutomationControlled"],
                    )
                    context = browser.new_context(
                        user_agent=UA,
                        locale="zh-CN",
                        timezone_id="Asia/Shanghai",
                        viewport={"width": 1440, "height": 1400},
                    )
                    context.route(
                        "**/*",
                        lambda route: route.abort()
                        if route.request.resource_type in {"image", "media", "font"}
                        else route.continue_(),
                    )
                    context.add_init_script(
                        """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                        """
                    )
                    page = context.new_page()
                    page.on("response", collect_response_items)
                    wait_until_state = "commit" if deep_mode else "domcontentloaded"
                    page.goto("https://www.trip.com/hotels", wait_until=wait_until_state, timeout=goto_timeout_ms)
                    page.wait_for_timeout(homepage_wait_ms)
                    for attempt, initial_wait_ms in enumerate(attempt_waits_ms, start=1):
                        if attempt > 1:
                            response_items = list(html_items)
                        page.goto(url, wait_until=wait_until_state, timeout=goto_timeout_ms)
                        page.wait_for_timeout(initial_wait_ms)
                        publish_response_items(response_items)
                        if wanted_tax_prices_found(response_items):
                            break
                        last_count = len(self._dedupe_hotel_items(response_items))
                        stale_rounds = 0
                        for scroll_index in range(scroll_rounds):
                            page.mouse.wheel(0, 1800)
                            page.wait_for_timeout(scroll_wait_ms)
                            response_items = self._merge_hotel_lists(
                                response_items,
                                self._hotel_cards_from_html(page.content(), checkin_date, checkout_date, target),
                            )
                            current_count = len(self._dedupe_hotel_items(response_items))
                            if current_count > last_count:
                                last_count = current_count
                                stale_rounds = 0
                                publish_response_items(response_items)
                                if wanted_tax_prices_found(response_items):
                                    break
                            elif scroll_index >= min_scroll_rounds:
                                stale_rounds += 1
                            if scroll_index >= min_scroll_rounds and stale_rounds >= 3:
                                break
                        response_items = self._merge_hotel_lists(
                            response_items,
                            self._hotel_cards_from_html(page.content(), checkin_date, checkout_date, target),
                        )
                        publish_response_items(response_items)
                        if response_items:
                            break
                    page.remove_listener("response", collect_response_items)
                    browser.close()
            except PlaywrightTimeoutError as exc:
                if response_items:
                    return self._dedupe_hotel_items(response_items)
                raise ProviderError("Trip.com 页面加载超时，请稍后重试") from exc
            except Exception as exc:
                if response_items:
                    return self._dedupe_hotel_items(response_items)
                raise ProviderError(f"Trip.com 酒店列表抓取失败：{exc}") from exc

        result = self._dedupe_hotel_items(response_items)
        publish_response_items(result)
        return result

    def _fetch_hotel_list_from_html(self, target: dict[str, Any], check_in: str, fast_mode: bool = False) -> list[dict[str, Any]]:
        checkin_date = parse_date(check_in)
        checkout_date = checkin_date + dt.timedelta(days=1)
        url = self._build_list_url(target, checkin_date, checkout_date)
        req = Request(
            url,
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "currency": "CNY",
                "locale": LIST_LOCALE,
                "user-agent": UA,
            },
            method="GET",
        )
        html = ""
        try:
            with urlopen(req, timeout=3 if fast_mode else 12) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception:
            html = ""
        if not html:
            html = self._fetch_hotel_list_with_curl(url, fast_mode=fast_mode)
        if not html:
            return []
        return self._hotel_cards_from_html(html, checkin_date, checkout_date, target)

    def _fetch_hotel_list_with_curl(self, url: str, fast_mode: bool = False) -> str:
        try:
            completed = subprocess.run(
                [
                    "/usr/bin/curl",
                    "-L",
                    "--max-time",
                    "5" if fast_mode else "15",
                    "-sS",
                    "-H",
                    "accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "-H",
                    "accept-language: zh-CN,zh;q=0.9,en;q=0.8",
                    "-H",
                    "currency: CNY",
                    "-H",
                    f"locale: {LIST_LOCALE}",
                    "-H",
                    f"user-agent: {UA}",
                    url,
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=7 if fast_mode else 18,
            )
        except Exception:
            return ""
        if completed.returncode != 0:
            return ""
        return completed.stdout or ""

    def _hotel_cards_from_html(
        self,
        html: str,
        check_in: dt.date,
        check_out: dt.date,
        target: dict[str, Any],
    ) -> list[dict[str, Any]]:
        cards = re.findall(
            r'<div class="hotel-card" id="([^"]+)"(.*?)(?=<div class="list-item"><div class="hotel-card" id="|</body>|$)',
            html,
            flags=re.DOTALL,
        )
        hotels: list[dict[str, Any]] = []
        city_id = target.get("cityId") or 0
        for hotel_id, card_body in cards:
            if not str(hotel_id).strip().isdigit():
                continue
            card = html_lib.unescape(card_body)
            name_match = re.search(r'<span class="hotelName">(.+?)</span>', card, flags=re.DOTALL)
            raw_name = self._strip_html(name_match.group(1)) if name_match else ""
            price, price_includes_tax = self._extract_html_card_price_info(card)
            visible_price = price if price is not None and not price_includes_tax else None
            current_price = price if price is not None and price_includes_tax else None
            if not raw_name:
                continue
            name_payload = hotel_name_payload_from_sources(
                [raw_name],
                hotel_id=hotel_id,
                source="Trip.com 繁体转简体",
            )
            hotel_name = name_payload["hotelName"]
            detected = detectHotelBrand(hotel_name) or {}
            hotels.append(
                {
                    "hotelId": str(hotel_id),
                    "hotelName": hotel_name,
                    "hotelOriginalName": name_payload["hotelOriginalName"],
                    "hotelNameSimplified": name_payload["hotelNameSimplified"],
                    "hotelNameSource": name_payload["hotelNameSource"],
                    "city": simplify_chinese_text(target.get("city") or ""),
                    "cityId": int(city_id or 0),
                    "brand": detected.get("brand") or "",
                    "group": detected.get("group") or "",
                    "starRating": self._extract_html_card_star(card),
                    "latitude": None,
                    "longitude": None,
                    "distanceKm": self._extract_html_card_distance(card),
                    "currentPrice": current_price,
                    "visiblePrice": visible_price,
                    "visiblePriceDate": check_in.isoformat() if visible_price is not None else "",
                    "currentPricePreview": visible_price,
                    "priceDate": check_in.isoformat() if current_price is not None else "",
                    "priceIncludesTax": price_includes_tax,
                    "priceSource": (
                        "Trip.com card total incl. taxes & fees"
                        if current_price is not None
                        else ("Trip.com card visible price, tax pending" if visible_price is not None else "")
                    ),
                    "imageUrl": self._extract_html_card_image(card),
                    "tripUrl": self._detail_url(str(hotel_id), city_id, check_in, check_out),
                    "rating": self._extract_html_card_rating(card),
                    "reviewCount": self._extract_html_card_review_count(card),
                    "source": self.source_name,
                }
            )
        return hotels

    def _strip_html(self, value: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html_lib.unescape(str(value or "")))).strip()

    def _extract_html_card_price_info(self, card: str) -> tuple[int | None, bool]:
        tax_match = re.search(
            r"Total\s*\(incl\.\s*taxes\s*&\s*fees\):\s*CNY\s*([\d,]+)",
            card,
            flags=re.IGNORECASE,
        )
        if tax_match:
            return int(tax_match.group(1).replace(",", "")), True
        for match in re.finditer(r"CNY\s*([\d,]+)", card, flags=re.IGNORECASE):
            window = card[max(0, match.start() - 80) : min(len(card), match.end() + 80)]
            if re.search(r"incl|tax|fees?|含税|税费|总价", window, flags=re.IGNORECASE):
                return int(match.group(1).replace(",", "")), True
        sale_match = re.search(r"(?:Current price\s*)?CNY\s*([\d,]+)", card, flags=re.IGNORECASE)
        if sale_match:
            return int(sale_match.group(1).replace(",", "")), False
        return None, False

    def _extract_html_card_price(self, card: str) -> int | None:
        return self._extract_html_card_price_info(card)[0]

    def _extract_html_card_star(self, card: str) -> float:
        match = re.search(r'aria-label="([1-5](?:\.\d+)?)\s*out of 5 stars"', card, flags=re.IGNORECASE)
        return float(match.group(1)) if match else 0

    def _extract_html_card_distance(self, card: str) -> float | None:
        match = re.search(r"([\d.]+)\s*km\s*(?:(?:walk|drive)\s*)?from", card, flags=re.IGNORECASE)
        return round(float(match.group(1)), 2) if match else None

    def _extract_html_card_rating(self, card: str) -> float | None:
        match = re.search(r'aria-label="([\d.]+)\s*out of 10"', card, flags=re.IGNORECASE)
        if not match:
            return None
        return round(float(match.group(1)) / 2, 1)

    def _extract_html_card_review_count(self, card: str) -> int | None:
        match = re.search(r"([\d,]+)\s+reviews", card, flags=re.IGNORECASE)
        return int(match.group(1).replace(",", "")) if match else None

    def _extract_html_card_image(self, card: str) -> str:
        match = re.search(r'<img[^>]+src="([^"]+)"', card, flags=re.IGNORECASE)
        return match.group(1) if match else ""

    def _build_list_url(self, target: dict[str, Any], check_in: dt.date, check_out: dt.date) -> str:
        params = {
            "city": int(target.get("cityId") or 0),
            "cityName": target.get("city") or "",
            "provinceId": int(target.get("provinceId") or 0),
            "countryId": int(target.get("countryId") or 0),
            "districtId": 0,
            "checkin": check_in.strftime("%Y/%m/%d"),
            "checkout": check_out.strftime("%Y/%m/%d"),
            "lat": target.get("latitude") or 0,
            "lon": target.get("longitude") or 0,
            "searchType": target.get("searchType") or "H",
            "searchWord": target.get("hotelName") or "",
            "searchValue": target.get("searchValue") or "",
            "searchCoordinate": target.get("searchCoordinate") or "",
            "crn": 1,
            "adult": 2,
            "children": 0,
            "searchBoxArg": "t",
            "travelPurpose": 0,
            "ctm_ref": "ix_sb_dl",
            "domestic": "true",
            "locale": LIST_LOCALE,
            "curr": "CNY",
        }
        return "https://www.trip.com/hotels/list?" + urlencode(params)

    def _hotel_list_rows(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        if isinstance(data.get("hotelList"), list):
            return data["hotelList"]
        payload = data.get("data") if isinstance(data.get("data"), dict) else {}
        if isinstance(payload.get("hotelList"), list):
            return payload["hotelList"]
        for key in ("initListData", "hotelListData", "listData", "modelResult"):
            nested = payload.get(key) if isinstance(payload, dict) else data.get(key)
            if isinstance(nested, dict):
                rows = self._hotel_list_rows(nested)
                if rows:
                    return rows

        def find_hotel_list(node: Any) -> list[dict[str, Any]]:
            if isinstance(node, dict):
                value = node.get("hotelList")
                if isinstance(value, list):
                    return value
                for child in node.values():
                    rows = find_hotel_list(child)
                    if rows:
                        return rows
            elif isinstance(node, list):
                for child in node:
                    rows = find_hotel_list(child)
                    if rows:
                        return rows
            return []

        rows = find_hotel_list(data)
        if rows:
            return rows
        return []

    def _normalize_trip_hotel(
        self,
        row: dict[str, Any],
        check_in: dt.date,
        check_out: dt.date,
        target: dict[str, Any],
    ) -> dict[str, Any] | None:
        basic = row.get("hotelBasicInfo") or {}
        position = row.get("positionInfo") or {}
        hotel_id = str(basic.get("hotelId") or row.get("hotelId") or "").strip()
        raw_hotel_name = str(basic.get("hotelName") or "").strip()
        raw_hotel_en_name = str(basic.get("hotelEnName") or "").strip()
        raw_hotel_cn_name = self._extract_chinese_hotel_name(row)
        name_payload = hotel_name_payload_from_sources(
            [raw_hotel_name, raw_hotel_en_name, raw_hotel_cn_name],
            hotel_id=hotel_id,
            source="Trip.com 繁体转简体",
        )
        hotel_name = name_payload["hotelName"]
        price, price_includes_tax = self._extract_price_info(row)
        if not hotel_id or not hotel_name:
            return None
        coordinates = self._extract_coordinates(row)
        if not coordinates:
            return None
        detected = detectHotelBrand(hotel_name) or {}
        city_id = position.get("cityId") or target.get("cityId") or 0
        trip_url = self._detail_url(hotel_id, city_id, check_in, check_out)
        current_price = price if price is not None and price_includes_tax else None
        visible_price = price if price is not None and not price_includes_tax else None
        return {
            "hotelId": hotel_id,
            "hotelName": hotel_name,
            "hotelOriginalName": name_payload["hotelOriginalName"],
            "hotelNameSimplified": name_payload["hotelNameSimplified"],
            "hotelNameSource": name_payload["hotelNameSource"],
            "city": simplify_chinese_text(position.get("cityName") or target.get("city") or ""),
            "cityId": int(city_id or 0),
            "brand": detected.get("brand") or "",
            "group": detected.get("group") or "",
            "starRating": self._extract_star(row),
            "latitude": coordinates[0],
            "longitude": coordinates[1],
            "currentPrice": current_price,
            "visiblePrice": visible_price,
            "visiblePriceDate": check_in.isoformat() if visible_price is not None else "",
            "currentPricePreview": visible_price,
            "priceDate": check_in.isoformat() if current_price is not None else "",
            "priceIncludesTax": bool(current_price is not None and price_includes_tax),
            "priceSource": (
                "Trip.com tax-inclusive field"
                if current_price is not None
                else ("Trip.com base price field, tax pending" if visible_price is not None else "")
            ),
            "imageUrl": self._extract_image_url(row),
            "tripUrl": trip_url,
            "rating": self._extract_rating(row),
            "reviewCount": self._extract_review_count(row),
            "source": self.source_name,
        }

    def _detail_url(self, hotel_id: str, city_id: int | str, check_in: dt.date, check_out: dt.date) -> str:
        params = {
            "hotelId": hotel_id,
            "cityId": city_id,
            "checkin": check_in.strftime("%Y/%m/%d"),
            "checkout": check_out.strftime("%Y/%m/%d"),
            "curr": "CNY",
            "locale": "zh-CN",
            "crn": 1,
            "adult": 2,
            "children": 0,
        }
        return "https://www.trip.com/hotels/detail/?" + urlencode(params)

    def _extract_chinese_hotel_name(self, row: dict[str, Any]) -> str:
        candidates: list[str] = []

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                for key, child in node.items():
                    lowered = str(key).lower()
                    if isinstance(child, str) and "name" in lowered and contains_chinese_text(child):
                        candidates.append(simplify_chinese_text(child).strip())
                    visit(child)
            elif isinstance(node, list):
                for child in node:
                    visit(child)

        visit(row)
        cleaned = [value for value in candidates if 2 <= len(value) <= 80]
        if not cleaned:
            return ""

        hotel_suffix = re.compile(r"酒店|大酒店|饭店|宾馆|公寓|度假|客栈|民宿|旅店")
        brand_or_hotel_word = re.compile(
            r"酒店|大酒店|饭店|宾馆|公寓|希尔顿|万豪|雅高|温德姆|洲际|凯悦|"
            r"香格里拉|凯宾斯基|喜来登|铂尔曼|索菲特|华美达|格兰云天|维也纳|怡致|柏高|康帝"
        )
        preferred = [value for value in cleaned if hotel_suffix.search(value)]
        if preferred:
            return min(preferred, key=len)
        preferred = [value for value in cleaned if brand_or_hotel_word.search(value)]
        if preferred:
            return min(preferred, key=len)
        return cleaned[0]

    def _extract_price_info(self, row: dict[str, Any]) -> tuple[int | None, bool]:
        def values_by_exact_key(value: Any, keys: set[str]) -> list[Any]:
            found: list[Any] = []
            if isinstance(value, dict):
                for key, child in value.items():
                    if str(key).lower() in keys:
                        found.append(child)
                    found.extend(values_by_exact_key(child, keys))
            elif isinstance(value, list):
                for child in value:
                    found.extend(values_by_exact_key(child, keys))
            return found

        tax_price_keys = {
            "onlineandshoptaxprice",
            "onlinetaxprice",
            "taxprice",
            "taxinclusiveprice",
            "taxincludedprice",
            "totalprice",
            "displaytotalprice",
            "totalroomprice",
            "amountwithtax",
            "pricewithtax",
        }
        base_price_keys = {
            "price",
            "displayprice",
            "roomprice",
            "saleprice",
            "minprice",
            "lowestprice",
        }
        tax_candidates = values_by_exact_key(row, tax_price_keys)
        base_candidates = values_by_exact_key(row, base_price_keys)

        for value in tax_candidates:
            price = self._coerce_price_value(value)
            if price is not None:
                return price, True
        for value in base_candidates:
            price = self._coerce_price_value(value)
            if price is not None:
                return price, False

        text = json.dumps(row, ensure_ascii=False)
        for value in re.findall(r"CNY\s*([\d,]+)", text):
            price = self._coerce_price_value(value)
            if price is not None:
                return price, False
        return None, False

    def _extract_price(self, row: dict[str, Any]) -> int | None:
        return self._extract_price_info(row)[0]

    def _coerce_price_value(self, value: Any) -> int | None:
        if value in (None, ""):
            return None
        match = re.search(r"\d+(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?", str(value))
        if match:
            price = int(round(float(match.group(0).replace(",", ""))))
            if price > 0:
                return price
        return None

    def _coerce_float_value(self, value: Any) -> float | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        match = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", str(value))
        if match:
            return float(match.group(0).replace(",", ""))
        return None

    def _extract_star(self, row: dict[str, Any]) -> float:
        star_info = row.get("hotelStarInfo") if isinstance(row.get("hotelStarInfo"), dict) else {}
        for key in ("star", "diamond", "starLevel", "starRating"):
            value = star_info.get(key)
            if value in (None, ""):
                continue
            match = re.search(r"\d(?:\.\d)?", str(value))
            if match:
                star = float(match.group(0))
                if 2 <= star <= 5:
                    return min(5.0, star)
        basic = row.get("hotelBasicInfo") if isinstance(row.get("hotelBasicInfo"), dict) else {}
        for key in ("star", "diamond", "starLevel", "starRating", "hotelStar"):
            value = basic.get(key)
            if value in (None, ""):
                continue
            match = re.search(r"\d(?:\.\d)?", str(value))
            if match:
                star = float(match.group(0))
                if 2 <= star <= 5:
                    return min(5.0, star)

        text = json.dumps(row, ensure_ascii=False).lower()
        candidates = self._values_by_key(row, ("star", "diamond"))
        for value in candidates:
            match = re.search(r"\d(?:\.\d)?", str(value))
            if match:
                star = float(match.group(0))
                if 2 <= star <= 5:
                    return min(5.0, star)
            if re.search(r"五星|五钻|五鑽|luxury|deluxe", str(value), re.IGNORECASE):
                return 5.0
            if re.search(r"四星|四钻|四鑽|upscale|premium", str(value), re.IGNORECASE):
                return 4.0
        if re.search(r"五星|五钻|五鑽|豪华型|豪華型|奢华|奢華|luxury|deluxe|5-star|5 star", text):
            return 5.0
        if re.search(r"四星|四钻|四鑽|高档型|高檔型|高档|高檔|upscale|premium|4-star|4 star", text):
            return 4.0
        return 0

    def _extract_rating(self, row: dict[str, Any]) -> float | None:
        comment_info = row.get("commentInfo") if isinstance(row.get("commentInfo"), dict) else {}
        score = comment_info.get("commentScore")
        score_max = comment_info.get("scoreMax")
        if score not in (None, ""):
            try:
                rating = float(str(score).replace(",", ""))
                max_rating = float(str(score_max or 10).replace(",", ""))
            except ValueError:
                rating = 0
                max_rating = 0
            if rating > 0:
                return round(rating / max_rating * 5, 1) if max_rating > 5 else rating
        for value in self._values_by_key(row, ("score", "rating")):
            match = re.search(r"\d(?:\.\d)?", str(value))
            if match:
                rating = float(match.group(0))
                if 0 < rating <= 5:
                    return rating
        return None

    def _extract_review_count(self, row: dict[str, Any]) -> int | None:
        comment_info = row.get("commentInfo") if isinstance(row.get("commentInfo"), dict) else {}
        commenter_number = comment_info.get("commenterNumber")
        if commenter_number not in (None, ""):
            match = re.search(r"\d[\d,]*", str(commenter_number))
            if match:
                return int(match.group(0).replace(",", ""))
        for value in self._values_by_key(row, ("comment", "review")):
            match = re.search(r"\d[\d,]*", str(value))
            if match:
                count = int(match.group(0).replace(",", ""))
                if count > 0:
                    return count
        return None

    def _extract_image_url(self, row: dict[str, Any]) -> str:
        for value in self._values_by_key(row, ("image", "img", "picture", "pic")):
            text = str(value or "")
            match = re.search(r"https?://[^\"'\s]+", text)
            if match and any(ext in match.group(0).lower() for ext in (".jpg", ".jpeg", ".png", ".webp")):
                return match.group(0)
        return ""

    def _extract_coordinates(self, value: Any) -> tuple[float, float] | None:
        found: list[tuple[float, float]] = []

        def coerce_number(raw: Any) -> float | None:
            if raw in ("", None):
                return None
            if isinstance(raw, (int, float)):
                return float(raw)
            match = re.search(r"-?\d+(?:\.\d+)?", str(raw))
            return float(match.group(0)) if match else None

        def visit(node: Any) -> None:
            if found:
                return
            if isinstance(node, dict):
                lat_value = None
                lon_value = None
                for key, child in node.items():
                    lowered = str(key).lower()
                    if "lat" in lowered and "relation" not in lowered:
                        lat_value = coerce_number(child)
                    if any(token in lowered for token in ("lng", "lon", "longitude")):
                        lon_value = coerce_number(child)
                if lat_value is not None and lon_value is not None and -90 <= lat_value <= 90 and -180 <= lon_value <= 180:
                    found.append((lat_value, lon_value))
                    return
                for child in node.values():
                    visit(child)
            elif isinstance(node, list):
                for child in node:
                    visit(child)

        visit(value)
        return found[0] if found else None

    def _values_by_key(self, value: Any, key_tokens: tuple[str, ...]) -> list[Any]:
        found: list[Any] = []
        if isinstance(value, dict):
            for key, child in value.items():
                if any(token in str(key).lower() for token in key_tokens):
                    found.append(child)
                found.extend(self._values_by_key(child, key_tokens))
        elif isinstance(value, list):
            for child in value:
                found.extend(self._values_by_key(child, key_tokens))
        return found

    def _trip_head(self, client_id: str, pid: str, trace_id: str, locale: str = LIST_LOCALE) -> dict[str, Any]:
        return {
            "platform": "PC",
            "clientId": client_id,
            "bu": "ibu",
            "group": "TRIP",
            "aid": "",
            "sid": "",
            "ouid": "",
            "caid": "",
            "csid": "",
            "couid": "",
            "region": "XX",
            "locale": locale,
            "timeZone": "8",
            "currency": "CNY",
            "p": str(random.randint(10_000_000_000, 19_999_999_999)),
            "pageID": "10320668150",
            "deviceID": "PC",
            "clientVersion": "0",
            "frontend": {"vid": client_id, "sessionID": "1", "pvid": "1"},
            "extension": [
                {"name": "cityId", "value": ""},
                {"name": "checkIn", "value": ""},
                {"name": "checkOut", "value": ""},
                {"name": "region", "value": "XX"},
            ],
            "tripSub1": "",
            "qid": "",
            "pid": pid,
            "hotelExtension": {},
            "cid": client_id,
            "traceLogID": uuid.uuid4().hex[:13],
            "ticket": "",
            "href": "https://www.trip.com/hotels",
        }

    def _trace_id(self) -> str:
        prefix = str(random.randint(1_000_000_000, 1_999_999_999))
        millis = int(dt.datetime.now().timestamp() * 1000)
        suffix = random.randint(1_000_000_000, 1_999_999_999)
        return f"{prefix}-{millis}-{suffix}"


def provider_from_name(app_dir: Path, name: str | None) -> LocalJsonProvider | TripComProvider:
    provider_name = (name or os.environ.get("HOTEL_DEAL_PROVIDER") or "local").strip().lower()
    if provider_name in {"trip", "tripcom", "trip.com", "live"}:
        return TripComProvider()
    return LocalJsonProvider(app_dir)
