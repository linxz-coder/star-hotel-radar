from __future__ import annotations

import datetime as dt
import inspect
import math
import re
from typing import Any, Callable

from holiday_helper import HolidayCalendar, HolidayCalendarError, HolidayRange
from localization import contains_chinese_text, hotel_name_payload_from_sources, simplify_chinese_text


class HotelDealError(RuntimeError):
    pass


DEFAULT_HOLIDAY_CALENDAR = HolidayCalendar()
NAME_VERIFICATION_SUFFIX = "（中文名正在核验中...）"
NAME_VERIFICATION_MARKERS = ("中文名待核验", "中文名正在核验中")

BRAND_DEFINITIONS: list[dict[str, Any]] = [
    {
        "brand": "Hilton",
        "brandLabel": "希尔顿",
        "group": "Hilton Worldwide",
        "groupLabel": "希尔顿集团",
        "rank": 1,
        "aliases": ("hilton", "希尔顿", "康莱德", "conrad", "waldorf", "华尔道夫", "doubletree", "逸林"),
    },
    {
        "brand": "Marriott",
        "brandLabel": "万豪",
        "group": "Marriott International",
        "groupLabel": "万豪国际",
        "rank": 2,
        "aliases": (
            "marriott",
            "万豪",
            "jw",
            "ritz-carlton",
            "ritz carlton",
            "丽思卡尔顿",
            "westin",
            "威斯汀",
            "sheraton",
            "喜来登",
            "fairfield",
            "万枫",
            "st. regis",
            "st regis",
            "瑞吉",
            "renaissance",
            "万丽",
            "le meridien",
            "艾美",
            "w hotel",
            "w hotels",
            "w guangzhou",
            "guangzhou w",
            "w shanghai",
            "shanghai w",
            "w酒店",
            "w 广州",
            "w上海",
        ),
    },
    {
        "brand": "Accor",
        "brandLabel": "雅高",
        "group": "Accor",
        "groupLabel": "雅高集团",
        "rank": 3,
        "aliases": (
            "accor",
            "雅高",
            "sofitel",
            "索菲特",
            "pullman",
            "铂尔曼",
            "fairmont",
            "费尔蒙",
            "raffles",
            "莱佛士",
            "novotel",
            "诺富特",
        ),
    },
    {
        "brand": "Wyndham",
        "brandLabel": "温德姆",
        "group": "Wyndham Hotels & Resorts",
        "groupLabel": "温德姆集团",
        "rank": 4,
        "aliases": ("wyndham", "温德姆", "ramada", "华美达", "howard johnson", "豪生"),
    },
    {
        "brand": "IHG",
        "brandLabel": "洲际",
        "group": "InterContinental Hotels Group",
        "groupLabel": "洲际酒店集团",
        "rank": 5,
        "aliases": (
            "ihg",
            "intercontinental",
            "洲际",
            "crowne plaza",
            "皇冠假日",
            "hotel indigo",
            "英迪格",
            "holiday inn",
            "假日酒店",
            "regent",
            "丽晶",
            "kimpton",
        ),
    },
    {
        "brand": "Hyatt",
        "brandLabel": "凯悦",
        "group": "Hyatt Hotels",
        "groupLabel": "凯悦集团",
        "rank": 6,
        "aliases": ("hyatt", "凯悦", "君悦", "grand hyatt", "柏悦", "park hyatt", "andaz", "安达仕", "urcove", "逸扉"),
    },
    {
        "brand": "Shangri-La",
        "brandLabel": "香格里拉",
        "group": "Shangri-La Group",
        "groupLabel": "香格里拉集团",
        "rank": 7,
        "aliases": ("shangri-la", "shangri la", "香格里拉", "hotel jen", "今旅"),
    },
    {
        "brand": "Kempinski",
        "brandLabel": "凯宾斯基",
        "group": "Kempinski Hotels",
        "groupLabel": "凯宾斯基集团",
        "rank": 8,
        "aliases": ("kempinski", "凯宾斯基"),
    },
    {
        "brand": "Mandarin Oriental",
        "brandLabel": "文华东方",
        "group": "Mandarin Oriental Hotel Group",
        "groupLabel": "文华东方酒店集团",
        "rank": 9,
        "aliases": ("mandarin oriental", "文华东方", "文華東方"),
    },
    {
        "brand": "Lingnan Oriental",
        "brandLabel": "岭南东方",
        "group": "Lingnan Hotel Group",
        "groupLabel": "岭南酒店集团",
        "rank": 10,
        "aliases": ("lingnan oriental", "岭南东方", "嶺南東方", "岭南东方酒店", "嶺南東方酒店"),
    },
]

CHAIN_BRAND_DEFINITIONS: list[dict[str, Any]] = [
    {
        "brand": "Atour",
        "brandLabel": "亚朵",
        "group": "Atour Group",
        "groupLabel": "亚朵集团",
        "aliases": ("atour", "亚朵", "亚朵s", "轻居"),
    },
    {
        "brand": "Grand Skylight",
        "brandLabel": "格兰云天",
        "group": "Grand Skylight Hotels",
        "groupLabel": "格兰云天酒店集团",
        "aliases": ("grand skylight", "skytel", "格兰云天", "格兰云天阅"),
    },
    {
        "brand": "Huazhu",
        "brandLabel": "华住",
        "group": "H World Group",
        "groupLabel": "华住集团",
        "aliases": (
            "huazhu",
            "h world",
            "华住",
            "全季",
            "ji hotel",
            "han ting",
            "hanting",
            "汉庭",
            "桔子",
            "橘子",
            "orange hotel",
            "漫心",
            "星程",
            "海友",
            "宜必思",
            "ibis",
            "城际酒店",
            "intercityhotel",
            "intercity hotel",
        ),
    },
    {
        "brand": "Jin Jiang",
        "brandLabel": "锦江",
        "group": "Jin Jiang Hotels",
        "groupLabel": "锦江酒店集团",
        "aliases": (
            "jinjiang",
            "jin jiang",
            "锦江",
            "锦江之星",
            "维也纳",
            "vienna",
            "麗枫",
            "丽枫",
            "lavande",
            "喆啡",
            "james joyce",
            "希岸",
            "Xana",
            "凯里亚德",
            "kyriad",
            "康铂",
            "campanile",
            "7天",
            "7 days",
        ),
    },
    {
        "brand": "BTG Homeinn",
        "brandLabel": "首旅如家",
        "group": "BTG Homeinns Hotels",
        "groupLabel": "首旅如家集团",
        "aliases": ("home inn", "homeinn", "如家", "首旅如家", "和颐", "yitel", "莫泰", "motel 168", "璞隐"),
    },
    {
        "brand": "Dossen",
        "brandLabel": "东呈",
        "group": "Dossen International",
        "groupLabel": "东呈集团",
        "aliases": ("dossen", "东呈", "城市便捷", "宜尚", "怡程", "柏曼", "隐沫", "锋态度"),
    },
    {
        "brand": "GreenTree",
        "brandLabel": "格林",
        "group": "GreenTree Hospitality",
        "groupLabel": "格林酒店集团",
        "aliases": ("greentree", "green tree", "格林豪泰", "格林东方", "格美", "格菲"),
    },
    {
        "brand": "New Century",
        "brandLabel": "开元",
        "group": "New Century Hotels",
        "groupLabel": "开元酒店集团",
        "aliases": ("new century", "开元", "开元名都", "开元森泊", "开元曼居"),
    },
    {
        "brand": "Mehood",
        "brandLabel": "美豪",
        "group": "Mehood Hotels",
        "groupLabel": "美豪酒店集团",
        "aliases": ("mehood", "mehood lestie", "美豪", "美豪丽致", "美豪雅致"),
    },
    {
        "brand": "Rezen",
        "brandLabel": "丽呈",
        "group": "Rezen Group",
        "groupLabel": "丽呈集团",
        "aliases": ("rezen", "丽呈", "丽呈睿轩", "丽呈東谷", "丽呈東谷"),
    },
    {
        "brand": "Sunmei",
        "brandLabel": "尚美",
        "group": "Sunmei Group",
        "groupLabel": "尚美数智酒店集团",
        "aliases": ("sunmei", "尚美", "骏怡", "兰欧", "尚客优"),
    },
]


def parse_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.datetime.strptime(str(value), "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise HotelDealError("入住日期必须是 YYYY-MM-DD 格式") from exc


def _holiday_for_date(calendar: Any, day: dt.date) -> HolidayRange | None:
    try:
        return calendar.holiday_for_date(day)
    except (HolidayCalendarError, AttributeError):
        return None


def _is_statutory_holiday(calendar: Any, day: dt.date) -> bool:
    try:
        return bool(calendar.is_statutory_holiday(day))
    except (HolidayCalendarError, AttributeError):
        return bool(_holiday_for_date(calendar, day))


def _holiday_payload(holiday: HolidayRange | None) -> dict[str, Any] | None:
    if holiday is None:
        return None
    return {
        "code": holiday.code,
        "name": holiday.name,
        "start": holiday.start.isoformat(),
        "end": holiday.end.isoformat(),
        "days": holiday.days,
    }


def _holiday_compare_date_objects(selected: dt.date, holiday: HolidayRange) -> list[dt.date]:
    dates = holiday.dates()
    if len(dates) <= 4:
        return dates
    try:
        selected_index = dates.index(selected)
    except ValueError:
        selected_index = 0
    window_start = min(max(0, selected_index - 1), len(dates) - 4)
    return dates[window_start : window_start + 4]


def _ordinary_compare_date_objects(selected: dt.date, calendar: Any) -> list[dt.date]:
    weekday = selected.weekday()  # Monday=0, Sunday=6

    if weekday == 3:  # Thursday: Thu/Sun pairs
        pair_offsets = (0, 3)
    elif weekday == 5:  # Saturday: compare future Fri/Sat pairs
        pair_offsets = (6, 7)
    else:  # Sun-Wed weekday pairs, Fri weekend pairs
        pair_offsets = (0, 1)

    dates: list[dt.date] = []
    seen: set[dt.date] = set()
    for week_offset in range(52):
        for pair_offset in pair_offsets:
            candidate = selected + dt.timedelta(days=pair_offset + week_offset * 7)
            if candidate in seen or _is_statutory_holiday(calendar, candidate):
                continue
            dates.append(candidate)
            seen.add(candidate)
            if len(dates) == 4:
                return dates
    return dates


def get_compare_date_info(selected_date: str | dt.date, holiday_calendar: Any | None = None) -> dict[str, Any]:
    selected = parse_date(selected_date)
    calendar = holiday_calendar or DEFAULT_HOLIDAY_CALENDAR
    holiday = _holiday_for_date(calendar, selected)
    if holiday:
        dates = _holiday_compare_date_objects(selected, holiday)
        return {
            "selectedDate": selected.isoformat(),
            "compareDates": [item.isoformat() for item in dates],
            "compareMode": "holiday",
            "holiday": _holiday_payload(holiday),
            "compareNotice": f"你选择的是{holiday.name}公众假期，对比日已改为公众假期对比。",
        }

    dates = _ordinary_compare_date_objects(selected, calendar)
    return {
        "selectedDate": selected.isoformat(),
        "compareDates": [item.isoformat() for item in dates],
        "compareMode": "weekend" if selected.weekday() in (4, 5) else "weekday",
        "holiday": None,
        "compareNotice": "",
    }


def getCompareDateInfo(selectedDate: str | dt.date, holiday_calendar: Any | None = None) -> dict[str, Any]:
    return get_compare_date_info(selectedDate, holiday_calendar)


def get_compare_date_objects(selected_date: str | dt.date, holiday_calendar: Any | None = None) -> list[dt.date]:
    return [parse_date(item) for item in get_compare_date_info(selected_date, holiday_calendar)["compareDates"]]


def getCompareDates(selectedDate: str | dt.date, holiday_calendar: Any | None = None) -> list[str]:
    return get_compare_date_info(selectedDate, holiday_calendar)["compareDates"]


def calculateDealScore(currentPrice: int | float | None, comparePrices: list[int | float | None]) -> dict[str, Any]:
    if currentPrice in (None, ""):
        return {
            "averageComparePrice": None,
            "referencePrice": None,
            "referencePriceLabel": "平时参考价",
            "averageDiscountAmount": 0,
            "maxComparePrice": None,
            "maxSingleDayDiscountAmount": 0,
            "dealBasis": "",
            "discountAmount": 0,
            "discountPercent": 0,
            "isDeal": False,
        }

    current = float(currentPrice)
    valid_prices = [float(price) for price in comparePrices if price not in (None, "") and float(price) > 0]
    if not valid_prices:
        return {
            "averageComparePrice": None,
            "referencePrice": None,
            "referencePriceLabel": "平时参考价",
            "averageDiscountAmount": 0,
            "maxComparePrice": None,
            "maxSingleDayDiscountAmount": 0,
            "dealBasis": "",
            "discountAmount": 0,
            "discountPercent": 0,
            "isDeal": False,
        }

    average = sum(valid_prices) / len(valid_prices)
    average_discount = average - current
    max_compare = max(valid_prices)
    max_single_day_discount = max_compare - current
    is_average_deal = average_discount >= 100
    is_single_day_deal = max_single_day_discount >= 100
    use_single_day_reference = (not is_average_deal) and is_single_day_deal
    reference_price = max_compare if use_single_day_reference else average
    discount = reference_price - current
    percent = discount / reference_price * 100 if reference_price else 0
    return {
        "averageComparePrice": round(average),
        "referencePrice": round(reference_price),
        "referencePriceLabel": "最高对比价" if use_single_day_reference else "平时参考价",
        "averageDiscountAmount": round(average_discount),
        "maxComparePrice": round(max_compare),
        "maxSingleDayDiscountAmount": round(max_single_day_discount),
        "dealBasis": "single_day" if use_single_day_reference and is_single_day_deal else ("average" if is_average_deal else ""),
        "discountAmount": round(discount),
        "discountPercent": round(percent, 1),
        "isDeal": is_average_deal or is_single_day_deal,
    }


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", simplify_chinese_text(str(value or "").strip())).lower()


def _matched_brand_definition(hotelName: str, definitions: list[dict[str, Any]]) -> dict[str, Any] | None:
    text = normalize_text(hotelName)
    if not text:
        return None

    compact = re.sub(r"[\s·・,，.。()（）\-_/]+", "", text)
    for item in definitions:
        for alias in item["aliases"]:
            alias_text = normalize_text(alias)
            alias_compact = re.sub(r"[\s·・,，.。()（）\-_/]+", "", alias_text)
            if not alias_compact:
                continue
            if alias_text in text or alias_compact in compact:
                return {
                    "brand": item["brand"],
                    "brandLabel": item["brandLabel"],
                    "group": item["group"],
                    "groupLabel": item["groupLabel"],
                    "brandRank": item.get("rank", 99),
                }
    return None


def detectHotelBrand(hotelName: str) -> dict[str, Any] | None:
    return _matched_brand_definition(hotelName, BRAND_DEFINITIONS)


def detectHotelChainBrand(hotelName: str) -> dict[str, Any] | None:
    detected = _matched_brand_definition(hotelName, CHAIN_BRAND_DEFINITIONS)
    if not detected:
        return None
    detected["brandTier"] = "chain"
    return detected


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    )
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def filterByPrice(
    hotels: list[dict[str, Any]],
    minPrice: int | float | None = None,
    maxPrice: int | float | None = None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for hotel in hotels:
        current = hotel.get("currentPrice")
        if current in (None, ""):
            continue
        value = float(current)
        if minPrice is not None and value < float(minPrice):
            continue
        if maxPrice is not None and value > float(maxPrice):
            continue
        filtered.append(hotel)
    return filtered


def filter_candidates_by_price(
    hotels: list[dict[str, Any]],
    minPrice: int | float | None = None,
    maxPrice: int | float | None = None,
) -> list[dict[str, Any]]:
    if minPrice is None and maxPrice is None:
        return hotels
    priced = filterByPrice(hotels, minPrice, maxPrice)
    priced_keys = {str(hotel.get("hotelId") or hotel.get("hotelName") or "") for hotel in priced}
    pending = [
        hotel
        for hotel in hotels
        if hotel.get("currentPrice") in (None, "")
        and str(hotel.get("hotelId") or hotel.get("hotelName") or "") not in priced_keys
    ]
    return [*priced, *pending]


def getNearbyHotels(
    provider: Any,
    targetHotel: dict[str, Any],
    radiusKm: float = 3,
    minStar: int | float = 4,
    selectedDate: str | None = None,
    fastMode: bool = False,
    progressCallback: Callable[[list[dict[str, Any]]], None] | None = None,
) -> list[dict[str, Any]]:
    kwargs = {
        "target_hotel": targetHotel,
        "radius_km": float(radiusKm),
        "min_star": float(minStar),
        "selected_date": selectedDate,
        "fast_mode": fastMode,
    }
    if progressCallback is not None:
        try:
            signature = inspect.signature(provider.get_nearby_hotels)
            supports_progress = "progress_callback" in signature.parameters or any(
                param.kind == inspect.Parameter.VAR_KEYWORD
                for param in signature.parameters.values()
            )
        except (TypeError, ValueError):
            supports_progress = False
        if supports_progress:
            kwargs["progress_callback"] = progressCallback
    return provider.get_nearby_hotels(**kwargs)


def getHotelPrices(
    provider: Any,
    hotelIds: list[str],
    dates: list[str],
    progressCallback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, dict[str, int | None]]:
    return provider.get_hotel_prices(
        hotel_ids=hotelIds,
        dates=dates,
        progress_callback=progressCallback,
    )


def recommendation_reason(hotel: dict[str, Any]) -> str:
    brand_label = hotel.get("groupLabel") or hotel.get("brandLabel") or hotel.get("group") or hotel.get("brand")
    discount = hotel.get("discountAmount") or 0
    average_discount = hotel.get("averageDiscountAmount")
    single_day_discount = hotel.get("maxSingleDayDiscountAmount")
    distance = hotel.get("distanceKm")
    if hotel.get("isDeal") and hotel.get("dealBasis") == "single_day":
        comparison = f"某个同类型对比日约 {single_day_discount or discount} 元"
        if brand_label:
            return (
                f"这家酒店属于{brand_label}，目标日期价格低于{comparison}，"
                "即使均价差距不一定最大，也具备单日价差捡漏价值。"
            )
        return f"目标日期价格低于{comparison}，且星级和距离符合筛选条件。"
    if hotel.get("isDeal") and brand_label:
        return (
            f"这家酒店属于{brand_label}，目标日期价格低于同类型日期均价约 {average_discount or discount} 元，"
            "品牌、位置和价格同时具备捡漏价值。"
        )
    if hotel.get("isDeal"):
        return f"目标日期价格低于同类型日期均价约 {average_discount or discount} 元，且星级和距离符合筛选条件。"
    if brand_label and distance is not None:
        return f"这家酒店属于{brand_label}，距离目标酒店约 {distance}km，当前价格在筛选范围内。"
    return "这家酒店满足四星级以上、距离和价格筛选条件，可作为附近备选。"


def sort_hotels(hotels: list[dict[str, Any]], sort_by: str) -> list[dict[str, Any]]:
    if sort_by == "price":
        key = lambda item: (float(item.get("currentPrice") or 10**9), float(item.get("distanceKm") or 999))
    elif sort_by == "distance":
        key = lambda item: (float(item.get("distanceKm") or 999), float(item.get("currentPrice") or 10**9))
    elif sort_by == "star":
        key = lambda item: (-float(item.get("starRating") or 0), float(item.get("currentPrice") or 10**9))
    else:
        key = lambda item: (-float(item.get("discountAmount") or 0), float(item.get("currentPrice") or 10**9))
    return sorted(hotels, key=key)


def sort_recommended_hotels(hotels: list[dict[str, Any]], sort_by: str) -> list[dict[str, Any]]:
    if sort_by in {"price", "distance", "star", "discount"}:
        return sort_hotels(hotels, sort_by)
    return sorted(
        hotels,
        key=lambda item: (
            float(item.get("currentPrice") or 10**9),
            -float(item.get("starRating") or 0),
            float(item.get("distanceKm") or 999),
            int(item.get("brandRank") or 99),
        ),
    )


def hotel_price_for_date(
    hotel: dict[str, Any],
    date_value: str,
    *,
    allow_undated: bool = False,
) -> int | float | None:
    price = hotel.get("currentPrice")
    if price in (None, ""):
        return None
    if hotel.get("priceIncludesTax") is False:
        return None
    price_date = str(hotel.get("priceDate") or hotel.get("selectedDate") or "").strip()
    if price_date and price_date != date_value:
        return None
    if not price_date and not allow_undated:
        return None
    return price


def hotel_price_for_selected_date(hotel: dict[str, Any], selected_value: str) -> int | float | None:
    return hotel_price_for_date(hotel, selected_value, allow_undated=True)


def prefer_hotel_candidate(candidate: dict[str, Any], current: dict[str, Any], selected_value: str) -> bool:
    candidate_has_selected_price = hotel_price_for_selected_date(candidate, selected_value) is not None
    current_has_selected_price = hotel_price_for_selected_date(current, selected_value) is not None
    if candidate_has_selected_price != current_has_selected_price:
        return candidate_has_selected_price
    candidate_has_visible_price = candidate.get("visiblePrice") not in (None, "")
    current_has_visible_price = current.get("visiblePrice") not in (None, "")
    if candidate_has_visible_price != current_has_visible_price:
        return candidate_has_visible_price
    candidate_has_coords = candidate.get("latitude") not in (None, "") and candidate.get("longitude") not in (None, "")
    current_has_coords = current.get("latitude") not in (None, "") and current.get("longitude") not in (None, "")
    if candidate_has_coords != current_has_coords:
        return candidate_has_coords
    candidate_star = float(candidate.get("starRating") or 0)
    current_star = float(current.get("starRating") or 0)
    if candidate_star != current_star:
        return candidate_star > current_star
    return bool(candidate.get("imageUrl")) and not bool(current.get("imageUrl"))


def merge_nearby_hotels(
    existing_hotels: list[dict[str, Any]],
    discovered_hotels: list[dict[str, Any]],
    selected_value: str,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for hotel in [*existing_hotels, *discovered_hotels]:
        key = str(hotel.get("hotelId") or hotel.get("hotelName") or "").strip()
        if not key:
            continue
        item = dict(hotel)
        if key not in merged:
            merged[key] = item
            order.append(key)
            continue
        if prefer_hotel_candidate(item, merged[key], selected_value):
            merged[key] = {**merged[key], **item}
    return [merged[key] for key in order]


def cached_nearby_hotels_from_provider(
    provider: Any,
    target: dict[str, Any],
    *,
    radius_km: float,
    min_star: float,
    selected_value: str,
) -> list[dict[str, Any]]:
    getter = getattr(provider, "get_cached_nearby_hotels", None)
    if not callable(getter):
        return []
    try:
        hotels = getter(
            target_hotel=target,
            radius_km=radius_km,
            min_star=min_star,
            selected_date=selected_value,
        )
    except Exception:
        return []
    return hotels if isinstance(hotels, list) else []


def merge_known_price_cache(
    provider: Any,
    price_map: dict[str, dict[str, int | float | None]],
    hotel_ids: list[str],
    dates: list[str],
) -> None:
    getter = getattr(provider, "get_cached_hotel_prices", None)
    if not callable(getter):
        return
    try:
        cached_prices = getter(hotel_ids=hotel_ids, dates=dates)
    except Exception:
        return
    if not isinstance(cached_prices, dict):
        return
    for hotel_id, prices in cached_prices.items():
        if not isinstance(prices, dict):
            continue
        for date_value, price in prices.items():
            if price not in (None, ""):
                price_map.setdefault(str(hotel_id), {})[str(date_value)] = price


def radius_attempts(radius_km: float) -> list[float]:
    attempts = [float(radius_km)]
    for fallback_radius in (5.0, 10.0):
        if fallback_radius > float(radius_km) and fallback_radius not in attempts:
            attempts.append(fallback_radius)
    return attempts


def hotel_brand_payload(hotel: dict[str, Any]) -> dict[str, Any]:
    brand_text = " ".join(
        str(value or "")
        for value in (
            hotel.get("hotelName"),
            hotel.get("hotelNameSimplified"),
            hotel.get("hotelOriginalName"),
            hotel.get("brand"),
            hotel.get("group"),
        )
    )
    detected = detectHotelBrand(brand_text)
    if detected:
        detected["brandTier"] = "luxury"
        return detected
    chain_detected = detectHotelChainBrand(brand_text)
    if chain_detected:
        return chain_detected
    if hotel.get("brand"):
        return {
            "brand": hotel.get("brand"),
            "brandLabel": hotel.get("brand"),
            "group": hotel.get("group") or hotel.get("brand"),
            "groupLabel": hotel.get("group") or hotel.get("brand"),
            "brandRank": 99,
            "brandTier": "chain",
        }
    return {}


def has_name_verification_marker(value: Any) -> bool:
    text = str(value or "")
    return any(marker in text for marker in NAME_VERIFICATION_MARKERS)


def strip_name_verification_marker(value: Any) -> str:
    text = str(value or "")
    for marker in NAME_VERIFICATION_MARKERS:
        text = re.sub(rf"[（(]\s*{re.escape(marker)}\.*\s*[）)]", "", text)
    return text.strip()


def fallback_chinese_hotel_name(hotel: dict[str, Any], *, city: str = "", pending: bool = True) -> str:
    city_name = simplify_chinese_text(city or hotel.get("city") or "").strip()
    if not contains_chinese_text(city_name):
        city_name = ""
    brand_label = simplify_chinese_text(
        hotel.get("brandLabel") or hotel.get("brand") or hotel.get("groupLabel") or hotel.get("group") or ""
    ).strip()
    if brand_label in {"独立酒店", "酒店", "集团"} or not contains_chinese_text(brand_label):
        brand_label = ""
    if brand_label and brand_label.endswith("集团"):
        brand_label = brand_label[:-2]
    if brand_label and brand_label.endswith("酒店"):
        core = f"{city_name}{brand_label}" if city_name and not brand_label.startswith(city_name) else brand_label
    elif brand_label:
        core = f"{city_name}{brand_label}酒店" if city_name else f"{brand_label}酒店"
    elif city_name:
        hotel_id = str(hotel.get("hotelId") or "").strip()
        core = f"{city_name}星级酒店" if pending or not hotel_id else f"{city_name}携程酒店{hotel_id}"
    else:
        hotel_id = str(hotel.get("hotelId") or "").strip()
        core = "星级酒店" if pending or not hotel_id else f"携程酒店{hotel_id}"
    return f"{core}{NAME_VERIFICATION_SUFFIX if pending else ''}"


def hotel_result_payload(
    hotel: dict[str, Any],
    *,
    hotel_id: str,
    selected: dt.date,
    compare_dates: list[str],
    current_price: int | float | None,
    compare_prices: list[int | float | None],
    score: dict[str, Any],
    brand_payload: dict[str, Any],
    provider: Any,
    city: str = "",
) -> dict[str, Any]:
    hotel_name_payload = hotel_name_payload_from_sources(
        [
            hotel.get("hotelName"),
            hotel.get("hotelNameSimplified"),
            hotel.get("hotelOriginalName"),
        ],
        hotel_id=hotel_id,
        source=str(hotel.get("source") or provider.source_name),
    )
    item = {
        "hotelId": hotel_id,
        "hotelName": hotel_name_payload["hotelName"],
        "hotelOriginalName": hotel_name_payload["hotelOriginalName"] or hotel.get("hotelOriginalName") or "",
        "hotelNameSimplified": hotel_name_payload["hotelNameSimplified"] or hotel.get("hotelNameSimplified") or "",
        "hotelNameSource": hotel_name_payload["hotelNameSource"] or hotel.get("hotelNameSource") or "",
        "city": simplify_chinese_text(hotel.get("city") or city or ""),
        "cityId": hotel.get("cityId") or "",
        "brand": brand_payload.get("brand") or hotel.get("brand") or "独立酒店",
        "brandLabel": brand_payload.get("brandLabel") or hotel.get("brand") or "独立酒店",
        "group": brand_payload.get("group") or hotel.get("group") or "",
        "groupLabel": brand_payload.get("groupLabel") or hotel.get("group") or "",
        "brandRank": brand_payload.get("brandRank") or 99,
        "brandTier": brand_payload.get("brandTier") or ("chain" if brand_payload else ""),
        "starRating": hotel.get("starRating"),
        "distanceKm": round(float(hotel.get("distanceKm") or 0), 1),
        "selectedDate": selected.isoformat(),
        "currentPrice": int(current_price) if current_price not in (None, "") else None,
        "visiblePrice": int(hotel.get("visiblePrice")) if hotel.get("visiblePrice") not in (None, "") else None,
        "visiblePriceDate": hotel.get("visiblePriceDate") or "",
        "currentPricePreview": int(hotel.get("currentPricePreview")) if hotel.get("currentPricePreview") not in (None, "") else None,
        "priceIncludesTax": bool(hotel.get("priceIncludesTax")),
        "priceSource": hotel.get("priceSource") or "",
        "compareDates": compare_dates,
        "comparePrices": compare_prices,
        "rating": hotel.get("rating"),
        "reviewCount": hotel.get("reviewCount"),
        "imageUrl": hotel.get("imageUrl") or "",
        "tripUrl": hotel.get("tripUrl") or "",
        "source": hotel.get("source") or provider.source_name,
        "isRecommendedBrand": bool(brand_payload and brand_payload.get("brandRank") != 99),
        **score,
    }
    return normalize_result_hotel_name(item, city=city)


def pending_price_score() -> dict[str, Any]:
    return {
        "averageComparePrice": None,
        "referencePrice": None,
        "referencePriceLabel": "平时参考价",
        "averageDiscountAmount": None,
        "maxComparePrice": None,
        "maxSingleDayDiscountAmount": None,
        "dealBasis": "",
        "discountAmount": None,
        "discountPercent": None,
        "isDeal": False,
    }


def normalize_result_hotel_name(hotel: dict[str, Any], *, city: str = "") -> dict[str, Any]:
    item = dict(hotel)
    hotel_id = str(item.get("hotelId") or "")
    original_name = str(
        item.get("hotelOriginalName")
        or item.get("hotelName")
        or item.get("hotelNameSimplified")
        or ""
    ).strip()
    payload = hotel_name_payload_from_sources(
        [
            item.get("hotelName"),
            item.get("hotelNameSimplified"),
            item.get("hotelOriginalName"),
        ],
        hotel_id=hotel_id,
        source=str(item.get("hotelNameSource") or item.get("source") or ""),
    )
    item.update(payload)
    item["city"] = simplify_chinese_text(item.get("city") or city or "")
    if has_name_verification_marker(item.get("hotelName")) or not has_resolved_chinese_hotel_name(item):
        fallback_name = fallback_chinese_hotel_name(item, city=city)
        item["hotelName"] = fallback_name
        item["hotelNameSimplified"] = fallback_name
        item["hotelOriginalName"] = original_name
        item["hotelNameSource"] = "本地中文名兜底（原名正在核验中）"
        item["nameProcessing"] = True
    elif item.get("hotelNameSource") != "本地中文名兜底（原名正在核验中）":
        item.pop("nameProcessing", None)
    return item


def has_displayable_chinese_hotel_name(hotel: dict[str, Any]) -> bool:
    return any(
        contains_chinese_text(simplify_chinese_text(value))
        for value in (
            hotel.get("hotelName"),
            hotel.get("hotelNameSimplified"),
            hotel.get("hotelOriginalName"),
        )
    )


def has_resolved_chinese_hotel_name(hotel: dict[str, Any]) -> bool:
    return any(
        contains_chinese_text(simplify_chinese_text(value)) and not has_name_verification_marker(value)
        for value in (
            hotel.get("hotelName"),
            hotel.get("hotelNameSimplified"),
            hotel.get("hotelOriginalName"),
        )
    )


def hotel_name_needs_verification(hotel: dict[str, Any]) -> bool:
    source = str(hotel.get("hotelNameSource") or "")
    return bool(
        hotel.get("nameProcessing")
        or "正在核验" in source
        or "待核验" in source
        or has_name_verification_marker(hotel.get("hotelName"))
        or has_name_verification_marker(hotel.get("hotelNameSimplified"))
    )


def apply_verified_hotel_name_payload(
    hotel: dict[str, Any],
    payload: dict[str, Any],
    *,
    city: str = "",
) -> dict[str, Any]:
    item = dict(hotel)
    item["hotelName"] = str(payload.get("hotelName") or payload.get("name") or item.get("hotelName") or "").strip()
    item["hotelOriginalName"] = str(payload.get("hotelOriginalName") or item.get("hotelOriginalName") or "").strip()
    item["hotelNameSimplified"] = str(payload.get("hotelNameSimplified") or item.get("hotelName") or "").strip()
    item["hotelNameSource"] = str(payload.get("hotelNameSource") or payload.get("source") or "中文名核验完成").strip()
    item.pop("nameProcessing", None)
    return normalize_result_hotel_name(item, city=city)


def finalize_pending_hotel_name(hotel: dict[str, Any], *, city: str = "") -> dict[str, Any]:
    item = dict(hotel)
    if has_resolved_chinese_hotel_name(item) and not has_name_verification_marker(item.get("hotelName")):
        item.pop("nameProcessing", None)
        return normalize_result_hotel_name(item, city=city)
    original_name = strip_name_verification_marker(
        item.get("hotelOriginalName")
        or item.get("hotelName")
        or item.get("hotelNameSimplified")
        or ""
    )
    original_payload = hotel_name_payload_from_sources(
        [original_name],
        hotel_id=str(item.get("hotelId") or ""),
        source=str(item.get("source") or "Trip.com 原始名规则转中文"),
    )
    if original_payload.get("hotelName") and has_resolved_chinese_hotel_name(original_payload):
        item.update(original_payload)
        item.pop("nameProcessing", None)
        return normalize_result_hotel_name(item, city=city)
    final_name = strip_name_verification_marker(fallback_chinese_hotel_name(item, city=city, pending=False))
    item["hotelName"] = final_name
    item["hotelNameSimplified"] = final_name
    item["hotelOriginalName"] = original_name
    item["hotelNameSource"] = "本地中文名兜底（未匹配到标准中文名）"
    item.pop("nameProcessing", None)
    return item


def mark_hotel_name_processing_if_needed(hotel: dict[str, Any]) -> bool:
    if hotel.get("nameProcessing"):
        return True
    if has_displayable_chinese_hotel_name(hotel):
        return False
    if not (hotel.get("hotelName") or hotel.get("hotelOriginalName")):
        return False
    hotel["nameProcessing"] = True
    hotel["hotelNameSource"] = hotel.get("hotelNameSource") or "Trip.com 原始名称，后台中文名处理中"
    return True


def current_price_result_from_hotels(
    provider: Any,
    *,
    city: str,
    target_hotel_name: str,
    selected: dt.date,
    radius_km: float,
    min_star: float,
    min_price: int | None,
    max_price: int | None,
    sort_by: str,
    target: dict[str, Any],
    compare_info: dict[str, Any],
    compare_dates: list[str],
    attempts: list[float],
    effective_radius_km: float,
    nearby_hotels: list[dict[str, Any]],
    include_provisional_names: bool = False,
    defer_price_filter: bool = False,
    preserve_order: bool = False,
) -> dict[str, Any]:
    enriched_hotels: list[dict[str, Any]] = []
    provisional_name_count = 0
    for hotel in nearby_hotels:
        hotel_id = str(hotel.get("hotelId") or "")
        current_price = hotel_price_for_selected_date(hotel, selected.isoformat())
        if not hotel_id:
            continue
        has_current_price = current_price not in (None, "")
        brand_payload = hotel_brand_payload(hotel)
        item = hotel_result_payload(
            hotel,
            hotel_id=hotel_id,
            selected=selected,
            compare_dates=compare_dates,
            current_price=current_price,
            compare_prices=[],
            score=pending_price_score(),
            brand_payload=brand_payload,
            provider=provider,
            city=city,
        )
        if mark_hotel_name_processing_if_needed(item):
            provisional_name_count += 1
        item["pricePending"] = not has_current_price
        item["recommendationReason"] = (
            "已拿到目标日期实时价格，后台正在补齐同类型日期均价和优惠力度。"
            if has_current_price
            else "已匹配到附近四星级以上候选酒店，但 Trip.com 暂未返回该日期含税价，后台会继续尝试补价。"
        )
        enriched_hotels.append(item)

    recommended_source_hotels = list(enriched_hotels)
    price_filtered_hotels = (
        enriched_hotels
        if defer_price_filter
        else filter_candidates_by_price(enriched_hotels, min_price, max_price)
    )
    effective_hotels = [
        hotel
        for hotel in price_filtered_hotels
        if float(hotel.get("distanceKm") or 999) <= effective_radius_km
    ]
    recommended_hotels = sort_recommended_hotels(
        [hotel for hotel in recommended_source_hotels if hotel["isRecommendedBrand"]],
        sort_by if sort_by != "discount" else "recommendation",
    )

    return {
        "query": {
            "city": city,
            "targetHotel": target_hotel_name,
            "selectedDate": selected.isoformat(),
            "radiusKm": radius_km,
            "effectiveRadiusKm": effective_radius_km,
            "minStar": min_star,
            "minPrice": min_price,
            "maxPrice": max_price,
            "sortBy": sort_by,
            "compareMode": compare_info["compareMode"],
            "holiday": compare_info["holiday"],
        },
        "targetHotel": target,
        "compareDates": compare_dates,
        "allHotels": effective_hotels if preserve_order else sort_hotels(effective_hotels, sort_by),
        "dealHotels": [],
        "recommendedHotels": recommended_hotels,
        "summary": {
            "candidateCount": len(effective_hotels),
            "pricedHotelCount": sum(1 for hotel in effective_hotels if hotel.get("currentPrice") not in (None, "")),
            "unpricedCandidateCount": sum(1 for hotel in effective_hotels if hotel.get("currentPrice") in (None, "")),
            "rawCandidateCount": len(nearby_hotels),
            "provisionalNameCount": provisional_name_count,
            "nameNormalizationDeferred": provisional_name_count > 0 or include_provisional_names,
            "priceFilterDeferred": defer_price_filter,
            "sortDeferred": preserve_order,
            "dealCount": 0,
            "recommendedCount": len(recommended_hotels),
            "recommendedIgnoresPriceFilter": True,
            "source": provider.source_name,
            "compareMode": compare_info["compareMode"],
            "holiday": compare_info["holiday"],
            "holidayName": (compare_info["holiday"] or {}).get("name"),
            "requestedRadiusKm": radius_km,
            "effectiveRadiusKm": effective_radius_km,
            "attemptedRadii": attempts,
            "radiusExpanded": effective_radius_km > float(radius_km),
            "partial": True,
            "jobStatus": "pricing",
        },
    }


def search_current_prices(
    provider: Any,
    city: str,
    target_hotel_name: str,
    selected_date: str,
    radius_km: float = 3,
    min_star: float = 4,
    min_price: int | None = None,
    max_price: int | None = None,
    sort_by: str = "price",
    target_hint: dict[str, Any] | None = None,
    fast_mode: bool = True,
    include_provisional_names: bool = False,
    defer_price_filter: bool = False,
    preserve_order: bool = False,
) -> dict[str, Any]:
    selected = parse_date(selected_date)
    compare_info = get_compare_date_info(selected)
    compare_dates = compare_info["compareDates"]
    attempts = radius_attempts(radius_km)
    max_radius_km = max(attempts)

    target = provider.resolve_target_hotel(city=city, hotel_name=target_hotel_name, target_hint=target_hint)
    nearby_hotels = getNearbyHotels(
        provider,
        target,
        radiusKm=max_radius_km,
        minStar=min_star,
        selectedDate=selected.isoformat(),
        fastMode=fast_mode,
    )

    return current_price_result_from_hotels(
        provider,
        city=city,
        target_hotel_name=target_hotel_name,
        selected=selected,
        radius_km=radius_km,
        min_star=min_star,
        min_price=min_price,
        max_price=max_price,
        sort_by=sort_by,
        target=target,
        compare_info=compare_info,
        compare_dates=compare_dates,
        attempts=attempts,
        effective_radius_km=max_radius_km,
        nearby_hotels=nearby_hotels,
        include_provisional_names=include_provisional_names,
        defer_price_filter=defer_price_filter,
        preserve_order=preserve_order,
    )


def search_result_from_price_map(
    provider: Any,
    *,
    city: str,
    target_hotel_name: str,
    selected: dt.date,
    radius_km: float,
    min_star: float,
    min_price: int | None,
    max_price: int | None,
    sort_by: str,
    target: dict[str, Any],
    compare_info: dict[str, Any],
    compare_dates: list[str],
    attempts: list[float],
    max_radius_km: float,
    nearby_hotels: list[dict[str, Any]],
    price_map: dict[str, dict[str, int | float | None]],
    partial: bool = False,
    completed_compare_dates: list[str] | None = None,
) -> dict[str, Any]:
    selected_value = selected.isoformat()
    completed_compare_dates = completed_compare_dates or []
    enriched_hotels: list[dict[str, Any]] = []
    provisional_name_count = 0
    for hotel in nearby_hotels:
        hotel_id = str(hotel.get("hotelId") or "")
        if not hotel_id:
            continue
        prices = price_map.get(hotel_id, {})
        current_price = prices.get(selected_value)
        if current_price in (None, ""):
            current_price = hotel_price_for_selected_date(hotel, selected_value)
        has_current_price = current_price not in (None, "")

        compare_prices = [
            prices.get(compare_date)
            if prices.get(compare_date) not in (None, "")
            else (current_price if compare_date == selected_value else None)
            for compare_date in compare_dates
        ]
        score = calculateDealScore(current_price, compare_prices) if has_current_price else pending_price_score()
        brand_payload = hotel_brand_payload(hotel)
        item = hotel_result_payload(
            hotel,
            hotel_id=hotel_id,
            selected=selected,
            compare_dates=compare_dates,
            current_price=current_price,
            compare_prices=compare_prices,
            score=score,
            brand_payload=brand_payload,
            provider=provider,
            city=city,
        )
        if mark_hotel_name_processing_if_needed(item):
            provisional_name_count += 1
        item["pricePending"] = not has_current_price
        item["recommendationReason"] = (
            recommendation_reason(item)
            if has_current_price
            else "已匹配到附近四星级以上候选酒店，但 Trip.com 暂未返回该日期含税价，暂不能判断是否捡漏。"
        )
        enriched_hotels.append(item)

    recommended_source_hotels = list(enriched_hotels)
    price_filtered_hotels = filter_candidates_by_price(enriched_hotels, min_price, max_price)
    effective_radius_km = attempts[-1]
    attempted_radii: list[float] = []
    for attempt_radius in attempts:
        attempted_radii.append(attempt_radius)
        attempt_deals = [
            hotel
            for hotel in price_filtered_hotels
            if hotel["isDeal"] and float(hotel.get("distanceKm") or 999) <= attempt_radius
        ]
        if attempt_deals:
            effective_radius_km = attempt_radius
            break

    deal_scope_hotels = [
        hotel
        for hotel in price_filtered_hotels
        if float(hotel.get("distanceKm") or 999) <= effective_radius_km
    ]
    all_candidate_hotels = sort_hotels(price_filtered_hotels, sort_by)
    deal_hotels = sort_hotels([hotel for hotel in deal_scope_hotels if hotel["isDeal"]], sort_by)
    recommended_hotels = sort_recommended_hotels(
        [hotel for hotel in recommended_source_hotels if hotel["isRecommendedBrand"]],
        sort_by if sort_by != "discount" else "recommendation",
    )
    candidate_count = len(all_candidate_hotels)
    summary: dict[str, Any] = {
        "candidateCount": candidate_count,
        "pricedHotelCount": sum(1 for hotel in all_candidate_hotels if hotel.get("currentPrice") not in (None, "")),
        "unpricedCandidateCount": sum(1 for hotel in all_candidate_hotels if hotel.get("currentPrice") in (None, "")),
        "dealScopeCandidateCount": len(deal_scope_hotels),
        "candidateRadiusKm": max_radius_km,
        "dealCount": len(deal_hotels),
        "recommendedCount": len(recommended_hotels),
        "recommendedIgnoresPriceFilter": True,
        "rawCandidateCount": len(nearby_hotels),
        "provisionalNameCount": provisional_name_count,
        "nameNormalizationDeferred": provisional_name_count > 0,
        "source": provider.source_name,
        "compareMode": compare_info["compareMode"],
        "holiday": compare_info["holiday"],
        "holidayName": (compare_info["holiday"] or {}).get("name"),
        "requestedRadiusKm": radius_km,
        "effectiveRadiusKm": effective_radius_km,
        "attemptedRadii": attempted_radii,
        "radiusExpanded": effective_radius_km > float(radius_km),
        "priceCompareComplete": not partial,
        "completedCompareDateCount": len(completed_compare_dates),
        "totalCompareDateCount": len(compare_dates),
    }
    if partial:
        summary["partial"] = True
        summary["jobStatus"] = "pricing"

    return {
        "query": {
            "city": city,
            "targetHotel": target_hotel_name,
            "selectedDate": selected.isoformat(),
            "radiusKm": radius_km,
            "effectiveRadiusKm": effective_radius_km,
            "minStar": min_star,
            "minPrice": min_price,
            "maxPrice": max_price,
            "sortBy": sort_by,
            "compareMode": compare_info["compareMode"],
            "holiday": compare_info["holiday"],
        },
        "targetHotel": target,
        "compareDates": compare_dates,
        "allHotels": all_candidate_hotels,
        "dealHotels": deal_hotels,
        "recommendedHotels": recommended_hotels,
        "summary": summary,
    }


def search_deals(
    provider: Any,
    city: str,
    target_hotel_name: str,
    selected_date: str,
    radius_km: float = 3,
    min_star: float = 4,
    min_price: int | None = None,
    max_price: int | None = None,
    sort_by: str = "discount",
    target_hint: dict[str, Any] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    price_progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    selected = parse_date(selected_date)
    compare_info = get_compare_date_info(selected)
    compare_dates = compare_info["compareDates"]
    attempts = radius_attempts(radius_km)
    max_radius_km = max(attempts)

    target = provider.resolve_target_hotel(city=city, hotel_name=target_hotel_name, target_hint=target_hint)
    selected_value = selected.isoformat()
    streaming_nearby_hotels: list[dict[str, Any]] = []
    last_candidate_snapshot_signature: tuple[tuple[str, str, str, str, str], ...] = ()

    def publish_current_candidates(discovered_hotels: list[dict[str, Any]]) -> None:
        nonlocal streaming_nearby_hotels, last_candidate_snapshot_signature
        if not progress_callback:
            return
        streaming_nearby_hotels = merge_nearby_hotels(
            streaming_nearby_hotels,
            discovered_hotels,
            selected_value,
        )
        signature = tuple(
            (
                str(hotel.get("hotelId") or hotel.get("hotelName") or ""),
                str(hotel_price_for_selected_date(hotel, selected_value) or ""),
                str(hotel.get("visiblePrice") or ""),
                str(hotel.get("hotelName") or ""),
                str(hotel.get("hotelOriginalName") or ""),
            )
            for hotel in streaming_nearby_hotels
        )
        if not signature or signature == last_candidate_snapshot_signature:
            return
        last_candidate_snapshot_signature = signature
        progress_callback(
            current_price_result_from_hotels(
                provider,
                city=city,
                target_hotel_name=target_hotel_name,
                selected=selected,
                radius_km=radius_km,
                min_star=min_star,
                min_price=min_price,
                max_price=max_price,
                sort_by=sort_by,
                target=target,
                compare_info=compare_info,
                compare_dates=compare_dates,
                attempts=attempts,
                effective_radius_km=max_radius_km,
                nearby_hotels=streaming_nearby_hotels,
                include_provisional_names=True,
                defer_price_filter=True,
                preserve_order=True,
            )
        )

    nearby_hotels = getNearbyHotels(
        provider,
        target,
        radiusKm=max_radius_km,
        minStar=min_star,
        selectedDate=selected_value,
        progressCallback=publish_current_candidates if progress_callback else None,
    )
    if streaming_nearby_hotels:
        nearby_hotels = merge_nearby_hotels(streaming_nearby_hotels, nearby_hotels, selected_value)
    publish_current_candidates(nearby_hotels)
    hotel_ids = [str(hotel["hotelId"]) for hotel in nearby_hotels if hotel.get("hotelId")]
    price_map: dict[str, dict[str, int | float | None]] = {
        str(hotel.get("hotelId")): {selected_value: hotel_price_for_selected_date(hotel, selected_value)}
        for hotel in nearby_hotels
        if hotel.get("hotelId") and hotel_price_for_selected_date(hotel, selected_value) not in (None, "")
    }
    merge_known_price_cache(provider, price_map, hotel_ids, [selected_value])
    price_attempted_keys: set[tuple[str, str]] = {
        (str(hotel_id), selected_value)
        for hotel_id in hotel_ids
        if price_map.get(str(hotel_id), {}).get(selected_value) not in (None, "")
    }
    completed_compare_dates: list[str] = []
    selected_price_known = all(
        price_map.get(str(hotel_id), {}).get(selected_value) not in (None, "")
        for hotel_id in hotel_ids
    )
    last_price_progress_partial_signature: tuple[int, int, int] | None = None

    def merge_cached_candidate_snapshot() -> list[str]:
        nonlocal nearby_hotels, hotel_ids
        previous_ids = set(hotel_ids)
        discovered_hotels = cached_nearby_hotels_from_provider(
            provider,
            target,
            radius_km=max_radius_km,
            min_star=min_star,
            selected_value=selected_value,
        )
        if not discovered_hotels:
            return []
        nearby_hotels = merge_nearby_hotels(nearby_hotels, discovered_hotels, selected_value)
        hotel_ids = [str(hotel["hotelId"]) for hotel in nearby_hotels if hotel.get("hotelId")]
        merge_known_price_cache(provider, price_map, hotel_ids, [selected_value, *compare_dates])
        return [hotel_id for hotel_id in hotel_ids if hotel_id not in previous_ids]

    def ordered_price_dates(date_values: list[str]) -> list[str]:
        requested = {str(date_value) for date_value in date_values if date_value}
        ordered: list[str] = []
        if selected_value in requested:
            ordered.append(selected_value)
        for compare_date in compare_dates:
            if compare_date in requested and compare_date not in ordered:
                ordered.append(compare_date)
        for date_value in date_values:
            date_value = str(date_value)
            if date_value and date_value not in ordered:
                ordered.append(date_value)
        return ordered

    def sync_candidate_prices_from_hotels(date_values: list[str]) -> bool:
        changed = False
        for hotel in nearby_hotels:
            hotel_id = str(hotel.get("hotelId") or "")
            if not hotel_id:
                continue
            for date_value in ordered_price_dates(date_values):
                price = hotel_price_for_date(
                    hotel,
                    date_value,
                    allow_undated=date_value == selected_value,
                )
                if price in (None, ""):
                    continue
                if price_map.get(hotel_id, {}).get(date_value) in (None, ""):
                    price_map.setdefault(hotel_id, {})[date_value] = price
                    changed = True
        return changed

    def backfill_missing_prices_for_dates(
        date_values: list[str],
        *,
        force_retry: bool = False,
        backfill_mode: str = "pending",
    ) -> bool:
        dates_to_backfill = ordered_price_dates(date_values)
        if not dates_to_backfill:
            return False
        before_prices = {
            (str(hotel_id), date_value): price_map.get(str(hotel_id), {}).get(date_value)
            for hotel_id in hotel_ids
            for date_value in dates_to_backfill
        }
        changed = sync_candidate_prices_from_hotels(dates_to_backfill)
        merge_known_price_cache(provider, price_map, hotel_ids, dates_to_backfill)
        for date_value in dates_to_backfill:
            missing_hotel_ids: list[str] = []
            for hotel_id in hotel_ids:
                hotel_id = str(hotel_id)
                if price_map.get(hotel_id, {}).get(date_value) not in (None, ""):
                    price_attempted_keys.add((hotel_id, date_value))
                    continue
                attempt_key = (hotel_id, date_value)
                if attempt_key in price_attempted_keys and not force_retry:
                    continue
                price_attempted_keys.add(attempt_key)
                missing_hotel_ids.append(hotel_id)
            if not missing_hotel_ids:
                continue

            def publish_backfill_price_progress(progress_info: dict[str, Any]) -> None:
                phase = str(progress_info.get("phase") or "")
                normalized = dict(progress_info)
                normalized["backfillMode"] = backfill_mode
                normalized["totalExpectedPriceCount"] = len(hotel_ids) * len(dates_to_backfill)
                if price_progress_callback:
                    price_progress_callback(normalized)
                if phase in {"list", "dom-list", "detail", "deep", "complete"}:
                    publish_price_progress_candidate_snapshot()

            try:
                date_prices = getHotelPrices(
                    provider,
                    missing_hotel_ids,
                    [date_value],
                    progressCallback=publish_backfill_price_progress if (price_progress_callback or progress_callback) else None,
                )
            except Exception:
                continue
            for hotel_id, prices in date_prices.items():
                price = prices.get(date_value) if isinstance(prices, dict) else None
                if price not in (None, ""):
                    price_map.setdefault(str(hotel_id), {})[date_value] = price
            merge_cached_candidate_snapshot()
            changed = sync_candidate_prices_from_hotels(dates_to_backfill) or changed
            merge_known_price_cache(provider, price_map, hotel_ids, dates_to_backfill)
        return changed or any(
            before_prices.get((str(hotel_id), date_value)) != price_map.get(str(hotel_id), {}).get(date_value)
            for hotel_id in hotel_ids
            for date_value in dates_to_backfill
        )

    def publish_price_progress_candidate_snapshot() -> None:
        nonlocal nearby_hotels, hotel_ids, last_price_progress_partial_signature
        if not progress_callback:
            return
        merge_cached_candidate_snapshot()
        partial_result = search_result_from_price_map(
            provider,
            city=city,
            target_hotel_name=target_hotel_name,
            selected=selected,
            radius_km=radius_km,
            min_star=min_star,
            min_price=min_price,
            max_price=max_price,
            sort_by=sort_by,
            target=target,
            compare_info=compare_info,
            compare_dates=compare_dates,
            attempts=attempts,
            max_radius_km=max_radius_km,
            nearby_hotels=nearby_hotels,
            price_map=price_map,
            partial=True,
            completed_compare_dates=completed_compare_dates,
        )
        summary = partial_result.get("summary") or {}
        signature = (
            int(summary.get("candidateCount") or 0),
            int(summary.get("pricedHotelCount") or 0),
            int(summary.get("completedCompareDateCount") or 0),
        )
        if signature == last_price_progress_partial_signature:
            return
        last_price_progress_partial_signature = signature
        progress_callback(partial_result)

    if selected_price_known and selected_value in compare_dates:
        completed_compare_dates.append(selected_value)
        if price_progress_callback:
            priced_count = sum(
                1
                for hotel_id in hotel_ids
                if price_map.get(str(hotel_id), {}).get(selected_value) not in (None, "")
            )
            price_progress_callback(
                {
                    "stage": "compare-price",
                    "phase": "complete",
                    "date": selected_value,
                    "dateIndex": compare_dates.index(selected_value) + 1,
                    "completedDates": len(completed_compare_dates),
                    "totalDates": len(compare_dates),
                    "pricedHotelCount": priced_count,
                    "missingHotelCount": max(len(hotel_ids) - priced_count, 0),
                    "totalHotels": len(hotel_ids),
                }
            )

    if selected_value not in compare_dates:
        backfill_missing_prices_for_dates([selected_value])

    selected_date_price_checked = selected_price_known or selected_value not in compare_dates
    fetched_compare_dates: set[str] = {selected_value} if selected_date_price_checked else set()
    parallel_price_fetcher = getattr(provider, "get_hotel_prices_for_dates_parallel", None)
    parallel_compare_dates = [
        compare_date
        for compare_date in compare_dates
        if compare_date not in fetched_compare_dates
    ]
    if callable(parallel_price_fetcher) and len(parallel_compare_dates) > 1:
        requested_hotel_ids = list(hotel_ids)

        def publish_parallel_price_progress(progress_info: dict[str, Any]) -> None:
            date_value = str(progress_info.get("date") or "")
            phase = str(progress_info.get("phase") or "")
            normalized = dict(progress_info)
            date_index = compare_dates.index(date_value) + 1 if date_value in compare_dates else int(progress_info.get("dateIndex") or 0)
            completed_from_batch = int(progress_info.get("completedDates") or 0)
            normalized.update(
                {
                    "date": date_value,
                    "dateIndex": date_index,
                    "completedDates": min(len(completed_compare_dates) + completed_from_batch, len(compare_dates)),
                    "totalDates": len(compare_dates),
                    "totalExpectedPriceCount": len(hotel_ids) * len(compare_dates),
                    "parallelDates": True,
                }
            )
            if price_progress_callback:
                price_progress_callback(normalized)
            if phase in {"list", "dom-list", "detail", "deep", "complete"}:
                publish_price_progress_candidate_snapshot()

        try:
            bulk_prices = parallel_price_fetcher(
                requested_hotel_ids,
                parallel_compare_dates,
                progress_callback=publish_parallel_price_progress,
            )
        except Exception:
            bulk_prices = {}
        if bulk_prices:
            for hotel_id, prices in bulk_prices.items():
                if not isinstance(prices, dict):
                    continue
                for compare_date in parallel_compare_dates:
                    if compare_date in prices:
                        price_map.setdefault(str(hotel_id), {})[compare_date] = prices.get(compare_date)
            price_attempted_keys.update(
                (str(hotel_id), compare_date)
                for hotel_id in requested_hotel_ids
                for compare_date in parallel_compare_dates
            )
            fetched_compare_dates.update(parallel_compare_dates)
            for compare_date in parallel_compare_dates:
                if compare_date not in completed_compare_dates:
                    completed_compare_dates.append(compare_date)
            merge_cached_candidate_snapshot()
            backfill_changed = backfill_missing_prices_for_dates(list(fetched_compare_dates))
            if progress_callback:
                progress_callback(
                    search_result_from_price_map(
                        provider,
                        city=city,
                        target_hotel_name=target_hotel_name,
                        selected=selected,
                        radius_km=radius_km,
                        min_star=min_star,
                        min_price=min_price,
                        max_price=max_price,
                        sort_by=sort_by,
                        target=target,
                        compare_info=compare_info,
                        compare_dates=compare_dates,
                        attempts=attempts,
                        max_radius_km=max_radius_km,
                        nearby_hotels=nearby_hotels,
                        price_map=price_map,
                        partial=True,
                        completed_compare_dates=completed_compare_dates,
                    )
                )
                if backfill_changed:
                    publish_price_progress_candidate_snapshot()

    for compare_date in compare_dates:
        if compare_date in fetched_compare_dates:
            continue
        fetched_compare_dates.add(compare_date)
        date_index = compare_dates.index(compare_date) + 1

        def publish_date_price_progress(progress_info: dict[str, Any]) -> None:
            phase = str(progress_info.get("phase") or "")
            completed_count = len(completed_compare_dates) + (1 if phase == "complete" else 0)
            normalized = dict(progress_info)
            normalized.update(
                {
                    "date": compare_date,
                    "dateIndex": date_index,
                    "completedDates": min(completed_count, len(compare_dates)),
                    "totalDates": len(compare_dates),
                    "totalExpectedPriceCount": len(hotel_ids) * len(compare_dates),
                }
            )
            if price_progress_callback:
                price_progress_callback(normalized)
            if phase in {"list", "dom-list", "detail", "deep", "complete"}:
                publish_price_progress_candidate_snapshot()

        requested_hotel_ids = list(hotel_ids)
        date_prices = getHotelPrices(
            provider,
            requested_hotel_ids,
            [compare_date],
            progressCallback=publish_date_price_progress,
        )
        price_attempted_keys.update((str(hotel_id), compare_date) for hotel_id in requested_hotel_ids)
        for hotel_id, prices in date_prices.items():
            if compare_date in prices:
                price_map.setdefault(str(hotel_id), {})[compare_date] = prices.get(compare_date)
        merge_cached_candidate_snapshot()
        backfill_changed = backfill_missing_prices_for_dates(list(fetched_compare_dates))
        completed_compare_dates.append(compare_date)
        if progress_callback:
            progress_callback(
                search_result_from_price_map(
                    provider,
                    city=city,
                    target_hotel_name=target_hotel_name,
                    selected=selected,
                    radius_km=radius_km,
                    min_star=min_star,
                    min_price=min_price,
                    max_price=max_price,
                    sort_by=sort_by,
                    target=target,
                    compare_info=compare_info,
                    compare_dates=compare_dates,
                    attempts=attempts,
                    max_radius_km=max_radius_km,
                    nearby_hotels=nearby_hotels,
                    price_map=price_map,
                    partial=True,
                    completed_compare_dates=completed_compare_dates,
                )
            )
            if backfill_changed:
                publish_price_progress_candidate_snapshot()

    backfill_missing_prices_for_dates([selected_value, *compare_dates], force_retry=True, backfill_mode="final")

    return search_result_from_price_map(
        provider,
        city=city,
        target_hotel_name=target_hotel_name,
        selected=selected,
        radius_km=radius_km,
        min_star=min_star,
        min_price=min_price,
        max_price=max_price,
        sort_by=sort_by,
        target=target,
        compare_info=compare_info,
        compare_dates=compare_dates,
        attempts=attempts,
        max_radius_km=max_radius_km,
        nearby_hotels=nearby_hotels,
        price_map=price_map,
        partial=False,
        completed_compare_dates=completed_compare_dates,
    )
