from __future__ import annotations

from pathlib import Path

from hotel_deals import (
    calculateDealScore,
    detectHotelBrand,
    detectHotelChainBrand,
    filterByPrice,
    getCompareDateInfo,
    getCompareDates,
    hotel_brand_payload,
    search_current_prices,
    search_deals,
)
from localization import (
    contains_chinese_text,
    domestic_hotel_name_payload,
    hotel_name_payload_from_sources,
    normalized_hotel_name_payload,
    simplify_chinese_text,
)
from providers import LocalJsonProvider


APP_DIR = Path(__file__).resolve().parents[1]


def test_get_compare_dates_weekday_rules():
    assert getCompareDates("2026-06-07") == ["2026-06-07", "2026-06-08", "2026-06-14", "2026-06-15"]
    assert getCompareDates("2026-06-08") == ["2026-06-08", "2026-06-09", "2026-06-15", "2026-06-16"]
    assert getCompareDates("2026-06-09") == ["2026-06-09", "2026-06-10", "2026-06-16", "2026-06-17"]
    assert getCompareDates("2026-06-10") == ["2026-06-10", "2026-06-11", "2026-06-17", "2026-06-18"]
    assert getCompareDates("2026-06-11") == ["2026-06-11", "2026-06-14", "2026-06-18", "2026-06-25"]


def test_get_compare_dates_weekend_rules():
    assert getCompareDates("2026-06-12") == ["2026-06-12", "2026-06-13", "2026-06-26", "2026-06-27"]
    assert getCompareDates("2026-06-13") == ["2026-06-26", "2026-06-27", "2026-07-03", "2026-07-04"]


def test_get_compare_dates_holiday_uses_only_holiday_days():
    info = getCompareDateInfo("2026-05-01")
    assert info["compareMode"] == "holiday"
    assert info["holiday"]["name"] == "劳动节"
    assert info["compareNotice"] == "你选择的是劳动节公众假期，对比日已改为公众假期对比。"
    assert info["compareDates"] == ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"]
    assert getCompareDates("2026-05-05") == ["2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"]


def test_simplified_hotel_name_does_not_translate_english_names():
    assert simplify_chinese_text("深圳國際會展中心希爾頓花園酒店") == "深圳国际会展中心希尔顿花园酒店"
    payload = normalized_hotel_name_payload("Hilton Garden Inn Shenzhen World Exhibition", source="Trip.com 简体")
    assert payload["hotelName"] == "Hilton Garden Inn Shenzhen World Exhibition"
    assert payload["hotelNameSimplified"] == ""


def test_domestic_hotel_name_mapping_uses_verified_cn_name():
    payload = domestic_hotel_name_payload("Hilton Garden Inn Shenzhen Bao'an", source="Trip.com 简体")
    assert payload["hotelName"] == "深圳宝安华盛希尔顿花园酒店"
    assert payload["hotelNameSimplified"] == "深圳宝安华盛希尔顿花园酒店"
    assert payload["hotelOriginalName"] == "Hilton Garden Inn Shenzhen Bao'an"
    trip_payload = domestic_hotel_name_payload("The Skytel World Exhibition & Convention Center", hotel_id="49232307")
    assert trip_payload["hotelName"] == "格兰云天阅酒店（深圳国际会展中心店）"


def test_hotel_name_falls_back_to_tripcom_traditional_name():
    payload = hotel_name_payload_from_sources(
        ["Unknown English Hotel", "深圳國際會展中心皇冠假日酒店"],
        source="Trip.com 繁体转简体",
    )
    assert payload["hotelName"] == "深圳国际会展中心皇冠假日酒店"
    assert payload["hotelOriginalName"] == "深圳國際會展中心皇冠假日酒店"


def test_hotel_name_falls_back_to_rule_based_chinese_name_for_tripcom_english():
    payload = hotel_name_payload_from_sources(
        ["Zhuhai Lovers Road Sun Moon Shell Grand Theater Atour Hotel"],
        source="Trip.com 英文名",
    )
    assert payload["hotelName"] == "珠海情侣路日月贝大剧院亚朵酒店"
    assert payload["hotelNameSimplified"] == "珠海情侣路日月贝大剧院亚朵酒店"
    assert payload["hotelOriginalName"] == "Zhuhai Lovers Road Sun Moon Shell Grand Theater Atour Hotel"


def test_calculate_deal_score():
    score = calculateDealScore(638, [638, 910, 870, 898])
    assert score["averageComparePrice"] == 829
    assert score["discountAmount"] == 191
    assert score["dealBasis"] == "average"
    assert score["isDeal"] is True


def test_calculate_deal_score_uses_single_compare_day_threshold():
    score = calculateDealScore(900, [900, 900, 900, 1000])
    assert score["averageComparePrice"] == 925
    assert score["averageDiscountAmount"] == 25
    assert score["maxSingleDayDiscountAmount"] == 100
    assert score["referencePrice"] == 1000
    assert score["referencePriceLabel"] == "最高对比价"
    assert score["discountAmount"] == 100
    assert score["dealBasis"] == "single_day"
    assert score["isDeal"] is True


def test_detect_hotel_brand():
    assert detectHotelBrand("广州天河希尔顿酒店")["brand"] == "Hilton"
    assert detectHotelBrand("上海浦东丽思卡尔顿酒店")["brand"] == "Marriott"
    assert detectHotelBrand("广州富力君悦大酒店")["brand"] == "Hyatt"
    assert detectHotelBrand("广州花园酒店") is None


def test_detect_hotel_chain_brand_keeps_non_luxury_chains_out_of_recommended_brands():
    atour = detectHotelChainBrand("珠海情侣路日月贝大剧院亚朵酒店")
    assert atour["brandLabel"] == "亚朵"
    assert atour["groupLabel"] == "亚朵集团"
    assert atour["brandRank"] == 99
    assert atour["brandTier"] == "chain"
    assert detectHotelBrand("珠海情侣路日月贝大剧院亚朵酒店") is None

    skytel = hotel_brand_payload({"hotelName": "The Skytel World Exhibition & Convention Center"})
    assert skytel["brandLabel"] == "格兰云天"
    assert skytel["groupLabel"] == "格兰云天酒店集团"
    assert skytel["brandRank"] == 99

    huazhu = hotel_brand_payload({"hotelName": "全季酒店（珠海明珠南路店）"})
    assert huazhu["brandLabel"] == "华住"
    assert huazhu["groupLabel"] == "华住集团"


def test_filter_by_price_uses_current_price():
    hotels = [{"currentPrice": 500}, {"currentPrice": 900}, {"currentPrice": 1200}]
    assert filterByPrice(hotels, 600, 1000) == [{"currentPrice": 900}]


def test_search_deals_with_local_provider():
    provider = LocalJsonProvider(APP_DIR)
    price_progress_events = []
    partial_results = []
    result = search_deals(
        provider=provider,
        city="广州",
        target_hotel_name="广州天河希尔顿酒店",
        selected_date="2026-06-01",
        radius_km=3,
        min_star=4,
        min_price=None,
        max_price=None,
        progress_callback=partial_results.append,
        price_progress_callback=price_progress_events.append,
    )
    assert result["compareDates"] == ["2026-06-01", "2026-06-02", "2026-06-08", "2026-06-09"]
    assert result["summary"]["dealCount"] >= 1
    assert result["dealHotels"][0]["discountAmount"] >= result["dealHotels"][-1]["discountAmount"]
    assert all(hotel["starRating"] >= 4 for hotel in result["allHotels"])
    assert result["summary"]["candidateRadiusKm"] == 10.0
    assert result["summary"]["candidateCount"] >= result["summary"]["dealScopeCandidateCount"]
    assert all(hotel["distanceKm"] <= result["summary"]["candidateRadiusKm"] for hotel in result["allHotels"])
    assert any(hotel["distanceKm"] > 3 for hotel in result["allHotels"])
    assert price_progress_events
    assert price_progress_events[-1]["phase"] == "complete"
    assert price_progress_events[-1]["completedDates"] == price_progress_events[-1]["totalDates"]
    compare_partials = [
        item
        for item in partial_results
        if item.get("summary", {}).get("priceCompareComplete") is False
    ]
    assert compare_partials
    assert compare_partials[-1]["summary"]["completedCompareDateCount"] == len(result["compareDates"])


def test_search_expands_radius_when_no_deal_inside_three_km():
    provider = LocalJsonProvider(APP_DIR)
    result = search_deals(
        provider=provider,
        city="深圳",
        target_hotel_name="深圳国际会展中心希尔顿酒店",
        selected_date="2026-06-01",
        radius_km=3,
        min_star=4,
        min_price=None,
        max_price=None,
    )
    assert result["summary"]["radiusExpanded"] is True
    assert result["summary"]["effectiveRadiusKm"] == 5.0
    assert result["summary"]["attemptedRadii"] == [3.0, 5.0]
    assert result["summary"]["dealCount"] >= 1
    assert result["summary"]["candidateRadiusKm"] == 10.0
    assert all(hotel["distanceKm"] <= result["summary"]["candidateRadiusKm"] for hotel in result["allHotels"])
    assert any(hotel["distanceKm"] > result["summary"]["effectiveRadiusKm"] for hotel in result["allHotels"])
    assert all(hotel["distanceKm"] <= result["summary"]["effectiveRadiusKm"] for hotel in result["dealHotels"])
    assert any(hotel["distanceKm"] > 3 for hotel in result["dealHotels"])


def test_recommended_hotels_ignore_user_price_filter():
    provider = LocalJsonProvider(APP_DIR)
    result = search_deals(
        provider=provider,
        city="广州",
        target_hotel_name="广州天河希尔顿酒店",
        selected_date="2026-06-01",
        radius_km=3,
        min_star=4,
        min_price=None,
        max_price=700,
    )

    assert result["summary"]["recommendedIgnoresPriceFilter"] is True
    assert result["allHotels"]
    assert all(hotel["currentPrice"] <= 700 for hotel in result["allHotels"])
    assert any(hotel["currentPrice"] > 700 for hotel in result["recommendedHotels"])


class FakeLiveProvider:
    source_name = "Trip.com 测试"

    def resolve_target_hotel(self, city, hotel_name, target_hint=None):
        return {
            "hotelId": "target",
            "hotelName": "深圳国际会展中心希尔顿酒店",
            "latitude": 22.7,
            "longitude": 113.78,
        }

    def get_nearby_hotels(self, target_hotel, radius_km=3, min_star=4, selected_date=None, fast_mode=False):
        return [
            {
                "hotelId": "trad-cn",
                "hotelName": "Unknown Star Hotel Shenzhen",
                "hotelOriginalName": "深圳國際會展中心皇冠假日酒店",
                "starRating": 4,
                "distanceKm": 1.2,
                "currentPrice": 480,
            },
            {
                "hotelId": "unknown-en",
                "hotelName": "Unknown Star Hotel Shenzhen",
                "starRating": 4,
                "distanceKm": 1.8,
                "currentPrice": 490,
            },
            {
                "hotelId": "mapped-en",
                "hotelName": "Hilton Garden Inn Shenzhen Bao'an",
                "starRating": 4,
                "distanceKm": 2.4,
                "currentPrice": 520,
            },
            {
                "hotelId": "cn-name",
                "hotelName": "深圳测试高端酒店",
                "starRating": 4,
                "distanceKm": 2.8,
                "currentPrice": 560,
            },
        ]


class FakeUnpricedProvider:
    source_name = "Trip.com 测试"

    def resolve_target_hotel(self, city, hotel_name, target_hint=None):
        return {
            "hotelId": "target",
            "hotelName": "广州珠江新城",
            "latitude": 23.119,
            "longitude": 113.333,
        }

    def get_nearby_hotels(self, target_hotel, radius_km=3, min_star=4, selected_date=None, fast_mode=False):
        return [
            {
                "hotelId": "pending-price",
                "hotelName": "广州珠江新城假日酒店",
                "starRating": 4,
                "distanceKm": 1.1,
                "currentPrice": None,
            }
        ]

    def get_hotel_prices(self, hotel_ids, dates, progress_callback=None):
        if progress_callback:
            for index, date in enumerate(dates, start=1):
                progress_callback(
                    {
                        "phase": "complete",
                        "date": date,
                        "dateIndex": index,
                        "completedDates": index,
                        "totalDates": len(dates),
                        "pricedHotelCount": 0,
                        "missingHotelCount": len(hotel_ids),
                        "totalHotels": len(hotel_ids),
                    }
                )
        return {hotel_id: {date: None for date in dates} for hotel_id in hotel_ids}


class FakeDiscoveryProvider:
    source_name = "Trip.com 测试"

    def __init__(self):
        self.cached_hotels = []
        self.price_cache = {
            "initial": {"2026-06-01": 500},
        }

    def resolve_target_hotel(self, city, hotel_name, target_hint=None):
        return {
            "hotelId": "target",
            "hotelName": "广州珠江新城",
            "latitude": 23.119,
            "longitude": 113.333,
        }

    def get_nearby_hotels(self, target_hotel, radius_km=3, min_star=4, selected_date=None, fast_mode=False):
        return [
            {
                "hotelId": "initial",
                "hotelName": "广州珠江新城首屏酒店",
                "starRating": 4,
                "distanceKm": 1.0,
                "currentPrice": 500,
                "priceDate": "2026-06-01",
            }
        ]

    def get_hotel_prices(self, hotel_ids, dates, progress_callback=None):
        date = dates[0]
        if date == "2026-06-02" and not self.cached_hotels:
            self.cached_hotels.append(
                {
                    "hotelId": "discovered",
                    "hotelName": "广州珠江新城深度发现酒店",
                    "starRating": 4,
                    "distanceKm": 2.0,
                    "currentPrice": 680,
                    "priceDate": "2026-06-02",
                }
            )
            self.price_cache.setdefault("discovered", {})["2026-06-02"] = 680
        for hotel_id in hotel_ids:
            self.price_cache.setdefault(str(hotel_id), {})[date] = 900
        if progress_callback:
            progress_callback(
                {
                    "phase": "complete",
                    "date": date,
                    "dateIndex": 1,
                    "completedDates": 1,
                    "totalDates": 1,
                    "pricedHotelCount": len(hotel_ids),
                    "missingHotelCount": 0,
                    "totalHotels": len(hotel_ids),
                }
            )
        return {str(hotel_id): {date: self.price_cache.get(str(hotel_id), {}).get(date)} for hotel_id in hotel_ids}

    def get_cached_nearby_hotels(self, target_hotel, radius_km=3, min_star=4, selected_date=None):
        return list(self.cached_hotels)

    def get_cached_hotel_prices(self, hotel_ids, dates):
        return {
            str(hotel_id): {
                date: self.price_cache.get(str(hotel_id), {}).get(date)
                for date in dates
                if date in self.price_cache.get(str(hotel_id), {})
            }
            for hotel_id in hotel_ids
        }


class FakeProgressDiscoveryProvider:
    source_name = "Trip.com 测试"

    def __init__(self):
        self.cached_hotels = []
        self.price_cache = {}

    def resolve_target_hotel(self, city, hotel_name, target_hint=None):
        return {
            "hotelId": "target",
            "hotelName": "珠海情侣中路",
            "latitude": 22.26,
            "longitude": 113.58,
        }

    def get_nearby_hotels(self, target_hotel, radius_km=3, min_star=4, selected_date=None, fast_mode=False):
        return [
            {
                "hotelId": "initial",
                "hotelName": "珠海情侣中路初始酒店",
                "starRating": 4,
                "distanceKm": 1.0,
                "currentPrice": None,
            }
        ]

    def get_hotel_prices(self, hotel_ids, dates, progress_callback=None):
        date = dates[0]
        self.cached_hotels.append(
            {
                "hotelId": "progress-discovered",
                "hotelName": "珠海情侣中路进度发现酒店",
                "starRating": 4,
                "distanceKm": 1.8,
                "currentPrice": 880,
                "priceDate": date,
            }
        )
        self.price_cache.setdefault("progress-discovered", {})[date] = 880
        if progress_callback:
            progress_callback(
                {
                    "phase": "detail",
                    "date": date,
                    "dateIndex": 1,
                    "completedDates": 0,
                    "totalDates": 1,
                    "pricedHotelCount": 1,
                    "missingHotelCount": len(hotel_ids),
                    "totalHotels": len(hotel_ids) + 1,
                }
            )
            progress_callback(
                {
                    "phase": "complete",
                    "date": date,
                    "dateIndex": 1,
                    "completedDates": 1,
                    "totalDates": 1,
                    "pricedHotelCount": 1,
                    "missingHotelCount": len(hotel_ids),
                    "totalHotels": len(hotel_ids) + 1,
                }
            )
        return {str(hotel_id): {date: None} for hotel_id in hotel_ids}

    def get_cached_nearby_hotels(self, target_hotel, radius_km=3, min_star=4, selected_date=None):
        return list(self.cached_hotels)

    def get_cached_hotel_prices(self, hotel_ids, dates):
        return {
            str(hotel_id): {
                date: self.price_cache.get(str(hotel_id), {}).get(date)
                for date in dates
                if date in self.price_cache.get(str(hotel_id), {})
            }
            for hotel_id in hotel_ids
        }


class FakeStreamingNearbyProvider:
    source_name = "Trip.com 测试"

    def resolve_target_hotel(self, city, hotel_name, target_hint=None):
        return {
            "hotelId": "target",
            "hotelName": "珠海情侣中路",
            "latitude": 22.26,
            "longitude": 113.58,
        }

    def get_nearby_hotels(
        self,
        target_hotel,
        radius_km=3,
        min_star=4,
        selected_date=None,
        fast_mode=False,
        progress_callback=None,
    ):
        first = {
            "hotelId": "raw-first",
            "hotelName": "Raw English Star Hotel Zhuhai",
            "starRating": 4,
            "distanceKm": 1.2,
            "currentPrice": None,
        }
        second = {
            "hotelId": "cn-second",
            "hotelName": "珠海情侣中路星级酒店",
            "starRating": 4,
            "distanceKm": 1.6,
            "currentPrice": 520,
            "priceDate": selected_date,
        }
        if progress_callback:
            progress_callback([first])
            progress_callback([first, second])
        return [first, second]

    def get_hotel_prices(self, hotel_ids, dates, progress_callback=None):
        if progress_callback:
            progress_callback(
                {
                    "phase": "complete",
                    "date": dates[0],
                    "dateIndex": 1,
                    "completedDates": 1,
                    "totalDates": 1,
                    "pricedHotelCount": 0,
                    "missingHotelCount": len(hotel_ids),
                    "totalHotels": len(hotel_ids),
                }
            )
        return {str(hotel_id): {date: None for date in dates} for hotel_id in hotel_ids}


def test_current_price_results_keep_unlocalized_hotels_with_chinese_fallback_names():
    result = search_current_prices(
        provider=FakeLiveProvider(),
        city="深圳",
        target_hotel_name="深圳国际会展中心希尔顿酒店",
        selected_date="2026-06-01",
        radius_km=3,
    )
    hotels = {hotel["hotelId"]: hotel for hotel in result["allHotels"]}
    names = [hotel["hotelName"] for hotel in result["allHotels"]]
    assert "unknown-en" in hotels
    assert hotels["unknown-en"]["hotelName"] != "Unknown Star Hotel Shenzhen"
    assert hotels["unknown-en"]["nameProcessing"] is True
    assert "深圳国际会展中心皇冠假日酒店" in names
    assert "深圳宝安华盛希尔顿花园酒店" in names
    assert all(contains_chinese_text(name) for name in names)


def test_current_price_results_can_stream_raw_candidates_before_name_and_price_filtering():
    result = search_current_prices(
        provider=FakeLiveProvider(),
        city="深圳",
        target_hotel_name="深圳国际会展中心希尔顿酒店",
        selected_date="2026-06-01",
        radius_km=3,
        max_price=100,
        include_provisional_names=True,
        defer_price_filter=True,
        preserve_order=True,
    )

    hotel_ids = [hotel["hotelId"] for hotel in result["allHotels"]]
    assert hotel_ids == ["trad-cn", "unknown-en", "mapped-en", "cn-name"]
    assert result["summary"]["rawCandidateCount"] == 4
    assert result["summary"]["candidateCount"] == 4
    assert result["summary"]["provisionalNameCount"] == 1
    assert result["summary"]["nameNormalizationDeferred"] is True
    assert result["summary"]["priceFilterDeferred"] is True
    assert result["summary"]["sortDeferred"] is True
    unknown = next(hotel for hotel in result["allHotels"] if hotel["hotelId"] == "unknown-en")
    assert unknown["hotelName"] == "深圳星级酒店（中文名待核验）"
    assert unknown["hotelOriginalName"] == "Unknown Star Hotel Shenzhen"
    assert unknown["nameProcessing"] is True


def test_search_deals_keeps_unpriced_star_candidates_visible():
    result = search_deals(
        provider=FakeUnpricedProvider(),
        city="广州",
        target_hotel_name="广州珠江新城",
        selected_date="2026-06-01",
        radius_km=3,
        min_star=4,
    )

    assert result["summary"]["candidateCount"] == 1
    assert result["summary"]["pricedHotelCount"] == 0
    assert result["summary"]["unpricedCandidateCount"] == 1
    assert result["allHotels"][0]["hotelName"] == "广州珠江新城假日酒店"
    assert result["allHotels"][0]["currentPrice"] is None
    assert result["allHotels"][0]["pricePending"] is True
    assert result["dealHotels"] == []


def test_search_deals_adds_candidates_discovered_while_pricing():
    result = search_deals(
        provider=FakeDiscoveryProvider(),
        city="广州",
        target_hotel_name="广州珠江新城",
        selected_date="2026-06-01",
        radius_km=3,
        min_star=4,
    )

    hotels = {hotel["hotelId"]: hotel for hotel in result["allHotels"]}
    assert set(hotels) == {"initial", "discovered"}
    assert hotels["initial"]["currentPrice"] == 500
    assert hotels["discovered"]["currentPrice"] is None
    assert hotels["discovered"]["pricePending"] is True
    assert result["summary"]["candidateCount"] == 2
    assert result["summary"]["unpricedCandidateCount"] == 1


def test_search_deals_publishes_discovered_candidates_during_price_progress():
    events = []

    def record_partial(result):
        hotel_ids = [hotel["hotelId"] for hotel in result.get("allHotels") or []]
        events.append(("partial", hotel_ids))

    def record_price(progress):
        events.append(("price", progress.get("phase")))

    search_deals(
        provider=FakeProgressDiscoveryProvider(),
        city="珠海",
        target_hotel_name="情侣中路",
        selected_date="2026-05-29",
        radius_km=3,
        min_star=4,
        progress_callback=record_partial,
        price_progress_callback=record_price,
    )

    detail_index = events.index(("price", "detail"))
    complete_index = events.index(("price", "complete"))
    discovered_index = next(
        index
        for index, event in enumerate(events)
        if event[0] == "partial" and "progress-discovered" in event[1]
    )
    assert detail_index < discovered_index < complete_index


def test_search_deals_streams_nearby_candidates_before_name_price_and_sort_are_ready():
    partials = []

    result = search_deals(
        provider=FakeStreamingNearbyProvider(),
        city="珠海",
        target_hotel_name="情侣中路",
        selected_date="2026-05-29",
        radius_km=3,
        min_star=4,
        max_price=100,
        progress_callback=partials.append,
    )

    assert partials
    first = partials[0]
    assert [hotel["hotelId"] for hotel in first["allHotels"]] == ["raw-first"]
    assert first["summary"]["nameNormalizationDeferred"] is True
    assert first["summary"]["priceFilterDeferred"] is True
    assert first["summary"]["sortDeferred"] is True
    assert first["allHotels"][0]["hotelName"] == "珠海星级酒店（中文名待核验）"
    assert first["allHotels"][0]["hotelOriginalName"] == "Raw English Star Hotel Zhuhai"
    assert first["allHotels"][0]["nameProcessing"] is True

    second = next(result for result in partials if len(result["allHotels"]) >= 2)
    assert [hotel["hotelId"] for hotel in second["allHotels"][:2]] == ["raw-first", "cn-second"]

    final_hotels = {hotel["hotelId"]: hotel for hotel in result["allHotels"]}
    assert "raw-first" in final_hotels
    assert final_hotels["raw-first"]["hotelName"] == "珠海星级酒店（中文名待核验）"
    assert final_hotels["raw-first"]["hotelOriginalName"] == "Raw English Star Hotel Zhuhai"
    assert final_hotels["raw-first"]["nameProcessing"] is True
    assert result["summary"]["nameNormalizationDeferred"] is True
    assert result["summary"]["provisionalNameCount"] == 1
