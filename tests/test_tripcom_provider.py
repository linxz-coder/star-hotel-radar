from __future__ import annotations

import datetime as dt
import json
from urllib.parse import parse_qs, urlparse

from providers import ProviderError, TripComProvider


def target_hotel() -> dict:
    return {
        "hotelId": "71649086",
        "hotelName": "深圳国际会展中心希尔顿酒店",
        "city": "深圳",
        "cityId": 30,
        "provinceId": 2,
        "countryId": 1,
        "latitude": 22.705377,
        "longitude": 113.777817,
        "searchType": "H",
        "searchValue": "31~71649086*31*71649086*1",
    }


def test_name_verification_uses_ctrip_chinese_page(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()

    monkeypatch.setattr(provider, "_fetch_detail_page_name_payload", lambda hotel, selected_date: None)
    monkeypatch.setattr(
        provider,
        "_http_text",
        lambda url, timeout=8: "<html><head><title>深圳国际会展中心皇冠假日酒店预订价格查询-携程酒店</title></head></html>",
    )

    resolved = provider.verify_hotel_names(
        [
            {
                "hotelId": "99999999",
                "hotelName": "深圳星级酒店（中文名正在核验中...）",
                "hotelOriginalName": "Unresolved Original Name",
                "city": "深圳",
            }
        ],
        "2026-06-01",
    )

    assert resolved["99999999"]["hotelName"] == "深圳国际会展中心皇冠假日酒店"
    assert resolved["99999999"]["hotelNameSource"] == "携程中文页"


def test_name_verification_uses_elong_as_optional_source(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()

    monkeypatch.setenv("HOTEL_DEAL_ELONG_USER", "demo-user")
    monkeypatch.setenv("HOTEL_DEAL_ELONG_APP_KEY", "demo-app-key")
    monkeypatch.setenv("HOTEL_DEAL_ELONG_SECRET_KEY", "demo-secret")
    monkeypatch.setenv("HOTEL_DEAL_ELONG_REGION_ID_MAP", json.dumps({"深圳": "1314"}, ensure_ascii=False))
    monkeypatch.setattr(provider, "_fetch_detail_page_name_payload", lambda hotel, selected_date: None)
    monkeypatch.setattr(provider, "_fetch_ctrip_name_payload", lambda hotel, selected_date: None)
    monkeypatch.setattr(provider, "_fetch_map_poi_name_payload", lambda hotel: (_ for _ in ()).throw(AssertionError("map source should not run")))
    monkeypatch.setattr(provider, "_fetch_search_engine_name_payload", lambda hotel: (_ for _ in ()).throw(AssertionError("search source should not run")))

    requested_urls: list[str] = []

    def fake_http_json(url, timeout=8):
        requested_urls.append(url)
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        assert params["method"] == ["ihotel.list"]
        assert json.loads(params["data"][0])["regionId"] == 1314
        return {
            "Code": "0",
            "Result": {
                "Hotels": [
                        {
                            "HotelId": "elong-99999999",
                            "HotelNameCn": "深圳国际会展中心测试酒店",
                            "HotelNameEn": "Unresolved Hotel Near WECC",
                            "GeoInfo": {"Latitude": 22.709, "Longitude": 113.774},
                        }
                ]
            },
        }

    monkeypatch.setattr(provider, "_http_json", fake_http_json)

    resolved = provider.verify_hotel_names(
            [
                {
                    "hotelId": "99999999",
                    "hotelName": "深圳星级酒店（中文名正在核验中...）",
                    "hotelOriginalName": "Unresolved Hotel Near WECC",
                    "city": "深圳",
                    "latitude": 22.709,
                "longitude": 113.774,
            }
        ],
        "2026-06-01",
    )

    assert requested_urls
    assert resolved["99999999"]["hotelName"] == "深圳国际会展中心测试酒店"
    assert resolved["99999999"]["hotelNameSource"] == "艺龙中文酒店列表"


def test_hotel_target_uses_city_fallback_when_no_area_target(monkeypatch):
    provider = TripComProvider()
    calls: list[str] = []

    monkeypatch.setattr(provider, "_supplemental_search_targets", lambda target: [])

    def fake_fetch(search_target, selected_date, fast_mode=False):
        calls.append(search_target["searchType"])
        if search_target["searchType"] == "CT":
            return [
                {
                    "hotelId": "nearby",
                    "hotelName": "深圳会展中心测试酒店",
                    "starRating": 4,
                    "latitude": 22.715377,
                    "longitude": 113.777817,
                    "currentPrice": 500,
                }
            ]
        return []

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", fake_fetch)

    hotels = provider.get_nearby_hotels(
        target_hotel(),
        radius_km=5,
        min_star=4,
        selected_date="2026-06-01",
    )

    assert calls == ["CT"]
    assert hotels[0]["hotelId"] == "nearby"
    assert provider._last_search_targets[0]["searchType"] == "CT"


def test_fast_mode_continues_past_sparse_search_target(monkeypatch):
    provider = TripComProvider()
    calls: list[str] = []
    area_targets = [
        {"hotelId": "area-1", "hotelName": "会展中心", "searchType": "LM", "searchValue": "13~1*13*1*1"},
        {"hotelId": "area-2", "hotelName": "会展商圈", "searchType": "Z", "searchValue": "3~2*3*2*1"},
    ]

    monkeypatch.setattr(provider, "_supplemental_search_targets", lambda target: area_targets)

    def fake_fetch(search_target, selected_date, fast_mode=False):
        calls.append(search_target["hotelId"])
        return [
            {
                "hotelId": "nearby",
                "hotelName": "深圳会展中心测试酒店",
                "starRating": 4,
                "latitude": 22.715377,
                "longitude": 113.777817,
                "currentPrice": 500,
            }
        ]

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", fake_fetch)

    hotels = provider.get_nearby_hotels(
        target_hotel(),
        radius_km=5,
        min_star=4,
        selected_date="2026-06-01",
        fast_mode=True,
    )

    assert calls == ["area-1", "area-2"]
    assert hotels[0]["hotelId"] == "nearby"


def test_fast_mode_skips_timed_out_search_target(monkeypatch):
    provider = TripComProvider()
    area_targets = [
        {"hotelId": "area-timeout", "hotelName": "外滩", "searchType": "LM", "searchValue": "13~1*13*1*1"},
        {"hotelId": "area-ok", "hotelName": "外滩商圈", "searchType": "Z", "searchValue": "3~2*3*2*1"},
    ]

    monkeypatch.setattr(provider, "_supplemental_search_targets", lambda target: area_targets)

    def fake_fetch(search_target, selected_date, fast_mode=False):
        if search_target["hotelId"] == "area-timeout":
            raise ProviderError("Trip.com 页面加载超时，请稍后重试")
        return [
            {
                "hotelId": "nearby",
                "hotelName": "上海外滩测试酒店",
                "starRating": 4,
                "latitude": 22.715377,
                "longitude": 113.777817,
                "currentPrice": 600,
            }
        ]

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", fake_fetch)

    hotels = provider.get_nearby_hotels(
        target_hotel(),
        radius_km=5,
        min_star=4,
        selected_date="2026-06-01",
        fast_mode=True,
    )

    assert hotels[0]["hotelId"] == "nearby"


def test_supplemental_keywords_strip_common_brand_words():
    provider = TripComProvider()

    assert "上海外滩酒店" in provider._supplemental_keywords("上海", "上海外滩W酒店")
    assert "上海浦东酒店" in provider._supplemental_keywords("上海", "上海浦东丽思卡尔顿酒店")
    assert "惠州佳兆业酒店" in provider._supplemental_keywords("惠州", "惠州佳兆业雅高铂尔曼酒店")


def test_resolve_target_uses_top_tripcom_suggestion(monkeypatch):
    provider = TripComProvider()
    suggestions = [
        {"hotelId": "landmark", "hotelName": "深圳国际会展中心", "searchType": "LM"},
        {"hotelId": "hotel", "hotelName": "深圳国际会展中心皇冠假日酒店", "searchType": "H"},
    ]

    monkeypatch.setattr(provider, "suggest_targets", lambda city, query, limit=10: suggestions)

    target = provider.resolve_target_hotel(city="深圳", hotel_name="深圳国际会展中心")

    assert target["hotelId"] == "landmark"
    assert target["searchType"] == "LM"


def test_suggest_targets_stops_after_confident_exact_match(monkeypatch):
    provider = TripComProvider()
    calls: list[str] = []

    def fake_keyword_search(keyword):
        calls.append(keyword)
        return [
            {
                "resultType": "LM",
                "code": "10231465",
                "item": {
                    "data": {
                        "title": "深圳国际会展中心",
                        "filterID": "13|10231465",
                    }
                },
                "city": {"currentLocaleName": "深圳", "geoCode": 30},
                "province": {"geoCode": 23},
                "country": {"geoCode": 1},
                "coordinateInfos": [
                    {
                        "coordinateType": "GAODE",
                        "latitude": 22.6979544,
                        "longitude": 113.7742541,
                    }
                ],
            }
        ]

    monkeypatch.setattr(provider, "_keyword_search", fake_keyword_search)

    suggestions = provider.suggest_targets(city="深圳", query="深圳国际会展中心")

    assert len(calls) == 1
    assert suggestions[0]["hotelId"] == "10231465"
    assert suggestions[0]["searchType"] == "LM"


def test_resolve_generic_place_query_prefers_matching_landmark(monkeypatch):
    provider = TripComProvider()
    suggestions = [
        {"hotelId": "hotel", "hotelName": "深圳国际会展中心洲际酒店", "searchType": "H"},
        {"hotelId": "landmark", "hotelName": "深圳国际会展中心", "searchType": "LM"},
    ]

    monkeypatch.setattr(provider, "suggest_targets", lambda city, query, limit=10: suggestions)

    target = provider.resolve_target_hotel(city="深圳", hotel_name="深圳国际会展中心")

    assert target["hotelId"] == "landmark"
    assert target["searchType"] == "LM"


def test_resolve_hotel_query_keeps_hotel_intent(monkeypatch):
    provider = TripComProvider()
    suggestions = [
        {"hotelId": "hotel", "hotelName": "深圳国际会展中心洲际酒店", "searchType": "H"},
        {"hotelId": "landmark", "hotelName": "深圳国际会展中心", "searchType": "LM"},
    ]

    monkeypatch.setattr(provider, "suggest_targets", lambda city, query, limit=10: suggestions)

    target = provider.resolve_target_hotel(city="深圳", hotel_name="深圳国际会展中心洲际酒店")

    assert target["hotelId"] == "hotel"
    assert target["searchType"] == "H"


def test_extract_price_prefers_tax_inclusive_price():
    provider = TripComProvider()

    assert provider._extract_price(
        {
            "hotelBasicInfo": {
                "price": 440,
                "onlineTaxPrice": 518,
            },
            "minRoomInfo": {
                "price": 430,
            },
        }
    ) == 518
    assert provider._extract_price(
        {
            "hotelBasicInfo": {
                "price": 440,
            },
            "minRoomInfo": {
                "onlineAndShopTaxPrice": "CNY 536",
            },
        }
    ) == 536


def test_merge_prefers_tax_inclusive_price_over_lower_base_price():
    provider = TripComProvider()

    merged = provider._merge_hotel_lists(
        [
            {
                "hotelId": "54506880",
                "hotelName": "深圳国际会展中心皇冠假日酒店",
                "currentPrice": 513,
                "priceIncludesTax": True,
            }
        ],
        [
            {
                "hotelId": "54506880",
                "hotelName": "Crowne Plaza SHENZHEN WECC by IHG",
                "currentPrice": 440,
                "priceIncludesTax": False,
            }
        ],
    )

    assert merged[0]["currentPrice"] == 513


def test_get_hotel_prices_keeps_partial_prices_when_one_fetch_fails(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()
    provider._last_search_targets = [target_hotel()]
    provider._price_cache = {"54506880": {"2026-06-01": 513}}

    def fake_fetch(search_target, date_value, fast_mode=False, deep_mode=False):
        if date_value == "2026-06-02":
            raise ProviderError("Trip.com 页面加载超时，请稍后重试")
        return []

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", fake_fetch)

    prices = provider.get_hotel_prices(["54506880"], ["2026-06-01", "2026-06-02"])

    assert prices["54506880"]["2026-06-01"] == 513
    assert prices["54506880"]["2026-06-02"] is None


def test_get_hotel_prices_uses_detail_context_for_missing_date(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()
    provider._last_search_targets = [target_hotel()]
    provider._candidate_cache = {
        "54506880": {
            "hotelId": "54506880",
            "hotelName": "深圳国际会展中心皇冠假日酒店",
            "starRating": 5,
            "distanceKm": 1.5,
            "currentPrice": 552,
        }
    }
    provider._price_cache = {"54506880": {"2026-06-01": 552}}
    calls: list[tuple[str, bool]] = []

    def fake_fetch(search_target, date_value, fast_mode=False, deep_mode=False):
        calls.append((date_value, deep_mode))
        return []

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", fake_fetch)
    monkeypatch.setattr(
        provider,
        "_fetch_hotel_detail_context_for_date",
        lambda seed, date_value: [
            {
                **seed,
                "currentPrice": 1164,
                "priceIncludesTax": True,
                "priceSource": "Trip.com detail room total incl. taxes & fees",
            }
        ],
    )

    progress_events = []
    prices = provider.get_hotel_prices(
        ["54506880"],
        ["2026-06-01", "2026-06-08"],
        progress_callback=progress_events.append,
    )

    assert prices["54506880"]["2026-06-08"] == 1164
    assert ("2026-06-08", True) not in calls
    assert any(
        event["date"] == "2026-06-08"
        and event["phase"] == "complete"
        and event["pricedHotelCount"] == 1
        for event in progress_events
    )


def test_get_hotel_prices_treats_cached_none_as_still_missing(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()
    provider._last_search_targets = [target_hotel()]
    provider._candidate_cache = {
        "40365204": {
            "hotelId": "40365204",
            "hotelName": "上海待补价酒店",
            "starRating": 5,
            "distanceKm": 1.5,
            "currentPrice": None,
        }
    }
    provider._price_cache = {"40365204": {"2026-06-02": None}}
    detail_calls: list[str] = []

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", lambda *args, **kwargs: [])

    def fake_detail(seed, date_value):
        detail_calls.append(str(seed.get("hotelId")))
        return [
            {
                **seed,
                "currentPrice": 888,
                "priceDate": date_value,
                "priceIncludesTax": True,
                "priceSource": "Trip.com detail room total incl. taxes & fees",
            }
        ]

    monkeypatch.setattr(provider, "_fetch_hotel_detail_context_for_date", fake_detail)

    prices = provider.get_hotel_prices(["40365204"], ["2026-06-02"])

    assert detail_calls == ["40365204"]
    assert prices["40365204"]["2026-06-02"] == 888
    assert provider.get_cached_hotel_prices(["40365204"], ["2026-06-02"])["40365204"]["2026-06-02"] == 888


def test_get_hotel_prices_opens_own_detail_when_list_card_has_no_price(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()
    provider._last_search_targets = [target_hotel()]
    provider._candidate_cache = {
        "40365204": {
            "hotelId": "40365204",
            "hotelName": "上海待补价酒店",
            "starRating": 5,
            "distanceKm": 1.5,
            "currentPrice": None,
        }
    }
    provider._price_cache = {}
    detail_calls: list[str] = []

    def fake_fetch(search_target, date_value, fast_mode=False, deep_mode=False):
        return [
            {
                "hotelId": "40365204",
                "hotelName": "上海待补价酒店",
                "starRating": 5,
                "distanceKm": 1.5,
                "currentPrice": None,
            }
        ]

    def fake_detail(seed, date_value):
        detail_calls.append(str(seed.get("hotelId")))
        return [
            {
                **seed,
                "currentPrice": 777,
                "priceDate": date_value,
                "priceIncludesTax": True,
                "priceSource": "Trip.com detail room total incl. taxes & fees",
            }
        ]

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", fake_fetch)
    monkeypatch.setattr(provider, "_fetch_hotel_detail_context_for_date", fake_detail)

    prices = provider.get_hotel_prices(["40365204"], ["2026-06-02"])

    assert detail_calls == ["40365204"]
    assert prices["40365204"]["2026-06-02"] == 777


def test_detail_context_recovers_city_id_from_cached_trip_url():
    provider = TripComProvider()
    provider._last_target = target_hotel()
    seed = {
        "hotelId": "40365204",
        "hotelName": "缓存待补价酒店",
        "tripUrl": "https://www.trip.com/hotels/detail/?hotelId=40365204&cityId=251",
    }

    assert provider._hotel_city_id(seed, target_hotel()) == 251


def test_get_hotel_prices_retries_detail_seed_when_seed_price_missing(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()
    provider._last_search_targets = [target_hotel()]
    provider._candidate_cache = {
        "54506880": {
            "hotelId": "54506880",
            "hotelName": "深圳国际会展中心皇冠假日酒店",
            "starRating": 5,
            "distanceKm": 1.0,
            "currentPrice": 552,
        }
    }
    provider._price_cache = {"54506880": {"2026-06-01": 552}}
    calls = 0

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", lambda *args, **kwargs: [])

    def fake_detail(seed, date_value):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [
                {
                    "hotelId": "70801018",
                    "hotelName": "深圳国际会展中心希尔顿花园酒店（深圳前海华发冰雪世界店）",
                    "currentPrice": 487,
                    "priceIncludesTax": True,
                    "priceSource": "Trip.com detail nearby after-tax price",
                }
            ]
        return [
            {
                **seed,
                "currentPrice": 1164,
                "priceIncludesTax": True,
                "priceSource": "Trip.com detail room total incl. taxes & fees",
            }
        ]

    monkeypatch.setattr(provider, "_fetch_hotel_detail_context_for_date", fake_detail)

    prices = provider.get_hotel_prices(["54506880"], ["2026-06-01", "2026-06-08"])

    assert calls == 2
    assert prices["54506880"]["2026-06-08"] == 1164


def test_get_hotel_prices_fetches_own_detail_for_remaining_brand_seed(monkeypatch):
    provider = TripComProvider()
    provider._last_target = target_hotel()
    provider._last_search_targets = [target_hotel()]
    provider._candidate_cache = {
        "near-a": {
            "hotelId": "near-a",
            "hotelName": "附近独立酒店A",
            "starRating": 4,
            "distanceKm": 0.4,
            "currentPrice": 320,
        },
        "near-b": {
            "hotelId": "near-b",
            "hotelName": "附近独立酒店B",
            "starRating": 4,
            "distanceKm": 0.5,
            "currentPrice": 330,
        },
        "54506880": {
            "hotelId": "54506880",
            "hotelName": "深圳国际会展中心皇冠假日酒店",
            "starRating": 5,
            "distanceKm": 1.5,
            "currentPrice": 552,
        },
    }
    provider._price_cache = {
        "near-a": {"2026-06-01": 320},
        "near-b": {"2026-06-01": 330},
        "54506880": {"2026-06-01": 552},
    }
    detail_seed_ids: list[str] = []

    monkeypatch.setattr(provider, "_fetch_hotel_list_for_date", lambda *args, **kwargs: [])

    def fake_detail(seed, date_value):
        detail_seed_ids.append(str(seed.get("hotelId")))
        if seed.get("hotelId") == "54506880":
            return [
                {
                    **seed,
                    "currentPrice": 1164,
                    "priceIncludesTax": True,
                    "priceSource": "Trip.com detail room total incl. taxes & fees",
                }
            ]
        return []

    monkeypatch.setattr(provider, "_fetch_hotel_detail_context_for_date", fake_detail)

    prices = provider.get_hotel_prices(
        ["near-a", "near-b", "54506880"],
        ["2026-06-01", "2026-06-08"],
    )

    assert "54506880" in detail_seed_ids
    assert prices["54506880"]["2026-06-08"] == 1164


def test_detail_room_price_uses_total_tax_price():
    provider = TripComProvider()
    data = {
        "data": {
            "saleRoomMap": {
                "room-a": {
                    "priceInfo": {
                        "price": 998,
                        "priceExplanation": "Total (incl. taxes & fees): CNY 1,164",
                    },
                    "totalPriceInfo": {
                        "totalNoApprox": {"content": "CNY 1,163.67"},
                        "payTax": {"content": "CNY 165.67"},
                    },
                },
                "room-b": {
                    "priceInfo": {
                        "price": 1009,
                        "priceExplanation": "Total (incl. taxes & fees): CNY 1,175",
                    },
                    "totalPriceInfo": {"total": {"content": "CNY 1,174.67"}},
                },
            }
        }
    }

    assert provider._extract_detail_room_tax_price(data) == 1164


def test_detail_nearby_hotel_uses_after_tax_price():
    provider = TripComProvider()
    row = {
        "base": {
            "hotelId": "70801018",
            "hotelName": "Hilton Garden Inn Shenzhen World Exhibition & Convention Center（Qianhai Snow World Store)",
            "hotelLevel": {"star": 4},
            "imageUrl": "https://example.test/hotel.jpg",
        },
        "position": {"lat": "22.708026408003647", "lng": "113.77333374316119"},
        "comment": {"score": "9.5", "scoreMax": "10", "totalReviews": "1,604 reviews"},
        "money": {
            "price": "460",
            "priceNote": "After tax CNY 487",
            "priceFloatInfo": {
                "priceSum": {"price": 487.39, "extraTitle": "含税/费"},
            },
        },
    }

    item = provider._normalize_trip_detail_nearby_hotel(
        row,
        dt.date(2026, 6, 8),
        dt.date(2026, 6, 9),
        target_hotel(),
    )

    assert item["hotelName"] == "深圳国际会展中心希尔顿花园酒店（深圳前海华发冰雪世界店）"
    assert item["currentPrice"] == 487
    assert item["priceIncludesTax"] is True
    assert item["rating"] == 4.8
    assert item["reviewCount"] == 1604


def test_html_list_parser_uses_total_tax_price_and_distance():
    provider = TripComProvider()
    html = """
    <div class="list-item"><div class="hotel-card" id="54506880">
      <img src="https://ak-d.tripcdn.com/images/test.jpg" />
      <span class="hotelName">Crowne Plaza SHENZHEN WECC by IHG</span>
      <div class="hotelStar" aria-label="5 out of 5 stars"></div>
      <div class="comment-score" aria-label="9.7 out of 10"></div>
      <span class="comment-num">3,187 reviews</span>
      <span class="position-desc">1.5 km walk from Shenzhen World Exhibition &amp; Convention Center</span>
      <span class="sale" aria-label="Current price CNY 440">CNY 440</span>
      <p class="price-explain">Total (incl. taxes &amp; fees): CNY 513</p>
    </div></div>
    """
    target = {**target_hotel(), "hotelId": "landmark", "hotelName": "深圳国际会展中心"}

    hotels = provider._hotel_cards_from_html(html, dt.date(2026, 6, 1), dt.date(2026, 6, 2), target)
    filtered = provider._filter_nearby_hotels(
        hotels=hotels,
        target_hotel=target,
        radius_km=5,
        min_star=4,
        selected_date="2026-06-01",
    )

    assert filtered[0]["hotelName"] == "深圳国际会展中心皇冠假日酒店"
    assert filtered[0]["currentPrice"] == 513
    assert filtered[0]["priceIncludesTax"] is True
    assert filtered[0]["distanceKm"] == 1.5
    assert filtered[0]["rating"] == 4.8
    assert filtered[0]["reviewCount"] == 3187


def test_html_list_parser_keeps_star_hotel_without_price():
    provider = TripComProvider()
    html = """
    <div class="list-item"><div class="hotel-card" id="8784327">
      <img src="https://ak-d.tripcdn.com/images/test.jpg" />
      <span class="hotelName">廣州珠江新城假日酒店</span>
      <div class="hotelStar" aria-label="4 out of 5 stars"></div>
      <span class="position-desc">1.2 km from Zhujiang New Town</span>
    </div></div>
    """
    target = {**target_hotel(), "hotelId": "landmark", "hotelName": "珠江新城", "latitude": 23.119, "longitude": 113.333}

    hotels = provider._hotel_cards_from_html(html, dt.date(2026, 6, 1), dt.date(2026, 6, 2), target)
    filtered = provider._filter_nearby_hotels(
        hotels=hotels,
        target_hotel=target,
        radius_km=3,
        min_star=4,
        selected_date="2026-06-01",
    )

    assert filtered[0]["hotelName"] == "广州珠江新城假日酒店"
    assert filtered[0]["currentPrice"] is None
    assert filtered[0]["priceIncludesTax"] is False
    assert filtered[0]["distanceKm"] == 1.2


def test_trip_hotel_normalizer_keeps_star_hotel_without_price():
    provider = TripComProvider()
    row = {
        "hotelBasicInfo": {
            "hotelId": "8784327",
            "hotelName": "廣州珠江新城假日酒店",
            "star": "4",
        },
        "positionInfo": {
            "cityName": "广州",
            "cityId": 32,
            "lat": 23.122586,
            "lng": 113.322618,
        },
    }

    item = provider._normalize_trip_hotel(row, dt.date(2026, 6, 1), dt.date(2026, 6, 2), target_hotel())

    assert item is not None
    assert item["hotelName"] == "广州珠江新城假日酒店"
    assert item["cityId"] == 32
    assert parse_qs(urlparse(item["tripUrl"]).query)["cityId"] == ["32"]
    assert item["currentPrice"] is None
    assert item["priceDate"] == ""
    assert item["priceIncludesTax"] is False


def test_html_distance_parser_accepts_drive_distance():
    provider = TripComProvider()

    assert provider._extract_html_card_distance(
        "3 km drive from Shenzhen World Exhibition & Convention Center"
    ) == 3.0
