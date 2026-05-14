from __future__ import annotations

import importlib
import threading
import time

import pytest


@pytest.fixture(autouse=True)
def disable_mysql_cache(monkeypatch):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "MYSQL_SEARCH_CACHE", None)
    monkeypatch.setattr(app_module, "MYSQL_HOTEL_NAME_CACHE", None)
    with app_module.HOTEL_NAME_CACHE_LOCK:
        app_module.HOTEL_NAME_MEMORY_CACHE = {"byHotelId": {}, "byNameKey": {}}
        app_module.HOTEL_NAME_CACHE_LOADED = False


def sample_result(sort_by: str = "discount") -> dict:
    hotels = [
        {
            "hotelId": "expensive",
            "hotelName": "高价高优惠酒店",
            "currentPrice": 900,
            "discountAmount": 300,
            "distanceKm": 2.0,
            "starRating": 5,
            "brandRank": 2,
            "isDeal": True,
            "isRecommendedBrand": True,
        },
        {
            "hotelId": "cheap",
            "hotelName": "低价酒店",
            "currentPrice": 500,
            "discountAmount": 120,
            "distanceKm": 1.0,
            "starRating": 4,
            "brandRank": 4,
            "isDeal": True,
            "isRecommendedBrand": True,
        },
    ]
    return {
        "query": {"sortBy": sort_by},
        "targetHotel": {},
        "compareDates": ["2026-06-01"],
        "allHotels": hotels,
        "dealHotels": hotels,
        "recommendedHotels": hotels,
        "summary": {"dealCount": 2, "recommendedCount": 2},
    }


def test_search_cache_ignores_sort_and_reuses_result(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path)
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    app_module.SEARCH_CACHE.clear()

    calls = {"count": 0}

    def fake_search_deals(**kwargs):
        calls["count"] += 1
        return sample_result(kwargs.get("sort_by") or "discount")

    monkeypatch.setattr(app_module, "search_deals", fake_search_deals)

    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "3",
        "minStar": "4",
        "provider": "local",
        "sortBy": "discount",
    }

    with app_module.app.test_client() as client:
        first = client.post("/api/search", json=payload).get_json()
        second_payload = {**payload, "sortBy": "price"}
        second = client.post("/api/search", json=second_payload).get_json()

    assert calls["count"] == 1
    assert first["summary"]["cacheHit"] is False
    assert second["summary"]["cacheHit"] is True
    assert second["query"]["sortBy"] == "price"
    assert second["allHotels"][0]["hotelId"] == "cheap"


def test_search_cache_reads_from_disk_after_memory_clear(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path)
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    app_module.SEARCH_CACHE.clear()

    calls = {"count": 0}

    def fake_search_deals(**kwargs):
        calls["count"] += 1
        return sample_result(kwargs.get("sort_by") or "discount")

    monkeypatch.setattr(app_module, "search_deals", fake_search_deals)
    payload = {
        "city": "广州",
        "targetHotel": "广州天河希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "3",
        "minStar": "4",
        "provider": "local",
        "sortBy": "discount",
    }

    with app_module.app.test_client() as client:
        client.post("/api/search", json=payload)
        app_module.SEARCH_CACHE.clear()
        cached = client.post("/api/search", json=payload).get_json()

    assert calls["count"] == 1
    assert cached["summary"]["cacheHit"] is True
    assert cached["summary"]["cacheSource"] == "disk"


def test_force_refresh_skips_cached_response_and_merges_into_cache(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    app_module.SEARCH_CACHE.clear()

    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "3",
        "minStar": "4",
        "provider": "local",
        "sortBy": "discount",
    }
    old_result = sample_result("discount")
    old_result["allHotels"][0]["hotelName"] = "旧缓存高价高优惠酒店"
    key = app_module.cache_key(payload)
    app_module.SEARCH_CACHE[key] = (time.time() + 3600, old_result, time.time())

    calls = {"count": 0}

    def fake_search_deals(**kwargs):
        calls["count"] += 1
        fresh = sample_result(kwargs.get("sort_by") or "discount")
        fresh["allHotels"] = [
            {
                **fresh["allHotels"][1],
                "currentPrice": 450,
                "discountAmount": 180,
            },
            {
                "hotelId": "fresh",
                "hotelName": "实时新增酒店",
                "currentPrice": 520,
                "discountAmount": 130,
                "distanceKm": 1.5,
                "starRating": 4,
                "brandRank": 3,
                "isDeal": True,
                "isRecommendedBrand": True,
            },
        ]
        fresh["dealHotels"] = list(fresh["allHotels"])
        fresh["recommendedHotels"] = list(fresh["allHotels"])
        return fresh

    monkeypatch.setattr(app_module, "search_deals", fake_search_deals)

    with app_module.app.test_client() as client:
        refreshed = client.post("/api/search", json={**payload, "forceRefresh": "1"}).get_json()
        cached = client.post("/api/search", json=payload).get_json()

    assert calls["count"] == 1
    assert refreshed["summary"]["cacheHit"] is False
    assert refreshed["summary"]["mergedFromCache"] is True
    assert refreshed["summary"]["cacheCarriedHotelCount"] == 1
    assert refreshed["summary"]["cacheCorrectedHotelCount"] == 1
    hotel_ids = [hotel["hotelId"] for hotel in refreshed["allHotels"]]
    assert "fresh" in hotel_ids
    assert "expensive" in hotel_ids
    cheap = next(hotel for hotel in refreshed["allHotels"] if hotel["hotelId"] == "cheap")
    assert cheap["currentPrice"] == 450
    assert cached["summary"]["cacheHit"] is True
    assert cached["summary"]["mergedFromCache"] is True


def test_search_defaults_to_tripcom_and_passes_target_hint(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path)
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    app_module.SEARCH_CACHE.clear()

    captured = {}

    class FakeTripProvider:
        pass

    def fake_provider_from_name(app_dir, provider_name):
        captured["provider_name"] = provider_name
        return FakeTripProvider()

    def fake_search_deals(**kwargs):
        captured.update(kwargs)
        return sample_result(kwargs.get("sort_by") or "discount")

    monkeypatch.setattr(app_module, "provider_from_name", fake_provider_from_name)
    monkeypatch.setattr(app_module, "search_deals", fake_search_deals)
    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "5",
        "minStar": "4",
        "asyncMode": "0",
        "targetHint": {
            "hotelId": "trip-123",
            "hotelName": "深圳国际会展中心希尔顿酒店",
            "searchType": "H",
        },
    }

    with app_module.app.test_client() as client:
        result = client.post("/api/search", json=payload).get_json()

    assert result["summary"]["cacheHit"] is False
    assert captured["provider_name"] == "tripcom"
    assert captured["target_hint"]["hotelId"] == "trip-123"
    assert captured["target_hint"]["searchType"] == "H"


def test_tripcom_search_returns_immediate_progress_job(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    monkeypatch.setattr(app_module, "HOTEL_NAME_CACHE_PATH", tmp_path / "hotel_name_cache.json")
    app_module.SEARCH_CACHE.clear()
    app_module.SEARCH_JOBS.clear()
    with app_module.HOTEL_NAME_CACHE_LOCK:
        app_module.HOTEL_NAME_MEMORY_CACHE = {"byHotelId": {}, "byNameKey": {}}
        app_module.HOTEL_NAME_CACHE_LOADED = False

    calls = []

    def fake_run_search_payload(payload, provider_name, quick=False):
        calls.append((provider_name, quick))
        result = sample_result("price")
        result["summary"]["partial"] = quick
        return result

    monkeypatch.setattr(app_module, "run_search_payload", fake_run_search_payload)
    monkeypatch.setattr(app_module, "start_background_search_job", lambda payload, key, provider_name: "job-123")
    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "5",
        "minStar": "4",
        "provider": "tripcom",
        "sortBy": "price",
    }

    with app_module.app.test_client() as client:
        result = client.post("/api/search", json=payload).get_json()

    assert calls == []
    assert result["summary"]["partial"] is True
    assert result["summary"]["jobId"] == "job-123"
    assert result["summary"]["jobStatus"] == "queued"
    assert result["summary"]["cacheSource"] == "live-progress"
    assert "正在连接 Trip.com" in result["summary"]["progress"]["message"]


def test_search_status_returns_completed_job_with_requested_sort(monkeypatch):
    app_module = importlib.import_module("app")
    app_module.SEARCH_JOBS.clear()
    with app_module.SEARCH_JOB_LOCK:
        app_module.SEARCH_JOBS["job-done"] = {
            "id": "job-done",
            "status": "complete",
            "result": sample_result("discount"),
        }

    with app_module.app.test_client() as client:
        response = client.get("/api/search/status/job-done?sortBy=price")

    result = response.get_json()["result"]
    assert response.status_code == 200
    assert result["summary"]["jobStatus"] == "complete"
    assert result["summary"]["partial"] is False
    assert result["query"]["sortBy"] == "price"
    assert result["allHotels"][0]["hotelId"] == "cheap"


def test_search_events_streams_completed_job_with_requested_sort(monkeypatch):
    app_module = importlib.import_module("app")
    app_module.SEARCH_JOBS.clear()
    with app_module.SEARCH_JOB_LOCK:
        app_module.SEARCH_JOBS["job-done"] = {
            "id": "job-done",
            "status": "complete",
            "result": sample_result("discount"),
        }

    with app_module.app.test_client() as client:
        response = client.get("/api/search/events/job-done?sortBy=price")

    text = response.get_data(as_text=True)
    assert response.status_code == 200
    assert response.content_type.startswith("text/event-stream")
    assert "event: message" in text
    assert '"status": "complete"' in text
    assert '"hotelId": "cheap"' in text


def test_search_status_returns_running_progress_and_partial_result(monkeypatch):
    app_module = importlib.import_module("app")
    app_module.SEARCH_JOBS.clear()
    partial = sample_result("discount")
    partial["summary"]["partial"] = True
    with app_module.SEARCH_JOB_LOCK:
        app_module.SEARCH_JOBS["job-running"] = {
            "id": "job-running",
            "status": "running",
            "startedAt": 1000,
            "partialResult": partial,
            "progress": {"stage": "compare-price", "message": "已找到 2 家候选，正在补齐比价。"},
        }

    with app_module.app.test_client() as client:
        response = client.get("/api/search/status/job-running?sortBy=price")

    payload = response.get_json()
    result = payload["result"]
    assert response.status_code == 200
    assert payload["status"] == "running"
    assert payload["progress"]["stage"] == "compare-price"
    assert result["summary"]["partial"] is True
    assert result["summary"]["jobStatus"] == "pricing"
    assert result["allHotels"][0]["hotelId"] == "cheap"


def test_search_status_keeps_provisional_raw_candidates_before_cn_name_ready(monkeypatch):
    app_module = importlib.import_module("app")
    app_module.SEARCH_JOBS.clear()
    partial = {
        "query": {"sortBy": "discount"},
        "targetHotel": {},
        "compareDates": ["2026-06-01"],
        "allHotels": [
            {
                "hotelId": "raw-en",
                "hotelName": "Unknown Star Hotel",
                "starRating": 4,
                "distanceKm": 1.2,
                "currentPrice": 480,
                "nameProcessing": True,
            }
        ],
        "dealHotels": [],
        "recommendedHotels": [],
        "summary": {
            "partial": True,
            "candidateCount": 1,
            "nameNormalizationDeferred": True,
            "sortDeferred": True,
        },
    }
    with app_module.SEARCH_JOB_LOCK:
        app_module.SEARCH_JOBS["job-raw"] = {
            "id": "job-raw",
            "status": "running",
            "startedAt": 1000,
            "partialResult": partial,
            "progress": {"stage": "first-screen", "message": "已拿到原始候选。"},
        }

    with app_module.app.test_client() as client:
        response = client.get("/api/search/status/job-raw?sortBy=discount")

    payload = response.get_json()
    result = payload["result"]
    assert response.status_code == 200
    assert result["summary"]["candidateCount"] == 1
    assert result["allHotels"][0]["hotelId"] == "raw-en"
    assert result["allHotels"][0]["hotelName"] == "星级酒店（中文名正在核验中...）"
    assert result["allHotels"][0]["hotelOriginalName"] == "Unknown Star Hotel"


def test_apply_sort_retains_unlocalized_deal_hotels_without_deferred_flag():
    app_module = importlib.import_module("app")
    result = {
        "query": {"city": "深圳", "sortBy": "discount"},
        "targetHotel": {},
        "compareDates": ["2026-06-01"],
        "allHotels": [
            {
                "hotelId": "raw-deal",
                "hotelName": "Raw Deal Hotel",
                "starRating": 4,
                "distanceKm": 1.2,
                "currentPrice": 480,
                "discountAmount": 180,
                "isDeal": True,
                "isRecommendedBrand": False,
            }
        ],
        "dealHotels": [
            {
                "hotelId": "raw-deal",
                "hotelName": "Raw Deal Hotel",
                "starRating": 4,
                "distanceKm": 1.2,
                "currentPrice": 480,
                "discountAmount": 180,
                "isDeal": True,
                "isRecommendedBrand": False,
            }
        ],
        "recommendedHotels": [],
        "summary": {"candidateCount": 1, "dealCount": 1, "recommendedCount": 0},
    }

    sorted_result = app_module.apply_sort_to_result(result, "discount")

    assert sorted_result["summary"]["candidateCount"] == 1
    assert sorted_result["summary"]["dealCount"] == 1
    assert sorted_result["allHotels"][0]["hotelName"] == "深圳星级酒店（中文名正在核验中...）"
    assert sorted_result["allHotels"][0]["hotelOriginalName"] == "Raw Deal Hotel"
    assert sorted_result["dealHotels"][0]["hotelName"] == "深圳星级酒店（中文名正在核验中...）"
    assert sorted_result["dealHotels"][0]["hotelOriginalName"] == "Raw Deal Hotel"


def test_name_verification_finalizes_pending_names_without_remote_match():
    app_module = importlib.import_module("app")
    result = {
        "query": {"city": "深圳", "sortBy": "discount"},
        "targetHotel": {},
        "compareDates": ["2026-06-01"],
        "allHotels": [
            {
                "hotelId": "12345",
                "hotelName": "深圳星级酒店（中文名正在核验中...）",
                "hotelOriginalName": "Raw Star Hotel Shenzhen",
                "starRating": 4,
                "distanceKm": 1.2,
                "currentPrice": 480,
                "isDeal": False,
                "isRecommendedBrand": False,
                "nameProcessing": True,
            }
        ],
        "dealHotels": [],
        "recommendedHotels": [],
        "summary": {"candidateCount": 1, "dealCount": 0, "recommendedCount": 0},
    }

    verified = app_module.verify_result_hotel_names(object(), result, "2026-06-01")

    assert verified["allHotels"][0]["hotelName"] == "深圳携程酒店12345"
    assert "nameProcessing" not in verified["allHotels"][0]
    assert verified["summary"]["nameVerificationComplete"] is True
    assert verified["summary"]["nameVerificationRemainingCount"] == 0


def test_verified_hotel_name_is_cached_and_reused(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "HOTEL_NAME_CACHE_PATH", tmp_path / "hotel_name_cache.json")
    with app_module.HOTEL_NAME_CACHE_LOCK:
        app_module.HOTEL_NAME_MEMORY_CACHE = {"byHotelId": {}, "byNameKey": {}}
        app_module.HOTEL_NAME_CACHE_LOADED = False

    hotel = {
        "hotelId": "name-cache-1",
        "hotelName": "深圳星级酒店（中文名正在核验中...）",
        "hotelOriginalName": "Raw Cache Hotel",
        "city": "深圳",
        "nameProcessing": True,
    }
    payload = {
        "hotelName": "深圳中文名缓存酒店",
        "hotelOriginalName": "Raw Cache Hotel",
        "hotelNameSimplified": "深圳中文名缓存酒店",
        "hotelNameSource": "携程中文页",
    }

    app_module.cache_hotel_name_payload(hotel, payload, "tripcom")
    result = {
        "query": {"city": "深圳", "sortBy": "discount"},
        "targetHotel": {},
        "compareDates": ["2026-06-01"],
        "allHotels": [
            {
                **hotel,
                "starRating": 4,
                "distanceKm": 1.2,
                "currentPrice": 520,
                "isDeal": False,
                "isRecommendedBrand": False,
            }
        ],
        "dealHotels": [],
        "recommendedHotels": [],
        "summary": {"partial": True, "candidateCount": 1},
    }

    updated = app_module.apply_cached_hotel_names_to_result(result, "tripcom")

    assert updated["allHotels"][0]["hotelName"] == "深圳中文名缓存酒店"
    assert updated["allHotels"][0]["hotelNameSource"] == "携程中文页"
    assert "nameProcessing" not in updated["allHotels"][0]
    assert updated["summary"]["nameCacheHit"] is True


def test_apply_sort_repairs_cached_brand_labels():
    app_module = importlib.import_module("app")
    cached = {
        "query": {"city": "广州", "sortBy": "discount"},
        "targetHotel": {},
        "compareDates": ["2026-06-01"],
        "allHotels": [
            {
                "hotelId": "w-guangzhou",
                "hotelName": "W Guangzhou",
                "hotelOriginalName": "W Guangzhou",
                "brand": "独立酒店",
                "brandLabel": "独立酒店",
                "brandRank": 99,
                "isRecommendedBrand": False,
                "starRating": 5,
                "distanceKm": 1.5,
                "currentPrice": 1200,
                "isDeal": False,
            },
            {
                "hotelId": "intercity-guangzhou",
                "hotelName": "广州珠江新城城际酒店",
                "brand": "独立酒店",
                "brandLabel": "独立酒店",
                "brandRank": 99,
                "isRecommendedBrand": False,
                "starRating": 4,
                "distanceKm": 1.0,
                "currentPrice": 520,
                "isDeal": False,
            },
        ],
        "dealHotels": [],
        "recommendedHotels": [],
        "summary": {"candidateCount": 2, "recommendedCount": 0},
    }

    repaired = app_module.apply_sort_to_result(cached, "discount")
    by_id = {hotel["hotelId"]: hotel for hotel in repaired["allHotels"]}

    assert by_id["w-guangzhou"]["brandLabel"] == "万豪"
    assert by_id["w-guangzhou"]["groupLabel"] == "万豪国际"
    assert by_id["w-guangzhou"]["isRecommendedBrand"] is True
    assert repaired["recommendedHotels"][0]["hotelId"] == "w-guangzhou"
    assert by_id["intercity-guangzhou"]["brandLabel"] == "华住"
    assert by_id["intercity-guangzhou"]["groupLabel"] == "华住集团"
    assert by_id["intercity-guangzhou"]["isRecommendedBrand"] is False


def test_background_search_starts_name_verification_from_first_partial(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    monkeypatch.setattr(app_module, "HOTEL_NAME_CACHE_PATH", tmp_path / "hotel_name_cache.json")
    app_module.SEARCH_CACHE.clear()
    app_module.SEARCH_JOBS.clear()
    with app_module.HOTEL_NAME_CACHE_LOCK:
        app_module.HOTEL_NAME_MEMORY_CACHE = {"byHotelId": {}, "byNameKey": {}}
        app_module.HOTEL_NAME_CACHE_LOADED = False
    verify_started = threading.Event()

    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心",
        "selectedDate": "2026-06-01",
        "radiusKm": "5",
        "minStar": "4",
        "provider": "tripcom",
        "sortBy": "discount",
    }

    def pending_result(partial: bool) -> dict:
        return {
            "query": {"city": "深圳", "sortBy": "discount"},
            "targetHotel": {"hotelId": "target", "hotelName": "深圳国际会展中心", "searchType": "LM"},
            "compareDates": ["2026-06-01", "2026-06-02"],
            "allHotels": [
                {
                    "hotelId": "name-1",
                    "hotelName": "深圳星级酒店（中文名正在核验中...）",
                    "hotelOriginalName": "Raw Hotel Name",
                    "hotelNameSource": "本地中文名兜底（原名正在核验中）",
                    "city": "深圳",
                    "starRating": 4,
                    "distanceKm": 1.2,
                    "currentPrice": 520,
                    "isDeal": False,
                    "isRecommendedBrand": False,
                    "nameProcessing": True,
                }
            ],
            "dealHotels": [],
            "recommendedHotels": [],
            "summary": {
                "partial": partial,
                "candidateCount": 1,
                "dealCount": 0,
                "recommendedCount": 0,
                "source": "Trip.com 实时抓取",
            },
        }

    class FakeProvider:
        source_name = "Trip.com 实时抓取"

        def verify_hotel_names(self, hotels, selected_date, progress_callback=None, lightweight_only=False):
            assert lightweight_only is True
            verify_started.set()
            payload = {
                "hotelName": "深圳测试酒店",
                "hotelOriginalName": "Raw Hotel Name",
                "hotelNameSimplified": "深圳测试酒店",
                "hotelNameSource": "测试中文名来源",
            }
            if progress_callback:
                progress_callback({"phase": "resolved", "hotelId": "name-1", "completed": 1, "total": 1, "payload": payload})
            return {"name-1": payload}

    monkeypatch.setattr(app_module, "provider_from_name", lambda app_dir, provider_name: FakeProvider())
    monkeypatch.setattr(app_module, "search_current_prices", lambda **kwargs: pending_result(partial=True))

    def fake_search_deals(**kwargs):
        assert verify_started.wait(2)
        return pending_result(partial=False)

    monkeypatch.setattr(app_module, "search_deals", fake_search_deals)

    key = app_module.cache_key(payload)
    job_id = app_module.start_background_search_job(payload, key, "tripcom", force_refresh=True)
    deadline = time.time() + 4
    job = {}
    while time.time() < deadline:
        with app_module.SEARCH_JOB_LOCK:
            job = dict(app_module.SEARCH_JOBS.get(job_id) or {})
        if job.get("status") in {"complete", "error"}:
            break
        time.sleep(0.05)

    assert verify_started.is_set()
    assert job.get("status") == "complete", job
    assert job["result"]["allHotels"][0]["hotelName"] == "深圳测试酒店"


def test_stale_tripcom_cache_returns_immediately_and_refreshes(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    monkeypatch.setattr(app_module, "TRIPCOM_REFRESH_AFTER_SECONDS", 10)
    app_module.SEARCH_CACHE.clear()

    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "5",
        "minStar": "4",
        "provider": "tripcom",
        "sortBy": "discount",
    }
    key = app_module.cache_key(payload)
    app_module.SEARCH_CACHE[key] = (time.time() + 3600, sample_result("discount"), time.time() - 3600)
    captured = {}

    def fake_start_background_search_job(payload_arg, key_arg, provider_name, *, force_refresh=False, base_cached_result=None):
        captured["payload"] = payload_arg
        captured["key"] = key_arg
        captured["provider"] = provider_name
        captured["force_refresh"] = force_refresh
        captured["base_cached_result"] = base_cached_result
        return "refresh-job"

    monkeypatch.setattr(app_module, "start_background_search_job", fake_start_background_search_job)

    with app_module.app.test_client() as client:
        result = client.post("/api/search", json=payload).get_json()

    assert result["summary"]["cacheHit"] is True
    assert result["summary"]["refreshing"] is True
    assert result["summary"]["refreshJobId"] == "refresh-job"
    assert captured["force_refresh"] is True
    assert captured["base_cached_result"] is not None
    assert result["allHotels"][0]["hotelId"] == "expensive"


def test_empty_result_is_not_cached(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    app_module.SEARCH_CACHE.clear()
    key = app_module.cache_key(
        {
            "city": "深圳",
            "targetHotel": "深圳国际会展中心希尔顿酒店",
            "selectedDate": "2026-06-01",
            "radiusKm": "5",
            "minStar": "4",
            "provider": "tripcom",
        }
    )
    empty_result = {
        "allHotels": [],
        "dealHotels": [],
        "recommendedHotels": [],
        "summary": {"partial": False, "candidateCount": 0},
    }

    app_module.remember_search_result(key, "tripcom", empty_result)

    assert key not in app_module.SEARCH_CACHE


def test_nonempty_partial_result_is_cached_without_stale_job_metadata(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    app_module.SEARCH_CACHE.clear()
    key = app_module.cache_key(
        {
            "city": "深圳",
            "targetHotel": "深圳国际会展中心希尔顿酒店",
            "selectedDate": "2026-06-01",
            "radiusKm": "5",
            "minStar": "4",
            "provider": "tripcom",
        }
    )
    partial = sample_result("discount")
    partial["summary"].update(
        {
            "partial": True,
            "priceCompareComplete": False,
            "candidateCount": 2,
            "jobId": "old-job",
            "jobStatus": "pricing",
            "progress": {"message": "old"},
        }
    )

    app_module.remember_search_result(key, "tripcom", partial)
    cached = app_module.cached_search_result(key, "tripcom", "discount")

    assert cached is not None
    assert cached["summary"]["cacheHit"] is True
    assert cached["summary"]["partial"] is True
    assert "jobId" not in cached["summary"]
    assert "jobStatus" not in cached["summary"]
    assert "progress" not in cached["summary"]
    assert cached["summary"]["priceCompareComplete"] is False


def test_target_only_partial_result_is_cached_for_resume(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    app_module.SEARCH_CACHE.clear()
    key = app_module.cache_key(
        {
            "city": "广州",
            "targetHotel": "广州珠江新城",
            "selectedDate": "2026-06-01",
            "radiusKm": "3",
            "minStar": "4",
            "provider": "tripcom",
        }
    )
    partial = {
        "query": {"sortBy": "discount"},
        "targetHotel": {
            "hotelId": "4343358",
            "hotelName": "珠江新城",
            "searchType": "LM",
            "resultTypeLabel": "地标",
        },
        "compareDates": ["2026-06-01"],
        "allHotels": [],
        "dealHotels": [],
        "recommendedHotels": [],
        "summary": {"partial": True, "candidateCount": 0},
    }

    app_module.remember_search_result(key, "tripcom", partial)
    cached = app_module.cached_search_result(key, "tripcom", "discount")

    assert cached is not None
    assert cached["summary"]["cacheHit"] is True
    assert cached["summary"]["partial"] is True
    assert cached["targetHotel"]["hotelId"] == "4343358"
    assert cached["summary"]["candidateCount"] == 0


def test_cached_target_hint_is_reused_for_resume():
    app_module = importlib.import_module("app")
    payload = {"city": "广州", "targetHotel": "广州珠江新城"}
    cached = {
        "targetHotel": {
            "hotelId": "4343358",
            "hotelName": "珠江新城",
            "searchType": "LM",
            "resultTypeLabel": "地标",
        }
    }

    resumed = app_module.payload_with_cached_target_hint(payload, cached)

    assert resumed["targetHint"]["hotelId"] == "4343358"
    assert resumed["targetHint"]["searchType"] == "LM"


def test_hot_targets_track_successful_searches(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    app_module.SEARCH_CACHE.clear()

    class FakeTripProvider:
        pass

    monkeypatch.setattr(app_module, "provider_from_name", lambda app_dir, provider_name: FakeTripProvider())
    monkeypatch.setattr(app_module, "search_deals", lambda **kwargs: sample_result(kwargs.get("sort_by") or "discount"))
    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "5",
        "minStar": "4",
        "asyncMode": "0",
        "targetHint": {
            "hotelId": "71649086",
            "hotelName": "深圳国际会展中心希尔顿酒店",
            "searchType": "H",
            "resultTypeLabel": "酒店",
            "city": "深圳",
            "cityId": 30,
            "latitude": 22.705377,
            "longitude": 113.777817,
            "searchValue": "31~71649086*31*71649086*1",
        },
    }

    with app_module.app.test_client() as client:
        client.post("/api/search", json=payload)
        hot_targets = client.get("/api/hot-targets").get_json()["targets"]

    assert hot_targets[0]["city"] == "深圳"
    assert hot_targets[0]["targetHotel"] == "深圳国际会展中心希尔顿酒店"
    assert hot_targets[0]["targetType"] == "酒店"
    assert hot_targets[0]["heatLabel"] == "1次搜索"
    assert hot_targets[0]["targetHint"]["hotelId"] == "71649086"


def test_search_cache_prefers_mysql_after_memory_clear(monkeypatch, tmp_path):
    app_module = importlib.import_module("app")
    monkeypatch.setattr(app_module, "SEARCH_CACHE_DIR", tmp_path / "search_cache")
    monkeypatch.setattr(app_module, "HOT_SEARCH_PATH", tmp_path / "hot_searches.json")
    app_module.SEARCH_CACHE.clear()

    class FakeMySQLCache:
        def __init__(self):
            self.record = None
            self.provider = None
            self.expires_at = None

        def get(self, key, provider):
            if self.record is None or provider != self.provider:
                return None
            return {"expiresAt": self.expires_at, "result": self.record}

        def store(self, key, provider, result, expires_at):
            self.provider = provider
            self.record = result
            self.expires_at = expires_at

        def clear(self, provider=None):
            self.record = None

    fake_mysql = FakeMySQLCache()
    monkeypatch.setattr(app_module, "MYSQL_SEARCH_CACHE", fake_mysql)

    calls = {"count": 0}

    class FakeTripProvider:
        pass

    def fake_search_deals(**kwargs):
        calls["count"] += 1
        return sample_result(kwargs.get("sort_by") or "discount")

    monkeypatch.setattr(app_module, "provider_from_name", lambda app_dir, provider_name: FakeTripProvider())
    monkeypatch.setattr(app_module, "search_deals", fake_search_deals)
    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心希尔顿酒店",
        "selectedDate": "2026-06-01",
        "radiusKm": "5",
        "minStar": "4",
        "provider": "tripcom",
        "sortBy": "discount",
        "asyncMode": "0",
    }

    with app_module.app.test_client() as client:
        first = client.post("/api/search", json=payload).get_json()
        app_module.SEARCH_CACHE.clear()
        second = client.post("/api/search", json={**payload, "sortBy": "price"}).get_json()

    assert calls["count"] == 1
    assert first["summary"]["cacheHit"] is False
    assert second["summary"]["cacheHit"] is True
    assert second["summary"]["cacheSource"] == "mysql"
    assert second["query"]["sortBy"] == "price"
    assert app_module.TRIPCOM_CACHE_TTL_SECONDS == 7 * 24 * 60 * 60
