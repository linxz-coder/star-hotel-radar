from __future__ import annotations

import argparse
import json
import time
from typing import Any
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_TARGETS = [
    ("深圳", "深圳国际会展中心希尔顿酒店"),
    ("上海", "上海外滩W酒店"),
    ("广州", "广州天河希尔顿酒店"),
    ("惠州", "惠州佳兆业铂尔曼酒店"),
    ("东莞", "东莞康帝国际酒店"),
]


def request_json(url: str, *, payload: dict[str, Any] | None = None, timeout: int = 240) -> dict[str, Any]:
    if payload is None:
        req = Request(url, headers={"accept": "application/json"})
    else:
        req = Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"accept": "application/json", "content-type": "application/json"},
            method="POST",
        )
    try:
        with urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            data = {"error": body[:500]}
        data["_httpStatus"] = exc.code
        return data
    except URLError as exc:
        return {"error": str(exc)}
    except Exception as exc:
        return {"error": str(exc)}


def suggest(base_url: str, city: str, query: str) -> dict[str, Any] | None:
    params = urlencode({"city": city, "q": query, "limit": 1})
    data = request_json(f"{base_url}/api/tripcom/suggest?{params}", timeout=60)
    suggestions = data.get("suggestions") if isinstance(data, dict) else []
    return suggestions[0] if suggestions else None


def quick_search(base_url: str, city: str, query: str, selected_date: str) -> dict[str, Any]:
    target_hint = suggest(base_url, city, query)
    if not target_hint:
        return {
            "city": city,
            "query": query,
            "suggestionFound": False,
            "error": "Trip.com suggestion not found",
        }

    payload = {
        "provider": "tripcom",
        "city": city,
        "targetHotel": query,
        "selectedDate": selected_date,
        "radiusKm": "5",
        "minStar": "4",
        "sortBy": "discount",
        "asyncMode": "1",
        "backgroundMode": "0",
        "targetHint": target_hint,
    }
    started_at = time.perf_counter()
    data = request_json(f"{base_url}/api/search", payload=payload, timeout=240)
    elapsed_seconds = round(time.perf_counter() - started_at, 2)
    if data.get("error"):
        return {
            "city": city,
            "query": query,
            "matchedName": target_hint.get("hotelName"),
            "suggestionFound": True,
            "httpStatus": data.get("_httpStatus"),
            "error": data.get("error"),
            "clientElapsedSeconds": elapsed_seconds,
        }
    summary = data.get("summary") or {}
    all_hotels = data.get("allHotels") or []
    recommended_hotels = data.get("recommendedHotels") or []
    return {
        "city": city,
        "query": query,
        "matchedName": (data.get("targetHotel") or {}).get("hotelName"),
        "suggestionFound": True,
        "candidateCount": summary.get("candidateCount"),
        "recommendedCount": summary.get("recommendedCount"),
        "dealCount": summary.get("dealCount"),
        "partial": summary.get("partial"),
        "serverElapsedMs": summary.get("elapsedMs"),
        "clientElapsedSeconds": elapsed_seconds,
        "firstHotel": (all_hotels[0] or {}).get("hotelName") if all_hotels else "",
        "firstPrice": (all_hotels[0] or {}).get("currentPrice") if all_hotels else None,
        "firstRecommended": (recommended_hotels[0] or {}).get("hotelName") if recommended_hotels else "",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:5013")
    parser.add_argument("--date", default="2026-06-01")
    args = parser.parse_args()

    rows = [quick_search(args.base_url.rstrip("/"), city, query, args.date) for city, query in DEFAULT_TARGETS]
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
