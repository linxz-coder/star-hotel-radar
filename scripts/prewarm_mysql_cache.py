from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import (  # noqa: E402
    MYSQL_HOTEL_CANDIDATE_CACHE,
    MYSQL_HOTEL_PRICE_CACHE,
    MYSQL_SEARCH_CACHE,
    cache_key,
    current_hot_targets,
    default_check_in,
    remember_search_result,
    run_search_payload,
)
from hotel_deals import get_compare_date_info  # noqa: E402
from providers import ProviderError, TripComProvider  # noqa: E402


def parse_dates(value: str | None) -> list[str]:
    text = (value or os.environ.get("HOTEL_DEAL_PREWARM_DATES") or "").strip()
    if text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [default_check_in()]


def resolve_target_hint(city: str, target_hotel: str) -> dict[str, Any] | None:
    try:
        suggestions = TripComProvider().suggest_targets(city=city, query=target_hotel, limit=1)
    except ProviderError:
        return None
    return suggestions[0] if suggestions else None


def build_payload(
    target: dict[str, Any],
    selected_date: str,
    *,
    radius_km: str,
    min_star: str,
    with_hint: bool = True,
) -> dict[str, Any]:
    payload = {
        "provider": "tripcom",
        "city": str(target.get("city") or "").strip(),
        "targetHotel": str(target.get("targetHotel") or "").strip(),
        "selectedDate": selected_date,
        "radiusKm": radius_km,
        "minStar": min_star,
        "sortBy": "discount",
        "asyncMode": "0",
        "backgroundMode": "0",
    }
    target_hint = target.get("targetHint")
    if with_hint and target_hint:
        payload["targetHint"] = target_hint
    return payload


def store_result_for_payloads(payload: dict[str, Any], result: dict[str, Any], provider_name: str = "tripcom") -> None:
    remember_search_result(cache_key(payload), provider_name, result)
    if payload.get("targetHint"):
        no_hint_payload = dict(payload)
        no_hint_payload.pop("targetHint", None)
        remember_search_result(cache_key(no_hint_payload), provider_name, result)


def prewarm_price_dates(selected_date: str) -> list[str]:
    dates: list[str] = []
    for date_value in [selected_date, *get_compare_date_info(selected_date)["compareDates"]]:
        date_value = str(date_value or "").strip()
        if date_value and date_value not in dates:
            dates.append(date_value)
    return dates


def ensure_price_cache_schema() -> bool:
    if not MYSQL_HOTEL_PRICE_CACHE or not MYSQL_HOTEL_PRICE_CACHE.available():
        return False
    try:
        MYSQL_HOTEL_PRICE_CACHE.ensure_schema()
        return True
    except Exception:
        return False


def ensure_candidate_cache_schema() -> bool:
    if not MYSQL_HOTEL_CANDIDATE_CACHE or not MYSQL_HOTEL_CANDIDATE_CACHE.available():
        return False
    try:
        MYSQL_HOTEL_CANDIDATE_CACHE.ensure_schema()
        return True
    except Exception:
        return False


def prewarm_one(
    target: dict[str, Any],
    selected_date: str,
    *,
    mode: str,
    radius_km: str,
    min_star: str,
    fallback_quick: bool,
) -> dict[str, Any]:
    if not target.get("targetHint"):
        target = dict(target)
        target["targetHint"] = resolve_target_hint(str(target.get("city") or ""), str(target.get("targetHotel") or ""))

    payload = build_payload(target, selected_date, radius_km=radius_km, min_star=min_star)
    started_at = time.perf_counter()
    quick = mode == "quick"
    used_mode = mode
    try:
        result = run_search_payload(payload, "tripcom", quick=quick)
    except Exception as exc:
        if not fallback_quick or quick:
            raise
        used_mode = "quick"
        result = run_search_payload(payload, "tripcom", quick=True)
        result.setdefault("summary", {})["prewarmFallbackError"] = str(exc)

    summary = result.setdefault("summary", {})
    summary["prewarmedAt"] = time.time()
    summary["prewarmMode"] = used_mode
    summary["prewarmElapsedMs"] = round((time.perf_counter() - started_at) * 1000, 1)
    summary["prewarmPriceDates"] = prewarm_price_dates(selected_date)
    store_result_for_payloads(payload, result)

    return {
        "city": payload["city"],
        "targetHotel": payload["targetHotel"],
        "selectedDate": selected_date,
        "mode": used_mode,
        "candidateCount": summary.get("candidateCount"),
        "dealCount": summary.get("dealCount"),
        "recommendedCount": summary.get("recommendedCount"),
        "elapsedMs": summary["prewarmElapsedMs"],
        "priceDateCount": len(summary["prewarmPriceDates"]),
        "hotelPriceCacheEnabled": bool(MYSQL_HOTEL_PRICE_CACHE and MYSQL_HOTEL_PRICE_CACHE.available()),
        "targetHint": bool(payload.get("targetHint")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Prewarm popular Trip.com hotel searches into MySQL/file cache.")
    parser.add_argument("--limit", type=int, default=int(os.environ.get("HOTEL_DEAL_PREWARM_LIMIT", "6")))
    parser.add_argument("--dates", default=None, help="Comma-separated YYYY-MM-DD dates. Defaults to app default_check_in().")
    parser.add_argument("--mode", choices=["full", "quick"], default=os.environ.get("HOTEL_DEAL_PREWARM_MODE", "full"))
    parser.add_argument("--radius-km", default=os.environ.get("HOTEL_DEAL_PREWARM_RADIUS_KM", "5"))
    parser.add_argument("--min-star", default=os.environ.get("HOTEL_DEAL_PREWARM_MIN_STAR", "4"))
    parser.add_argument("--sleep", type=float, default=float(os.environ.get("HOTEL_DEAL_PREWARM_SLEEP_SECONDS", "5")))
    parser.add_argument("--no-fallback-quick", action="store_true")
    args = parser.parse_args()

    targets = current_hot_targets(limit=max(1, args.limit))
    dates = parse_dates(args.dates)
    price_schema_ready = ensure_price_cache_schema()
    candidate_schema_ready = ensure_candidate_cache_schema()
    output: list[dict[str, Any]] = []
    print(
        json.dumps(
            {
                "event": "prewarm_start",
                "targetCount": len(targets),
                "dates": dates,
                "mode": args.mode,
                "mysqlEnabled": bool(MYSQL_SEARCH_CACHE and MYSQL_SEARCH_CACHE.available()),
                "hotelPriceCacheEnabled": bool(MYSQL_HOTEL_PRICE_CACHE and MYSQL_HOTEL_PRICE_CACHE.available()),
                "hotelPriceSchemaReady": price_schema_ready,
                "hotelCandidateCacheEnabled": bool(MYSQL_HOTEL_CANDIDATE_CACHE and MYSQL_HOTEL_CANDIDATE_CACHE.available()),
                "hotelCandidateSchemaReady": candidate_schema_ready,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    for target in targets:
        for selected_date in dates:
            try:
                row = prewarm_one(
                    target,
                    selected_date,
                    mode=args.mode,
                    radius_km=args.radius_km,
                    min_star=args.min_star,
                    fallback_quick=not args.no_fallback_quick,
                )
                row["ok"] = True
            except Exception as exc:
                row = {
                    "ok": False,
                    "city": target.get("city"),
                    "targetHotel": target.get("targetHotel"),
                    "selectedDate": selected_date,
                    "error": str(exc),
                }
            output.append(row)
            print(json.dumps({"event": "prewarm_item", **row}, ensure_ascii=False), flush=True)
            if args.sleep > 0:
                time.sleep(args.sleep)

    print(json.dumps({"event": "prewarm_done", "results": output}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
