from __future__ import annotations

import html
import json
import subprocess
from pathlib import Path
from textwrap import dedent


APP_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = APP_DIR / "promo_exports"
HTML_PATH = OUT_DIR / "mobile_promo_cards.html"
SLIDE_EXPORTS = [
    ("search", "00_深圳国际会展中心_0601_搜索结果截图.png"),
    ("1", "01_行程不用改_酒店可以更值.png"),
    ("2", "02_六一深圳实测_高星酒店价格洼地.png"),
    ("3", "03_不只看便宜_看真正值得.png"),
    ("4", "04_不用迁就低价日历.png"),
    ("5", "05_搜索不用空等_结果逐步刷新.png"),
    ("6", "06_从3公里扩到10公里_不放过候选.png"),
    ("7", "07_高端品牌价格洼地_一起看清.png"),
    ("8", "08_今天选的酒店_到底值不值.png"),
]


def fetch_search_result() -> dict:
    cached = newest_disk_result()
    if cached is not None:
        return cached

    payload = {
        "city": "深圳",
        "targetHotel": "深圳国际会展中心",
        "selectedDate": "2026-06-01",
        "radiusKm": "3",
        "minStar": "4",
        "sortBy": "discount",
        "provider": "tripcom",
        "asyncMode": "1",
        "backgroundMode": "1",
    }
    result = subprocess.run(
        [
            "curl",
            "--noproxy",
            "*",
            "-s",
            "-H",
            "Content-Type: application/json",
            "-d",
            json.dumps(payload, ensure_ascii=False),
            "http://127.0.0.1:5013/api/search",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)
    job_id = (data.get("summary") or {}).get("jobId")
    if (data.get("summary") or {}).get("partial") and job_id:
        status = subprocess.run(
            [
                "curl",
                "--noproxy",
                "*",
                "-s",
                f"http://127.0.0.1:5013/api/search/status/{job_id}?sortBy=discount",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        status_data = json.loads(status.stdout)
        data = status_data.get("result") or data
    return data


def newest_disk_result() -> dict | None:
    cache_dir = APP_DIR / ".cache" / "search_cache"
    best_record: dict | None = None
    best_time = 0.0
    for path in cache_dir.glob("*.json"):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        result = record.get("result") or {}
        query = result.get("query") or {}
        summary = result.get("summary") or {}
        target = str(query.get("targetHotel") or "")
        if record.get("provider") != "tripcom":
            continue
        if query.get("city") != "深圳" or query.get("selectedDate") != "2026-06-01":
            continue
        if "深圳国际会展中心" not in target:
            continue
        if not result.get("allHotels"):
            continue
        if summary.get("partial"):
            continue
        updated_at = float(record.get("updatedAt") or record.get("createdAt") or 0)
        deal_count = int(summary.get("dealCount") or len(result.get("dealHotels") or []))
        score_time = updated_at + deal_count * 1000000000
        if score_time > best_time:
            best_time = score_time
            best_record = result
    return best_record


def short_name(value: str, max_len: int = 22) -> str:
    value = str(value or "").strip()
    return value if len(value) <= max_len else f"{value[:max_len - 1]}…"


def currency(value: int | float | None) -> str:
    if value in (None, ""):
        return "-"
    return f"¥{int(round(float(value))):,}"


def percent(value: int | float | None) -> str:
    if value in (None, ""):
        return "-"
    return f"{float(value):.1f}%".replace(".0%", "%")


def public_hotel(hotel: dict) -> dict:
    return {
        "name": short_name(hotel.get("hotelName") or "", 24),
        "fullName": hotel.get("hotelName") or "",
        "brand": hotel.get("brandLabel") or hotel.get("brand") or "高星酒店",
        "star": hotel.get("starRating") or "",
        "distance": hotel.get("distanceKm"),
        "current": hotel.get("currentPrice"),
        "reference": hotel.get("referencePrice") or hotel.get("averageComparePrice"),
        "discount": hotel.get("discountAmount"),
        "discountPercent": hotel.get("discountPercent"),
        "rating": hotel.get("rating"),
        "reviews": hotel.get("reviewCount"),
        "isDeal": bool(hotel.get("isDeal")),
        "imageUrl": hotel.get("imageUrl") or "",
        "tripUrl": hotel.get("tripUrl") or "",
        "pricePending": hotel.get("currentPrice") in (None, ""),
        "nameSource": hotel.get("hotelNameSource") or hotel.get("source") or "",
    }


def build_html_legacy(data: dict) -> str:
    deals = [public_hotel(item) for item in (data.get("dealHotels") or [])[:5]]
    candidates = [public_hotel(item) for item in (data.get("allHotels") or [])[:6]]
    recommended = [public_hotel(item) for item in (data.get("recommendedHotels") or [])[:5]]
    summary = data.get("summary") or {}
    payload = {
        "deals": deals,
        "candidates": candidates,
        "recommended": recommended,
        "summary": {
            "dealCount": summary.get("dealCount") or len(deals),
            "candidateCount": summary.get("candidateCount") or len(candidates),
            "recommendedCount": summary.get("recommendedCount") or len(recommended),
            "date": "2026-06-01",
            "target": "深圳国际会展中心",
        },
    }
    payload_json = json.dumps(payload, ensure_ascii=False)
    return dedent(
        f"""
        <!doctype html>
        <html lang="zh-CN">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>星级酒店捡漏雷达 手机宣传图</title>
          <style>
            * {{ box-sizing: border-box; }}
            body {{
              margin: 0;
              width: 1080px;
              min-height: 1920px;
              overflow: hidden;
              color: #132033;
              background: #edf3f8;
              font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei", sans-serif;
              letter-spacing: 0;
            }}
            .poster {{
              position: relative;
              width: 1080px;
              height: 1920px;
              overflow: hidden;
              padding: 92px 76px 76px;
              background:
                radial-gradient(circle at 82% 10%, rgba(45, 122, 214, 0.18), transparent 310px),
                linear-gradient(180deg, #f7fbff 0%, #e8f1f7 100%);
            }}
            .poster.alt {{
              background:
                radial-gradient(circle at 18% 18%, rgba(15, 118, 110, 0.16), transparent 290px),
                linear-gradient(180deg, #fbfdfd 0%, #ecf5f3 100%);
            }}
            .kicker {{
              color: #0f766e;
              font-size: 30px;
              font-weight: 850;
              text-transform: uppercase;
              margin-bottom: 24px;
            }}
            h1 {{
              margin: 0;
              color: #111c2f;
              font-size: 82px;
              line-height: 1.06;
              font-weight: 900;
            }}
            .sub {{
              margin-top: 28px;
              color: #4f6074;
              font-size: 36px;
              line-height: 1.46;
              font-weight: 620;
            }}
            .copy {{
              margin-top: 34px;
              color: #243348;
              font-size: 38px;
              line-height: 1.58;
              font-weight: 680;
            }}
            .em {{
              color: #1c5fc7;
              font-weight: 900;
            }}
            .phone {{
              position: absolute;
              left: 76px;
              right: 76px;
              bottom: 86px;
              height: 1000px;
              border: 14px solid #172033;
              border-radius: 58px;
              background: #eef3f7;
              overflow: hidden;
              box-shadow: 0 46px 90px rgba(20, 34, 54, 0.25);
            }}
            .phone.compact {{
              height: 880px;
            }}
            .app-head {{
              padding: 34px 34px 22px;
              background: #fff;
              border-bottom: 1px solid #dbe5ef;
            }}
            .app-title {{
              font-size: 32px;
              line-height: 1.15;
              font-weight: 900;
            }}
            .app-query {{
              margin-top: 14px;
              display: flex;
              gap: 12px;
              flex-wrap: wrap;
            }}
            .pill {{
              min-height: 44px;
              display: inline-flex;
              align-items: center;
              border-radius: 999px;
              padding: 0 18px;
              background: #e9f4f1;
              color: #0f766e;
              font-size: 24px;
              font-weight: 800;
            }}
            .stats {{
              display: grid;
              grid-template-columns: repeat(3, 1fr);
              gap: 10px;
              padding: 18px 24px;
            }}
            .stat {{
              border: 1px solid #d8e2ed;
              border-radius: 18px;
              background: #fff;
              padding: 16px 12px;
              text-align: center;
            }}
            .stat b {{
              display: block;
              font-size: 40px;
              line-height: 1;
              color: #1c5fc7;
            }}
            .stat span {{
              display: block;
              margin-top: 8px;
              color: #66768a;
              font-size: 20px;
              font-weight: 760;
            }}
            .cards {{
              display: grid;
              gap: 16px;
              padding: 0 24px 24px;
            }}
            .hotel {{
              display: grid;
              gap: 14px;
              border: 1px solid #d9e4ef;
              border-radius: 22px;
              background: #fff;
              padding: 22px;
              box-shadow: 0 12px 26px rgba(25, 38, 63, 0.08);
            }}
            .hotel-top {{
              display: grid;
              grid-template-columns: minmax(0, 1fr) auto;
              gap: 16px;
              align-items: start;
            }}
            .hotel-name {{
              min-width: 0;
              font-size: 29px;
              line-height: 1.26;
              font-weight: 900;
            }}
            .badge {{
              border-radius: 999px;
              background: #fff1d8;
              color: #a85b00;
              padding: 8px 14px;
              font-size: 21px;
              font-weight: 900;
              white-space: nowrap;
            }}
            .meta {{
              color: #66768a;
              font-size: 21px;
              line-height: 1.35;
              font-weight: 650;
            }}
            .price-row {{
              display: grid;
              grid-template-columns: repeat(3, minmax(0, 1fr));
              gap: 10px;
            }}
            .price {{
              border-radius: 16px;
              background: #f8fbfd;
              border: 1px solid #e2e9f1;
              padding: 12px;
            }}
            .price span {{
              display: block;
              color: #66768a;
              font-size: 18px;
              font-weight: 760;
            }}
            .price b {{
              display: block;
              margin-top: 8px;
              color: #132033;
              font-size: 28px;
              line-height: 1;
              font-weight: 900;
            }}
            .price.good b {{ color: #0f766e; }}
            .proof {{
              position: absolute;
              left: 76px;
              right: 76px;
              top: 650px;
              display: grid;
              gap: 18px;
            }}
            .proof-card {{
              border-radius: 28px;
              background: #fff;
              border: 1px solid #d9e4ef;
              box-shadow: 0 18px 42px rgba(31,45,68,.10);
              padding: 28px;
            }}
            .proof-card .big {{
              font-size: 58px;
              color: #0f766e;
              font-weight: 950;
              line-height: 1;
            }}
            .proof-card .label {{
              margin-top: 10px;
              color: #4f6074;
              font-size: 26px;
              line-height: 1.35;
              font-weight: 720;
            }}
            .feature-list {{
              position: absolute;
              left: 76px;
              right: 76px;
              top: 620px;
              display: grid;
              gap: 22px;
            }}
            .feature {{
              display: grid;
              grid-template-columns: 64px minmax(0,1fr);
              gap: 22px;
              align-items: center;
              padding: 28px;
              border-radius: 28px;
              background: #fff;
              border: 1px solid #dbe5ef;
              box-shadow: 0 14px 34px rgba(31,45,68,.08);
            }}
            .num {{
              width: 64px;
              height: 64px;
              display: grid;
              place-items: center;
              border-radius: 50%;
              background: #1c5fc7;
              color: #fff;
              font-size: 30px;
              font-weight: 950;
            }}
            .feature b {{
              display: block;
              font-size: 34px;
              line-height: 1.2;
              font-weight: 900;
            }}
            .feature span {{
              display: block;
              margin-top: 8px;
              color: #66768a;
              font-size: 25px;
              line-height: 1.4;
              font-weight: 650;
            }}
            .timeline {{
              position: absolute;
              left: 76px;
              right: 76px;
              top: 600px;
              display: grid;
              gap: 18px;
            }}
            .timeline-item {{
              display: grid;
              grid-template-columns: 76px minmax(0, 1fr);
              gap: 20px;
              align-items: center;
              padding: 26px;
              border-radius: 28px;
              background: #fff;
              border: 1px solid #dbe5ef;
              box-shadow: 0 14px 34px rgba(31,45,68,.08);
            }}
            .timeline-dot {{
              width: 76px;
              height: 76px;
              display: grid;
              place-items: center;
              border-radius: 24px;
              background: #0f766e;
              color: #fff;
              font-size: 28px;
              font-weight: 950;
            }}
            .timeline-item b {{
              display: block;
              font-size: 34px;
              line-height: 1.2;
              font-weight: 900;
            }}
            .timeline-item span {{
              display: block;
              margin-top: 8px;
              color: #66768a;
              font-size: 25px;
              line-height: 1.4;
              font-weight: 650;
            }}
            .radius-row {{
              position: absolute;
              left: 76px;
              right: 76px;
              top: 620px;
              display: grid;
              grid-template-columns: repeat(3, 1fr);
              gap: 18px;
            }}
            .radius-card {{
              min-height: 210px;
              display: grid;
              align-content: center;
              justify-items: center;
              text-align: center;
              border-radius: 30px;
              background: #fff;
              border: 1px solid #dbe5ef;
              box-shadow: 0 16px 36px rgba(31,45,68,.09);
              padding: 24px 16px;
            }}
            .radius-card b {{
              color: #1c5fc7;
              font-size: 62px;
              line-height: 1;
              font-weight: 950;
            }}
            .radius-card span {{
              margin-top: 14px;
              color: #4f6074;
              font-size: 25px;
              line-height: 1.35;
              font-weight: 760;
            }}
            .brand-grid {{
              position: absolute;
              left: 76px;
              right: 76px;
              top: 610px;
              display: grid;
              grid-template-columns: repeat(2, minmax(0, 1fr));
              gap: 16px;
            }}
            .brand-card {{
              min-height: 178px;
              border-radius: 28px;
              background: #fff;
              border: 1px solid #dbe5ef;
              box-shadow: 0 14px 34px rgba(31,45,68,.08);
              padding: 24px;
            }}
            .brand-card b {{
              display: block;
              color: #132033;
              font-size: 29px;
              line-height: 1.25;
              font-weight: 900;
            }}
            .brand-card span {{
              display: block;
              margin-top: 12px;
              color: #66768a;
              font-size: 22px;
              line-height: 1.35;
              font-weight: 680;
            }}
            .compare-panel {{
              position: absolute;
              left: 76px;
              right: 76px;
              top: 610px;
              display: grid;
              gap: 18px;
            }}
            .compare-card {{
              display: grid;
              grid-template-columns: minmax(0, 1fr) 220px;
              gap: 20px;
              align-items: center;
              border-radius: 28px;
              background: #fff;
              border: 1px solid #dbe5ef;
              box-shadow: 0 14px 34px rgba(31,45,68,.08);
              padding: 26px;
            }}
            .compare-card b {{
              display: block;
              font-size: 31px;
              line-height: 1.24;
              font-weight: 900;
            }}
            .compare-card span {{
              display: block;
              margin-top: 9px;
              color: #66768a;
              font-size: 23px;
              line-height: 1.35;
              font-weight: 670;
            }}
            .compare-save {{
              text-align: right;
              color: #0f766e;
              font-size: 42px;
              line-height: 1;
              font-weight: 950;
            }}
            .cta-panel {{
              position: absolute;
              left: 76px;
              right: 76px;
              top: 1050px;
              border-radius: 34px;
              background: #132033;
              color: #fff;
              padding: 38px;
              box-shadow: 0 28px 60px rgba(19,32,51,.24);
            }}
            .cta-panel b {{
              display: block;
              font-size: 44px;
              line-height: 1.2;
              font-weight: 950;
            }}
            .cta-panel span {{
              display: block;
              margin-top: 18px;
              color: #d6e2ef;
              font-size: 30px;
              line-height: 1.45;
              font-weight: 680;
            }}
            .brand {{
              position: absolute;
              left: 76px;
              bottom: 48px;
              color: #66768a;
              font-size: 25px;
              font-weight: 760;
            }}
          </style>
        </head>
        <body>
          <div id="root"></div>
          <script>
            const DATA = {payload_json};
            const slide = new URLSearchParams(location.search).get("slide") || "1";
            const money = v => v === null || v === undefined || v === "" ? "-" : "¥" + Math.round(Number(v)).toLocaleString("zh-CN");
            const pct = v => v === null || v === undefined || v === "" ? "-" : Number(v).toFixed(1).replace(".0", "") + "%";
            const distance = v => v === null || v === undefined || v === "" ? "-" : Number(v || 0).toFixed(1) + "km";
            function hotelCard(h, compact=false) {{
              return `<div class="hotel">
                <div class="hotel-top">
                  <div>
                    <div class="hotel-name">${{h.name}}</div>
                    <div class="meta">${{h.star}}星级｜${{h.brand}}｜距离 ${{distance(h.distance)}}</div>
                  </div>
                  <div class="badge">省 ${{money(h.discount)}}</div>
                </div>
                <div class="price-row">
                  <div class="price"><span>目标日期</span><b>${{money(h.current)}}</b></div>
                  <div class="price"><span>参考价</span><b>${{money(h.reference)}}</b></div>
                  <div class="price good"><span>优惠</span><b>${{pct(h.discountPercent)}}</b></div>
                </div>
              </div>`;
            }}
            function brandCard(h) {{
              return `<div class="brand-card">
                <b>${{h.name || "高星酒店"}}</b>
                <span>${{h.star || "-"}}星级｜${{h.brand || "高端品牌"}}｜距离 ${{distance(h.distance)}}<br>6月1日含税价 ${{money(h.current)}}，参考 ${{money(h.reference)}}</span>
              </div>`;
            }}
            function compareCard(h) {{
              return `<div class="compare-card">
                <div>
                  <b>${{h.name || "高星酒店"}}</b>
                  <span>目标日 ${{money(h.current)}}｜参考 ${{money(h.reference)}}｜优惠 ${{pct(h.discountPercent)}}</span>
                </div>
                <div class="compare-save">省<br>${{money(h.discount)}}</div>
              </div>`;
            }}
            function phone(cards=DATA.deals.slice(0,3), compact=false) {{
              return `<div class="phone ${{compact ? "compact" : ""}}">
                <div class="app-head">
                  <div class="app-title">星级酒店捡漏雷达</div>
                  <div class="app-query"><span class="pill">深圳国际会展中心</span><span class="pill">6月1日</span><span class="pill">含税价</span></div>
                </div>
                <div class="stats">
                  <div class="stat"><b>${{DATA.summary.candidateCount}}</b><span>候选酒店</span></div>
                  <div class="stat"><b>${{DATA.summary.dealCount}}</b><span>捡漏酒店</span></div>
                  <div class="stat"><b>${{DATA.summary.recommendedCount}}</b><span>连锁推荐</span></div>
                </div>
                <div class="cards">${{cards.map(h => hotelCard(h)).join("")}}</div>
              </div>`;
            }}
            const topDeal = DATA.deals[0] || {{}};
            const secondDeal = DATA.deals[1] || DATA.deals[0] || {{}};
            const thirdDeal = DATA.deals[2] || secondDeal || {{}};
            const brandItems = (DATA.recommended.length ? DATA.recommended : DATA.candidates).slice(0,4);
            const compareItems = DATA.deals.slice(0,3);
            const htmls = {{
              "1": `<section class="poster">
                <div class="kicker">Hotel Deal Radar</div>
                <h1>行程不用改<br>酒店可以更值</h1>
                <div class="sub">不是让你换一天，而是在你已经选定的日期里，找到更值得住的高星酒店。</div>
                ${{phone(DATA.deals.slice(0,3), true)}}
                <div class="brand">星级酒店捡漏雷达｜深圳国际会展中心 6月1日实测</div>
              </section>`,
              "2": `<section class="poster alt">
                <div class="kicker">真实搜索结果</div>
                <h1>六一深圳实测<br>高星酒店出现价格洼地</h1>
                <div class="proof">
                  <div class="proof-card"><div class="big">${{money(topDeal.current)}} → 省 ${{money(topDeal.discount)}}</div><div class="label">${{topDeal.name}}｜参考价 ${{money(topDeal.reference)}}｜优惠 ${{pct(topDeal.discountPercent)}}</div></div>
                  <div class="proof-card"><div class="big">${{money(secondDeal.current)}} → 省 ${{money(secondDeal.discount)}}</div><div class="label">${{secondDeal.name}}｜参考价 ${{money(secondDeal.reference)}}｜优惠 ${{pct(secondDeal.discountPercent)}}</div></div>
                </div>
                ${{phone(DATA.deals.slice(0,2), true)}}
                <div class="brand">目标日期：2026-06-01｜价格展示为含税价</div>
              </section>`,
              "3": `<section class="poster">
                <div class="kicker">不只看便宜</div>
                <h1>哪些是真的捡漏<br>哪些只是看起来高级？</h1>
                <div class="feature-list">
                  <div class="feature"><div class="num">1</div><div><b>同类型日期比价</b><span>工作日只和工作日比，周末只和周末比。</span></div></div>
                  <div class="feature"><div class="num">2</div><div><b>含税价格判断</b><span>展示给用户的是最终价格，不被基础价误导。</span></div></div>
                  <div class="feature"><div class="num">3</div><div><b>高端连锁单独推荐</b><span>希尔顿、洲际等品牌一起看，选择更多。</span></div></div>
                </div>
                ${{phone(DATA.recommended.slice(0,3), true)}}
                <div class="brand">坚持原计划，住到最值得那一家</div>
              </section>`,
              "4": `<section class="poster alt">
                <div class="kicker">给有限时间里的旅行</div>
                <h1>不用迁就<br>低价日历</h1>
                <div class="copy">很多时候，我们不是在自由安排旅行，而是在工作的缝隙里，努力给生活留一点空间。<br><br><span class="em">你只需要坚持原本的计划，剩下的交给星级酒店捡漏雷达。</span></div>
                ${{phone(DATA.candidates.slice(0,3), true)}}
                <div class="brand">赶紧来试试吧｜星级酒店捡漏雷达</div>
              </section>`,
              "5": `<section class="poster">
                <div class="kicker">搜索体验</div>
                <h1>不用空等<br>结果逐步刷新</h1>
                <div class="sub">先展示能确定的首屏结果，后台继续深度搜索。每发现一个候选，就让用户先看到。</div>
                <div class="timeline">
                  <div class="timeline-item"><div class="timeline-dot">01</div><div><b>先返回首批星级候选</b><span>不用等全量搜索结束，页面先有结果。</span></div></div>
                  <div class="timeline-item"><div class="timeline-dot">02</div><div><b>继续补齐价格对比</b><span>含税价、参考价、优惠金额逐步刷新。</span></div></div>
                  <div class="timeline-item"><div class="timeline-dot">03</div><div><b>缓存已搜到的酒店</b><span>下次同条件搜索，可以沿着缓存继续找。</span></div></div>
                  <div class="timeline-item"><div class="timeline-dot">04</div><div><b>没结果也告诉进程</b><span>用户看到的是进度，不是空白等待。</span></div></div>
                </div>
                <div class="brand">更快看到结果，不牺牲搜索质量</div>
              </section>`,
              "6": `<section class="poster alt">
                <div class="kicker">不放过可能候选</div>
                <h1>3公里没有<br>继续扩到10公里</h1>
                <div class="sub">附近找不到捡漏酒店时，自动扩展到 5 公里、10 公里。能先展示的先展示，后台继续补齐。</div>
                <div class="radius-row">
                  <div class="radius-card"><b>3km</b><span>优先找近处高星酒店</span></div>
                  <div class="radius-card"><b>5km</b><span>无捡漏时自动扩展</span></div>
                  <div class="radius-card"><b>10km</b><span>再找不到就明确告知</span></div>
                </div>
                ${{phone(DATA.candidates.slice(0,3), true)}}
                <div class="brand">深圳国际会展中心 6月1日实测候选：${{DATA.summary.candidateCount}} 家</div>
              </section>`,
              "7": `<section class="poster">
                <div class="kicker">高端品牌推荐</div>
                <h1>别只看低价<br>也看品牌和品质</h1>
                <div class="sub">希尔顿、洲际、皇冠假日等高星品牌单独呈现，价格不限死，让用户保留更多好选择。</div>
                <div class="brand-grid">${{brandItems.map(h => brandCard(h)).join("")}}</div>
                <div class="cta-panel"><b>适合带家人出行，也适合差旅升级</b><span>不是在几十家酒店里盲刷，而是把真正值得考虑的高星酒店先挑出来。</span></div>
                <div class="brand">高端连锁推荐不按价格上限隐藏</div>
              </section>`,
              "8": `<section class="poster alt">
                <div class="kicker">值不值，一眼看清</div>
                <h1>今天选的酒店<br>到底划不划算？</h1>
                <div class="sub">同类型日期对比，含税价展示。低于平均价 100 元，或低于任一对比日 100 元，就进入捡漏判断。</div>
                <div class="compare-panel">${{compareItems.map(h => compareCard(h)).join("")}}</div>
                <div class="brand">目标日期：2026-06-01｜深圳国际会展中心｜真实搜索结果</div>
              </section>`,
              "search": `<section class="poster">
                ${{phone(DATA.deals.slice(0,4), false)}}
              </section>`
            }};
            document.getElementById("root").innerHTML = htmls[slide] || htmls["1"];
          </script>
        </body>
        </html>
        """
    )


def build_html(data: dict) -> str:
    def unique_public_hotels(items: list[dict]) -> list[dict]:
        seen: set[str] = set()
        result: list[dict] = []
        for item in items:
            key = str(item.get("hotelId") or item.get("hotelName") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            result.append(public_hotel(item))
        return result

    deal_source = data.get("dealHotels") or []
    candidate_source = data.get("allHotels") or []
    recommended_source = data.get("recommendedHotels") or []
    deals = unique_public_hotels(deal_source)[:6]
    candidates = unique_public_hotels(candidate_source)[:10]
    recommended = unique_public_hotels(recommended_source)[:8]
    hero_hotels = unique_public_hotels([*deal_source, *recommended_source, *candidate_source])[:10]
    summary = data.get("summary") or {}
    query = data.get("query") or {}
    payload = {
        "deals": deals,
        "candidates": candidates,
        "recommended": recommended,
        "heroHotels": hero_hotels,
        "compareDates": data.get("compareDates") or [],
        "summary": {
            "dealCount": summary.get("dealCount") or len(deals),
            "candidateCount": summary.get("candidateCount") or len(candidates),
            "recommendedCount": summary.get("recommendedCount") or len(recommended),
            "date": query.get("selectedDate") or "2026-06-01",
            "city": query.get("city") or "深圳",
            "target": query.get("targetHotel") or "深圳国际会展中心",
        },
    }
    payload_json = json.dumps(payload, ensure_ascii=False)
    template = r"""
        <!doctype html>
        <html lang="zh-CN">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>星级酒店捡漏雷达 手机宣传图</title>
          <style>
            * { box-sizing: border-box; }
            html, body {
              margin: 0;
              width: 1080px;
              height: 1920px;
              overflow: hidden;
              font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei", sans-serif;
              color: #172034;
              letter-spacing: 0;
            }
            body {
              background: #f4f6f2;
            }
            .poster {
              position: relative;
              width: 1080px;
              height: 1920px;
              overflow: hidden;
              padding: 78px 72px;
              background:
                radial-gradient(circle at 10% 0%, rgba(255, 213, 122, 0.34), transparent 28%),
                linear-gradient(145deg, #fbf7ec 0%, #e7f0ee 46%, #eef4fb 100%);
            }
            .poster.dark {
              color: #fff;
              background: #101927;
            }
            .poster.photo-led {
              color: #fff;
              background: #101927;
            }
            .bg-photo {
              position: absolute;
              inset: 0;
              background: #1c2430;
            }
            .bg-photo img,
            .photo img,
            .tile-photo img,
            .hero-card img {
              width: 100%;
              height: 100%;
              object-fit: cover;
              display: block;
            }
            .bg-photo::after {
              content: "";
              position: absolute;
              inset: 0;
              background:
                linear-gradient(180deg, rgba(10, 18, 32, 0.66) 0%, rgba(10, 18, 32, 0.24) 42%, rgba(10, 18, 32, 0.9) 100%),
                linear-gradient(90deg, rgba(10, 18, 32, 0.58), transparent 66%);
            }
            .kicker {
              position: relative;
              display: inline-flex;
              align-items: center;
              gap: 12px;
              min-height: 42px;
              padding: 0 18px;
              border-radius: 999px;
              background: rgba(255, 255, 255, 0.72);
              border: 1px solid rgba(23, 32, 52, 0.1);
              color: #2c5f5f;
              font-size: 25px;
              font-weight: 700;
              backdrop-filter: blur(16px);
            }
            .dark .kicker,
            .photo-led .kicker {
              color: #ffe2a1;
              background: rgba(255, 255, 255, 0.16);
              border-color: rgba(255, 255, 255, 0.24);
            }
            .kicker::before {
              content: "";
              width: 10px;
              height: 10px;
              border-radius: 50%;
              background: #e8923b;
            }
            h1 {
              position: relative;
              margin: 28px 0 0;
              max-width: 890px;
              font-size: 82px;
              line-height: 1.08;
              font-weight: 900;
              letter-spacing: 0;
            }
            h1 .accent {
              color: #0c8f79;
            }
            .dark h1 .accent,
            .photo-led h1 .accent {
              color: #ffcf6a;
            }
            .sub {
              position: relative;
              margin-top: 26px;
              max-width: 820px;
              color: rgba(23, 32, 52, 0.72);
              font-size: 33px;
              line-height: 1.45;
              font-weight: 520;
            }
            .dark .sub,
            .photo-led .sub {
              color: rgba(255, 255, 255, 0.82);
            }
            .brand-foot {
              position: absolute;
              left: 72px;
              right: 72px;
              bottom: 58px;
              display: flex;
              align-items: center;
              justify-content: space-between;
              gap: 28px;
              color: rgba(23, 32, 52, 0.66);
              font-size: 24px;
              font-weight: 650;
              z-index: 5;
            }
            .dark .brand-foot,
            .photo-led .brand-foot {
              color: rgba(255, 255, 255, 0.74);
            }
            .logo-pill {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              height: 50px;
              padding: 0 20px;
              border-radius: 999px;
              background: #172034;
              color: #fff;
              font-size: 24px;
              font-weight: 800;
            }
            .dark .logo-pill,
            .photo-led .logo-pill {
              background: rgba(255, 255, 255, 0.18);
              border: 1px solid rgba(255, 255, 255, 0.2);
            }
            .phone {
              position: relative;
              width: 640px;
              height: 1290px;
              border-radius: 64px;
              padding: 16px;
              background: #111827;
              box-shadow: 0 38px 90px rgba(16, 24, 39, 0.32);
              z-index: 3;
            }
            .phone.large {
              width: 706px;
              height: 1378px;
            }
            .phone.compact-phone {
              width: 560px;
              height: 1120px;
              border-radius: 56px;
            }
            .screen {
              height: 100%;
              overflow: hidden;
              border-radius: 50px;
              background: #f7f8f4;
              color: #172034;
              padding: 30px 26px 28px;
              position: relative;
            }
            .phone.large .screen {
              border-radius: 54px;
              padding: 34px 30px 30px;
            }
            .status {
              display: flex;
              align-items: center;
              justify-content: space-between;
              height: 26px;
              color: rgba(23, 32, 52, 0.72);
              font-size: 18px;
              font-weight: 800;
            }
            .status i {
              display: inline-block;
              width: 68px;
              height: 18px;
              border-radius: 99px;
              background: #111827;
            }
            .app-title {
              display: flex;
              align-items: flex-end;
              justify-content: space-between;
              gap: 18px;
              margin-top: 28px;
            }
            .app-title b {
              font-size: 33px;
              line-height: 1.12;
              font-weight: 900;
            }
            .app-title span {
              color: #0c8f79;
              font-size: 20px;
              font-weight: 800;
              white-space: nowrap;
            }
            .search-card {
              margin-top: 24px;
              padding: 20px;
              border-radius: 28px;
              background: #fff;
              border: 1px solid rgba(23, 32, 52, 0.08);
              box-shadow: 0 18px 40px rgba(43, 57, 77, 0.08);
            }
            .field-row {
              display: grid;
              grid-template-columns: 1fr 1fr;
              gap: 14px;
            }
            .field {
              min-height: 72px;
              padding: 14px 16px;
              border-radius: 18px;
              background: #eef4f0;
            }
            .field strong {
              display: block;
              color: rgba(23, 32, 52, 0.52);
              font-size: 16px;
              font-weight: 800;
            }
            .field span {
              display: block;
              margin-top: 5px;
              font-size: 22px;
              font-weight: 900;
              white-space: nowrap;
              overflow: hidden;
              text-overflow: ellipsis;
            }
            .search-button {
              display: flex;
              align-items: center;
              justify-content: center;
              height: 64px;
              margin-top: 14px;
              border-radius: 20px;
              background: #0c8f79;
              color: #fff;
              font-size: 23px;
              font-weight: 900;
            }
            .progress {
              margin-top: 22px;
              display: grid;
              gap: 10px;
            }
            .progress-top {
              display: flex;
              justify-content: space-between;
              color: rgba(23, 32, 52, 0.62);
              font-size: 18px;
              font-weight: 800;
            }
            .bar {
              height: 12px;
              border-radius: 999px;
              overflow: hidden;
              background: rgba(12, 143, 121, 0.12);
            }
            .bar span {
              display: block;
              height: 100%;
              border-radius: inherit;
              background: linear-gradient(90deg, #0c8f79, #efb14f);
            }
            .tabs {
              display: grid;
              grid-template-columns: repeat(3, 1fr);
              gap: 10px;
              margin-top: 20px;
            }
            .tab {
              display: flex;
              align-items: center;
              justify-content: center;
              min-height: 54px;
              border-radius: 17px;
              background: #e9efeb;
              color: rgba(23, 32, 52, 0.68);
              font-size: 17px;
              font-weight: 900;
              text-align: center;
            }
            .tab.active {
              background: #172034;
              color: #fff;
            }
            .feed {
              display: grid;
              gap: 16px;
              margin-top: 20px;
            }
            .hotel-card {
              display: grid;
              grid-template-columns: 138px 1fr;
              gap: 16px;
              min-height: 158px;
              padding: 12px;
              border-radius: 24px;
              background: #fff;
              box-shadow: 0 16px 34px rgba(43, 57, 77, 0.09);
              border: 1px solid rgba(23, 32, 52, 0.06);
              position: relative;
              overflow: hidden;
            }
            .phone.large .hotel-card {
              grid-template-columns: 156px 1fr;
              min-height: 176px;
            }
            .photo {
              position: relative;
              min-height: 134px;
              border-radius: 18px;
              overflow: hidden;
              background: linear-gradient(135deg, #dbe9e4, #f3d29c);
            }
            .photo.is-missing::after {
              content: "HOTEL";
              position: absolute;
              inset: 0;
              display: flex;
              align-items: center;
              justify-content: center;
              color: rgba(23, 32, 52, 0.5);
              font-size: 18px;
              font-weight: 900;
            }
            .hotel-info {
              min-width: 0;
              padding: 4px 4px 2px 0;
            }
            .hotel-name {
              font-size: 22px;
              line-height: 1.22;
              font-weight: 900;
              display: -webkit-box;
              -webkit-line-clamp: 2;
              -webkit-box-orient: vertical;
              overflow: hidden;
            }
            .phone.large .hotel-name {
              font-size: 24px;
            }
            .meta {
              display: flex;
              flex-wrap: wrap;
              gap: 8px;
              margin-top: 10px;
            }
            .meta span {
              height: 26px;
              padding: 0 9px;
              border-radius: 999px;
              background: #edf3ef;
              color: rgba(23, 32, 52, 0.62);
              font-size: 15px;
              line-height: 26px;
              font-weight: 800;
            }
            .price-row {
              display: flex;
              align-items: flex-end;
              justify-content: space-between;
              gap: 10px;
              margin-top: 12px;
            }
            .price {
              color: #d85d3f;
              font-size: 30px;
              line-height: 1;
              font-weight: 950;
            }
            .save {
              display: inline-flex;
              align-items: center;
              min-height: 36px;
              padding: 0 12px;
              border-radius: 12px;
              background: #fff2d9;
              color: #b45b16;
              font-size: 17px;
              font-weight: 950;
              white-space: nowrap;
            }
            .save.pending {
              color: #0c6c74;
              background: #e2f4f0;
            }
            .save.muted {
              color: rgba(23, 32, 52, 0.56);
              background: #eef1f4;
            }
            .reference {
              margin-top: 8px;
              color: rgba(23, 32, 52, 0.55);
              font-size: 15px;
              font-weight: 780;
            }
            .search-stage {
              position: absolute;
              left: 72px;
              right: 72px;
              top: 74px;
              z-index: 5;
            }
            .search-stage .sub {
              max-width: 880px;
            }
            .search-phone-wrap {
              position: absolute;
              left: 202px;
              top: 500px;
              z-index: 4;
            }
            .hero-copy {
              position: relative;
              z-index: 4;
              max-width: 780px;
            }
            .hero-phone-wrap {
              position: absolute;
              right: 54px;
              bottom: 122px;
              transform: rotate(4deg);
              z-index: 5;
            }
            .hero-deal {
              position: absolute;
              left: 72px;
              bottom: 220px;
              width: 506px;
              padding: 26px;
              border-radius: 34px;
              background: rgba(255, 255, 255, 0.16);
              border: 1px solid rgba(255, 255, 255, 0.24);
              backdrop-filter: blur(18px);
              z-index: 6;
            }
            .hero-deal strong {
              display: block;
              font-size: 34px;
              line-height: 1.2;
              font-weight: 900;
            }
            .hero-deal .deal-price {
              margin-top: 20px;
              display: flex;
              align-items: baseline;
              gap: 18px;
            }
            .hero-deal .deal-price b {
              color: #ffcf6a;
              font-size: 58px;
              line-height: 1;
            }
            .hero-deal .deal-price span {
              color: rgba(255, 255, 255, 0.8);
              font-size: 23px;
              font-weight: 800;
            }
            .deal-grid {
              position: relative;
              z-index: 3;
              display: grid;
              grid-template-columns: 1fr;
              gap: 22px;
              margin-top: 46px;
            }
            .deal-tile {
              display: grid;
              grid-template-columns: 310px 1fr;
              gap: 24px;
              min-height: 245px;
              padding: 18px;
              border-radius: 34px;
              background: rgba(255, 255, 255, 0.9);
              box-shadow: 0 26px 64px rgba(28, 44, 68, 0.13);
            }
            .tile-photo {
              overflow: hidden;
              border-radius: 25px;
              background: #dde7e3;
            }
            .deal-tile h2 {
              margin: 6px 0 0;
              font-size: 32px;
              line-height: 1.18;
              display: -webkit-box;
              -webkit-line-clamp: 2;
              -webkit-box-orient: vertical;
              overflow: hidden;
            }
            .deal-tile .big-save {
              margin-top: 18px;
              color: #d85d3f;
              font-size: 54px;
              font-weight: 950;
              line-height: 1;
            }
            .deal-tile p {
              margin: 14px 0 0;
              color: rgba(23, 32, 52, 0.62);
              font-size: 22px;
              line-height: 1.35;
              font-weight: 650;
            }
            .split-stage {
              position: relative;
              z-index: 2;
              display: grid;
              grid-template-columns: 560px 1fr;
              gap: 24px;
              margin-top: 46px;
              align-items: start;
            }
            .value-stack {
              display: grid;
              gap: 18px;
              padding-top: 48px;
            }
            .value-card {
              padding: 20px;
              border-radius: 28px;
              background: rgba(255, 255, 255, 0.78);
              border: 1px solid rgba(23, 32, 52, 0.08);
              box-shadow: 0 22px 48px rgba(28, 44, 68, 0.1);
            }
            .value-card b {
              display: block;
              font-size: 25px;
              line-height: 1.2;
            }
            .value-card span {
              display: block;
              margin-top: 10px;
              color: rgba(23, 32, 52, 0.64);
              font-size: 18px;
              line-height: 1.42;
              font-weight: 650;
            }
            .date-panel {
              position: relative;
              z-index: 3;
              display: grid;
              grid-template-columns: 1.05fr 0.95fr;
              gap: 26px;
              margin-top: 48px;
            }
            .date-card {
              min-height: 420px;
              padding: 30px;
              border-radius: 34px;
              background: rgba(255, 255, 255, 0.9);
              box-shadow: 0 26px 64px rgba(28, 44, 68, 0.12);
            }
            .date-card .label {
              color: #0c8f79;
              font-size: 22px;
              font-weight: 900;
            }
            .date-card .date {
              margin-top: 22px;
              font-size: 60px;
              line-height: 1;
              font-weight: 950;
            }
            .date-card .copy {
              margin-top: 26px;
              color: rgba(23, 32, 52, 0.68);
              font-size: 28px;
              line-height: 1.42;
              font-weight: 650;
            }
            .compare-list {
              display: grid;
              gap: 15px;
            }
            .compare-date {
              display: flex;
              justify-content: space-between;
              align-items: center;
              min-height: 72px;
              padding: 0 22px;
              border-radius: 20px;
              background: #f1f5ee;
              font-size: 24px;
              font-weight: 850;
            }
            .picked-result {
              position: relative;
              z-index: 4;
              display: grid;
              grid-template-columns: 360px minmax(0, 1fr);
              gap: 24px;
              align-items: stretch;
              margin-top: 34px;
              padding: 18px;
              border-radius: 36px;
              background: rgba(255, 255, 255, 0.92);
              box-shadow: 0 28px 70px rgba(28, 44, 68, 0.14);
              border: 1px solid rgba(23, 32, 52, 0.08);
            }
            .picked-result .tile-photo {
              min-height: 300px;
              border-radius: 26px;
            }
            .picked-body {
              min-width: 0;
              padding: 20px 16px 18px 0;
            }
            .picked-body .label {
              color: #0c8f79;
              font-size: 22px;
              font-weight: 900;
            }
            .picked-body h2 {
              margin: 14px 0 0;
              font-size: 34px;
              line-height: 1.18;
              display: -webkit-box;
              -webkit-line-clamp: 2;
              -webkit-box-orient: vertical;
              overflow: hidden;
            }
            .picked-price {
              display: flex;
              align-items: baseline;
              gap: 18px;
              margin-top: 26px;
            }
            .picked-price b {
              color: #d85d3f;
              font-size: 58px;
              line-height: 1;
              font-weight: 950;
            }
            .picked-price span {
              color: rgba(23, 32, 52, 0.58);
              font-size: 23px;
              font-weight: 800;
            }
            .picked-body p {
              margin: 18px 0 0;
              color: rgba(23, 32, 52, 0.62);
              font-size: 23px;
              line-height: 1.38;
              font-weight: 650;
            }
            .timeline {
              position: relative;
              z-index: 3;
              display: grid;
              gap: 18px;
              margin-top: 50px;
              width: 440px;
            }
            .step {
              display: grid;
              grid-template-columns: 64px 1fr;
              gap: 18px;
              align-items: start;
              padding: 22px;
              border-radius: 28px;
              background: rgba(255, 255, 255, 0.88);
              box-shadow: 0 20px 48px rgba(28, 44, 68, 0.1);
            }
            .step i {
              width: 64px;
              height: 64px;
              border-radius: 22px;
              display: flex;
              align-items: center;
              justify-content: center;
              background: #0c8f79;
              color: #fff;
              font-size: 26px;
              font-style: normal;
              font-weight: 950;
            }
            .step b {
              display: block;
              font-size: 28px;
              line-height: 1.18;
            }
            .step span {
              display: block;
              margin-top: 8px;
              color: rgba(23, 32, 52, 0.62);
              font-size: 20px;
              line-height: 1.36;
              font-weight: 650;
            }
            .progress-phone {
              position: absolute;
              right: 34px;
              top: 680px;
              transform: rotate(-3deg);
            }
            .progress-phone .phone {
              width: 500px;
              height: 1000px;
            }
            .progress-phone .screen {
              border-radius: 42px;
              padding: 24px 20px;
            }
            .progress-phone .hotel-card {
              grid-template-columns: 112px 1fr;
              min-height: 136px;
            }
            .progress-phone .photo {
              min-height: 112px;
              border-radius: 16px;
            }
            .mosaic {
              position: relative;
              z-index: 3;
              display: grid;
              grid-template-columns: repeat(2, 1fr);
              gap: 20px;
              margin-top: 44px;
            }
            .mosaic-card {
              min-height: 300px;
              overflow: hidden;
              border-radius: 32px;
              background: #fff;
              box-shadow: 0 22px 52px rgba(28, 44, 68, 0.12);
            }
            .mosaic-card .tile-photo {
              height: 178px;
              border-radius: 0;
            }
            .mosaic-body {
              padding: 18px;
            }
            .mosaic-body b {
              display: -webkit-box;
              -webkit-line-clamp: 2;
              -webkit-box-orient: vertical;
              overflow: hidden;
              font-size: 23px;
              line-height: 1.22;
            }
            .mosaic-body span {
              display: block;
              margin-top: 12px;
              color: #d85d3f;
              font-size: 28px;
              font-weight: 950;
            }
            .brand-grid {
              position: relative;
              z-index: 3;
              display: grid;
              grid-template-columns: repeat(2, 1fr);
              gap: 20px;
              margin-top: 46px;
            }
            .brand-card {
              min-height: 370px;
              overflow: hidden;
              border-radius: 34px;
              background: rgba(255, 255, 255, 0.12);
              border: 1px solid rgba(255, 255, 255, 0.18);
              box-shadow: 0 24px 60px rgba(0, 0, 0, 0.18);
            }
            .brand-card .tile-photo {
              height: 230px;
              border-radius: 0;
            }
            .brand-body {
              padding: 22px;
            }
            .brand-body b {
              display: -webkit-box;
              -webkit-line-clamp: 2;
              -webkit-box-orient: vertical;
              overflow: hidden;
              color: #fff;
              font-size: 25px;
              line-height: 1.22;
            }
            .brand-body span {
              display: block;
              margin-top: 12px;
              color: #ffcf6a;
              font-size: 22px;
              font-weight: 950;
            }
            .final-layout {
              position: relative;
              z-index: 4;
              display: grid;
              grid-template-columns: 1fr 500px;
              gap: 28px;
              margin-top: 52px;
              align-items: end;
            }
            .final-copy {
              align-self: start;
              padding-top: 60px;
            }
            .final-copy .line {
              margin-bottom: 20px;
              padding: 24px;
              border-radius: 28px;
              background: rgba(255, 255, 255, 0.14);
              border: 1px solid rgba(255, 255, 255, 0.18);
              color: rgba(255, 255, 255, 0.86);
              font-size: 24px;
              line-height: 1.35;
              font-weight: 750;
            }
            .photo-strip {
              position: absolute;
              left: 0;
              right: 0;
              bottom: 0;
              height: 470px;
              display: grid;
              grid-template-columns: 1fr 1.15fr 1fr;
              opacity: 0.92;
            }
            .photo-strip .tile-photo {
              border-radius: 0;
            }
            .photo-strip::after {
              content: "";
              position: absolute;
              inset: 0;
              background: linear-gradient(180deg, rgba(16, 25, 39, 0), #101927 92%);
            }
            .corner-note {
              position: absolute;
              right: 72px;
              top: 80px;
              z-index: 6;
              max-width: 330px;
              padding: 20px 24px;
              border-radius: 26px;
              background: rgba(255, 255, 255, 0.16);
              color: rgba(255, 255, 255, 0.88);
              border: 1px solid rgba(255, 255, 255, 0.22);
              font-size: 24px;
              line-height: 1.35;
              font-weight: 760;
              backdrop-filter: blur(16px);
            }
            .slide-search h1 {
              font-size: 72px;
              max-width: 820px;
            }
            .slide-search .sub {
              max-width: 900px;
              line-height: 1.56;
            }
            .slide-search .brand-foot {
              bottom: 38px;
            }
            .slide-search .phone.large {
              width: 676px;
              height: 1264px;
            }
            .slide-progress h1 {
              max-width: 720px;
            }
            .slide-progress .sub {
              max-width: 650px;
            }
            .slide-final h1 {
              font-size: 76px;
              max-width: 890px;
            }
            .slide-final .sub {
              max-width: 760px;
            }
            .slide-final .phone {
              width: 500px;
              height: 1040px;
            }
            .slide-final .screen {
              border-radius: 42px;
              padding: 24px 20px;
            }
            .slide-final .hotel-card {
              grid-template-columns: 112px 1fr;
              min-height: 136px;
            }
            .slide-final .photo {
              min-height: 112px;
              border-radius: 16px;
            }
            .slide-brand h1 {
              font-size: 72px;
            }
          </style>
        </head>
        <body>
          <div id="root"></div>
          <script>
            const DATA = __PAYLOAD__;
            const slide = new URLSearchParams(location.search).get("slide") || "1";
            const hero = DATA.heroHotels[0] || DATA.deals[0] || DATA.candidates[0] || {};
            const hero2 = DATA.heroHotels[1] || hero;
            const hero3 = DATA.heroHotels[2] || hero;
            const allVisible = [...DATA.deals, ...DATA.candidates, ...DATA.recommended]
              .filter((item, index, arr) => arr.findIndex(next => next.fullName === item.fullName) === index);
            const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({
              "&": "&amp;",
              "<": "&lt;",
              ">": "&gt;",
              '"': "&quot;",
              "'": "&#39;"
            }[char]));
            const money = value => {
              if (value === null || value === undefined || value === "") return "待补价";
              const number = Number(value);
              if (!Number.isFinite(number)) return "待补价";
              return `¥${Math.round(number).toLocaleString("zh-CN")}`;
            };
            const percent = value => {
              const number = Number(value);
              if (!Number.isFinite(number)) return "";
              return `${number.toFixed(number % 1 === 0 ? 0 : 1)}%`;
            };
            const distance = hotel => {
              const number = Number(hotel.distance);
              return Number.isFinite(number) ? `${number.toFixed(1)}km` : "附近";
            };
            const star = hotel => {
              const number = Number(hotel.star);
              return Number.isFinite(number) ? `${number.toFixed(number % 1 === 0 ? 0 : 1)}星` : "高星";
            };
            const photo = (hotel, className = "photo") => {
              if (!hotel || !hotel.imageUrl) {
                return `<div class="${className} is-missing"></div>`;
              }
              return `<div class="${className}"><img src="${esc(hotel.imageUrl)}" alt="" loading="eager" onerror="this.parentElement.classList.add('is-missing');this.remove()" /></div>`;
            };
            const saveBadge = hotel => {
              const save = Number(hotel.discount);
              if (hotel.pricePending) return `<span class="save pending">待补价</span>`;
              if (hotel.isDeal && Number.isFinite(save) && save > 0) return `<span class="save">省${money(save)}</span>`;
              return `<span class="save muted">候选</span>`;
            };
            const meta = hotel => `
              <div class="meta">
                <span>${esc(star(hotel))}</span>
                <span>${esc(hotel.brand || "高星酒店")}</span>
                <span>${esc(distance(hotel))}</span>
              </div>`;
            const hotelCard = hotel => `
              <article class="hotel-card">
                ${photo(hotel)}
                <div class="hotel-info">
                  <div class="hotel-name">${esc(hotel.name || hotel.fullName || "星级酒店")}</div>
                  ${meta(hotel)}
                  <div class="price-row">
                    <div class="price">${money(hotel.current)}</div>
                    ${saveBadge(hotel)}
                  </div>
                  <div class="reference">参考价 ${money(hotel.reference)} · 含税价展示</div>
                </div>
              </article>`;
            const phone = (hotels, options = {}) => {
              const items = (hotels && hotels.length ? hotels : allVisible).slice(0, options.limit || 4);
              const mode = options.mode || "deal";
              const title = mode === "candidate" ? "附近星级候选" : mode === "brand" ? "高端品牌推荐" : "适合捡漏";
              const tabLabel = mode === "candidate" ? "星级候选" : mode === "brand" ? "知名连锁" : "捡漏酒店";
              return `
                <div class="phone ${options.large ? "large" : ""} ${options.compact ? "compact-phone" : ""}">
                  <div class="screen">
                    <div class="status"><span>9:41</span><i></i><span>100%</span></div>
                    <div class="app-title">
                      <b>星级酒店<br>捡漏雷达</b>
                      <span>实时搜索</span>
                    </div>
                    <div class="search-card">
                      <div class="field-row">
                        <div class="field"><strong>目标城市</strong><span>${esc(DATA.summary.city)}</span></div>
                        <div class="field"><strong>入住日期</strong><span>6月1日</span></div>
                      </div>
                      <div class="field" style="margin-top:14px"><strong>目标酒店/地区</strong><span>${esc(DATA.summary.target)}</span></div>
                      <div class="search-button">开始捡漏</div>
                    </div>
                    <div class="progress">
                      <div class="progress-top"><span>${esc(options.progress || "结果已出现，继续补充中")}</span><span>${esc(options.progressValue || "76%")}</span></div>
                      <div class="bar"><span style="width:${esc(options.bar || "76%")}"></span></div>
                    </div>
                    <div class="tabs">
                      <div class="tab ${mode === "deal" ? "active" : ""}">${tabLabel}</div>
                      <div class="tab ${mode === "candidate" ? "active" : ""}">星级候选</div>
                      <div class="tab ${mode === "brand" ? "active" : ""}">高端连锁</div>
                    </div>
                    <div class="feed">${items.map(hotelCard).join("")}</div>
                  </div>
                </div>`;
            };
            const dealTile = hotel => `
              <article class="deal-tile">
                ${photo(hotel, "tile-photo")}
                <div>
                  <h2>${esc(hotel.fullName || hotel.name)}</h2>
                  ${meta(hotel)}
                  <div class="big-save">省${money(hotel.discount).replace("待补价", "¥0")}</div>
                  <p>目标日 ${money(hotel.current)}，参考价 ${money(hotel.reference)}，按同类型日期做价格对比。</p>
                </div>
              </article>`;
            const mosaicCard = hotel => `
              <article class="mosaic-card">
                ${photo(hotel, "tile-photo")}
                <div class="mosaic-body">
                  <b>${esc(hotel.name || hotel.fullName)}</b>
                  <span>${money(hotel.current)}</span>
                </div>
              </article>`;
            const brandCard = hotel => `
              <article class="brand-card">
                ${photo(hotel, "tile-photo")}
                <div class="brand-body">
                  <b>${esc(hotel.name || hotel.fullName)}</b>
                  <span>${esc(hotel.brand || "高端品牌")} · ${money(hotel.current)}</span>
                </div>
              </article>`;
            const pickedResult = hotel => `
              <article class="picked-result">
                ${photo(hotel, "tile-photo")}
                <div class="picked-body">
                  <div class="label">按你选定的日期判断</div>
                  <h2>${esc(hotel.fullName || hotel.name || "附近星级酒店")}</h2>
                  ${meta(hotel)}
                  <div class="picked-price"><b>${money(hotel.current)}</b><span>6月1日含税价</span></div>
                  <p>不用为了低价改行程；在原本日期里，把附近值得比较的星级酒店放到你面前。</p>
                </div>
              </article>`;
            const backgroundPhoto = hotel => hotel && hotel.imageUrl
              ? `<div class="bg-photo"><img src="${esc(hotel.imageUrl)}" alt="" loading="eager" /></div>`
              : `<div class="bg-photo"></div>`;
            const foot = text => `<div class="brand-foot"><span class="logo-pill">星级酒店捡漏雷达</span><span>${text}</span></div>`;
            const compareDates = DATA.compareDates.length ? DATA.compareDates : ["2026-06-01", "2026-06-02", "2026-06-08", "2026-06-09"];
            const slides = {
              "search": `
                <section class="poster slide-search">
                  <div class="search-stage">
                    <div class="kicker">实际搜索效果</div>
                    <h1>深圳国际会展中心<br><span class="accent">6月1日实测</span></h1>
                    <div class="sub">真实酒店图片、含税价、优惠金额和距离一起展示。<br>已找到的结果先出现，更多信息继续补充。</div>
                  </div>
                  <div class="search-phone-wrap">${phone(DATA.deals.slice(0, 4), { large: true, limit: 4, progress: "结果已出现，继续补充中", progressValue: "实时" })}</div>
                  ${foot("手机端 1080 × 1920 宣传图")}
                </section>`,
              "1": `
                <section class="poster photo-led">
                  ${backgroundPhoto(hero)}
                  <div class="hero-copy">
                    <div class="kicker">行程不用改</div>
                    <h1>按原计划出发<br>也能住到<span class="accent">更值</span>的高星酒店</h1>
                    <div class="sub">不用迁就低价日历。输入目标城市、目标酒店或地区，系统把附近价格洼地先找出来。</div>
                  </div>
                  <div class="hero-deal">
                    <strong>${esc(hero.name || "深圳国际会展中心洲际酒店")}</strong>
                    <div class="deal-price"><b>${money(hero.current)}</b><span>含税价</span></div>
                    <div class="sub" style="font-size:24px;margin-top:18px">比同类型日期参考价低 ${money(hero.discount)}</div>
                  </div>
                  <div class="hero-phone-wrap">${phone(DATA.deals.slice(0, 3), { compact: true, limit: 3, progress: "实时比价中", progressValue: "同步" })}</div>
                  ${foot("真实搜索数据来自深圳国际会展中心 2026-06-01")}
                </section>`,
              "2": `
                <section class="poster">
                  <div class="kicker">六一深圳实测</div>
                  <h1>不是便宜一点<br>是高星酒店出现<br><span class="accent">价格洼地</span></h1>
                  <div class="sub">同类型日期对比：低于平均价 100 元，或低于任一对比日 100 元，都会进入捡漏判断。</div>
                  <div class="deal-grid">${DATA.deals.slice(0, 3).map(dealTile).join("")}</div>
                  ${foot("含税价展示 · 自动计算省了多少")}
                </section>`,
              "3": `
                <section class="poster">
                  <div class="kicker">实际显示效果</div>
                  <h1>打开手机<br>直接看到能选的酒店</h1>
                  <div class="sub">每张卡片都带酒店图片、星级、品牌、距离、当前含税价和参考价，不用在几十个列表里盲刷。</div>
                  <div class="split-stage">
                    ${phone(DATA.deals.slice(0, 3), { compact: true, limit: 3, progress: "捡漏结果先展示", progressValue: "已找到" })}
                    <div class="value-stack">
                      <div class="value-card"><b>先看结果</b><span>已找到的酒店先出现，后续更多候选继续补充。</span></div>
                      <div class="value-card"><b>再看差价</b><span>目标日期含税价和参考价直接对比，优惠力度一眼看清。</span></div>
                      <div class="value-card"><b>保留选择</b><span>即使不符合捡漏，也会保留星级候选酒店给你继续判断。</span></div>
                    </div>
                  </div>
                  ${foot("适合差旅，也适合家庭短途出行")}
                </section>`,
              "4": `
                <section class="poster">
                  <div class="kicker">不用迁就低价日历</div>
                  <h1>不是让你改日期<br>而是帮你判断<br><span class="accent">今天值不值</span></h1>
                  <div class="sub">工作日只跟工作日比，周末只跟周末比。价格展示使用最终含税价。</div>
                  <div class="date-panel">
                    <div class="date-card">
                      <div class="label">你选择的入住日</div>
                      <div class="date">6月1日</div>
                      <div class="copy">以深圳国际会展中心为目标，搜索附近四星级以上酒店，实时补齐价格。</div>
                    </div>
                    <div class="date-card">
                      <div class="label">本次对比日</div>
                      <div class="compare-list">
                        ${compareDates.map((date, index) => `<div class="compare-date"><span>${date}</span><strong>${index === 0 ? "目标日" : "同类型"}</strong></div>`).join("")}
                      </div>
                    </div>
                  </div>
                  ${pickedResult(hero2)}
                  ${foot("同类型日期对比，避免误判")}
                </section>`,
              "5": `
                <section class="poster slide-progress">
                  <div class="kicker">搜索不用空等</div>
                  <h1>不用等到最后<br>选择会陆续出现</h1>
                  <div class="sub">输入目标城市和日期后，先看到已经找到的星级酒店；更多酒店、含税价和优惠对比会陆续补上，不让页面一直空白。</div>
                  <div class="timeline">
                    <div class="step"><i>1</i><div><b>先有酒店可看</b><span>先展示已找到的星级候选，马上知道附近有哪些选择。</span></div></div>
                    <div class="step"><i>2</i><div><b>价格陆续补齐</b><span>含税价、参考价、便宜多少，会随着结果一起更新。</span></div></div>
                    <div class="step"><i>3</i><div><b>选择越来越完整</b><span>后续找到的星级酒店继续加入列表，方便一起比较。</span></div></div>
                    <div class="step"><i>4</i><div><b>没有捡漏也有备选</b><span>暂时没有明显低价，也能看到附近高星酒店。</span></div></div>
                  </div>
                  <div class="progress-phone">${phone(DATA.candidates.slice(0, 4), { compact: true, mode: "candidate", limit: 4, progress: "更多酒店陆续出现", progressValue: "更新中", bar: "62%" })}</div>
                  ${foot("不用盯着空白页面等待")}
                </section>`,
              "6": `
                <section class="poster">
                  <div class="kicker">候选不丢失</div>
                  <h1>即使没有捡漏<br>附近星级酒店<br><span class="accent">也全部保留</span></h1>
                  <div class="sub">先看 3 公里内，附近不够就继续扩大到 5 公里、10 公里；搜到的四星级以上酒店都会保留，方便你一起比较。</div>
                  <div class="mosaic">${DATA.candidates.slice(0, 6).map(mosaicCard).join("")}</div>
                  ${foot("所有星级候选都会保留给你比较")}
                </section>`,
              "7": `
                <section class="poster dark slide-brand">
                  <div class="photo-strip">
                    ${photo(hero, "tile-photo")}
                    ${photo(hero2, "tile-photo")}
                    ${photo(hero3, "tile-photo")}
                  </div>
                  <div class="kicker">高端品牌推荐</div>
                  <h1>高端品牌<br>一起看清</h1>
                  <div class="sub">知名高端连锁推荐不再按价格上限隐藏，所有可参考的品牌酒店都保留。</div>
                  <div class="brand-grid">${DATA.recommended.slice(0, 4).map(brandCard).join("")}</div>
                  ${foot("品牌、距离、价格和星级一起排序")}
                </section>`,
              "8": `
                <section class="poster photo-led slide-final">
                  ${backgroundPhoto(hero3)}
                  <div class="corner-note">深圳国际会展中心 6月1日实测<br>四星级以上 · 含税价 · 实时比价</div>
                  <div class="hero-copy">
                    <div class="kicker">赶紧来试试</div>
                    <h1>旅行不只是<br>哪天最便宜</h1>
                    <div class="sub">而是在你有限的时间里，住到更值得的那一家。</div>
                  </div>
                  <div class="final-layout">
                    <div class="final-copy">
                      <div class="line">哪些星级酒店今天突然不贵了</div>
                      <div class="line">哪些高端品牌正在出现价格洼地</div>
                      <div class="line">你现在选的酒店，到底值不值</div>
                    </div>
                    ${phone(DATA.deals.slice(0, 3), { compact: true, limit: 3, progress: "实时刷新", progressValue: "完成" })}
                  </div>
                  ${foot("作者：lxz@underfitting.com")}
                </section>`
            };
            document.getElementById("root").innerHTML = slides[slide] || slides["1"];
          </script>
        </body>
        </html>
    """
    return dedent(template).replace("__PAYLOAD__", payload_json)


def export_pngs() -> None:
    base_url = HTML_PATH.as_uri()
    for slide, filename in SLIDE_EXPORTS:
        output_path = OUT_DIR / filename
        subprocess.run(
            [
                "playwright",
                "screenshot",
                "--viewport-size",
                "1080,1920",
                "--wait-for-selector",
                ".poster",
                "--wait-for-timeout",
                "1800",
                "--timeout",
                "60000",
                f"{base_url}?slide={slide}",
                str(output_path),
            ],
            check=True,
        )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = fetch_search_result()
    (OUT_DIR / "shenzhen_0601_search_result.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    HTML_PATH.write_text(build_html(data), encoding="utf-8")
    export_pngs()
    print(HTML_PATH)
    for _, filename in SLIDE_EXPORTS:
        print(OUT_DIR / filename)
    print(f"dealCount={(data.get('summary') or {}).get('dealCount')} candidateCount={(data.get('summary') or {}).get('candidateCount')}")


if __name__ == "__main__":
    main()
