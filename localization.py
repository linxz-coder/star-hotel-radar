from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

try:  # Optional dependency; the local mapping keeps the app usable without it.
    from opencc import OpenCC
except ImportError:  # pragma: no cover
    OpenCC = None  # type: ignore[assignment]


try:
    OPENCC_T2S = OpenCC("t2s") if OpenCC is not None else None
except Exception:  # pragma: no cover
    OPENCC_T2S = None


T2S_PHRASE_REPLACEMENTS = {
    "希爾頓花園酒店": "希尔顿花园酒店",
    "皇冠假日酒店": "皇冠假日酒店",
    "洲際酒店": "洲际酒店",
    "凱悅酒店": "凯悦酒店",
    "格蘭雲天": "格兰云天",
    "維也納酒店": "维也纳酒店",
    "萬豪酒店": "万豪酒店",
    "喜來登酒店": "喜来登酒店",
    "朗廷酒店": "朗廷酒店",
    "文華東方": "文华东方",
    "嶺南東方": "岭南东方",
    "廣州增城": "广州增城",
    "深圳國際會展中心": "深圳国际会展中心",
    "光明虹橋": "光明虹桥",
}

T2S_CHAR_MAP = str.maketrans(
    {
        "廣": "广", "東": "东", "門": "门", "雲": "云", "國": "国", "際": "际", "會": "会",
        "聞": "闻", "頭": "头", "灣": "湾", "橋": "桥", "園": "园", "華": "华", "凱": "凯",
        "悅": "悦", "爾": "尔", "頓": "顿", "維": "维", "納": "纳", "蘭": "兰", "瀾": "澜",
        "麗": "丽", "貝": "贝", "濱": "滨", "樓": "楼", "閣": "阁", "館": "馆", "莊": "庄",
        "龍": "龙", "寧": "宁", "蘇": "苏", "滬": "沪", "縣": "县", "區": "区", "內": "内",
        "陽": "阳", "陰": "阴", "長": "长", "慶": "庆", "達": "达", "連": "连", "遼": "辽",
        "瀋": "沈", "濟": "济", "鄭": "郑", "漢": "汉", "貴": "贵", "樂": "乐", "兒": "儿",
        "親": "亲", "雙": "双", "張": "张", "萬": "万", "與": "与", "裏": "里", "裡": "里",
        "臺": "台", "島": "岛", "飯": "饭", "體": "体", "號": "号", "衛": "卫", "潔": "洁",
        "寶": "宝", "緣": "缘", "錦": "锦", "匯": "汇", "恆": "恒", "榮": "荣", "業": "业",
        "廈": "厦", "廳": "厅", "庫": "库", "營": "营", "適": "适", "選": "选", "鄰": "邻",
        "韓": "韩", "歐": "欧", "羅": "罗", "倫": "伦", "紐": "纽", "舊": "旧", "聖": "圣",
        "侶": "侣",
        "爺": "爷", "鬆": "松", "鬧": "闹", "豐": "丰", "齋": "斋", "齊": "齐", "淺": "浅",
        "澀": "涩", "環": "环", "購": "购", "碼": "码", "棕": "棕", "櫚": "榈", "壯": "壮",
        "劇": "剧", "鐵": "铁", "盧": "卢", "奧": "奥", "馬": "马", "強": "强", "現": "现",
        "藝": "艺", "廠": "厂", "產": "产", "發": "发", "黃": "黄", "態": "态",
    }
)

ENGLISH_HOTEL_NAME_EXACT = {
    "Zhuhai Lovers Road Sun Moon Shell Grand Theater Atour Hotel": "珠海情侣路日月贝大剧院亚朵酒店",
    "HUI Hotel (Riyuebei Branch, Qinglv Road, Zhuhai)": "慧酒店（珠海情侣路日月贝店）",
    "RAMADA INTERNATIONAL(Zhuhai Lovers Middle Road Riyuebei Grand Theater Branch)": "珠海情侣中路日月贝大剧院华美达国际酒店",
    "Days Inn Zhuhai Selected Wyndham Hotel (Couple Middle Road Branch)": "珠海戴斯精选温德姆酒店（情侣中路店）",
    "sky·Holiday Hotel": "天·假日酒店",
    "De Castle Debao Garden Hotel (Zhuhai Lovers Road Seaside Swimming Pool)": "珠海德堡花园酒店（情侣路海滨泳场店）",
    "JI HOTEL": "全季酒店",
    "Holiday Inn Express Zhuhai Grand Theater by IHG": "珠海大剧院智选假日酒店",
    "JI Hotel (Zhuhai IN CITY Sam's Club MixC)": "全季酒店（珠海印象城山姆会员店万象城店）",
    "All-season Zhuhai Universal Pearl South Road Hotel": "全季珠海明珠南路酒店",
    "Yijingwan Hotel (Zhuhai Qinglv Road, Seashore Swimming Pool)": "怡景湾酒店（珠海情侣路海滨泳场店）",
    "MEHOOD LESTIE Hotel (Zhuhai Qinglü Road Haibin Swimming Pool)": "美豪丽致酒店（珠海情侣路海滨泳场店）",
    "Atour Hotel 【Zhuhai Love Post Office on Qinglv Road】": "亚朵酒店（珠海情侣路爱情邮局店）",
    "Zhuhai Xumeishan Hotel (Love Post Office Store, Lovers Road Seaside Swimming Pool)": "珠海旭美山酒店（情侣路爱情邮局海滨泳场店）",
    "Zhuhai Delong M Seaview Hotel (Lovers Road Seaside Swimming Pool Store)": "珠海德隆M海景酒店（情侣路海滨泳场店）",
    "Zhuhai Dehan Hotel": "珠海德翰大酒店",
    "Zhuhai Lishang Hotel (Lvren Road Beach Swimming Pool)": "珠海丽尚酒店（情侣路海滨泳场店）",
    "Zhuhai Luoxi Hotel (Lovers Road Seaside Swimming Pool)": "珠海洛溪酒店（情侣路海滨泳场店）",
    "Palm Music Hotel": "棕榈音乐酒店",
    "Xingcheng Hotel (Zhuhai Seashore Swimming Pool Love Road)": "星程酒店（珠海海滨泳场情侣路店）",
    "Crowne Plaza ZHUHAI CITY CENTER by IHG": "珠海市中心皇冠假日酒店",
    "Zhuhai Holiday Resort": "珠海度假村酒店",
    "Starview Mansion": "星景公馆",
    "Holiday Inn ZHUHAI CITY CENTER by IHG": "珠海市中心假日酒店",
    "Grand Ocean View Hotel Zhuhai": "珠海观海酒店",
    "Atour Hotel Zhuhai Qinglü South Road Gongbei Port": "珠海情侣南路拱北口岸亚朵酒店",
    "Orange Hotel Zhuhai Lovers Middle Road Riyue Bei Seaview": "桔子酒店（珠海情侣中路日月贝海景店）",
    "Overseas Chinese Hotel": "华侨酒店",
    "Zhuhai 2000 Hotel (Lovers Road Grand Theatre)": "珠海2000酒店（情侣路大剧院店）",
    "Wanda Moments, Zhuhai": "珠海万达美华酒店",
    "Zhuhai Luxe Hotel 【Gongbei Port & High-speed Rail Station, Xiangzhou District】": "珠海丽呈酒店（拱北口岸高铁站香洲区店）",
    "Atour S Hotel(Shihua West Road, Gongbei Port, Zhuhai)": "珠海拱北口岸石花西路亚朵S酒店",
    "Grand Bay Hotel Zhuhai": "珠海海湾大酒店",
    "UrCove by HYATT Zhuhai Gongbei Port": "珠海拱北口岸逸扉酒店",
    "Hampton by Hilton Zhuhai Gongbei Port": "珠海拱北口岸希尔顿欢朋酒店",
    "InterContinental Zhuhai": "珠海仁恒洲际酒店",
    "Zhuhai Gongbei Hyatt Regency Hotel": "珠海拱北凯悦酒店",
    "Pullman Zhuhai (Gongbei Port Fuhua Li Branch)": "珠海中海铂尔曼酒店（拱北口岸富华里店）",
    "Holiday Inn Express ZHUHAI GONGBEI by IHG": "珠海拱北智选假日酒店",
    "Fairfield by Marriott Zhuhai": "珠海万枫酒店",
    "Hotel Indigo ZHUHAI XIANGZHOU by IHG": "珠海香洲英迪格酒店",
    "Renaissance Zhuhai": "珠海中海万丽酒店",
    "Hyatt Place Zhuhai Jinshi": "珠海金石凯悦嘉轩酒店",
}

ENGLISH_HOTEL_NAME_REPLACEMENTS = [
    ("Sun Moon Shell Grand Theater", "日月贝大剧院"),
    ("Riyuebei Grand Theater", "日月贝大剧院"),
    ("Seashore Swimming Pool", "海滨泳场"),
    ("Beach Swimming Pool", "海滨泳场"),
    ("Haibin Swimming Pool", "海滨泳场"),
    ("Seaside Swimming Pool", "海滨泳场"),
    ("Universal Pearl South Road", "明珠南路"),
    ("Qinglü South Road", "情侣南路"),
    ("Lovers Middle Road", "情侣中路"),
    ("Couple Middle Road", "情侣中路"),
    ("Lovers Road", "情侣路"),
    ("Qinglv Road", "情侣路"),
    ("Qinglü Road", "情侣路"),
    ("Lvren Road", "情侣路"),
    ("Love Road", "情侣路"),
    ("Love Post Office", "爱情邮局"),
    ("Gongbei Port", "拱北口岸"),
    ("High-speed Rail Station", "高铁站"),
    ("Xiangzhou District", "香洲区"),
    ("Shihua West Road", "石花西路"),
    ("Fuhua Li", "富华里"),
    ("Riyue Bei", "日月贝"),
    ("Sam's Club", "山姆会员店"),
    ("IN CITY", "印象城"),
    ("Riyuebei", "日月贝"),
    ("Zhuhai", "珠海"),
    ("Ramada International", "华美达国际酒店"),
    ("RAMADA INTERNATIONAL", "华美达国际酒店"),
    ("Crowne Plaza", "皇冠假日酒店"),
    ("Holiday Inn Express", "智选假日酒店"),
    ("Holiday Inn", "假日酒店"),
    ("Selected Wyndham Hotel", "精选温德姆酒店"),
    ("Wyndham Hotel", "温德姆酒店"),
    ("InterContinental", "洲际酒店"),
    ("Hotel Indigo", "英迪格酒店"),
    ("Renaissance", "万丽酒店"),
    ("Fairfield by Marriott", "万枫酒店"),
    ("Hampton by Hilton", "希尔顿欢朋酒店"),
    ("Hyatt Place", "凯悦嘉轩酒店"),
    ("Hyatt Regency", "凯悦酒店"),
    ("UrCove by HYATT", "逸扉酒店"),
    ("Pullman", "铂尔曼酒店"),
    ("Wanda Moments", "万达美华酒店"),
    ("Orange Hotel", "桔子酒店"),
    ("MEHOOD LESTIE Hotel", "美豪丽致酒店"),
    ("All-season", "全季"),
    ("Days Inn", "戴斯酒店"),
    ("JI Hotel", "全季酒店"),
    ("JI HOTEL", "全季酒店"),
    ("Yijingwan Hotel", "怡景湾酒店"),
    ("Delong M Seaview Hotel", "德隆M海景酒店"),
    ("Dehan Hotel", "德翰大酒店"),
    ("Lishang Hotel", "丽尚酒店"),
    ("Luoxi Hotel", "洛溪酒店"),
    ("Xumeishan Hotel", "旭美山酒店"),
    ("Grand Bay Hotel", "海湾大酒店"),
    ("Grand Ocean View Hotel", "观海酒店"),
    ("Overseas Chinese Hotel", "华侨酒店"),
    ("Holiday Resort", "度假村酒店"),
    ("Starview Mansion", "星景公馆"),
    ("Palm Music Hotel", "棕榈音乐酒店"),
    ("Xingcheng Hotel", "星程酒店"),
    ("Atour Hotel", "亚朵酒店"),
    ("HUI Hotel", "慧酒店"),
    ("Holiday Hotel", "假日酒店"),
    ("Debao Garden Hotel", "德堡花园酒店"),
    ("De Castle", "德堡"),
    ("Grand Theater", "大剧院"),
    ("MixC", "万象城"),
    ("by IHG", ""),
    ("International", "国际"),
    ("Branch", "店"),
    ("Store", "店"),
    ("Hotel", "酒店"),
]

DOMESTIC_HOTEL_NAME_PATH = Path(__file__).resolve().parent / "data" / "domestic_hotel_names.json"
_DOMESTIC_HOTEL_NAME_CACHE: dict[str, Any] | None = None


def contains_chinese_text(value: Any) -> bool:
    return bool(re.search(r"[\u3400-\u9fff]", str(value or "")))


def simplify_chinese_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    if OPENCC_T2S is not None:
        try:
            return OPENCC_T2S.convert(text)
        except Exception:  # pragma: no cover
            pass
    for traditional, simplified in T2S_PHRASE_REPLACEMENTS.items():
        text = text.replace(traditional, simplified)
    return text.translate(T2S_CHAR_MAP)


def domestic_hotel_name_key(value: Any) -> str:
    text = simplify_chinese_text(value)
    return re.sub(r"[\s·・,，.。()（）\-_/]+", "", text).casefold()


def load_domestic_hotel_names() -> dict[str, Any]:
    global _DOMESTIC_HOTEL_NAME_CACHE
    if _DOMESTIC_HOTEL_NAME_CACHE is not None:
        return _DOMESTIC_HOTEL_NAME_CACHE
    try:
        payload = json.loads(DOMESTIC_HOTEL_NAME_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        payload = {}
    payload.setdefault("byHotelId", {})
    payload.setdefault("byName", {})
    payload["_byNameKey"] = {
        domestic_hotel_name_key(name): row
        for name, row in payload.get("byName", {}).items()
        if domestic_hotel_name_key(name) and isinstance(row, dict)
    }
    _DOMESTIC_HOTEL_NAME_CACHE = payload
    return payload


def domestic_hotel_name_override(value: Any, *, hotel_id: Any = None) -> dict[str, str] | None:
    payload = load_domestic_hotel_names()
    row: dict[str, Any] | None = None
    if hotel_id not in (None, ""):
        candidate = payload.get("byHotelId", {}).get(str(hotel_id))
        if isinstance(candidate, dict):
            row = candidate
    if row is None:
        original = str(value or "").strip()
        candidate = payload.get("byName", {}).get(original)
        if isinstance(candidate, dict):
            row = candidate
    if row is None:
        candidate = payload.get("_byNameKey", {}).get(domestic_hotel_name_key(value))
        if isinstance(candidate, dict):
            row = candidate
    if not row:
        return None

    name = simplify_chinese_text(row.get("name") or "").strip()
    if not name or not contains_chinese_text(name):
        return None
    return {
        "name": name,
        "source": str(row.get("source") or "国内 OTA 标准中文名"),
    }


def domestic_hotel_name_payload(value: Any, *, hotel_id: Any = None, source: str = "") -> dict[str, str]:
    original = str(value or "").strip()
    override = domestic_hotel_name_override(original, hotel_id=hotel_id)
    if override:
        name = override["name"]
        return {
            "hotelName": name,
            "hotelOriginalName": original if original and original != name else "",
            "hotelNameSimplified": name,
            "hotelNameSource": override["source"],
        }
    fallback = english_hotel_name_fallback(original)
    if fallback:
        return {
            "hotelName": fallback,
            "hotelOriginalName": original if original and original != fallback else "",
            "hotelNameSimplified": fallback,
            "hotelNameSource": "Trip.com 英文名规则转中文",
        }
    return normalized_hotel_name_payload(original, source=source)


def hotel_name_payload_from_sources(values: list[Any], *, hotel_id: Any = None, source: str = "") -> dict[str, str]:
    originals = [str(value or "").strip() for value in values if str(value or "").strip()]
    primary = originals[0] if originals else ""

    for original in originals or [primary]:
        override = domestic_hotel_name_override(original, hotel_id=hotel_id)
        if override:
            name = override["name"]
            return {
                "hotelName": name,
                "hotelOriginalName": original if original and original != name else "",
                "hotelNameSimplified": name,
                "hotelNameSource": override["source"],
            }

    for original in originals:
        simplified = simplify_chinese_text(original).strip()
        if simplified and contains_chinese_text(simplified):
            return {
                "hotelName": simplified,
                "hotelOriginalName": original if original != simplified else "",
                "hotelNameSimplified": simplified,
                "hotelNameSource": source or ("繁体转简体" if original != simplified else "Trip.com 中文名"),
            }

    for original in originals:
        fallback = english_hotel_name_fallback(original)
        if fallback:
            return {
                "hotelName": fallback,
                "hotelOriginalName": original if original and original != fallback else "",
                "hotelNameSimplified": fallback,
                "hotelNameSource": "Trip.com 英文名规则转中文",
            }

    return normalized_hotel_name_payload(primary, source=source)


def is_displayable_chinese_hotel_name(value: Any, *, hotel_id: Any = None) -> bool:
    override = domestic_hotel_name_override(value, hotel_id=hotel_id)
    if override:
        return True
    if english_hotel_name_fallback(value):
        return True
    return contains_chinese_text(simplify_chinese_text(value))


def english_hotel_name_fallback(value: Any) -> str:
    original = str(value or "").strip()
    if not original or contains_chinese_text(original):
        return ""
    exact = ENGLISH_HOTEL_NAME_EXACT.get(original)
    if exact:
        return exact
    text = original
    for english, chinese in ENGLISH_HOTEL_NAME_REPLACEMENTS:
        text = re.sub(re.escape(english), chinese, text, flags=re.IGNORECASE)
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"\s*,\s*", "，", text)
    text = re.sub(r"\s+", "", text)
    text = text.replace("(", "（").replace(")", "）")
    text = re.sub(r"（店）", "店", text)
    text = re.sub(r"（([^（）]*店)）", r"（\1）", text)
    if not contains_chinese_text(text):
        return ""
    if re.search(r"[A-Za-z]{2,}", text):
        return ""
    return text.strip("，,·-_/ ")


def normalized_hotel_name_payload(value: Any, *, source: str = "") -> dict[str, str]:
    original = str(value or "").strip()
    simplified = simplify_chinese_text(original).strip()
    if original and simplified and simplified != original and contains_chinese_text(simplified):
        return {
            "hotelName": simplified,
            "hotelOriginalName": original,
            "hotelNameSimplified": simplified,
            "hotelNameSource": source or "繁体转简体",
        }
    payload = {
        "hotelName": original,
        "hotelOriginalName": "",
        "hotelNameSimplified": simplified if contains_chinese_text(simplified) else "",
        "hotelNameSource": source if original else "",
    }
    return payload
