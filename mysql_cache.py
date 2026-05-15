from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any

try:  # pragma: no cover - availability depends on the local runtime
    import pymysql
    from pymysql.cursors import DictCursor
except Exception:  # pragma: no cover
    pymysql = None
    DictCursor = None


def cache_digest(cache_key: str) -> str:
    return hashlib.sha256(cache_key.encode("utf-8")).hexdigest()


@dataclass
class MySQLSearchCache:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "star_hotel_deal_app"
    table: str = "hotel_search_cache"
    enabled: bool = True
    connect_timeout: float = 1.0
    read_timeout: float = 2.0
    write_timeout: float = 2.0
    cooldown_seconds: float = 60.0

    _schema_ready: bool = False
    _disabled_until: float = 0
    last_error: str = ""

    @classmethod
    def from_env(cls) -> "MySQLSearchCache":
        enabled_value = os.environ.get("HOTEL_DEAL_MYSQL_ENABLED", "auto").strip().lower()
        enabled = enabled_value not in {"0", "false", "no", "off", "disabled"}
        return cls(
            host=os.environ.get("HOTEL_DEAL_MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("HOTEL_DEAL_MYSQL_PORT", "3306")),
            user=os.environ.get("HOTEL_DEAL_MYSQL_USER", "root"),
            password=os.environ.get("HOTEL_DEAL_MYSQL_PASSWORD", ""),
            database=os.environ.get("HOTEL_DEAL_MYSQL_DATABASE", "star_hotel_deal_app"),
            table=os.environ.get("HOTEL_DEAL_MYSQL_TABLE", "hotel_search_cache"),
            enabled=enabled,
            connect_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_CONNECT_TIMEOUT", "1")),
            read_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_READ_TIMEOUT", "2")),
            write_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_WRITE_TIMEOUT", "2")),
            cooldown_seconds=float(os.environ.get("HOTEL_DEAL_MYSQL_ERROR_COOLDOWN_SECONDS", "60")),
        )

    def available(self) -> bool:
        return bool(self.enabled and pymysql is not None and time.time() >= self._disabled_until)

    def get(self, cache_key: str, provider: str) -> dict[str, Any] | None:
        if not self.available():
            return None
        digest = cache_digest(cache_key)
        now = time.time()
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT result_json, created_at, updated_at, expires_at
                        FROM `{self.table}`
                        WHERE cache_hash = %s AND provider = %s
                        LIMIT 1
                        """,
                        (digest, provider),
                    )
                    row = cursor.fetchone()
                    if not row:
                        return None
                    expires_at = float(row["expires_at"] or 0)
                    if expires_at < now:
                        cursor.execute(f"DELETE FROM `{self.table}` WHERE cache_hash = %s", (digest,))
                        return None
                    cursor.execute(
                        f"""
                        UPDATE `{self.table}`
                        SET hit_count = hit_count + 1, last_hit_at = %s
                        WHERE cache_hash = %s
                        """,
                        (now, digest),
                    )
            result = json.loads(row["result_json"])
            if not isinstance(result, dict):
                return None
            return {
                "createdAt": float(row.get("created_at") or 0),
                "updatedAt": float(row.get("updated_at") or 0),
                "expiresAt": expires_at,
                "result": result,
            }
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)
            return None

    def store(self, cache_key: str, provider: str, result: dict[str, Any], expires_at: float) -> None:
        if not self.available():
            return
        digest = cache_digest(cache_key)
        now = time.time()
        columns = self.cache_columns(cache_key)
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        INSERT INTO `{self.table}` (
                            cache_hash, cache_key_json, provider, logic_version,
                            city, target_hotel, selected_date, result_json,
                            created_at, updated_at, expires_at, last_hit_at, hit_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 0)
                        ON DUPLICATE KEY UPDATE
                            cache_key_json = VALUES(cache_key_json),
                            provider = VALUES(provider),
                            logic_version = VALUES(logic_version),
                            city = VALUES(city),
                            target_hotel = VALUES(target_hotel),
                            selected_date = VALUES(selected_date),
                            result_json = VALUES(result_json),
                            updated_at = VALUES(updated_at),
                            expires_at = VALUES(expires_at)
                        """,
                        (
                            digest,
                            cache_key,
                            provider,
                            columns["logic_version"],
                            columns["city"],
                            columns["target_hotel"],
                            columns["selected_date"],
                            json.dumps(result, ensure_ascii=False),
                            now,
                            now,
                            expires_at,
                        ),
                    )
                    cursor.execute(f"DELETE FROM `{self.table}` WHERE expires_at < %s", (now,))
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)

    def clear(self, provider: str | None = None) -> None:
        if not self.available():
            return
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    if provider:
                        cursor.execute(f"DELETE FROM `{self.table}` WHERE provider = %s", (provider,))
                    else:
                        cursor.execute(f"TRUNCATE TABLE `{self.table}`")
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self.connect(use_database=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE DATABASE IF NOT EXISTS `{self.database}`
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{self.table}` (
                        cache_hash CHAR(64) NOT NULL PRIMARY KEY,
                        cache_key_json MEDIUMTEXT NOT NULL,
                        provider VARCHAR(32) NOT NULL,
                        logic_version VARCHAR(128) NOT NULL,
                        city VARCHAR(128) NULL,
                        target_hotel VARCHAR(255) NULL,
                        selected_date VARCHAR(16) NULL,
                        result_json LONGTEXT NOT NULL,
                        created_at DOUBLE NOT NULL,
                        updated_at DOUBLE NOT NULL,
                        expires_at DOUBLE NOT NULL,
                        last_hit_at DOUBLE NULL,
                        hit_count INT NOT NULL DEFAULT 0,
                        INDEX idx_provider_expires (provider, expires_at),
                        INDEX idx_updated_at (updated_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
        self._schema_ready = True

    def connect(self, use_database: bool = True):
        if pymysql is None:
            raise RuntimeError("PyMySQL is not installed")
        kwargs = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "charset": "utf8mb4",
            "autocommit": True,
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
            "write_timeout": self.write_timeout,
            "cursorclass": DictCursor,
        }
        if use_database:
            kwargs["database"] = self.database
        return pymysql.connect(**kwargs)

    def remember_error(self, exc: Exception) -> None:
        self.last_error = str(exc)
        self._disabled_until = time.time() + self.cooldown_seconds

    def cache_columns(self, cache_key: str) -> dict[str, str]:
        try:
            payload = json.loads(cache_key)
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return {
            "logic_version": str(payload.get("logicVersion") or ""),
            "city": str(payload.get("city") or ""),
            "target_hotel": str(payload.get("targetHotel") or ""),
            "selected_date": str(payload.get("selectedDate") or ""),
        }


@dataclass
class MySQLHotelNameCache(MySQLSearchCache):
    table: str = "hotel_name_cache"

    @classmethod
    def from_env(cls) -> "MySQLHotelNameCache":
        enabled_value = os.environ.get("HOTEL_DEAL_MYSQL_ENABLED", "auto").strip().lower()
        enabled = enabled_value not in {"0", "false", "no", "off", "disabled"}
        return cls(
            host=os.environ.get("HOTEL_DEAL_MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("HOTEL_DEAL_MYSQL_PORT", "3306")),
            user=os.environ.get("HOTEL_DEAL_MYSQL_USER", "root"),
            password=os.environ.get("HOTEL_DEAL_MYSQL_PASSWORD", ""),
            database=os.environ.get("HOTEL_DEAL_MYSQL_DATABASE", "star_hotel_deal_app"),
            table=os.environ.get("HOTEL_DEAL_MYSQL_NAME_TABLE", "hotel_name_cache"),
            enabled=enabled,
            connect_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_CONNECT_TIMEOUT", "1")),
            read_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_READ_TIMEOUT", "2")),
            write_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_WRITE_TIMEOUT", "2")),
            cooldown_seconds=float(os.environ.get("HOTEL_DEAL_MYSQL_ERROR_COOLDOWN_SECONDS", "60")),
        )

    def get(self, provider: str, *, hotel_id: str = "", original_name_key: str = "") -> dict[str, Any] | None:
        if not self.available():
            return None
        now = time.time()
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    row = None
                    if hotel_id:
                        cursor.execute(
                            f"""
                            SELECT payload_json, expires_at
                            FROM `{self.table}`
                            WHERE provider = %s AND hotel_id = %s AND expires_at >= %s
                            ORDER BY updated_at DESC
                            LIMIT 1
                            """,
                            (provider, hotel_id, now),
                        )
                        row = cursor.fetchone()
                    if row is None and original_name_key:
                        cursor.execute(
                            f"""
                            SELECT payload_json, expires_at
                            FROM `{self.table}`
                            WHERE provider = %s AND original_name_key = %s AND expires_at >= %s
                            ORDER BY updated_at DESC
                            LIMIT 1
                            """,
                            (provider, original_name_key, now),
                        )
                        row = cursor.fetchone()
                    if not row:
                        return None
                    cache_hash = cache_digest(f"{provider}:{hotel_id or original_name_key}")
                    cursor.execute(
                        f"""
                        UPDATE `{self.table}`
                        SET hit_count = hit_count + 1, last_hit_at = %s
                        WHERE cache_hash = %s
                        """,
                        (now, cache_hash),
                    )
            payload = json.loads(row["payload_json"])
            return payload if isinstance(payload, dict) else None
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)
            return None

    def store(
        self,
        provider: str,
        *,
        hotel_id: str,
        original_name: str,
        original_name_key: str,
        payload: dict[str, Any],
        expires_at: float,
    ) -> None:
        if not self.available():
            return
        cache_hash = cache_digest(f"{provider}:{hotel_id or original_name_key}")
        now = time.time()
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        INSERT INTO `{self.table}` (
                            cache_hash, provider, hotel_id, original_name_key,
                            original_name, hotel_name, hotel_name_source, payload_json,
                            created_at, updated_at, expires_at, last_hit_at, hit_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 0)
                        ON DUPLICATE KEY UPDATE
                            original_name_key = VALUES(original_name_key),
                            original_name = VALUES(original_name),
                            hotel_name = VALUES(hotel_name),
                            hotel_name_source = VALUES(hotel_name_source),
                            payload_json = VALUES(payload_json),
                            updated_at = VALUES(updated_at),
                            expires_at = VALUES(expires_at)
                        """,
                        (
                            cache_hash,
                            provider,
                            hotel_id,
                            original_name_key,
                            original_name,
                            str(payload.get("hotelName") or ""),
                            str(payload.get("hotelNameSource") or payload.get("source") or ""),
                            json.dumps(payload, ensure_ascii=False),
                            now,
                            now,
                            expires_at,
                        ),
                    )
                    cursor.execute(f"DELETE FROM `{self.table}` WHERE expires_at < %s", (now,))
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self.connect(use_database=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE DATABASE IF NOT EXISTS `{self.database}`
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{self.table}` (
                        cache_hash CHAR(64) NOT NULL PRIMARY KEY,
                        provider VARCHAR(32) NOT NULL,
                        hotel_id VARCHAR(64) NOT NULL,
                        original_name_key CHAR(64) NULL,
                        original_name VARCHAR(255) NULL,
                        hotel_name VARCHAR(255) NOT NULL,
                        hotel_name_source VARCHAR(128) NULL,
                        payload_json MEDIUMTEXT NOT NULL,
                        created_at DOUBLE NOT NULL,
                        updated_at DOUBLE NOT NULL,
                        expires_at DOUBLE NOT NULL,
                        last_hit_at DOUBLE NULL,
                        hit_count INT NOT NULL DEFAULT 0,
                        INDEX idx_provider_hotel (provider, hotel_id),
                        INDEX idx_provider_name_key (provider, original_name_key),
                        INDEX idx_updated_at (updated_at),
                        INDEX idx_expires_at (expires_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
        self._schema_ready = True


@dataclass
class MySQLHotelPriceCache(MySQLSearchCache):
    table: str = "hotel_price_cache"

    @classmethod
    def from_env(cls) -> "MySQLHotelPriceCache":
        enabled_value = os.environ.get("HOTEL_DEAL_MYSQL_ENABLED", "auto").strip().lower()
        enabled = enabled_value not in {"0", "false", "no", "off", "disabled"}
        return cls(
            host=os.environ.get("HOTEL_DEAL_MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("HOTEL_DEAL_MYSQL_PORT", "3306")),
            user=os.environ.get("HOTEL_DEAL_MYSQL_USER", "root"),
            password=os.environ.get("HOTEL_DEAL_MYSQL_PASSWORD", ""),
            database=os.environ.get("HOTEL_DEAL_MYSQL_DATABASE", "star_hotel_deal_app"),
            table=os.environ.get("HOTEL_DEAL_MYSQL_PRICE_TABLE", "hotel_price_cache"),
            enabled=enabled,
            connect_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_CONNECT_TIMEOUT", "1")),
            read_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_READ_TIMEOUT", "2")),
            write_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_WRITE_TIMEOUT", "2")),
            cooldown_seconds=float(os.environ.get("HOTEL_DEAL_MYSQL_ERROR_COOLDOWN_SECONDS", "60")),
        )

    def get_many(
        self,
        provider: str,
        hotel_ids: list[str],
        dates: list[str],
        *,
        max_age_seconds: float,
    ) -> dict[str, dict[str, int]]:
        if not self.available():
            return {}
        hotel_ids = [str(hotel_id) for hotel_id in hotel_ids if str(hotel_id or "").strip()]
        dates = [str(date_value) for date_value in dates if str(date_value or "").strip()]
        if not hotel_ids or not dates:
            return {}
        now = time.time()
        min_updated_at = now - float(max_age_seconds)
        placeholders_hotels = ", ".join(["%s"] * len(hotel_ids))
        placeholders_dates = ", ".join(["%s"] * len(dates))
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT hotel_id, price_date, current_price
                        FROM `{self.table}`
                        WHERE provider = %s
                          AND hotel_id IN ({placeholders_hotels})
                          AND price_date IN ({placeholders_dates})
                          AND updated_at >= %s
                          AND expires_at >= %s
                        """,
                        (provider, *hotel_ids, *dates, min_updated_at, now),
                    )
                    rows = cursor.fetchall() or []
                    if rows:
                        hashes = [
                            cache_digest(f"{provider}:{row['hotel_id']}:{row['price_date']}")
                            for row in rows
                        ]
                        hash_placeholders = ", ".join(["%s"] * len(hashes))
                        cursor.execute(
                            f"""
                            UPDATE `{self.table}`
                            SET hit_count = hit_count + 1, last_hit_at = %s
                            WHERE cache_hash IN ({hash_placeholders})
                            """,
                            (now, *hashes),
                        )
            result: dict[str, dict[str, int]] = {}
            for row in rows:
                hotel_id = str(row.get("hotel_id") or "")
                price_date = str(row.get("price_date") or "")
                try:
                    price = int(round(float(row.get("current_price"))))
                except (TypeError, ValueError):
                    continue
                if hotel_id and price_date and price > 0:
                    result.setdefault(hotel_id, {})[price_date] = price
            return result
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)
            return {}

    def store_price(
        self,
        provider: str,
        *,
        hotel_id: str,
        price_date: str,
        current_price: int | float,
        price_source: str = "",
        expires_at: float,
    ) -> None:
        if not self.available():
            return
        hotel_id = str(hotel_id or "").strip()
        price_date = str(price_date or "").strip()
        if not hotel_id or not price_date:
            return
        try:
            price = int(round(float(current_price)))
        except (TypeError, ValueError):
            return
        if price <= 0:
            return
        cache_hash = cache_digest(f"{provider}:{hotel_id}:{price_date}")
        now = time.time()
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        INSERT INTO `{self.table}` (
                            cache_hash, provider, hotel_id, price_date,
                            current_price, price_includes_tax, price_source,
                            created_at, updated_at, expires_at, last_hit_at, hit_count
                        )
                        VALUES (%s, %s, %s, %s, %s, 1, %s, %s, %s, %s, NULL, 0)
                        ON DUPLICATE KEY UPDATE
                            current_price = VALUES(current_price),
                            price_includes_tax = 1,
                            price_source = VALUES(price_source),
                            updated_at = VALUES(updated_at),
                            expires_at = VALUES(expires_at)
                        """,
                        (
                            cache_hash,
                            provider,
                            hotel_id,
                            price_date,
                            price,
                            str(price_source or ""),
                            now,
                            now,
                            expires_at,
                        ),
                    )
                    cursor.execute(f"DELETE FROM `{self.table}` WHERE expires_at < %s", (now,))
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)

    def clear(self, provider: str | None = None) -> None:
        if not self.available():
            return
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    if provider:
                        cursor.execute(f"DELETE FROM `{self.table}` WHERE provider = %s", (provider,))
                    else:
                        cursor.execute(f"TRUNCATE TABLE `{self.table}`")
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self.connect(use_database=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE DATABASE IF NOT EXISTS `{self.database}`
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{self.table}` (
                        cache_hash CHAR(64) NOT NULL PRIMARY KEY,
                        provider VARCHAR(32) NOT NULL,
                        hotel_id VARCHAR(64) NOT NULL,
                        price_date VARCHAR(16) NOT NULL,
                        current_price INT NOT NULL,
                        price_includes_tax TINYINT(1) NOT NULL DEFAULT 1,
                        price_source VARCHAR(128) NULL,
                        created_at DOUBLE NOT NULL,
                        updated_at DOUBLE NOT NULL,
                        expires_at DOUBLE NOT NULL,
                        last_hit_at DOUBLE NULL,
                        hit_count INT NOT NULL DEFAULT 0,
                        UNIQUE KEY uq_provider_hotel_date (provider, hotel_id, price_date),
                        INDEX idx_provider_date (provider, price_date),
                        INDEX idx_provider_hotel (provider, hotel_id),
                        INDEX idx_updated_at (updated_at),
                        INDEX idx_expires_at (expires_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
        self._schema_ready = True


@dataclass
class MySQLHotelCandidateCache(MySQLSearchCache):
    table: str = "hotel_candidate_cache"

    @classmethod
    def from_env(cls) -> "MySQLHotelCandidateCache":
        enabled_value = os.environ.get("HOTEL_DEAL_MYSQL_ENABLED", "auto").strip().lower()
        enabled = enabled_value not in {"0", "false", "no", "off", "disabled"}
        return cls(
            host=os.environ.get("HOTEL_DEAL_MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("HOTEL_DEAL_MYSQL_PORT", "3306")),
            user=os.environ.get("HOTEL_DEAL_MYSQL_USER", "root"),
            password=os.environ.get("HOTEL_DEAL_MYSQL_PASSWORD", ""),
            database=os.environ.get("HOTEL_DEAL_MYSQL_DATABASE", "star_hotel_deal_app"),
            table=os.environ.get("HOTEL_DEAL_MYSQL_CANDIDATE_TABLE", "hotel_candidate_cache"),
            enabled=enabled,
            connect_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_CONNECT_TIMEOUT", "1")),
            read_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_READ_TIMEOUT", "2")),
            write_timeout=float(os.environ.get("HOTEL_DEAL_MYSQL_WRITE_TIMEOUT", "2")),
            cooldown_seconds=float(os.environ.get("HOTEL_DEAL_MYSQL_ERROR_COOLDOWN_SECONDS", "60")),
        )

    def get(
        self,
        provider: str,
        *,
        target_key: str,
        radius_km: float,
        min_star: float,
        max_age_seconds: float,
    ) -> list[dict[str, Any]]:
        if not self.available():
            return []
        cache_hash = cache_digest(f"{provider}:{target_key}:{float(radius_km):.1f}:{float(min_star):.1f}")
        now = time.time()
        min_updated_at = now - float(max_age_seconds)
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT payload_json, updated_at, expires_at
                        FROM `{self.table}`
                        WHERE cache_hash = %s
                          AND provider = %s
                          AND updated_at >= %s
                          AND expires_at >= %s
                        LIMIT 1
                        """,
                        (cache_hash, provider, min_updated_at, now),
                    )
                    row = cursor.fetchone()
                    if not row:
                        return []
                    cursor.execute(
                        f"""
                        UPDATE `{self.table}`
                        SET hit_count = hit_count + 1, last_hit_at = %s
                        WHERE cache_hash = %s
                        """,
                        (now, cache_hash),
                    )
            payload = json.loads(row["payload_json"])
            rows = payload.get("hotels") if isinstance(payload, dict) else payload
            return rows if isinstance(rows, list) else []
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)
            return []

    def store(
        self,
        provider: str,
        *,
        target_key: str,
        target_name: str,
        radius_km: float,
        min_star: float,
        hotels: list[dict[str, Any]],
        expires_at: float,
    ) -> None:
        if not self.available() or not hotels:
            return
        cache_hash = cache_digest(f"{provider}:{target_key}:{float(radius_km):.1f}:{float(min_star):.1f}")
        now = time.time()
        payload = {"hotels": hotels}
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        INSERT INTO `{self.table}` (
                            cache_hash, provider, target_key, target_name,
                            radius_km, min_star, candidate_count, payload_json,
                            created_at, updated_at, expires_at, last_hit_at, hit_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 0)
                        ON DUPLICATE KEY UPDATE
                            target_name = VALUES(target_name),
                            radius_km = VALUES(radius_km),
                            min_star = VALUES(min_star),
                            candidate_count = VALUES(candidate_count),
                            payload_json = VALUES(payload_json),
                            updated_at = VALUES(updated_at),
                            expires_at = VALUES(expires_at)
                        """,
                        (
                            cache_hash,
                            provider,
                            target_key,
                            target_name,
                            float(radius_km),
                            float(min_star),
                            len(hotels),
                            json.dumps(payload, ensure_ascii=False),
                            now,
                            now,
                            expires_at,
                        ),
                    )
                    cursor.execute(f"DELETE FROM `{self.table}` WHERE expires_at < %s", (now,))
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)

    def clear(self, provider: str | None = None) -> None:
        if not self.available():
            return
        try:
            self.ensure_schema()
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    if provider:
                        cursor.execute(f"DELETE FROM `{self.table}` WHERE provider = %s", (provider,))
                    else:
                        cursor.execute(f"TRUNCATE TABLE `{self.table}`")
        except Exception as exc:  # pragma: no cover - depends on local MySQL
            self.remember_error(exc)

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self.connect(use_database=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE DATABASE IF NOT EXISTS `{self.database}`
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{self.table}` (
                        cache_hash CHAR(64) NOT NULL PRIMARY KEY,
                        provider VARCHAR(32) NOT NULL,
                        target_key CHAR(64) NOT NULL,
                        target_name VARCHAR(255) NULL,
                        radius_km DECIMAL(5,1) NOT NULL,
                        min_star DECIMAL(3,1) NOT NULL,
                        candidate_count INT NOT NULL DEFAULT 0,
                        payload_json LONGTEXT NOT NULL,
                        created_at DOUBLE NOT NULL,
                        updated_at DOUBLE NOT NULL,
                        expires_at DOUBLE NOT NULL,
                        last_hit_at DOUBLE NULL,
                        hit_count INT NOT NULL DEFAULT 0,
                        INDEX idx_provider_target (provider, target_key),
                        INDEX idx_updated_at (updated_at),
                        INDEX idx_expires_at (expires_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
        self._schema_ready = True
