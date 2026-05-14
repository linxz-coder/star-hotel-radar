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
