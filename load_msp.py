#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterator, Optional, Tuple

from lxml import etree
import psycopg
from dotenv import load_dotenv


# ---------- .env ----------
load_dotenv()


# ---------- настройки ----------
XML_DIR = Path(os.getenv("XML_DIR", "./xml"))

POSTGRES_DB = os.getenv("POSTGRES_DB", "mydb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

PG_TABLE = os.getenv("PG_TABLE", "msp_inn_region")

PG_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20000"))


# ---------- postgres ----------
DDL = f"""
CREATE TABLE IF NOT EXISTS {PG_TABLE} (
    inn         VARCHAR(12) PRIMARY KEY,   -- ИННФЛ (12) или ИННЮЛ (10)
    inn_type    CHAR(2)     NOT NULL,      -- 'IP' | 'UL'
    kodregion   VARCHAR(3)  NOT NULL,
    source_file TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

UPSERT_SQL = f"""
INSERT INTO {PG_TABLE} (inn, inn_type, kodregion, source_file)
VALUES (%s, %s, %s, %s)
ON CONFLICT (inn)
DO UPDATE SET
    inn_type    = EXCLUDED.inn_type,
    kodregion   = EXCLUDED.kodregion,
    source_file = EXCLUDED.source_file,
    updated_at  = now();
"""

SQL_CREATE_INDEX = f"""
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_msp_inn
ON {PG_TABLE} (inn);
"""


# ---------- xml parsing ----------
def extract_inn_and_type(doc: etree._Element) -> Tuple[Optional[str], Optional[str]]:
    """
    ИП: <ИПВклМСП ИННФЛ="...">
    ЮЛ: <ОргВклМСП ИННЮЛ="...">
    """
    ip_node = doc.find("ИПВклМСП")
    if ip_node is not None:
        inn = ip_node.get("ИННФЛ")
        if inn:
            return inn.strip(), "IP"

    ul_node = doc.find("ОргВклМСП")
    if ul_node is not None:
        inn = ul_node.get("ИННЮЛ")
        if inn:
            return inn.strip(), "UL"

    return None, None


def iter_rows_from_xml(xml_path: Path) -> Iterator[Tuple[str, str, str, str]]:
    try:
        for _, doc in etree.iterparse(
            str(xml_path),
            events=("end",),
            tag="Документ",
            recover=True,
            huge_tree=True,
        ):
            inn, inn_type = extract_inn_and_type(doc)

            mn_node = doc.find("СведМН")
            kodregion = (
                mn_node.get("КодРегион").strip()
                if mn_node is not None and mn_node.get("КодРегион")
                else None
            )

            if inn and inn_type and kodregion:
                yield inn, inn_type, kodregion, xml_path.name

            # чистка памяти
            doc.clear()
            while doc.getprevious() is not None:
                del doc.getparent()[0]

    except etree.XMLSyntaxError as e:
        print(f"[WARN] XML error in {xml_path}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] Failed to parse {xml_path}: {e}", file=sys.stderr)


def iter_all_rows(xml_dir: Path) -> Iterator[Tuple[str, str, str, str]]:
    for p in sorted(xml_dir.glob("*.xml")):
        yield from iter_rows_from_xml(p)


# ---------- index ----------
def create_index_after_load() -> None:
    """
    Создание индекса ПОСЛЕ загрузки данных.
    Отдельное соединение + autocommit (обязательно для CONCURRENTLY).
    """
    print("[INFO] Creating index idx_msp_inn (CONCURRENTLY)...")

    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_CREATE_INDEX)

    print("[INFO] Index ready")


# ---------- main ----------
def load_to_postgres() -> None:
    if not XML_DIR.exists():
        raise FileNotFoundError(f"XML_DIR not found: {XML_DIR.resolve()}")

    print(f"[INFO] XML_DIR   : {XML_DIR}")
    print(f"[INFO] PG_TABLE  : {PG_TABLE}")
    print(f"[INFO] Batch size: {BATCH_SIZE}")

    # 1️⃣ загрузка данных
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        total = 0
        buf: list[Tuple[str, str, str, str]] = []

        with conn.cursor() as cur:
            for rec in iter_all_rows(XML_DIR):
                buf.append(rec)

                if len(buf) >= BATCH_SIZE:
                    cur.executemany(UPSERT_SQL, buf)
                    conn.commit()
                    total += len(buf)
                    print(f"[INFO] Loaded: {total}")
                    buf.clear()

            if buf:
                cur.executemany(UPSERT_SQL, buf)
                conn.commit()
                total += len(buf)

    print(f"[DONE] Total rows loaded: {total}")

    # 2️⃣ индекс после загрузки
    create_index_after_load()


if __name__ == "__main__":
    load_to_postgres()
