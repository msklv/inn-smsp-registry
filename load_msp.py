#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterator, Tuple

from lxml import etree
import psycopg
from dotenv import load_dotenv


# ---------- .env ----------
load_dotenv()  # автоматически ищет .env рядом с файлом / cwd


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


# ---------- postgres ----------
DDL = f"""
CREATE TABLE IF NOT EXISTS {PG_TABLE} (
    innfl       VARCHAR(12) PRIMARY KEY,
    kodregion   VARCHAR(3)  NOT NULL,
    source_file TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

UPSERT_SQL = f"""
INSERT INTO {PG_TABLE} (innfl, kodregion, source_file)
VALUES (%s, %s, %s)
ON CONFLICT (innfl)
DO UPDATE SET
    kodregion   = EXCLUDED.kodregion,
    source_file = EXCLUDED.source_file,
    updated_at  = now();
"""


# ---------- xml parsing ----------
def iter_pairs_from_xml(xml_path: Path) -> Iterator[Tuple[str, str]]:
    try:
        for _, doc in etree.iterparse(
            str(xml_path),
            events=("end",),
            tag="Документ",
            recover=True,
            huge_tree=True,
        ):
            innfl = None
            kodregion = None

            ip_node = doc.find("ИПВклМСП")
            if ip_node is not None:
                innfl = ip_node.get("ИННФЛ")

            mn_node = doc.find("СведМН")
            if mn_node is not None:
                kodregion = mn_node.get("КодРегион")

            if innfl and kodregion:
                yield innfl.strip(), kodregion.strip()

            # чистим память
            doc.clear()
            while doc.getprevious() is not None:
                del doc.getparent()[0]

    except etree.XMLSyntaxError as e:
        print(f"[WARN] XML error in {xml_path}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] Failed to parse {xml_path}: {e}", file=sys.stderr)


def iter_all_pairs(xml_dir: Path) -> Iterator[Tuple[str, str, str]]:
    for p in sorted(xml_dir.glob("*.xml")):
        for innfl, kodregion in iter_pairs_from_xml(p):
            yield innfl, kodregion, p.name


# ---------- main ----------
def load_to_postgres(batch_size: int = 5000) -> None:
    if not XML_DIR.exists():
        raise FileNotFoundError(f"XML_DIR not found: {XML_DIR.resolve()}")

    print(f"[INFO] XML_DIR: {XML_DIR}")
    print(f"[INFO] PG_TABLE: {PG_TABLE}")
    print(f"[INFO] Connecting to: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        total = 0
        buffer = []

        with conn.cursor() as cur:
            for row in iter_all_pairs(XML_DIR):
                buffer.append(row)

                if len(buffer) >= batch_size:
                    cur.executemany(UPSERT_SQL, buffer)
                    conn.commit()
                    total += len(buffer)
                    print(f"[INFO] Upserted: {total}")
                    buffer.clear()

            if buffer:
                cur.executemany(UPSERT_SQL, buffer)
                conn.commit()
                total += len(buffer)

        print(f"[DONE] Total rows processed: {total}")


if __name__ == "__main__":
    load_to_postgres()
