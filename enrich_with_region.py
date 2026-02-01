#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import csv
from pathlib import Path
from typing import Dict, List

import psycopg
from dotenv import load_dotenv


# ---------- .env ----------
load_dotenv()


# ---------- настройки ----------
INPUT_FILE = Path(os.getenv("INPUT_FILE", "input.csv"))
OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE", "output_enriched.csv"))

PG_TABLE = os.getenv("PG_TABLE", "msp_inn_region")

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

PG_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

BATCH_SIZE = 10_000  # безопасно для IN (...)


# ---------- postgres ----------
SQL_LOOKUP = f"""
SELECT innfl, kodregion
FROM {PG_TABLE}
WHERE innfl = ANY(%s);
"""


def load_regions(conn, inns: List[str]) -> Dict[str, str]:
    """
    Возвращает dict: ИНН -> КодРегион
    """
    with conn.cursor() as cur:
        cur.execute(SQL_LOOKUP, (inns,))
        return {inn: region for inn, region in cur.fetchall()}


# ---------- main ----------
def enrich_file() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(INPUT_FILE)

    print(f"[INFO] Input : {INPUT_FILE}")
    print(f"[INFO] Output: {OUTPUT_FILE}")
    print(f"[INFO] Table : {PG_TABLE}")

    with psycopg.connect(PG_DSN) as conn, \
         INPUT_FILE.open("r", encoding="utf-8") as fin, \
         OUTPUT_FILE.open("w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin, delimiter=";")
        fieldnames = reader.fieldnames

        if not fieldnames or "ИНН" not in fieldnames or "Регион" not in fieldnames:
            raise ValueError("Ожидаются колонки: ИНН и Регион")

        writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()

        batch = []
        rows = []
        total = 0

        def flush():
            nonlocal total
            if not batch:
                return

            region_map = load_regions(conn, batch)

            for row in rows:
                inn = row["ИНН"].strip()
                if not row["Регион"] and inn in region_map:
                    row["Регион"] = region_map[inn]
                writer.writerow(row)
                total += 1

            batch.clear()
            rows.clear()

            if total % 10_000 == 0:
                print(f"[INFO] Processed: {total}")

        for row in reader:
            inn = row["ИНН"].strip()
            batch.append(inn)
            rows.append(row)

            if len(batch) >= BATCH_SIZE:
                flush()

        flush()

        print(f"[DONE] Rows processed: {total}")


if __name__ == "__main__":
    enrich_file()
