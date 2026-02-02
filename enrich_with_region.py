#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import csv
from pathlib import Path
from typing import Dict, List, Tuple

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

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))


# ---------- SQL ----------
SQL_LOOKUP = f"""
SELECT inn, kodregion
FROM {PG_TABLE}
WHERE inn = ANY(%s);
"""


def normalize_inn(raw: str) -> str:
    """
    Оставляет только цифры (на случай пробелов/кавычек/прочего мусора),
    ведущие нули сохраняются.
    """
    return "".join(ch for ch in (raw or "").strip() if ch.isdigit())


def load_regions(conn, inns: List[str]) -> Dict[str, str]:
    """
    Возвращает dict: inn -> kodregion
    """
    with conn.cursor() as cur:
        cur.execute(SQL_LOOKUP, (inns,))
        return {inn: region for inn, region in cur.fetchall()}


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

        if not fieldnames:
            raise ValueError("Пустой файл или не удалось прочитать заголовок")

        # ожидаем эти колонки, как в примере пользователя
        if "ИНН" not in fieldnames or "Регион" not in fieldnames:
            raise ValueError("Ожидаются колонки: ИНН и Регион (разделитель ';')")

        writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()

        batch_inns: List[str] = []
        batch_rows: List[dict] = []
        total = 0

        def flush():
            nonlocal total
            if not batch_inns:
                return

            region_map = load_regions(conn, batch_inns)

            for row in batch_rows:
                inn = normalize_inn(row.get("ИНН", ""))
                if inn and (not (row.get("Регион") or "").strip()):
                    reg = region_map.get(inn)
                    if reg:
                        row["Регион"] = reg
                writer.writerow(row)
                total += 1

            batch_inns.clear()
            batch_rows.clear()

            if total % 10000 == 0:
                print(f"[INFO] Processed: {total}")

        for row in reader:
            inn = normalize_inn(row.get("ИНН", ""))
            batch_inns.append(inn)
            batch_rows.append(row)

            if len(batch_inns) >= BATCH_SIZE:
                flush()

        flush()
        
        print(f"[DONE] Rows processed: {total}")


if __name__ == "__main__":
    enrich_file()
