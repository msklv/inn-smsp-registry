# inn-smsp-registry

Сопоставление списка ИНН с реестром Единый реестр субъектов малого и среднего предпринимательства ФНС

## Исходные данные

- https://www.nalog.gov.ru/opendata/7707329152-rsmp/
- Дата актуальности - 10.02.2026
- `data-10012026-structure-10062025` - каталог с данными

## Запуск приложения

```bash
# Поднимаем PG
docker compose up -d

# Создаем виртуальное окружение
uv venv
source .venv/bin/activate

# установка зависимостей из pyproject.toml 
uv sync 

# Запускаем импорт
python load_msp.py
uv run python load_msp.py

# Запускаем обогащение 
uv run python enrich_with_region.py
```

## Для ускорения БД
```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_msp_inn
ON msp_inn_region (innfl);
```