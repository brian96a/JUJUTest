# ETL prueba técnica (local)

## Qué hace
Lee pedidos JSON (`sample_data/` o API), join usuarios CSV, escribe `output/raw` y `output/curated` (Parquet particionado + CSV de vista).

Diagrama ER (Mermaid): [docs/data_model.md](docs/data_model.md).

## Setup

```bash
cd /Users/brianavi/etl-test
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Correr el job

```bash
cd /Users/brianavi/etl-test
source .venv/bin/activate
python -m src.etl_job --input-json sample_data/api_orders.json --users sample_data/users.csv --products sample_data/products.csv --out output
```

Incremental (ejemplo):

```bash
python -m src.etl_job --since 2025-08-21T00:00:00Z --last-processed /tmp/etl_cp.json --input-json sample_data/api_orders.json --out output
```

API (mock real):

```bash
python -m src.etl_job --api-url https://httpbin.org/json --out output
```

## Tests

```bash
cd /Users/brianavi/etl-test
source .venv/bin/activate
pytest -q
```

## Tiempo y supuestos
- Esfuerzo total aproximado: **~8 h** (pipeline, datos de muestra, tests, SQL de ejemplo, docs y salidas en `output/`).
- Asumí CSV como fuente “SQL opcional”.
- Pedidos mal armados en JSON (no dict) se ignoran; sin `created_at` se descartan del curated.

## Limitaciones
- Merge idempotente lee parquet previo por partición: no escala a datasets enormes sin cambiar formato/tablas.
