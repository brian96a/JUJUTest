import argparse
import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd

from src import api_client, transforms

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("etl")


def leer_checkpoint(path):
    if not path or not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f).get("last_created_at")


def guardar_checkpoint(path, ts_iso):
    if not path:
        return
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"last_created_at": ts_iso}, f)


def filtro_incremental(df, since, last_ts):
    out = df
    if since:
        s = pd.to_datetime(since, utc=True)
        out = out[out["created_at"] >= s]
    if last_ts:
        t = pd.to_datetime(last_ts, utc=True)
        out = out[out["created_at"] > t]
    return out


def merge_parquet(df_nuevo, path_archivo, clave):
    if os.path.isfile(path_archivo):
        viejo = pd.read_parquet(path_archivo)
        mix = pd.concat([viejo, df_nuevo], ignore_index=True)
        orden = [clave] + (["ingest_ts"] if "ingest_ts" in mix.columns else [])
        mix = mix.sort_values(orden).drop_duplicates(clave, keep="last")
    else:
        mix = df_nuevo
    mix.to_parquet(path_archivo, index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-json", default="sample_data/api_orders.json")
    ap.add_argument("--api-url", default=None, help="Si viene, ignora input-json y pega a la API")
    ap.add_argument("--users", default="sample_data/users.csv")
    ap.add_argument("--products", default="sample_data/products.csv")
    ap.add_argument("--out", default="output")
    ap.add_argument("--since", default=None, help="ISO fecha/hora, filtra created_at >=")
    ap.add_argument("--last-processed", default=None, help="Archivo JSON con last_created_at")
    args = ap.parse_args()

    raw_dir = os.path.join(args.out, "raw")
    cur = os.path.join(args.out, "curated")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(cur, exist_ok=True)

    if args.api_url:
        data = api_client.traer_pedidos_api(args.api_url)
    else:
        data = api_client.leer_json_archivo(args.input_json)

    with open(os.path.join(raw_dir, "orders.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    df = transforms.df_pedidos(data)
    if df.empty:
        log.info("sin pedidos validos")
        return

    last_ts = leer_checkpoint(args.last_processed)
    df = filtro_incremental(df, args.since, last_ts)
    if df.empty:
        log.info("nada que procesar con los filtros")
        return

    df = transforms.join_users(df, args.users)
    run_ts = datetime.now(timezone.utc).isoformat()
    df["ingest_ts"] = run_ts

    dim_u = pd.read_csv(args.users)
    dim_p = transforms.dim_productos(args.products)

    os.makedirs(os.path.join(cur, "dim_user"), exist_ok=True)
    merge_parquet(dim_u, os.path.join(cur, "dim_user", "dim_user.parquet"), "user_id")

    os.makedirs(os.path.join(cur, "dim_product"), exist_ok=True)
    merge_parquet(dim_p, os.path.join(cur, "dim_product", "dim_product.parquet"), "sku")

    fact_base = os.path.join(cur, "fact_order")
    for fecha, g in df.groupby("order_date"):
        carpeta = os.path.join(fact_base, f"order_date={fecha}")
        os.makedirs(carpeta, exist_ok=True)
        arch = os.path.join(carpeta, "part.parquet")
        merge_parquet(g, arch, "order_id")

    df.to_csv(os.path.join(cur, "fact_order_preview.csv"), index=False)

    max_ts = df["created_at"].max()
    guardar_checkpoint(args.last_processed, max_ts.isoformat())
    log.info("listo, max created_at=%s", max_ts)


if __name__ == "__main__":
    main()
