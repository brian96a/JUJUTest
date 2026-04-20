import argparse
import json
import logging
import os
import shutil
import uuid
from datetime import datetime, timezone

import pandas as pd

from src import api_client, transforms

log = logging.getLogger("etl")


def _configurar_logging(log_dir: str) -> tuple[str, str]:
    """Consola + archivo por corrida (estilo artefacto de ejecución tipo CloudWatch en disco)."""
    os.makedirs(log_dir, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(log_dir, f"run_{ts}_{run_id}.log")
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    log.setLevel(logging.INFO)
    log.propagate = False
    log.handlers.clear()
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return path, run_id


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

    log_dir = os.path.join(args.out, "logs")
    log_path, run_id = _configurar_logging(log_dir)
    log.info(
        "inicio run_id=%s log_file=%s out=%s api_url=%s since=%s last_processed=%s",
        run_id,
        log_path,
        args.out,
        args.api_url,
        args.since,
        args.last_processed,
    )

    raw_dir = os.path.join(args.out, "raw")
    cur = os.path.join(args.out, "curated")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(cur, exist_ok=True)

    raw_users = os.path.join(raw_dir, "users.csv")
    raw_products = os.path.join(raw_dir, "products.csv")
    shutil.copy2(args.users, raw_users)
    shutil.copy2(args.products, raw_products)
    log.info("raw: copiados users.csv y products.csv")

    if args.api_url:
        data = api_client.traer_pedidos_api(args.api_url)
    else:
        data = api_client.leer_json_archivo(args.input_json)

    with open(os.path.join(raw_dir, "orders.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    df = transforms.df_pedidos(data)
    log.info("pedidos validos tras parseo: %s filas", len(df))
    if df.empty:
        log.info("fin_sin_salida run_id=%s razon=sin_pedidos_validos log_file=%s", run_id, log_path)
        return

    last_ts = leer_checkpoint(args.last_processed)
    df = filtro_incremental(df, args.since, last_ts)
    log.info("pedidos tras filtro incremental: %s filas", len(df))
    if df.empty:
        log.info("fin_sin_salida run_id=%s razon=filtro_incremental_vacio log_file=%s", run_id, log_path)
        return

    df = transforms.join_users(df, raw_users)
    run_ts = datetime.now(timezone.utc).isoformat()
    df["ingest_ts"] = run_ts

    dim_u = pd.read_csv(raw_users)
    dim_p = transforms.dim_productos(raw_products)

    os.makedirs(os.path.join(cur, "dim_user"), exist_ok=True)
    merge_parquet(dim_u, os.path.join(cur, "dim_user", "dim_user.parquet"), "user_id")

    os.makedirs(os.path.join(cur, "dim_product"), exist_ok=True)
    merge_parquet(dim_p, os.path.join(cur, "dim_product", "dim_product.parquet"), "sku")

    fact_base = os.path.join(cur, "fact_order")
    n_particiones = 0
    for fecha, g in df.groupby("order_date"):
        carpeta = os.path.join(fact_base, f"order_date={fecha}")
        os.makedirs(carpeta, exist_ok=True)
        arch = os.path.join(carpeta, "part.parquet")
        merge_parquet(g, arch, "order_id")
        n_particiones += 1
        log.info("fact_order particion order_date=%s filas=%s -> %s", fecha, len(g), arch)

    df.to_csv(os.path.join(cur, "fact_order_preview.csv"), index=False)

    max_ts = df["created_at"].max()
    guardar_checkpoint(args.last_processed, max_ts.isoformat())
    log.info(
        "fin_ok run_id=%s filas_fact=%s particiones=%s max_created_at=%s log_file=%s",
        run_id,
        len(df),
        n_particiones,
        max_ts,
        log_path,
    )


if __name__ == "__main__":
    main()
