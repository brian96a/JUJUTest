import json
from datetime import datetime

import duckdb
import pandas as pd


def lista_a_pedidos(lista):
    out = []
    if not isinstance(lista, list):
        return out
    for fila in lista:
        if not isinstance(fila, dict):
            continue
        out.append(fila)
    return out


def pedido_ok(p):
    if not p.get("order_id"):
        return False, "sin order_id"
    if not p.get("created_at"):
        return False, "sin created_at"
    return True, ""


def contar_items(items):
    if not items or not isinstance(items, list):
        return 0
    n = 0
    for it in items:
        if isinstance(it, dict) and it.get("sku"):
            n += int(it.get("qty") or 0)
    return n


def aplanar_pedidos(lista):
    filas = []
    for p in lista_a_pedidos(lista):
        ok, razon = pedido_ok(p)
        if not ok:
            filas.append({"_skip": True, "razon": razon, "raw": json.dumps(p, ensure_ascii=False)})
            continue
        items = p.get("items") or []
        meta = p.get("metadata") or {}
        filas.append(
            {
                "_skip": False,
                "order_id": p["order_id"],
                "user_id": p.get("user_id"),
                "amount": float(p.get("amount") or 0),
                "currency": p.get("currency") or "USD",
                "created_at": p["created_at"],
                "item_count": contar_items(items),
                "promo": meta.get("promo"),
                "source": meta.get("source") or "api",
                "items_json": json.dumps(items, ensure_ascii=False),
            }
        )
    return filas


def df_pedidos(lista):
    rows = aplanar_pedidos(lista)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[df["_skip"] == False].copy()  # noqa: E712
    if df.empty:
        return df
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["order_date"] = df["created_at"].dt.strftime("%Y-%m-%d")
    df = df.sort_values(["order_id", "created_at"]).drop_duplicates("order_id", keep="last")
    return df.drop(columns=["_skip"])


def join_users(df_orders, path_users):
    con = duckdb.connect(database=":memory:")
    con.register("orders", df_orders)
    sql = f"""
    SELECT o.*, u.email, u.country
    FROM orders o
    LEFT JOIN read_csv_auto('{path_users}') u ON o.user_id = u.user_id
    """
    return con.execute(sql).df()


def dim_productos(path_products):
    return pd.read_csv(path_products)
