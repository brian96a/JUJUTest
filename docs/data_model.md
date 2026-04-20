# Modelo de datos

Fuentes vs salida (como en `sql/redshift-ddl.sql`). Join pedido–usuario: DuckDB en memoria + `read_csv_auto` sobre usuarios.

Si el preview no dibuja nada: extensión **Markdown Preview Mermaid** en Cursor/VS Code, o abrí el archivo en GitHub (render Mermaid en bloques fenced).

## Entradas

```mermaid
flowchart TB
  subgraph api["api_orders.json"]
    o["cabecera pedido (order_id, user_id, amount, currency, created_at)"]
    it["items[] (sku, qty, price)"]
    md["metadata (source, promo)"]
    o --- it
    o --- md
  end
  subgraph users["users.csv"]
    u["user_id, email, created_at, country"]
  end
  subgraph prod["products.csv"]
    p["sku, name, category, price"]
  end
  o -.->|user_id| u
  it -.->|sku| p
```

## Curated (Parquet / Redshift)

```mermaid
flowchart TB
  subgraph dim["Dimensiones"]
    du["dim_user (user_id, email, created_at, country)"]
    dp["dim_product (sku, name, category, price)"]
  end
  subgraph fact["Hechos"]
    fo["fact_order (order_id, user_id, order_date, amount, currency, item_count, promo, source, email, country, items_json, created_at, ingest_ts)"]
  end
  du -->|user_id| fo
  dp -.->|sku en items_json| fo
```

Línea punteada: no hay FK física pedido→producto; los sku van en el JSON de líneas.
