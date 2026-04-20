CREATE TABLE IF NOT EXISTS dim_user (
  user_id VARCHAR(64) PRIMARY KEY,
  email VARCHAR(255),
  created_at DATE,
  country VARCHAR(8)
);

CREATE TABLE IF NOT EXISTS dim_product (
  sku VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255),
  category VARCHAR(100),
  price DECIMAL(12,2)
);

CREATE TABLE IF NOT EXISTS fact_order (
  order_id VARCHAR(64) NOT NULL,
  user_id VARCHAR(64),
  order_date DATE NOT NULL,
  amount DECIMAL(14,2),
  currency VARCHAR(8),
  item_count INTEGER,
  promo VARCHAR(128),
  source VARCHAR(64),
  email VARCHAR(255),
  country VARCHAR(8),
  items_json VARCHAR(65535),
  created_at TIMESTAMP,
  ingest_ts TIMESTAMP
);

-- dedupe por order_id (última ingest_ts)
WITH ranked AS (
  SELECT
    order_id,
    user_id,
    order_date,
    amount,
    currency,
    item_count,
    promo,
    source,
    email,
    country,
    items_json,
    created_at,
    ingest_ts,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY ingest_ts DESC, created_at DESC
    ) AS rn
  FROM fact_order
)
SELECT
  order_id,
  user_id,
  order_date,
  amount,
  currency,
  item_count,
  promo,
  source,
  email,
  country,
  items_json,
  created_at,
  ingest_ts
FROM ranked
WHERE rn = 1;

-- agregado por fecha y país
SELECT
  order_date,
  country,
  COUNT(*) AS ordenes,
  SUM(amount) AS monto_total,
  SUM(item_count) AS unidades_items
FROM fact_order
GROUP BY 1, 2
ORDER BY order_date DESC, country;

-- agregado por país (dim_user)
SELECT
  u.country,
  COUNT(DISTINCT f.order_id) AS ordenes_distintas,
  SUM(f.amount) AS monto_total
FROM fact_order f
JOIN dim_user u ON u.user_id = f.user_id
GROUP BY 1
ORDER BY monto_total DESC;

-- agregado por moneda y promo
SELECT
  currency,
  COALESCE(promo, '(sin promo)') AS promo_etiqueta,
  COUNT(*) AS ordenes,
  SUM(amount) AS monto_total
FROM fact_order
GROUP BY 1, 2
ORDER BY currency, monto_total DESC;
