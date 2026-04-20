# Notas de diseño

## Stack
pandas + DuckDB. Spark descartado por coste de instalación y tamaño de datos de la prueba.

## Raw / curated
- Raw: `output/raw/orders.json` (payload API o archivo JSON leído) y **copia fiel** de `users.csv` y `products.csv` en el mismo directorio (`users.csv`, `products.csv`). Así la capa raw refleja todo lo ingestado en esa corrida, alineado al enunciado (API + fuente).
- Curated: Parquet bajo `fact_order/order_date=YYYY-MM-DD/part.parquet`; `fact_order_preview.csv`; dims en `dim_user/`, `dim_product/`.

## Particionado
`order_date` desde `created_at` UTC. COPY por prefijo S3 o por columna.

## Claves
Hechos: `order_id`. Dimensiones: `user_id`, `sku`.

## Idempotencia
Merge de parquet existente + nuevas filas; `drop_duplicates` por clave (`order_id` en fact, `user_id`/`sku` en dims), `keep=last` tras ordenar por clave e `ingest_ts` donde aplica.

## Incremental
`--since`: `created_at >= since`. `--last-processed`: JSON con `last_created_at`; filtro `created_at >`; al OK se persiste el máximo `created_at` del lote.

## API / errores
Tres reintentos en `api_client.py`. Pedidos sin `order_id` o `created_at` fuera del curated.

## Registros malformados (muestra)
En `sample_data/api_orders.json`: un elemento que no es objeto (`"not_an_object"`) se ignora; un pedido con `created_at: null` no pasa `pedido_ok` y no entra al `fact` en curated (hoy no hay tabla de rechazos en disco; mejora futura: landear rejects en raw o staging).

## Redshift
Parquet a S3; `COPY ... FORMAT AS PARQUET`. Dimensiones: COPY o staging + política de carga del equipo.

## Monitorización
Logs con run_id, conteos, max `created_at`, errores. Métricas: duración, filas escritas, fallos API. Alertas: fallo de job, SLA de duración, cero salidas inesperadas.

En local, cada corrida escribe además un archivo en `output/logs/run_<UTC>_<run_id>.log` (mismo contenido que consola): sirve como artefacto de auditoría parecido a exportar un tail de CloudWatch, sin AWS.

## Trade-offs
No hay conexión a SQL Server en tiempo de ejecución: usuarios y productos vienen de CSV para que cualquiera clone el repo y corra el job sin instalar base de datos ni drivers.

Para no duplicar pedidos al re-ejecutar, el job vuelve a leer los Parquet que ya escribió en disco y los mezcla con lo nuevo. Eso está bien para el volumen de una prueba o un negocio chico; si un día los datos fueran enormes, habría que cambiar de enfoque (por ejemplo tablas tipo Iceberg en el lago, donde el motor ya sabe versionar y compactar sin leer todo el historial a mano).
