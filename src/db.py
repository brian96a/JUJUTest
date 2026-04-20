import os

import pandas as pd


def leer_users_csv(path):
    return pd.read_csv(path)


def leer_products_csv(path):
    return pd.read_csv(path)


def leer_users_db():
    """MSSQL no implementado; ingest por CSV."""
    conn = os.getenv("MSSQL_CONN")
    if not conn:
        return None
    raise NotImplementedError("MSSQL: usar CSV o agregar pyodbc/sqlalchemy en tu entorno")
