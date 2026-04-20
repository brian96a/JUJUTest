from unittest.mock import patch

import pandas as pd

from src import api_client, transforms


def test_df_pedidos_dedupe_mismo_order_id():
    data = [
        {"order_id": "a", "user_id": "u1", "amount": 1, "currency": "USD", "created_at": "2025-01-01T00:00:00Z", "items": [], "metadata": {}},
        {"order_id": "a", "user_id": "u1", "amount": 2, "currency": "USD", "created_at": "2025-01-01T00:00:00Z", "items": [], "metadata": {}},
    ]
    df = transforms.df_pedidos(data)
    assert len(df) == 1
    assert df.iloc[0]["amount"] == 2


def test_df_pedidos_salta_sin_fecha():
    data = [{"order_id": "x", "created_at": None, "items": [], "metadata": {}}]
    df = transforms.df_pedidos(data)
    assert df.empty


def test_api_mock():
    with patch("src.api_client.requests.get") as g:
        g.return_value.status_code = 200
        g.return_value.json.return_value = [{"k": 1}]
        g.return_value.raise_for_status = lambda: None
        out = api_client.traer_pedidos_api("http://fake/api")
        assert out == [{"k": 1}]
