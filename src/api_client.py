import json
import time

import requests


def leer_json_archivo(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def traer_pedidos_api(url, intentos=3):
    ultimo_error = None
    for i in range(intentos):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            ultimo_error = e
            if i < intentos - 1:
                time.sleep(2)
    raise ultimo_error
