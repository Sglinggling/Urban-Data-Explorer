import io
import os
import time

import pandas as pd
import requests


def collect_csv(outputfile, url, retries=3, timeout=30):
    output_dir = os.path.join("data", "bronze")
    os.makedirs(output_dir, exist_ok=True)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text), sep=";", encoding="utf-8")
            output_path = os.path.join(output_dir, outputfile)
            df.to_csv(output_path, index=False, encoding="utf-8")
            print(f"[COLLECT] OK: {output_path}")
            return
        except Exception as e:
            last_err = e
            if attempt < retries:
                wait = 2 ** attempt
                print(f"[COLLECT] Tentative {attempt}/{retries} échouée pour {outputfile}, retry dans {wait}s… ({e})")
                time.sleep(wait)

    raise RuntimeError(
        f"Échec après {retries} tentatives pour {outputfile} ({url}): {last_err}"
    )
