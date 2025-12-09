import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import requests


DEFAULT_PROXY = "http://47.109.97.125:8080/tushare"
DEFAULT_APIS: List[Dict[str, Any]] = [
    {
        "api_name": "stock_basic",
        "params": {"list_status": "L"},
        "fields": "ts_code,name,area,industry,list_date",
    },
    {
        "api_name": "trade_cal",
        "params": {"exchange": "SSE"},
        "fields": "exchange,cal_date,is_open,pretrade_date",
    },
]


def load_api_config() -> Iterable[Dict[str, Any]]:
    """Allow overriding the API list via env TUSHARE_APIS (JSON string)."""
    raw = os.getenv("TUSHARE_APIS")
    if not raw:
        return DEFAULT_APIS
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return DEFAULT_APIS


def fetch_one(api: Dict[str, Any], token: str, proxy_url: str) -> pd.DataFrame:
    payload = {
        "api_name": api["api_name"],
        "token": token,
        "params": api.get("params", {}) or {},
        "fields": api.get("fields"),
    }
    resp = requests.post(proxy_url, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"{api['api_name']} failed: {data}")

    rows = data.get("data", {}).get("items") or []
    cols = data.get("data", {}).get("fields") or []
    return pd.DataFrame(rows, columns=cols)


def save_df(df: pd.DataFrame, api_name: str, date_str: str, fmt: str = "parquet") -> None:
    out_dir = Path("data") / api_name
    out_dir.mkdir(parents=True, exist_ok=True)

    fmt = fmt.lower()
    if fmt not in {"parquet", "csv"}:
        fmt = "parquet"

    if fmt == "parquet":
        df.to_parquet(out_dir / f"{api_name}_{date_str}.parquet", index=False)
        df.to_parquet(out_dir / "latest.parquet", index=False)
    else:
        df.to_csv(out_dir / f"{api_name}_{date_str}.csv", index=False)
        df.to_csv(out_dir / "latest.csv", index=False)


def main() -> None:
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise SystemExit("TUSHARE_TOKEN is required")

    proxy_url = os.getenv("TUSHARE_PROXY", DEFAULT_PROXY)
    fmt = os.getenv("TUSHARE_FORMAT", "parquet")
    date_str = os.getenv("DATA_DATE") or datetime.date.today().strftime("%Y%m%d")

    apis = list(load_api_config())
    for api in apis:
        df = fetch_one(api, token=token, proxy_url=proxy_url)
        save_df(df, api_name=api["api_name"], date_str=date_str, fmt=fmt)
        print(f"{api['api_name']}: rows={len(df)} saved as {fmt}")


if __name__ == "__main__":
    main()
