#!/usr/bin/env python3
"""Build a compact cache from no-key US government macro APIs.

The artifact intentionally contains only BLS and Treasury values already
published by federal agencies.  It is a latency/rate-limit cache for downstream
consumers, not a replacement authority: consumers must validate freshness and
fall back to the agencies directly when this file is stale or malformed.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
TREASURY_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    "v2/accounting/od/avg_interest_rates"
)
USER_AGENT = "Top5StocksPublicCache/1.0 top5stocksdaily@gmail.com"
BLS_SERIES = {
    "CUSR0000SA0": "cpi_all_urban_sa",
    "LNS14000000": "unemployment_rate",
    "CES0000000001": "nonfarm_payrolls",
    "WPSFD4": "ppi_final_demand",
}


def _safe_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
        return parsed if parsed == parsed else None
    except (TypeError, ValueError):
        return None


def _fetch_json(
    url: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    if params:
        url = f"{url}?{urlencode(params)}"
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, headers=headers, method="POST" if body else "GET")
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _latest_series_value(series: Dict[str, Any]) -> Dict[str, Any]:
    rows = series.get("data") or []
    if not rows:
        return {}
    latest = rows[0]
    prior = rows[1] if len(rows) > 1 else {}
    latest_value = _safe_float(latest.get("value"))
    prior_value = _safe_float(prior.get("value"))
    change = None
    if latest_value is not None and prior_value not in {None, 0}:
        change = round(((latest_value - prior_value) / prior_value) * 100, 3)
    return {
        "value": latest_value,
        "period": latest.get("periodName") or latest.get("period"),
        "year": latest.get("year"),
        "prior_value": prior_value,
        "period_change_pct": change,
    }


def fetch_bls_snapshot(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    out: Dict[str, Any] = {
        "status": "unavailable",
        "source": "BLS Public Data API",
        "series": {},
        "last_updated": now.isoformat(),
    }
    try:
        data = _fetch_json(
            BLS_URL,
            payload={
                "seriesid": list(BLS_SERIES),
                "startyear": str(now.year - 1),
                "endyear": str(now.year),
            },
        )
        for row in data.get("Results", {}).get("series", []):
            series_id = row.get("seriesID")
            label = BLS_SERIES.get(series_id, series_id)
            out["series"][label] = _latest_series_value(row)
        out["status"] = "ok" if out["series"] else "empty"
        out["message"] = data.get("message")
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def fetch_treasury_snapshot(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    out: Dict[str, Any] = {
        "status": "unavailable",
        "source": "Treasury Fiscal Data API",
        "last_updated": now.isoformat(),
    }
    try:
        data = _fetch_json(TREASURY_URL, params={"sort": "-record_date", "page[size]": 1})
        rows = data.get("data", [])
        latest = rows[0] if rows else {}
        out.update(
            {
                "status": "ok" if latest else "empty",
                "record_date": latest.get("record_date"),
                "avg_interest_rate_amt": _safe_float(latest.get("avg_interest_rate_amt")),
                "security_desc": latest.get("security_desc"),
            }
        )
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def build_snapshot(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    bls = fetch_bls_snapshot(now)
    treasury = fetch_treasury_snapshot(now)
    sources_ok = [
        name
        for name, row in (("bls", bls), ("treasury", treasury))
        if row.get("status") == "ok"
    ]
    return {
        "schema_version": 1,
        "status": "ok" if len(sources_ok) == 2 else ("partial" if sources_ok else "unavailable"),
        "source": "us_federal_public_data",
        "generated_at": now.isoformat(),
        "sources_ok": sources_ok,
        "bls": bls,
        "treasury": treasury,
        "attribution": [
            {"name": "U.S. Bureau of Labor Statistics", "url": "https://www.bls.gov/developers/"},
            {"name": "U.S. Treasury Fiscal Data", "url": "https://fiscaldata.treasury.gov/api-documentation/"},
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/public_macro_latest.json"))
    args = parser.parse_args()
    snapshot = build_snapshot()
    if snapshot["status"] == "unavailable":
        raise SystemExit("Both public macro sources were unavailable; preserving the prior cache")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    print(f"wrote {args.output} ({snapshot['status']}; {','.join(snapshot['sources_ok'])})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
