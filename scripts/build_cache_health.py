#!/usr/bin/env python3
"""Summarize freshness and coverage of public cache artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def _read(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def build_health(iex_path: Path, macro_path: Path, now: datetime | None = None) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    iex = _read(iex_path)
    macro = _read(macro_path)
    symbols = iex.get("symbols") or {}
    coverage = iex.get("coverage") or {}
    iex_ok = iex.get("source") == "iex_hist" and bool(symbols)
    macro_ok = macro.get("status") in {"ok", "partial"} and bool(macro.get("sources_ok"))
    return {
        "schema_version": 1,
        "status": "ok" if iex_ok and macro_ok else "degraded",
        "generated_at": now.isoformat(),
        "artifacts": {
            "iex_hist_latest.json": {
                "status": "ok" if iex_ok else "invalid",
                "as_of_date": iex.get("as_of_date"),
                "generated_at": iex.get("generated_at"),
                "symbol_count": len(symbols),
                "coverage": {
                    label: {
                        "rows": row.get("iex_hist_rows"),
                        "requested": row.get("requested"),
                    }
                    for label, row in coverage.items()
                    if isinstance(row, dict)
                },
            },
            "public_macro_latest.json": {
                "status": macro.get("status"),
                "generated_at": macro.get("generated_at"),
                "sources_ok": macro.get("sources_ok", []),
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iex", type=Path, default=Path("data/iex_hist_latest.json"))
    parser.add_argument("--macro", type=Path, default=Path("data/public_macro_latest.json"))
    parser.add_argument("--output", type=Path, default=Path("data/cache_health.json"))
    args = parser.parse_args()
    health = build_health(args.iex, args.macro)
    if health["status"] != "ok":
        raise SystemExit("public cache health is degraded; preserving the prior health manifest")
    args.output.write_text(json.dumps(health, indent=2, sort_keys=True) + "\n")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
