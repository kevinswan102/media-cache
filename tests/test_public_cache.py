import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import build_cache_health, public_macro_cache


class PublicMacroCacheTests(unittest.TestCase):
    def test_build_snapshot_keeps_only_normalized_public_fields(self):
        now = datetime(2026, 8, 13, tzinfo=timezone.utc)
        with patch.object(
            public_macro_cache,
            "_fetch_json",
            side_effect=[
                {
                    "Results": {
                        "series": [
                            {
                                "seriesID": "LNS14000000",
                                "data": [
                                    {"value": "4.2", "periodName": "July", "year": "2026"},
                                    {"value": "4.1", "periodName": "June", "year": "2026"},
                                ],
                            }
                        ]
                    }
                },
                {
                    "data": [
                        {
                            "record_date": "2026-07-31",
                            "avg_interest_rate_amt": "3.71",
                            "security_desc": "Treasury Notes",
                            "cusip": "must-not-pass-through",
                        }
                    ]
                },
            ],
        ):
            snapshot = public_macro_cache.build_snapshot(now)

        self.assertEqual(snapshot["status"], "ok")
        self.assertEqual(snapshot["sources_ok"], ["bls", "treasury"])
        self.assertEqual(snapshot["bls"]["series"]["unemployment_rate"]["value"], 4.2)
        self.assertAlmostEqual(
            snapshot["bls"]["series"]["unemployment_rate"]["period_change_pct"],
            2.439,
        )
        self.assertNotIn("cusip", json.dumps(snapshot).lower())

    def test_one_failed_source_produces_usable_partial_snapshot(self):
        now = datetime(2026, 8, 13, tzinfo=timezone.utc)
        with patch.object(
            public_macro_cache,
            "_fetch_json",
            side_effect=[TimeoutError("BLS timeout"), {"data": [{"record_date": "2026-07-31"}]}],
        ):
            snapshot = public_macro_cache.build_snapshot(now)
        self.assertEqual(snapshot["status"], "partial")
        self.assertEqual(snapshot["sources_ok"], ["treasury"])


class CacheHealthTests(unittest.TestCase):
    def test_health_summarizes_without_copying_symbol_payloads(self):
        now = datetime(2026, 8, 13, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            iex_path = root / "iex.json"
            macro_path = root / "macro.json"
            iex_path.write_text(
                json.dumps(
                    {
                        "source": "iex_hist",
                        "as_of_date": "20260812",
                        "generated_at": now.isoformat(),
                        "symbols": {"AAPL": {"bars": {"Close": [1]}}, "MSFT": {}},
                        "coverage": {"sp500": {"iex_hist_rows": 497, "requested": 500}},
                    }
                )
            )
            macro_path.write_text(
                json.dumps(
                    {
                        "status": "ok",
                        "generated_at": now.isoformat(),
                        "sources_ok": ["bls", "treasury"],
                    }
                )
            )
            health = build_cache_health.build_health(iex_path, macro_path, now)

        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["artifacts"]["iex_hist_latest.json"]["symbol_count"], 2)
        self.assertNotIn("AAPL", json.dumps(health))


if __name__ == "__main__":
    unittest.main()
