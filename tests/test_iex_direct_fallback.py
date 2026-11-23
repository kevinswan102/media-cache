import unittest
from datetime import date

from scripts import iex_hist_daily_cache as iex


class DirectFallbackTests(unittest.TestCase):
    def test_skips_weekend_and_picks_newest_existing_file(self):
        seen = []

        def probe(url):
            seen.append(url)
            return 123 if "20261120" in url else None

        entry = iex._direct_entry(None, date(2026, 11, 23), probe=probe)  # Monday
        self.assertEqual(entry["date"], "20261120")
        self.assertEqual(entry["size"], 123)
        self.assertEqual((entry["feed"], entry["version"], entry["protocol"]), ("TOPS", "1.6", "IEXTP1"))
        self.assertIn("20261120_IEXTP1_TOPS1.6.pcap.gz", entry["link"])
        self.assertEqual(len(seen), 1)  # Sun/Sat skipped, Fri found first

    def test_requested_date_only_and_failure_raises(self):
        with self.assertRaises(RuntimeError):
            iex._direct_entry("20261112", date(2026, 11, 23), probe=lambda u: None)
        e = iex._direct_entry("20261112", date(2026, 11, 23), probe=lambda u: 5)
        self.assertEqual(e["date"], "20261112")


if __name__ == "__main__":
    unittest.main()
