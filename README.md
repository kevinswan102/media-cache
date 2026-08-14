# media-cache
Static media cache.
This repository is a static media cache.

The `IEX Daily Cache` workflow streams one IEX HIST TOPS file through a FIFO,
keeps no raw PCAP, and commits only a compact one-day OHLCV cache plus a
coverage report. It filters to the configured S&P 500 + MidCap 400 universe
before writing output.

One HIST file is one trading day. The output is therefore a current/T+1 price
backfill, not a complete 52-week history. The downstream value route must
merge it with longer history or accumulate daily runs before using long-window
signals.

IEX data is venue-only and the cache retains the required attribution from the
[IEX Historical Data Terms](https://www.iex.io/legal/hist-data-terms).

The same weekday run also publishes two small, source-safe operational files:

- `data/public_macro_latest.json` caches normalized BLS and U.S. Treasury
  Fiscal Data values. It contains no API keys, customer data, trading state,
  proprietary identifiers, or unreleased picks. Downstream clients validate
  freshness and fall back to the agencies directly.
- `data/cache_health.json` summarizes artifact timestamps and IEX universe
  coverage without duplicating the price rows.

Keeping these updates in the existing IEX workflow adds no second schedule or
runner allocation. Private application code, paid API payloads, restricted
vendor data, alerts, and trading jobs deliberately remain outside this public
repository.
