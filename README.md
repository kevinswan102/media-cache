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
