#!/usr/bin/env python3
"""Stream one IEX HIST TOPS file into a compact daily-price cache.

The raw PCAP is deliberately never written to disk.  A named pipe connects
curl to ``iex-to-csv``; only rows for the requested universe are emitted by
the parser and only one OHLCV row per symbol is retained in the final JSON.

This job is intentionally a daily/current-price backfill.  One IEX HIST file
contains one trading day, so it cannot manufacture a 52-week history.  The
consumer must merge these rows with an existing history cache or accumulate
them over successive runs before using long-window technical signals.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import subprocess
import struct
import tempfile
import sys
import time
from typing import Any, Iterable
from urllib.request import Request, urlopen


CATALOG_URL = "https://iextrading.com/api/1.0/hist"
HF_SYMBOLS_URL = "https://api.hfdatalibrary.com/v1/symbols"
IEX_ATTRIBUTION = (
    "Data provided for free by IEX. By accessing or using IEX Historical Data, "
    "you agree to the IEX Historical Data Terms of Use."
)
IEX_TERMS_URL = "https://www.iex.io/legal/hist-data-terms"


def _get_json(url: str) -> Any:
    request = Request(url, headers={"User-Agent": "Top5Stocks-IEX-Ingest/1.0"})
    with urlopen(request, timeout=90) as response:
        return json.load(response)


def _as_symbols(payload: Any) -> set[str]:
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key in ("symbol", "ticker"):
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    found.add(candidate.strip().upper())
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return found


def _load_universe(path: Path) -> dict[str, set[str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("coverage universe must be an object of named symbol lists")
    groups: dict[str, set[str]] = {}
    for label, values in payload.items():
        if not isinstance(values, list):
            raise ValueError(f"coverage universe group {label!r} is not a list")
        groups[str(label)] = {
            str(symbol).strip().upper()
            for symbol in values
            if str(symbol).strip()
        }
    if not groups or not set().union(*groups.values()):
        raise ValueError("coverage universe is empty")
    return groups


def _choose_tops(catalog: dict[str, Any], requested_date: str | None) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for date, rows in catalog.items():
        if requested_date and date != requested_date:
            continue
        for row in rows or []:
            if row.get("feed") == "TOPS" and row.get("version") == "1.6":
                entries.append({**row, "date": row.get("date") or date})
    if not entries:
        raise RuntimeError("No IEX TOPS v1.6 catalog entry matched the requested date")
    return max(entries, key=lambda row: row.get("date", ""))


def _run_streaming_decode(
    entry: dict[str, Any],
    symbols: Iterable[str],
    output_root: Path,
) -> dict[str, dict[str, Any]]:
    date = str(entry["date"])
    protocol = str(entry.get("protocol") or "IEXTP1")
    version = str(entry.get("version") or "1.6")
    filename = f"data_feeds_{date}_{date}_{protocol}_TOPS{version}.pcap.gz"
    fifo = output_root / filename
    os.mkfifo(fifo)

    symbols_path = output_root / "requested_symbols.json"
    stats_path = output_root / "stream_stats.json"
    symbols_path.write_text(
        json.dumps(sorted(set(symbols))) + "\n", encoding="utf-8"
    )
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--parse-fifo",
        str(fifo),
        "--symbols-file",
        str(symbols_path),
        "--output",
        str(stats_path),
        "--protocol",
        "TOPSv1.6",
    ]

    parser_process: subprocess.Popen[bytes] | None = None
    download_process: subprocess.Popen[bytes] | None = None
    try:
        parser_process = subprocess.Popen(command)
        download_process = subprocess.Popen([
            "curl",
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            "--retry",
            "2",
            "--retry-all-errors",
            "--output",
            str(fifo),
            str(entry["link"]),
        ])
        while parser_process.poll() is None or download_process.poll() is None:
            parser_rc = parser_process.poll()
            download_rc = download_process.poll()
            if parser_rc is not None and parser_rc != 0:
                raise RuntimeError(f"stream parser failed with exit code {parser_rc}")
            if download_rc is not None and download_rc != 0:
                raise RuntimeError(f"IEX download failed with exit code {download_rc}")
            time.sleep(0.5)
        if parser_process.returncode != 0:
            raise RuntimeError(
                f"stream parser failed with exit code {parser_process.returncode}"
            )
        if download_process.returncode != 0:
            raise RuntimeError(
                f"IEX download failed with exit code {download_process.returncode}"
            )
    finally:
        for process in (download_process, parser_process):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=30)
    try:
        return json.loads(stats_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("stream parser did not produce valid statistics") from exc


def _read_exact(stream: Any, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise EOFError("truncated IEX PCAP stream")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _iter_pcap_frames(stream: Any, first_magic: bytes) -> Iterable[bytes]:
    """Yield Ethernet frames from classic PCAP or PCAPNG bytes."""
    if first_magic == b"\x0a\x0d\x0d\x0a":
        # IEX HIST TOPS 1.6 is PCAPNG.  The section header's byte-order magic
        # determines the endianness for all subsequent block lengths.
        next_header = first_magic + _read_exact(stream, 4)
        endian: str | None = None
        linktype: int | None = None
        while True:
            block_type = struct.unpack("<I", next_header[:4])[0]
            raw_length = next_header[4:8]
            if block_type == 0x0A0D0D0A:
                # The first section header is little-endian in IEX files.  If
                # a future file is big-endian, inspect both interpretations.
                little_length = struct.unpack("<I", raw_length)[0]
                big_length = struct.unpack(">I", raw_length)[0]
                block_length = (
                    little_length
                    if 12 <= little_length <= 1024 * 1024 * 1024
                    else big_length
                )
                body = _read_exact(stream, block_length - 12)
                _read_exact(stream, 4)  # duplicated trailing block length
                byte_order = body[:4]
                if byte_order == b"\x4d\x3c\x2b\x1a":
                    endian = "<"
                elif byte_order == b"\x1a\x2b\x3c\x4d":
                    endian = ">"
                else:
                    raise RuntimeError(f"unsupported PCAPNG byte order: {byte_order!r}")
            else:
                if endian is None:
                    raise RuntimeError("PCAPNG section header missing")
                block_length = struct.unpack(endian + "I", raw_length)[0]
                if block_length < 12:
                    raise RuntimeError(f"invalid PCAPNG block length: {block_length}")
                body = _read_exact(stream, block_length - 12)
                _read_exact(stream, 4)
                if block_type == 0x00000001:  # Interface Description Block
                    linktype = struct.unpack(endian + "H", body[:2])[0]
                elif block_type == 0x00000006:  # Enhanced Packet Block
                    if linktype not in (None, 1):
                        continue
                    if len(body) < 20:
                        continue
                    _interface, _ts_high, _ts_low, captured, _original = struct.unpack(
                        endian + "IIIII", body[:20]
                    )
                    yield body[20:20 + captured]
                elif block_type == 0x00000002:  # legacy Packet Block
                    if linktype not in (None, 1) or len(body) < 24:
                        continue
                    _interface, _drops, _ts_high, _ts_low, captured, _original = struct.unpack(
                        endian + "IIIIII", body[:24]
                    )
                    yield body[24:24 + captured]
                elif block_type == 0x00000003:  # Simple Packet Block
                    if linktype in (None, 1) and len(body) >= 4:
                        yield body[4:]
            try:
                next_header = _read_exact(stream, 8)
            except EOFError:
                return
    else:
        if first_magic in {b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1"}:
            endian = "<"
        elif first_magic in {b"\xa1\xb2\xc3\xd4", b"\xa1\xb2\x3c\x4d"}:
            endian = ">"
        else:
            raise RuntimeError(f"unsupported IEX PCAP magic: {first_magic!r}")
        global_header = _read_exact(stream, 20)
        _version_major, _version_minor, _tz, _sig, _snaplen, linktype = struct.unpack(
            endian + "HHIIII", global_header
        )
        if linktype != 1:
            raise RuntimeError(f"unsupported IEX PCAP link type: {linktype}")
        while True:
            try:
                packet_header = _read_exact(stream, 16)
            except EOFError:
                return
            _sec, _usec, capture_length, _wire_length = struct.unpack(
                endian + "IIII", packet_header
            )
            yield _read_exact(stream, capture_length)


def _udp_payload(frame: bytes) -> bytes | None:
    """Extract UDP payload without constructing a Scapy packet object."""
    if len(frame) < 14:
        return None
    ethernet_type = struct.unpack("!H", frame[12:14])[0]
    network_offset = 14
    if ethernet_type in (0x8100, 0x88A8, 0x9100):
        if len(frame) < 18:
            return None
        ethernet_type = struct.unpack("!H", frame[16:18])[0]
        network_offset = 18
    if ethernet_type == 0x0800:  # IPv4
        if len(frame) < network_offset + 20:
            return None
        version_ihl = frame[network_offset]
        if version_ihl >> 4 != 4:
            return None
        ip_header_length = (version_ihl & 0x0F) * 4
        if ip_header_length < 20 or len(frame) < network_offset + ip_header_length:
            return None
        if frame[network_offset + 9] != 17:  # UDP
            return None
        udp_offset = network_offset + ip_header_length
    elif ethernet_type == 0x86DD:  # IPv6 without extension-header walking
        if len(frame) < network_offset + 40 or frame[network_offset + 6] != 17:
            return None
        udp_offset = network_offset + 40
    else:
        return None
    if len(frame) < udp_offset + 8:
        return None
    udp_length = struct.unpack("!H", frame[udp_offset + 4:udp_offset + 6])[0]
    if udp_length < 8:
        return None
    end = min(len(frame), udp_offset + udp_length)
    return frame[udp_offset + 8:end]


def _parse_fifo_stream(
    fifo: str,
    symbols_file: str,
    output: str,
    protocol: str,
) -> None:
    """Parse a gzip-compressed PCAP from a FIFO without seeking.

    Scapy's normal ``PcapReader`` seeks after reading the gzip header, which
    makes it incompatible with a FIFO.  This small reader handles the fixed
    PCAP record framing sequentially and delegates IEX message decoding to the
    pinned ``iex-parser`` package.
    """
    from iex_parser.messages import decode_message

    requested = {
        str(symbol).strip().upper()
        for symbol in json.loads(Path(symbols_file).read_text(encoding="utf-8"))
    }
    stats: dict[str, dict[str, Any]] = {}
    parse_errors = 0
    with gzip.open(fifo, "rb") as stream:
        try:
            magic = _read_exact(stream, 4)
        except EOFError:
            raise RuntimeError("IEX PCAP stream was empty")
        for frame in _iter_pcap_frames(stream, magic):
            payload = _udp_payload(frame)
            if payload is None:
                continue
            if len(payload) < 40:
                continue
            try:
                _version, _protocol_id, _channel, _session, payload_length, count, _offset, _sequence, _send_time = struct.unpack(
                    "<BxHIIHHqqq", payload[:40]
                )
                if len(payload) != 40 + payload_length:
                    continue
                start = 40
                for _ in range(count):
                    if start + 2 > len(payload):
                        break
                    message_length = struct.unpack("<H", payload[start:start + 2])[0]
                    start += 2
                    end = start + message_length
                    if end > len(payload) or message_length < 1:
                        break
                    message_type_id = payload[start]
                    start = end
                    # TOPS 1.6 message types: 0x54 is a trade report and
                    # 0x58 is an official open/close.  Skip quote/status/
                    # auction messages before invoking the Python decoder.
                    if message_type_id not in (0x54, 0x58):
                        continue
                    message = decode_message(protocol, message_type_id, payload[end - message_length + 1:end])
                    symbol_raw = message.get("symbol")
                    if not isinstance(symbol_raw, bytes):
                        continue
                    symbol = symbol_raw.decode("ascii", errors="ignore").strip().upper()
                    if symbol not in requested:
                        continue
                    message_type = message.get("type")
                    if message_type == "trade_report":
                        price = float(message["price"])
                        size = int(message["size"])
                        if price <= 0 or size < 0:
                            continue
                        item = stats.setdefault(symbol, {
                            "open": price,
                            "high": price,
                            "low": price,
                            "last_trade": price,
                            "volume": 0,
                            "trade_count": 0,
                            "last_trade_timestamp": None,
                        })
                        item["high"] = max(item["high"], price)
                        item["low"] = min(item["low"], price)
                        item["last_trade"] = price
                        item["volume"] += size
                        item["trade_count"] += 1
                        item["last_trade_timestamp"] = message["timestamp"].isoformat()
                    elif message_type == "official_price":
                        price = float(message["price"])
                        if price <= 0:
                            continue
                        price_type = message.get("price_type")
                        if price_type == b"Q":
                            stats.setdefault(symbol, {})["open"] = price
                        elif price_type == b"M":
                            stats.setdefault(symbol, {})["official_close"] = price
            except (KeyError, TypeError, ValueError, struct.error, RuntimeError):
                parse_errors += 1

    rows: dict[str, dict[str, Any]] = {}
    for symbol, item in stats.items():
        if not item.get("trade_count"):
            continue
        close = item.get("official_close") or item.get("last_trade")
        rows[symbol] = {
            **item,
            "close": close,
            "parse_errors": parse_errors,
        }
    Path(output).write_text(json.dumps(rows, sort_keys=True) + "\n", encoding="utf-8")


def _daily_rows(
    stats: dict[str, dict[str, Any]],
    as_of: str,
) -> dict[str, dict[str, Any]]:

    output: dict[str, dict[str, Any]] = {}
    for symbol, item in stats.items():
        close = item.get("close") or item.get("last_trade")
        if not close:
            continue
        output[symbol] = {
            "source": "iex_hist",
            "as_of_date": as_of,
            "bars": {
                "dates": [as_of],
                "Open": [round(float(item.get("open") or close), 6)],
                "High": [round(float(item.get("high") or close), 6)],
                "Low": [round(float(item.get("low") or close), 6)],
                "Close": [round(float(close), 6)],
                "Volume": [int(item.get("volume") or 0)],
            },
            "source_metadata": {
                "feed": "TOPS",
                "version": "1.6",
                "method": "official close plus IEX trade reports",
                "trade_count": int(item.get("trade_count") or 0),
                "last_trade_timestamp": item.get("last_trade_timestamp"),
                "parser_errors": int(item.get("parse_errors") or 0),
                "venue": "IEX only; not consolidated US market volume",
            },
        }
    return output


def _coverage(
    groups: dict[str, set[str]],
    rows: dict[str, dict[str, Any]],
    hf_symbols: set[str] | None,
) -> dict[str, dict[str, Any]]:
    found = set(rows)
    report: dict[str, dict[str, Any]] = {}
    for label, requested in groups.items():
        item: dict[str, Any] = {
            "requested": len(requested),
            "iex_hist_rows": len(requested & found),
            "iex_missing": sorted(requested - found),
        }
        if hf_symbols is not None:
            item.update({
                "hf_metadata": len(requested & hf_symbols),
                "hf_plus_iex": len(requested & (hf_symbols | found)),
                "iex_additions_over_hf": len(requested & found - hf_symbols),
                "missing_both": sorted(requested - hf_symbols - found),
            })
        report[label] = item
    return report


def run(args: argparse.Namespace) -> dict[str, Any]:
    groups = _load_universe(Path(args.universe))
    requested = set().union(*groups.values())
    catalog = _get_json(args.catalog_url)
    entry = _choose_tops(catalog, args.date or None)

    try:
        hf_symbols = _as_symbols(_get_json(args.hf_symbols_url))
        hf_error = None
    except Exception as exc:  # The IEX cache remains useful if HF is down.
        hf_symbols = None
        hf_error = str(exc)

    with tempfile.TemporaryDirectory(prefix="iex-hist-cache-") as temp_dir:
        stats = _run_streaming_decode(entry, requested, Path(temp_dir))
        rows = _daily_rows(stats, str(entry["date"]))

    coverage = _coverage(groups, rows, hf_symbols)
    total_requested = len(requested)
    total_rows = len(rows)
    min_coverage = float(args.min_coverage)
    if total_requested and total_rows / total_requested < min_coverage:
        raise RuntimeError(
            f"IEX cache coverage {total_rows}/{total_requested} "
            f"({total_rows / total_requested:.1%}) is below --min-coverage "
            f"{min_coverage:.1%}"
        )
    if not rows:
        raise RuntimeError("IEX parser produced no requested-symbol daily rows")

    generated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "schema_version": 1,
        "generated_at": generated_at,
        "as_of_date": str(entry["date"]),
        "source": "iex_hist",
        "attribution": IEX_ATTRIBUTION,
        "terms_url": IEX_TERMS_URL,
        "limitations": (
            "Daily/T+1 IEX venue reference data; not live and not consolidated. "
            "This file contains one day; it must be accumulated or merged with a "
            "longer history before 52-week signals are used."
        ),
        "symbols": rows,
        "coverage": coverage,
        "source_metadata": {
            "feed": entry.get("feed"),
            "version": entry.get("version"),
            "protocol": entry.get("protocol"),
            "catalog_size_bytes": int(entry.get("size") or 0),
            "method": "FIFO streamed PCAP; filtered parser output; no raw PCAP retained",
            "hf_symbol_metadata_error": hf_error,
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = {
        "generated_at_utc": generated_at,
        "source": "IEX HIST TOPS v1.6",
        "date": entry["date"],
        "feed_size_bytes": int(entry.get("size") or 0),
        "method": "last-sale trade reports with official IEX close; venue-only",
        "hf_symbol_metadata_error": hf_error,
        "coverage": coverage,
        "symbols_with_iex_rows": sorted(rows),
        "cache_output": str(output),
    }
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parse-fifo", help=argparse.SUPPRESS)
    parser.add_argument("--symbols-file", help=argparse.SUPPRESS)
    parser.add_argument("--protocol", default="TOPSv1.6", help=argparse.SUPPRESS)
    parser.add_argument("--universe", default="data/coverage_universe.json")
    parser.add_argument("--output", default="data/iex_hist_latest.json")
    parser.add_argument("--report", default="data/iex_hist_coverage_report.json")
    parser.add_argument("--date", help="YYYYMMDD; blank uses latest TOPS v1.6")
    parser.add_argument("--catalog-url", default=CATALOG_URL)
    parser.add_argument("--hf-symbols-url", default=HF_SYMBOLS_URL)
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.0,
        help="Fail if IEX rows cover less than this fraction of the requested universe",
    )
    args = parser.parse_args()
    if args.parse_fifo:
        if not args.symbols_file or not args.output:
            raise SystemExit("--parse-fifo requires --symbols-file and --output")
        _parse_fifo_stream(
            args.parse_fifo,
            args.symbols_file,
            args.output,
            args.protocol,
        )
        return 0
    report = run(args)
    for label, coverage in report["coverage"].items():
        extras = ""
        if "hf_plus_iex" in coverage:
            extras = (
                f", HF+IEX={coverage['hf_plus_iex']}/{coverage['requested']}"
                f", IEX additions={coverage['iex_additions_over_hf']}"
            )
        print(
            f"{label}: IEX {coverage['iex_hist_rows']}/{coverage['requested']}"
            f"{extras}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
