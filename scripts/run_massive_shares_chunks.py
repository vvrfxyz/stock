from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROGRESS_PATH = PROJECT_ROOT / "logs" / "massive_shares_chunks_2026-05-15.jsonl"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Massive shares full-refresh in resumable symbol chunks.")
    parser.add_argument("--market", default="US", help="Market to process. Currently only US is supported.")
    parser.add_argument("--chunk-size", type=int, default=100, help="Number of symbols per child run.")
    parser.add_argument("--progress", type=Path, default=DEFAULT_PROGRESS_PATH, help="JSONL progress file.")
    parser.add_argument("--limit-chunks", type=int, default=0, help="Optional limit for smoke tests.")
    return parser.parse_args(argv)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def append_progress(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n")


def load_completed_chunks(path: Path) -> set[str]:
    completed_by_chunk: dict[str, bool] = {}
    if not path.exists():
        return set()

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            event = record.get("event")
            chunk_key = record.get("chunk_key")
            if not chunk_key:
                continue
            if event == "chunk_completed":
                completed_by_chunk[chunk_key] = record.get("returncode") == 0
            elif event == "chunk_invalidated":
                completed_by_chunk[chunk_key] = False
    return {chunk_key for chunk_key, completed in completed_by_chunk.items() if completed}


def get_symbols(market: str) -> list[str]:
    load_dotenv(PROJECT_ROOT / ".env")
    engine = create_engine(os.environ["DATABASE_URL"])
    query = text(
        """
        select symbol
        from securities
        where upper(market) = :market
          and upper(type) in ('CS', 'ETF')
        order by symbol asc
        """
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(query, {"market": market.upper()})]


def iter_chunks(items: list[str], chunk_size: int):
    for index in range(0, len(items), chunk_size):
        yield index // chunk_size + 1, items[index : index + chunk_size]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.market.upper() != "US":
        raise ValueError(f"Only US market is supported, got: {args.market}")
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")

    completed = load_completed_chunks(args.progress)
    symbols = get_symbols(args.market)
    chunks = list(iter_chunks(symbols, args.chunk_size))
    if args.limit_chunks > 0:
        chunks = chunks[: args.limit_chunks]

    append_progress(
        args.progress,
        {
            "event": "run_started",
            "utc_time": utc_now(),
            "market": args.market.upper(),
            "chunk_size": args.chunk_size,
            "symbol_count": len(symbols),
            "chunk_count": len(chunks),
        },
    )

    for chunk_index, chunk in chunks:
        chunk_key = f"{chunk_index:04d}:{chunk[0]}:{chunk[-1]}"
        if chunk_key in completed:
            print(f"[skip] {chunk_key}", flush=True)
            continue

        command = [
            "rtk",
            sys.executable,
            "main.py",
            "update_massive_shares",
            "--full-refresh",
            "--market",
            args.market.upper(),
            *chunk,
        ]
        started = time.monotonic()
        append_progress(
            args.progress,
            {
                "event": "chunk_started",
                "utc_time": utc_now(),
                "chunk_key": chunk_key,
                "symbol_count": len(chunk),
            },
        )
        print(f"[start] {chunk_key} ({len(chunk)} symbols)", flush=True)
        result = subprocess.run(command, cwd=PROJECT_ROOT)
        elapsed = time.monotonic() - started
        append_progress(
            args.progress,
            {
                "event": "chunk_completed",
                "utc_time": utc_now(),
                "chunk_key": chunk_key,
                "symbol_count": len(chunk),
                "returncode": result.returncode,
                "elapsed_seconds": round(elapsed, 3),
            },
        )
        print(f"[done] {chunk_key} returncode={result.returncode} elapsed={elapsed:.1f}s", flush=True)
        if result.returncode != 0:
            return result.returncode

    append_progress(args.progress, {"event": "run_completed", "utc_time": utc_now()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
