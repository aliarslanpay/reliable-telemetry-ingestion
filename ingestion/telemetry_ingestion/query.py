"""Bounded indexed retrieval; DSN is read from the environment, never printed."""
import argparse
import json
import os
import psycopg
from .core import DEVICE, STREAM, MAX_SEQUENCE


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("get", "range", "count", "plan"))
    parser.add_argument("--device", required=True)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--first", type=int, default=1)
    parser.add_argument("--last", type=int, default=1000)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if not DEVICE.fullmatch(args.device) or not STREAM.fullmatch(args.stream):
        parser.error("invalid identity")
    if not 1 <= args.first <= MAX_SEQUENCE or not 1 <= args.limit <= 1000:
        parser.error("sequence/limit outside bounds")
    if args.command in ("range", "plan") and (not args.first <= args.last <= MAX_SEQUENCE or args.last-args.first > 10000):
        parser.error("range/limit outside bounds")
    base = "SELECT sequence,fingerprint,data,stored_at FROM events WHERE device_id=%s AND stream_id=%s"
    params = (args.device, args.stream)
    if args.command == "count":
        query = "SELECT count(*) FROM events WHERE device_id=%s AND stream_id=%s"
    elif args.command == "get":
        query = base + " AND sequence=%s"
        params += (args.first,)
    else:
        query = base + " AND sequence BETWEEN %s AND %s ORDER BY sequence LIMIT %s"
        params += (args.first, args.last, args.limit)
        if args.command == "plan":
            query = "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query
    try:
        with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=2,
                options="-c statement_timeout=1500 -c lock_timeout=1000") as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall()
    except psycopg.Error as exc:
        raise SystemExit("Query failed: " + type(exc).__name__) from None
    print(json.dumps(result, default=str, separators=(",", ":")))


if __name__ == "__main__":
    main()
