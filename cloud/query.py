#!/usr/bin/env python3
import argparse
import json
import boto3
from botocore.config import Config
from telemetry_ingestion.core import DEVICE, STREAM, MAX_SEQUENCE
from telemetry_ingestion.dynamo import DynamoStore

parser = argparse.ArgumentParser()
parser.add_argument("command", choices=("get", "range", "count"))
parser.add_argument("--table", required=True)
parser.add_argument("--device", required=True)
parser.add_argument("--stream", required=True)
parser.add_argument("--first", type=int, default=1)
parser.add_argument("--last", type=int)
parser.add_argument("--max-pages", type=int, default=5)
args = parser.parse_args()
if args.last is None:
    args.last = MAX_SEQUENCE if args.command == "count" else 1000
if (
    not DEVICE.fullmatch(args.device)
    or not STREAM.fullmatch(args.stream)
    or not 1 <= args.first <= MAX_SEQUENCE
    or not 1 <= args.max_pages <= 10
):
    parser.error("bounded identity/range required")
if args.command != "get" and (
    not args.first <= args.last <= MAX_SEQUENCE
    or (args.command == "range" and args.last - args.first > 10000)
):
    parser.error("bounded sequence range required")
client = boto3.client(
    "dynamodb",
    region_name="eu-central-1",
    config=Config(
        connect_timeout=1,
        read_timeout=2,
        retries={"mode": "standard", "total_max_attempts": 2},
    ),
)
store = DynamoStore(client, args.table)
result = (
    store.get(args.device, args.stream, args.first)
    if args.command == "get"
    else store.query(
        args.device,
        args.stream,
        args.first,
        args.last,
        max_pages=args.max_pages,
        count=args.command == "count",
    )
)
print(json.dumps(result, separators=(",", ":")))
