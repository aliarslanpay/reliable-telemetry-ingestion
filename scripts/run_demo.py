#!/usr/bin/env python3
"""Run the disposable Compose demo without printing its credentials."""
import argparse
import json
import os
from pathlib import Path
import subprocess

root=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser()
parser.add_argument("--gateway",type=Path,default=root/"build/gateway")
parser.add_argument("--device",choices=("sensor-a","sensor-b"),default="sensor-a")
parser.add_argument("--count",type=int,default=10)
args=parser.parse_args()
if not 1<=args.count<=1000:parser.error("count range 1..1000")
try:password=json.loads((root/".runtime/credentials.json").read_text())[args.device]
except (OSError,ValueError,KeyError):raise SystemExit("Run scripts/local_setup.py and docker compose up -d --build first") from None
environment={**os.environ,"MQTT_USERNAME":args.device,"MQTT_PASSWORD":password}
for name in ("MQTT_CA_FILE","MQTT_CERT_FILE","MQTT_KEY_FILE"):environment.pop(name,None)
raise SystemExit(subprocess.run([str(args.gateway.resolve()),"run","--db",str(root/".runtime"/(args.device+".db")),
    "--device",args.device,"--port","18883","--count",str(args.count),"--duration-ms","20000"],env=environment,timeout=25).returncode)
