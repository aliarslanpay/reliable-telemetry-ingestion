#!/usr/bin/env python3
"""Generate disposable loopback demo credentials without printing their values."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess

root = Path(__file__).resolve().parents[1]
runtime = root / ".runtime"
runtime.mkdir(mode=0o700, exist_ok=True)
if (root / ".env").exists() or (runtime / "passwords").exists():
    raise SystemExit("Existing demo credentials found; retain them or remove .env and .runtime/passwords explicitly.")
passwords = {name: secrets.token_hex(16) for name in ("sensor-a", "sensor-b", "worker-1", "worker-2")}
if shutil.which("mosquitto_passwd") is None:
    raise SystemExit("Install mosquitto-clients (mosquitto_passwd) first.")
for index, (name, password) in enumerate(passwords.items()):
    subprocess.run(["mosquitto_passwd", "-b", *( ["-c"] if index == 0 else []), str(runtime / "passwords"), name, password], check=True, stdout=subprocess.DEVNULL)
# Containers need read access; the parent directory remains private on the host.
os.chmod(runtime / "passwords", 0o644)
(runtime / "credentials.json").write_text(json.dumps(passwords))
os.chmod(runtime / "credentials.json", 0o600)
environment = "POSTGRES_PASSWORD=" + secrets.token_hex(16) + "\nWORKER_DB_PASSWORD=" + secrets.token_hex(16) + "\nWORKER1_PASSWORD=" + passwords["worker-1"] + "\nWORKER2_PASSWORD=" + passwords["worker-2"] + "\n"
(root / ".env").write_text(environment)
os.chmod(root / ".env", 0o600)
print("Demo credentials created. Do not commit .env or .runtime.")
