#!/usr/bin/env python3
"""Reject private credentials, runtime state and build artifacts from Git."""
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[1]
try:
    files = (
        subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"])
        .decode()
        .split("\0")
    )
except subprocess.CalledProcessError:
    raise SystemExit(
        "Run this guard from a Git checkout or staged source tree"
    ) from None

private_names = {
    ".env",
    "credentials",
    "credentials.json",
    "resources.json",
    "samconfig.toml",
    "id_rsa",
    "id_ed25519",
}
private_suffixes = {
    ".db",
    ".sqlite",
    ".sqlite3",
    ".log",
    ".jsonl",
    ".pem",
    ".key",
    ".crt",
    ".p12",
    ".pfx",
    ".pyc",
    ".tfstate",
    ".o",
    ".a",
    ".so",
}
runtime_directories = {
    ".runtime",
    ".venv",
    ".venv-cloud",
    "venv",
    "__pycache__",
    ".aws-sam",
    ".aws",
    "node_modules",
    "results",
}
errors = []
for name in filter(None, files):
    path = Path(name)
    runtime = any(
        part in runtime_directories
        or part.startswith(("build-", ".venv-"))
        or part == "build"
        for part in path.parts
    )
    # Reviewed result summaries belong in docs/results, rather than transient results/.
    if path.parts[:2] == ("docs", "results"):
        runtime = any(part in runtime_directories - {"results"} for part in path.parts)
    if (
        path.name in private_names
        or path.name.startswith(".env.")
        or path.suffix in private_suffixes
        or runtime
    ):
        errors.append(name + ": private/runtime/build material")
    data = (root / path).read_bytes()
    if (
        re.search(rb"(?:AKIA|ASIA)[A-Z0-9]{16}", data)
        or re.search(
            rb"-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----", data
        )
        or re.search(
            rb"(?im)^\s*aws_(?:secret_access_key|session_token)\s*=\s*[A-Za-z0-9/+=]{16,}\s*$", data
        )
    ):
        errors.append(name + ": credential material")
if errors:
    raise SystemExit("\n".join(errors))
print("Tracked source passed the private/runtime/build material guard.")
