#!/usr/bin/env python3
"""CLI boundary checks against a real gateway process and its SQLite outbox."""
import argparse
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest

MAX_TIMESTAMP = 253402300799999
COMMANDS = ["enqueue", "status", "quarantine", "new-stream", "discard"]
GATEWAY = None


class GatewayCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="telemetry-cli-")
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "outbox.db"

    def invoke(self, command, *options):
        scope = [] if command == "version" else ["--db", str(self.database), "--device", "sensor-a"]
        result = subprocess.run([str(GATEWAY), command, *scope, *map(str, options)],
                                capture_output=True, text=True, timeout=5)
        entries = [json.loads(line) for line in result.stdout.splitlines()]
        rows = 0
        if self.database.exists():
            with sqlite3.connect(f"file:{self.database}?mode=ro", uri=True) as database:
                rows = database.execute("SELECT count(*) FROM outbox").fetchone()[0]
        return result, entries, rows

    def invalid(self, command, *options, detail):
        result, entries, rows = self.invoke(command, *options)
        self.assertEqual(result.returncode, 2, result.stderr)
        error = json.loads(result.stderr)
        self.assertEqual(error["kind"], "error")
        self.assertIn(detail, error["detail"])
        self.assertEqual(entries, [])
        self.assertEqual(rows, 0)


class EnqueueTests(GatewayCase):
    def test_invalid_timestamp_and_series_have_no_acceptance(self):
        for first, count in [(-9223372036854775808, 1), (-1, 1),
                             (MAX_TIMESTAMP + 1, 1), (9223372036854775807, 2),
                             (MAX_TIMESTAMP, 2), (MAX_TIMESTAMP - 9998, 10000)]:
            with self.subTest(first=first, count=count):
                self.invalid("enqueue", "--count", count, "--timestamp-ms", first, detail="timestamp-ms")

    def test_timestamp_boundaries_and_last_valid_series(self):
        for first, count in [(0, 1), (MAX_TIMESTAMP, 1), (MAX_TIMESTAMP - 1, 2)]:
            with self.subTest(first=first, count=count):
                result, entries, _ = self.invoke("enqueue", "--count", count, "--timestamp-ms", first)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual([e["kind"] for e in entries], ["accepted"] * count)
                self.assertEqual([e["event"]["timestamp_ms"] for e in entries], list(range(first, first + count)))

    def test_invalid_series_preserves_prior_event_and_sequence(self):
        result, entries, rows = self.invoke("enqueue", "--timestamp-ms", 0)
        self.assertEqual((result.returncode, rows), (0, 1))
        self.assertEqual(entries[0]["event"]["sequence"], 1)
        result, entries, rows = self.invoke("enqueue", "--count", 2, "--timestamp-ms", MAX_TIMESTAMP)
        self.assertEqual((result.returncode, entries, rows), (2, [], 1))
        result, entries, rows = self.invoke("enqueue", "--timestamp-ms", 1)
        self.assertEqual((result.returncode, rows), (0, 2))
        self.assertEqual(entries[0]["event"]["sequence"], 2)

    def test_unknown_option_rejected_for_every_command(self):
        for command in COMMANDS:
            with self.subTest(command=command):
                self.invalid(command, "--windwo", 2, detail="invalid option")

    def test_known_option_rejected_for_wrong_command(self):
        self.invalid("status", "--count", 2, detail="invalid option")

    def test_unknown_command_creates_no_database(self):
        self.invalid("enqeue", detail="unknown command")
        self.assertFalse(self.database.exists())


def main():
    global GATEWAY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", type=Path, required=True)
    args = parser.parse_args()
    GATEWAY = args.gateway.resolve()
    suite = unittest.defaultTestLoader.loadTestsFromModule(__import__(__name__))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
