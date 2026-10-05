"""Explicit opt-in hooks for isolated fault tests; unused by the normal worker."""
import json
import os
from pathlib import Path
import time


class Hooks:
    def __init__(self, directory, before_commit=False, after_commit="normal", delay_ms=0):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.pause = before_commit
        self.after = after_commit
        self.delay_ms = delay_ms
        if after_commit not in {"normal", "suppress", "crash"} or not 0 <= delay_ms <= 500:
            raise ValueError("test hook configuration")

    def before_commit(self, event):
        if self.pause:
            (self.directory / "before-commit.json").write_text(json.dumps(event))
            deadline = time.monotonic() + 5
            while not (self.directory / "release").exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("test commit pause deadline")
                time.sleep(0.02)
        if self.delay_ms:
            time.sleep(self.delay_ms / 1000)

    def after_commit(self, event):
        (self.directory / "after-commit.json").write_text(json.dumps(event))
        if self.after == "crash":
            os._exit(71)
        return self.after != "suppress"
