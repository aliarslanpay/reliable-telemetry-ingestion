import json
from pathlib import Path
import unittest
from telemetry_ingestion import core


class ContractTest(unittest.TestCase):
    def setUp(self):
        self.fixtures = json.loads((Path(__file__).parent / "fixtures/events.json").read_text())

    def test_fixtures_and_order(self):
        for f in self.fixtures:
            event = f["event"]
            self.assertEqual(core.canonical(event), f["canonical"])
            self.assertEqual(core.fingerprint(event), f["sha256"])
            reordered = dict(reversed(list(event.items())))
            self.assertEqual(core.validate(json.dumps(reordered), event["device_id"]), event)

    def test_guards(self):
        e = self.fixtures[0]["event"]
        for key, value in [("sequence", True), ("pressure_pa", 1.0), ("temperature_mc", 200001), ("schema_version", 2)]:
            with self.assertRaises(core.Invalid):
                core.validate(json.dumps({**e, key: value}), e["device_id"])
        with self.assertRaises(core.Invalid):
            core.validate(json.dumps(e), "sensor-b")
        for raw in ['{"a":1,"a":2}', "x" * 1025, '{"a":NaN}']:
            with self.assertRaises(core.Invalid):
                core.decode(raw)

    def test_terminal_envelope(self):
        event = self.fixtures[0]["event"]
        bad = json.dumps({**event, "temperature_mc": "wrong"})
        self.assertEqual(core.invalid_ack(bad, event["device_id"])["result"], "invalid")
        self.assertIsNone(core.invalid_ack(bad, "sensor-b"))
        self.assertIsNone(core.invalid_ack("x" * 1025, "sensor-a"))


if __name__ == "__main__":
    unittest.main()
