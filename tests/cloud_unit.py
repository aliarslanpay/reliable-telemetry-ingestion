#!/usr/bin/env python3
"""SDK parameter/response tests only; these do not verify AWS/IAM/service behavior."""
import base64
import json
from pathlib import Path
import unittest
import boto3
from botocore.stub import Stubber
from telemetry_ingestion.aws_handler import envelope
from telemetry_ingestion.core import Invalid, canonical, fingerprint, validate
from telemetry_ingestion.dynamo import DynamoStore, keys


class CloudUnit(unittest.TestCase):
    def setUp(self):
        self.event = json.loads(
            (Path(__file__).parent / "fixtures/events.json").read_text()
        )[0]["event"]
        self.client = boto3.client(
            "dynamodb",
            region_name="eu-central-1",
            aws_access_key_id="unit",
            aws_secret_access_key="unit",
        )
        self.item = {
            **keys(
                self.event["device_id"], self.event["stream_id"], self.event["sequence"]
            ),
            "fingerprint": {"S": self.event["fingerprint"]},
            "data": {"S": canonical(self.event)},
        }
        self.params = dict(
            TableName="events",
            Item=self.item,
            ConditionExpression="attribute_not_exists(pk)",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )

    def test_rule_envelope(self):
        raw = json.dumps(self.event).encode()
        data, device = envelope(
            {
                "event_b64": base64.b64encode(raw).decode(),
                "authorized_device": "sensor-a",
            }
        )
        self.assertEqual(validate(data, device), self.event)
        for value in (
            {"event_b64": "x" * 1369, "authorized_device": "sensor-a"},
            {"event_b64": "@@@", "authorized_device": "sensor-a"},
            {
                "event_b64": base64.b64encode(raw).decode(),
                "authorized_device": "sensor-a",
                "device_id": "sensor-b",
            },
        ):
            with self.assertRaises(Invalid):
                envelope(value)

    def test_conditional_results(self):
        with Stubber(self.client) as stub:
            stub.add_response("put_item", {}, self.params)
            self.assertEqual(
                DynamoStore(self.client, "events").store(self.event), "stored"
            )
            stub.add_client_error(
                "put_item",
                "ConditionalCheckFailedException",
                expected_params=self.params,
                modeled_fields={"Item": self.item},
            )
            self.assertEqual(
                DynamoStore(self.client, "events").store(self.event), "duplicate"
            )
            conflicting = {**self.event, "temperature_mc": 123}
            conflicting["fingerprint"] = fingerprint(conflicting)
            params = {
                **self.params,
                "Item": {
                    **self.item,
                    "fingerprint": {"S": conflicting["fingerprint"]},
                    "data": {"S": canonical(conflicting)},
                },
            }
            stub.add_client_error(
                "put_item",
                "ConditionalCheckFailedException",
                expected_params=params,
                modeled_fields={"Item": self.item},
            )
            self.assertEqual(
                DynamoStore(self.client, "events").store(conflicting), "conflict"
            )
            stub.assert_no_pending_responses()

    def test_consistent_fallback_and_transient(self):
        with Stubber(self.client) as stub:
            stub.add_client_error(
                "put_item",
                "ConditionalCheckFailedException",
                expected_params=self.params,
            )
            stub.add_response(
                "get_item",
                {"Item": self.item},
                dict(
                    TableName="events",
                    Key=keys(
                        self.event["device_id"],
                        self.event["stream_id"],
                        self.event["sequence"],
                    ),
                    ConsistentRead=True,
                ),
            )
            self.assertEqual(
                DynamoStore(self.client, "events").store(self.event), "duplicate"
            )
            stub.add_client_error(
                "put_item",
                "ProvisionedThroughputExceededException",
                expected_params=self.params,
            )
            with self.assertRaises(Exception):
                DynamoStore(self.client, "events").store(self.event)

    def test_bounded_query(self):
        key = keys(self.event["device_id"], self.event["stream_id"], 1)
        params = {
            "TableName": "events",
            "KeyConditionExpression": "pk=:pk AND sk BETWEEN :first AND :last",
            "ExpressionAttributeValues": {
                ":pk": key["pk"],
                ":first": key["sk"],
                ":last": {"S": "0000000000000010"},
            },
            "ConsistentRead": True,
            "Limit": 1,
            "Select": "COUNT",
        }
        with Stubber(self.client) as stub:
            stub.add_response("query", {"Count": 1, "LastEvaluatedKey": key}, params)
            result = DynamoStore(self.client, "events").query(
                self.event["device_id"],
                self.event["stream_id"],
                1,
                10,
                limit=1,
                max_pages=1,
                count=True,
            )
            self.assertEqual(result, {"items": [], "count": 1, "truncated": True})
            with self.assertRaises(ValueError):
                DynamoStore(self.client, "events").query(
                    "sensor-a", self.event["stream_id"], 1, 10, limit=101
                )

    def test_payload_metadata_cannot_override_envelope(self):
        event = {**self.event, "authorized_device": "sensor-b"}
        raw, device = envelope(
            {
                "event_b64": base64.b64encode(json.dumps(event).encode()).decode(),
                "authorized_device": "sensor-a",
            }
        )
        self.assertEqual(device, "sensor-a")
        with self.assertRaises(Invalid):
            validate(raw, device)


if __name__ == "__main__":
    unittest.main()
