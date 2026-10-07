#!/usr/bin/env python3
"""Offline rule-absence and recorded-bucket checks; no AWS access."""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

path = Path(__file__).resolve().parents[1] / "cloud/verify_teardown.py"
spec = importlib.util.spec_from_file_location("verify_teardown", path)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class RuleListing:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def list_topic_rules(self, **params):
        self.calls.append(params)
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        return page


class RuleAbsenceTests(unittest.TestCase):
    name = "telemetry_demo_ingest"
    arn = "arn:aws:iot:eu-central-1:000000000000:rule/telemetry_demo_ingest"

    def check(self, client, **kwargs):
        return checker.topic_rule_absent(client, self.name, self.arn, **kwargs)

    def test_empty_listing(self):
        self.assertTrue(self.check(RuleListing([{"rules": []}])))

    def test_target_present_by_name(self):
        listing = RuleListing([{"rules": [{"ruleName": self.name}]}])
        self.assertFalse(self.check(listing))

    def test_target_present_by_arn_on_later_page(self):
        listing = RuleListing([
            {"rules": [{"ruleName": "unrelated"}], "nextToken": "page-two"},
            {"rules": [{"ruleArn": self.arn}]},
        ])
        self.assertFalse(self.check(listing))
        self.assertEqual(listing.calls[1]["nextToken"], "page-two")

    def test_all_pages_needed_before_absence(self):
        listing = RuleListing([
            {"rules": [], "nextToken": "page-two"},
            {"rules": [{"ruleName": "unrelated"}]},
        ])
        self.assertTrue(self.check(listing))
        self.assertEqual(len(listing.calls), 2)

    def test_api_failure_does_not_prove_absence(self):
        listing = RuleListing([RuntimeError("UnauthorizedException")])
        with self.assertRaisesRegex(RuntimeError, "UnauthorizedException"):
            self.check(listing)

    def test_repeated_token_fails(self):
        listing = RuleListing([
            {"rules": [], "nextToken": "same"},
            {"rules": [], "nextToken": "same"},
        ])
        with self.assertRaisesRegex(RuntimeError, "Repeated"):
            self.check(listing)

    def test_page_limit_does_not_prove_absence(self):
        listing = RuleListing([{"rules": [], "nextToken": "more"}])
        with self.assertRaisesRegex(RuntimeError, "page limit"):
            self.check(listing, max_pages=1)


class BucketIdentityTests(unittest.TestCase):
    account = "000000000000"
    bucket = "telemetry-artifacts-unit-test"
    stack_id = "arn:aws:cloudformation:eu-central-1:000000000000:stack/telemetry-demo/test"

    def setUp(self):
        self.state = {
            "account": self.account, "region": "eu-central-1",
            "deployment_id": "a" * 32, "stack_id": self.stack_id,
            "stack": "telemetry-demo", "project_name": "demo_project",
            "teardown": "complete", "certificates": [],
            "deleted_certificates": [
                {"id": str(i) * 64, "arn": f"arn:aws:iot:eu-central-1:{self.account}:cert/{str(i) * 64}",
                 "deployment_id": "a" * 32, "stack_id": self.stack_id}
                for i in (1, 2)
            ],
        }
        self.state["deleted_bucket"] = {
            "name": self.bucket,
            **{key: self.state[key] for key in ("account", "region", "deployment_id", "stack_id")},
        }

    def test_recorded_identity_accepted(self):
        self.assertEqual(checker.recorded_artifact_bucket(self.state, self.bucket), self.bucket)

    def test_different_valid_bucket_name_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "differs from recorded identity"):
            checker.recorded_artifact_bucket(self.state, "telemetry-artifacts-unrelated-absent")

    def test_legacy_manifest_without_identity_rejected(self):
        self.state.pop("deleted_bucket")
        with self.assertRaisesRegex(RuntimeError, "identity required"):
            checker.recorded_artifact_bucket(self.state, self.bucket)

    def test_invalid_record_or_name_rejected(self):
        for value in (None, [], {}, {"name": None}, {"name": "unrelated-bucket"}):
            with self.subTest(value=value):
                self.state["deleted_bucket"] = value
                with self.assertRaises(RuntimeError):
                    checker.recorded_artifact_bucket(self.state, self.bucket)

    def test_every_scope_field_must_match(self):
        original = copy.deepcopy(self.state)
        for key, value in [("account", "111111111111"), ("region", "us-east-1"),
                           ("deployment_id", "b" * 32), ("stack_id", self.stack_id + "-other")]:
            with self.subTest(key=key):
                state = copy.deepcopy(original)
                state["deleted_bucket"][key] = value
                with self.assertRaisesRegex(RuntimeError, "scope mismatch"):
                    checker.recorded_artifact_bucket(state, self.bucket)
                del state["deleted_bucket"][key]
                with self.assertRaisesRegex(RuntimeError, "scope mismatch"):
                    checker.recorded_artifact_bucket(state, self.bucket)

    def test_active_bucket_record_rejected(self):
        self.state["bucket"] = self.bucket
        with self.assertRaisesRegex(RuntimeError, "cleanup incomplete"):
            checker.recorded_artifact_bucket(self.state, self.bucket)

    def files(self, directory):
        resources = []
        for kind, count in checker.EXPECTED.items():
            for i in range(count):
                name = "demo_rule" if kind == "AWS::IoT::TopicRule" else kind.split("::")[-1] + str(i)
                resources.append({"ResourceType": kind, "LogicalResourceId": name,
                                  "PhysicalResourceId": name, "ResourceStatus": "DELETE_COMPLETE"})
        before = {
            "StackId": self.stack_id, "StackName": self.state["stack"],
            "Parameters": [{"ParameterKey": "DeploymentId", "ParameterValue": self.state["deployment_id"]},
                           {"ParameterKey": "ProjectName", "ParameterValue": self.state["project_name"]}],
            "Tags": [{"Key": "TelemetryProject", "Value": "reliable-telemetry-ingestion"},
                     {"Key": "TelemetryDeployment", "Value": self.state["deployment_id"]},
                     {"Key": "TelemetryProjectName", "Value": self.state["project_name"]}],
            "Outputs": [{"OutputKey": "RuleName", "OutputValue": "demo_rule"}],
        }
        for name, data in [("resources.json", self.state), ("stack-before-teardown.json", before),
                           ("resources-before-teardown.json", {"StackResourceSummaries": resources})]:
            (directory / name).write_text(json.dumps(data))
        return resources

    def arguments(self, directory, bucket):
        return ["verify_teardown.py", "--account", self.account, "--bucket", bucket,
                "--directory", str(directory), "--output", str(directory / "result.json")]

    def test_main_rejects_wrong_bucket_before_session_or_api_calls(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            self.files(directory)
            with patch.object(sys, "argv", self.arguments(directory, "telemetry-artifacts-unrelated-absent")), \
                    patch("boto3.Session") as session:
                with self.assertRaisesRegex(RuntimeError, "differs from recorded identity"):
                    checker.main()
                session.assert_not_called()
            self.assertFalse((directory / "result.json").exists())

    def test_main_uses_recorded_bucket_and_keeps_result_sanitized(self):
        def missing(code, operation):
            return ClientError({"Error": {"Code": code, "Message": "does not exist"}}, operation)

        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            resources = self.files(directory)
            clients = {name: Mock() for name in ("sts", "cloudformation", "s3", "iot", "dynamodb", "lambda", "logs", "iam", "sqs", "cloudwatch")}
            clients["sts"].get_caller_identity.return_value = {"Account": self.account}
            clients["cloudformation"].describe_stacks.side_effect = lambda StackName: (
                {"Stacks": [{"StackStatus": "DELETE_COMPLETE"}]} if StackName == self.stack_id
                else (_ for _ in ()).throw(missing("ValidationError", "DescribeStacks")))
            clients["cloudformation"].get_paginator.return_value.paginate.return_value = [
                {"StackResourceSummaries": resources}]
            for service, operation, code in [
                ("s3", "head_bucket", "404"), ("iot", "describe_certificate", "ResourceNotFoundException"),
                ("iot", "describe_thing", "ResourceNotFoundException"), ("iot", "get_policy", "ResourceNotFoundException"),
                ("dynamodb", "describe_table", "ResourceNotFoundException"), ("lambda", "get_function", "ResourceNotFoundException"),
                ("logs", "filter_log_events", "ResourceNotFoundException"), ("iam", "get_role", "NoSuchEntity"),
                ("sqs", "get_queue_attributes", "QueueDoesNotExist")]:
                getattr(clients[service], operation).side_effect = missing(code, operation)
            clients["iot"].list_topic_rules.return_value = {"rules": []}
            clients["cloudwatch"].describe_alarms.return_value = {"MetricAlarms": [], "CompositeAlarms": []}
            session = Mock()
            session.client.side_effect = lambda service, **kwargs: clients[service]
            with patch.object(sys, "argv", self.arguments(directory, self.bucket)), \
                    patch("boto3.Session", return_value=session), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(checker.main(), 0)
            clients["s3"].head_bucket.assert_called_once_with(Bucket=self.bucket, ExpectedBucketOwner=self.account)
            body = (directory / "result.json").read_text()
            result = json.loads(body)
            self.assertEqual(result["status"], "passed")
            for private in (self.account, self.bucket, self.stack_id, self.state["deployment_id"]):
                self.assertNotIn(private, body)


if __name__ == "__main__":
    unittest.main()
