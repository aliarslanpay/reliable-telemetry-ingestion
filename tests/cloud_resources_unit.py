#!/usr/bin/env python3
"""Offline ownership and partial-lifecycle tests; no account access."""
import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber

spec = importlib.util.spec_from_file_location(
    "resources", Path(__file__).resolve().parents[1] / "cloud/resources.py"
)
resources = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resources)
ACCOUNT = "123456789012"
STACK_ID = (
    f"arn:aws:cloudformation:eu-central-1:{ACCOUNT}:stack/telemetry-demo/unique-id"
)


def not_found(service="iot", operation="DescribeCertificate"):
    code = "ValidationError" if service == "cf" else "ResourceNotFoundException"
    return ClientError(
        {"Error": {"Code": code, "Message": "Stack does not exist"}}, operation
    )


class FakeClient:
    def __init__(self, session, service):
        self.session, self.service = session, service

    def __getattr__(self, operation):
        def call(**params):
            s = self.session
            s.calls.append((self.service, operation, params))
            # Validate request shapes against the installed SDK service model.
            from botocore.validate import validate_parameters

            client = s.models[self.service]
            model = client.meta.service_model.operation_model(
                client.meta.method_to_api_mapping[operation]
            )
            validate_parameters(params, model.input_shape)
            if operation in s.fail:
                raise RuntimeError("Injected failure: " + operation)
            if operation == "get_caller_identity":
                return {"Account": s.account}
            if operation == "describe_stacks":
                name = params["StackName"]
                stack = s.stack if name == s.stack_name else s.old_stack
                if stack is None:
                    raise not_found("cf")
                return {"Stacks": [copy.deepcopy(stack)]}
            if operation == "create_bucket":
                s.bucket_exists = True
            if operation == "head_bucket" and not s.bucket_exists:
                raise ClientError(
                    {"Error": {"Code": "404", "Message": "Absent"}}, "HeadBucket"
                )
            if operation == "get_bucket_location":
                return {"LocationConstraint": "eu-central-1"}
            if operation == "get_bucket_tagging":
                return {
                    "TagSet": [{"Key": k, "Value": v} for k, v in s.bucket_tags.items()]
                }
            if operation == "put_bucket_tagging":
                s.bucket_tags = {
                    t["Key"]: t["Value"] for t in params["Tagging"]["TagSet"]
                }
            if operation == "get_bucket_versioning":
                return s.versioning
            if operation == "list_objects_v2":
                return {"Contents": []}
            if operation == "delete_bucket":
                s.bucket_exists = False
            if operation == "create_keys_and_certificate":
                cert_id = str(len(s.certificates) + 1) * 64
                arn = f"arn:aws:iot:eu-central-1:{ACCOUNT}:cert/{cert_id}"
                s.certificates[cert_id] = {
                    "certificateId": cert_id,
                    "certificateArn": arn,
                }
                return {
                    **s.certificates[cert_id],
                    "certificatePem": "fixture certificate",
                    "keyPair": {"PrivateKey": "fixture key"},
                }
            if operation == "describe_certificate":
                if params["certificateId"] not in s.certificates:
                    raise not_found()
                return {
                    "certificateDescription": s.certificates[params["certificateId"]]
                }
            if operation == "list_attached_policies":
                return {"policies": [{"policyName": p} for p in s.policies]}
            if operation == "list_principal_things":
                return {"things": s.things}
            if operation == "delete_certificate":
                s.certificates.pop(params["certificateId"], None)
            return {}

        return call

    def get_waiter(self, name):
        s = self.session

        class Waiter:
            def wait(self, **params):
                s.calls.append(("cf", "wait", params))
                if "wait" in s.fail:
                    raise RuntimeError("Injected wait failure")
                s.old_stack = None
                s.stack = None

        return Waiter()


class FakeSession:
    region_name = "eu-central-1"

    def __init__(self, state):
        self.account = ACCOUNT
        self.stack_name = state["stack"]
        self.stack = {
            "StackId": STACK_ID,
            "StackName": self.stack_name,
            "StackStatus": "CREATE_COMPLETE",
            "Tags": [
                {"Key": k, "Value": v}
                for k, v in resources.ownership_tags(state).items()
            ],
            "Parameters": [
                {
                    "ParameterKey": "ProjectName",
                    "ParameterValue": state["project_name"],
                },
                {
                    "ParameterKey": "DeploymentId",
                    "ParameterValue": state["deployment_id"],
                },
            ],
            "Outputs": [
                {"OutputKey": key, "OutputValue": value}
                for key, value in {
                    "DeviceAName": "sensor-a",
                    "DeviceBName": "sensor-b",
                    "DeviceAPolicyName": "demo_a",
                    "DeviceBPolicyName": "demo_b",
                }.items()
            ],
        }
        self.old_stack = self.stack
        self.bucket_exists = True
        self.bucket_tags = resources.ownership_tags(state)
        self.versioning = {}
        self.certificates = {}
        self.policies = []
        self.things = []
        self.calls = []
        self.fail = set()
        # Explicit dummy credentials prevent provider-chain or metadata lookup.
        self.models = {
            service: boto3.client(
                service,
                region_name=self.region_name,
                aws_access_key_id="unit",
                aws_secret_access_key="unit",
            )
            for service in ("sts", "cloudformation", "s3", "iot")
        }

    def client(self, service, **kwargs):
        return FakeClient(self, service)

    def destructive(self):
        return [
            c
            for c in self.calls
            if c[1].startswith(("delete_", "detach_")) or c[1] == "update_certificate"
        ]


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.manifest = self.directory / "resources.json"
        self.state = {
            "schema_version": 1,
            "project": resources.PROJECT,
            "deployment_id": "a" * 32,
            "account": ACCOUNT,
            "region": "eu-central-1",
            "stack": "telemetry-demo",
            "project_name": "demo_project",
            "stack_id": STACK_ID,
            "bucket": "telemetry-artifacts-unit-test",
            "certificates": [],
            "deleted_certificates": [],
            "stack_delete_requested": False,
        }
        self.session = FakeSession(self.state)
        self.args = argparse.Namespace(
            command="teardown",
            account=ACCOUNT,
            stack="telemetry-demo",
            region="eu-central-1",
            project_name="demo_project",
            directory=self.directory,
            bucket="telemetry-artifacts-unit-test",
        )
        self.write_state()

    def write_state(self):
        resources.save(self.manifest, self.state)

    def reject(self):
        with self.assertRaises((resources.OwnershipError, ClientError)):
            resources.run(self.args, self.session)
        self.assertEqual(self.session.destructive(), [])

    def add_certificate(self):
        cert_id = "1" * 64
        arn = f"arn:aws:iot:eu-central-1:{ACCOUNT}:cert/{cert_id}"
        record = {
            "id": cert_id,
            "arn": arn,
            "device": "sensor-a",
            "policy": "demo_a",
            "deployment_id": self.state["deployment_id"],
            "stack_id": STACK_ID,
        }
        self.state["certificates"].append(record)
        self.session.certificates[cert_id] = {
            "certificateId": cert_id,
            "certificateArn": arn,
        }
        self.write_state()
        return record

    def test_missing_manifest_no_destructive_calls(self):
        self.manifest.unlink()
        self.reject()
        self.assertEqual(self.session.calls, [])

    def test_invalid_manifest_no_destructive_calls(self):
        for data in ("{", "{}", "[]", '{"schema_version": 0}'):
            with self.subTest(data=data):
                self.manifest.write_text(data)
                self.reject()

    def test_scope_mismatches_no_destructive_calls(self):
        for key, value in (
            ("account", "999999999999"),
            ("stack", "another-stack"),
            ("region", "us-east-1"),
            ("project_name", "other_project"),
            ("project", "other-project"),
            ("stack_id", STACK_ID.replace(ACCOUNT, "999999999999")),
        ):
            with self.subTest(key=key):
                changed = copy.deepcopy(self.state)
                changed[key] = value
                resources.save(self.manifest, changed)
                self.reject()

    def test_active_account_mismatch(self):
        self.session.account = "999999999999"
        self.reject()

    def test_session_region_mismatch(self):
        self.session.region_name = "us-east-1"
        self.reject()

    def test_unregistered_stack(self):
        self.state.pop("stack_id")
        self.write_state()
        self.reject()

    def test_changed_stack_identity(self):
        self.session.stack = copy.deepcopy(self.session.stack)
        self.session.stack["StackId"] = STACK_ID.replace("unique-id", "replacement-id")
        self.reject()

    def test_ownership_tags_and_parameters(self):
        for field in ("Tags", "Parameters"):
            with self.subTest(field=field):
                self.session = FakeSession(self.state)
                self.session.stack[field] = []
                self.reject()

    def test_absent_stack_without_recorded_delete_request(self):
        self.session.stack = self.session.old_stack = None
        self.reject()

    def test_deleted_stack_name_reuse_rejected(self):
        self.state["stack_delete_requested"] = True
        self.write_state()
        self.session.old_stack = None
        self.session.stack["StackId"] = STACK_ID.replace("unique-id", "replacement-id")
        self.reject()

    def test_bucket_ownership_and_versioning_before_certificate_delete(self):
        self.add_certificate()
        self.session.bucket_tags = {}
        self.reject()
        self.session.bucket_tags = resources.ownership_tags(self.state)
        self.session.versioning = {"Status": "Enabled"}
        self.reject()

    def test_extra_certificate_attachments_rejected(self):
        self.add_certificate()
        self.session.policies = ["unrelated-policy"]
        self.reject()
        self.session.policies = []
        self.session.things = ["unrelated-thing"]
        self.reject()

    def test_validate_all_certificates_before_any_delete(self):
        self.add_certificate()
        bad = copy.deepcopy(self.state["certificates"][0])
        bad.update(
            id="2" * 64,
            arn=f"arn:aws:iot:eu-central-1:{ACCOUNT}:cert/" + "2" * 64,
            device="other-device",
        )
        self.state["certificates"].append(bad)
        self.write_state()
        self.reject()

    def test_success_deletes_recorded_id_and_keys(self):
        record = self.add_certificate()
        device = self.directory / record["device"]
        device.mkdir()
        (device / "private.key").write_text("fixture")
        resources.run(self.args, self.session)
        calls = self.session.destructive()
        self.assertEqual(
            [c[1] for c in calls],
            [
                "detach_policy",
                "detach_thing_principal",
                "update_certificate",
                "delete_certificate",
                "delete_stack",
                "delete_bucket",
            ],
        )
        self.assertEqual(calls[-2][2], {"StackName": STACK_ID})
        self.assertFalse(device.exists())
        self.assertEqual(json.loads(self.manifest.read_text())["teardown"], "complete")
        # A completed cleanup can be retried without a new delete operation.
        self.session.calls.clear()
        resources.run(self.args, self.session)
        self.assertEqual(self.session.destructive(), [])

    def test_certificate_delete_failure_retains_record_for_retry(self):
        self.add_certificate()
        self.session.fail.add("delete_certificate")
        with self.assertRaises(RuntimeError):
            resources.run(self.args, self.session)
        state = json.loads(self.manifest.read_text())
        self.assertEqual(len(state["certificates"]), 1)
        self.assertFalse(any(c[1] == "delete_stack" for c in self.session.calls))
        self.session.fail.clear()
        resources.run(self.args, self.session)
        self.assertEqual(json.loads(self.manifest.read_text())["teardown"], "complete")

    def test_wait_failure_can_resume_after_stack_disappears(self):
        self.session.fail.add("wait")
        with self.assertRaises(RuntimeError):
            resources.run(self.args, self.session)
        self.assertTrue(json.loads(self.manifest.read_text())["stack_delete_requested"])
        self.assertFalse(any(c[1] == "delete_bucket" for c in self.session.calls))
        self.session.fail.clear()
        self.session.stack = self.session.old_stack = None
        resources.run(self.args, self.session)
        self.assertEqual(json.loads(self.manifest.read_text())["teardown"], "complete")

    def test_bucket_delete_failure_can_resume(self):
        self.session.fail.add("delete_bucket")
        with self.assertRaises(RuntimeError):
            resources.run(self.args, self.session)
        self.assertIn("bucket", json.loads(self.manifest.read_text()))
        self.assertNotIn("deleted_bucket", json.loads(self.manifest.read_text()))
        self.session.fail.clear()
        resources.run(self.args, self.session)
        self.assertNotIn("bucket", json.loads(self.manifest.read_text()))
        self.assertEqual(json.loads(self.manifest.read_text())["deleted_bucket"], {
            "name": self.args.bucket, "account": ACCOUNT, "region": "eu-central-1",
            "deployment_id": self.state["deployment_id"], "stack_id": STACK_ID,
        })

    def test_deleted_bucket_record_survives_completed_teardown_retry(self):
        resources.run(self.args, self.session)
        before = json.loads(self.manifest.read_text())["deleted_bucket"]
        self.session.calls.clear()
        resources.run(self.args, self.session)
        self.assertEqual(json.loads(self.manifest.read_text())["deleted_bucket"], before)
        self.assertEqual(self.session.destructive(), [])

    def test_deleted_bucket_scope_mismatch_rejected_without_api_calls(self):
        record = {"name": self.args.bucket, "account": ACCOUNT, "region": "eu-central-1",
                  "deployment_id": self.state["deployment_id"], "stack_id": STACK_ID}
        for key, value in [("account", "000000000000"), ("region", "us-east-1"),
                           ("deployment_id", "b" * 32), ("stack_id", STACK_ID + "-other"),
                           ("name", "unrelated-bucket")]:
            with self.subTest(key=key):
                self.state["deleted_bucket"] = {**record, key: value}
                self.write_state()
                self.session.calls.clear()
                self.reject()
                self.assertEqual(self.session.calls, [])

    def test_new_bucket_clears_previous_predeployment_cleanup_record(self):
        self.state.pop("stack_id")
        self.write_state()
        self.session.stack = self.session.old_stack = None
        self.args.command = "cleanup-bucket"
        resources.run(self.args, self.session)
        self.assertIsNone(json.loads(self.manifest.read_text())["deleted_bucket"]["stack_id"])
        self.args.command = "artifact-bucket"
        resources.run(self.args, self.session)
        state = json.loads(self.manifest.read_text())
        self.assertEqual(state["bucket"], self.args.bucket)
        self.assertNotIn("deleted_bucket", state)

    def test_bucket_setup_failure_records_id_and_can_resume(self):
        self.state.pop("stack_id")
        self.state.pop("bucket")
        self.write_state()
        self.session.stack = self.session.old_stack = None
        self.args.command = "artifact-bucket"
        self.session.fail.add("put_bucket_tagging")
        with self.assertRaises(RuntimeError):
            resources.run(self.args, self.session)
        self.assertEqual(
            json.loads(self.manifest.read_text())["bucket"], self.args.bucket
        )
        self.session.fail.clear()
        resources.run(self.args, self.session)
        self.assertEqual(sum(c[1] == "create_bucket" for c in self.session.calls), 1)

    def test_bucket_only_cleanup_and_stack_rejection(self):
        self.state.pop("stack_id")
        self.write_state()
        self.args.command = "cleanup-bucket"
        self.reject()
        self.session.stack = self.session.old_stack = None
        resources.run(self.args, self.session)
        self.assertEqual([c[1] for c in self.session.destructive()], ["delete_bucket"])

    def test_register_failed_stack_without_outputs(self):
        self.state.pop("stack_id")
        self.write_state()
        self.args.command = "register-stack"
        self.session.stack["StackStatus"] = "ROLLBACK_COMPLETE"
        self.session.stack.pop("Outputs")
        resources.run(self.args, self.session)
        self.assertEqual(json.loads(self.manifest.read_text())["stack_id"], STACK_ID)
        self.assertEqual(self.session.destructive(), [])

    def test_partial_certificate_setup_keeps_id_and_private_key(self):
        self.args.command = "certificates"
        self.session.fail.add("attach_policy")
        with self.assertRaises(RuntimeError):
            resources.run(self.args, self.session)
        state = json.loads(self.manifest.read_text())
        self.assertEqual(len(state["certificates"]), 1)
        self.assertEqual(
            os.stat(self.directory / "sensor-a/private.key").st_mode & 0o777, 0o600
        )
        self.session.fail.clear()
        resources.run(self.args, self.session)
        self.assertEqual(
            sum(c[1] == "create_keys_and_certificate" for c in self.session.calls), 2
        )
        self.assertEqual(len(json.loads(self.manifest.read_text())["certificates"]), 2)

    def test_lost_private_key_does_not_allocate_duplicate(self):
        self.add_certificate()
        self.args.command = "certificates"
        self.reject()
        self.assertFalse(
            any(c[1] == "create_keys_and_certificate" for c in self.session.calls)
        )

    def test_execution_flag_prevents_sdk_session(self):
        with patch.object(resources.boto3, "Session") as session:
            with self.assertRaises(SystemExit):
                resources.main(
                    ["teardown", "--account", ACCOUNT, "--stack", "telemetry-demo"]
                )
            session.assert_not_called()

    def test_stack_identity_with_real_sdk_stubber(self):
        client = self.session.models["cloudformation"]
        stack = copy.deepcopy(self.session.stack)
        # CreationTime is required by the SDK response model.
        import datetime

        stack["CreationTime"] = datetime.datetime.now(datetime.timezone.utc)
        with Stubber(client) as stub:
            stub.add_response(
                "describe_stacks", {"Stacks": [stack]}, {"StackName": STACK_ID}
            )
            stub.add_response(
                "describe_stacks", {"Stacks": [stack]}, {"StackName": "telemetry-demo"}
            )
            self.assertEqual(
                resources.validate_stack(client, self.state)["StackId"], STACK_ID
            )
            stub.assert_no_pending_responses()


if __name__ == "__main__":
    unittest.main()
