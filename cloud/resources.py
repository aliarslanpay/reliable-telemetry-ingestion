#!/usr/bin/env python3
"""Provision and clean up a manifest-bound demo deployment."""
import argparse
import json
import os
from pathlib import Path
import re
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

PROJECT = "reliable-telemetry-ingestion"
REGION = "eu-central-1"
CONFIG = Config(
    connect_timeout=2,
    read_timeout=3,
    retries={"mode": "standard", "total_max_attempts": 2},
)


class OwnershipError(RuntimeError):
    """The supplied scope or recorded resource ownership cannot be verified."""


def save(path, state):
    """Atomically replace private state and flush both file and directory."""
    temp = path.with_suffix(".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as file:
        json.dump(state, file, indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def ownership_tags(state):
    return {
        "TelemetryProject": PROJECT,
        "TelemetryDeployment": state["deployment_id"],
        "TelemetryProjectName": state["project_name"],
    }


def check_stack_id(stack_id, state):
    pattern = (
        rf"arn:aws:cloudformation:{re.escape(state['region'])}:"
        rf"{state['account']}:stack/{re.escape(state['stack'])}/[A-Za-z0-9-]+"
    )
    if not isinstance(stack_id, str) or not re.fullmatch(pattern, stack_id):
        raise OwnershipError(
            "StackId does not match the recorded account, region and name"
        )


def load_manifest(path, account, stack, region, project_name):
    try:
        state = json.loads(path.read_text())
        valid = (
            state["schema_version"] == 1
            and state["project"] == PROJECT
            and (
                state["account"],
                state["stack"],
                state["region"],
                state["project_name"],
            )
            == (account, stack, region, project_name)
            and re.fullmatch(r"[a-f0-9]{32}", state["deployment_id"])
            and isinstance(state["certificates"], list)
            and isinstance(state["deleted_certificates"], list)
            and type(state["stack_delete_requested"]) is bool
        )
        if not valid:
            raise ValueError("invalid scope or schema")
        if state.get("stack_id"):
            check_stack_id(state["stack_id"], state)
        if state.get("bucket") and not re.fullmatch(
            r"telemetry-artifacts-[a-z0-9-]{3,43}", state["bucket"]
        ):
            raise ValueError("invalid artifact bucket")
        deleted_bucket = state.get("deleted_bucket")
        if deleted_bucket is not None:
            if not isinstance(deleted_bucket, dict) or not re.fullmatch(
                r"telemetry-artifacts-[a-z0-9-]{3,43}", deleted_bucket.get("name", "")
            ) or any(
                deleted_bucket.get(key) != state.get(key)
                for key in ("account", "region", "deployment_id", "stack_id")
            ):
                raise ValueError("invalid deleted artifact bucket ownership")
        seen = set()
        for record in state["certificates"] + state["deleted_certificates"]:
            if (
                record["deployment_id"] != state["deployment_id"]
                or record["stack_id"] != state.get("stack_id")
                or not re.fullmatch(r"[a-f0-9]{64}", record["id"])
                or record["arn"]
                != f"arn:aws:iot:{region}:{account}:cert/{record['id']}"
                or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,31}", record["device"])
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", record["policy"])
                or record["id"] in seen
            ):
                raise ValueError("invalid certificate ownership")
            seen.add(record["id"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise OwnershipError(
            "Existing valid resource manifest required; restore private state"
        ) from exc
    return state


def describe_stack(cf, name):
    try:
        stacks = cf.describe_stacks(StackName=name)["Stacks"]
    except ClientError as exc:
        error = exc.response["Error"]
        if error["Code"] == "ValidationError" and "does not exist" in error.get(
            "Message", ""
        ):
            return None
        raise
    if len(stacks) != 1:
        raise OwnershipError("Expected one specific CloudFormation stack")
    return stacks[0]


def check_live_stack(stack, state):
    check_stack_id(stack.get("StackId"), state)
    if stack.get("StackName") != state["stack"]:
        raise OwnershipError("Stack name mismatch")
    tags = {tag["Key"]: tag["Value"] for tag in stack.get("Tags", [])}
    params = {
        p["ParameterKey"]: p.get("ParameterValue") for p in stack.get("Parameters", [])
    }
    if any(tags.get(k) != v for k, v in ownership_tags(state).items()):
        raise OwnershipError("Stack ownership tags mismatch")
    if (params.get("ProjectName"), params.get("DeploymentId")) != (
        state["project_name"],
        state["deployment_id"],
    ):
        raise OwnershipError("Stack ownership parameters mismatch")
    if state.get("stack_id") and stack["StackId"] != state["stack_id"]:
        raise OwnershipError("Stack identity changed; refusing replacement stack")


def validate_stack(cf, state, allow_deleted=False):
    if not state.get("stack_id"):
        raise OwnershipError(
            "Register the actual StackId before certificates or teardown"
        )
    stack = describe_stack(cf, state["stack_id"])
    named = describe_stack(cf, state["stack"])
    if named is not None:
        check_live_stack(named, state)
    if stack is None or stack.get("StackStatus") == "DELETE_COMPLETE":
        if (
            allow_deleted
            and state["stack_delete_requested"]
            and not state["certificates"]
        ):
            return None
        raise OwnershipError("Recorded stack is absent or deleted")
    check_live_stack(stack, state)
    if named is None:
        raise OwnershipError(
            "Recorded stack name no longer resolves to the same identity"
        )
    return stack


def deployment_outputs(stack):
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def validate_bucket(s3, state):
    if not state.get("bucket"):
        return False
    params = {"Bucket": state["bucket"], "ExpectedBucketOwner": state["account"]}
    try:
        s3.head_bucket(**params)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            return False
        raise
    if s3.get_bucket_location(**params).get("LocationConstraint") != state["region"]:
        raise OwnershipError("Artifact bucket region mismatch")
    tags = {t["Key"]: t["Value"] for t in s3.get_bucket_tagging(**params)["TagSet"]}
    if any(tags.get(k) != v for k, v in ownership_tags(state).items()):
        raise OwnershipError("Artifact bucket ownership tags mismatch")
    versioning = s3.get_bucket_versioning(**params)
    if versioning.get("Status"):
        raise OwnershipError(
            "Unexpected bucket versioning; inspect versions before cleanup"
        )
    return True


def validate_certificates(iot, stack, state):
    outputs = deployment_outputs(stack) if stack else {}
    expected = {
        (
            outputs.get("Device" + letter + "Name"),
            outputs.get("Device" + letter + "PolicyName"),
        )
        for letter in ("A", "B")
    }
    for record in state["certificates"]:
        if (record["device"], record["policy"]) not in expected:
            raise OwnershipError(
                "Recorded certificate does not belong to stack outputs"
            )
        try:
            certificate = iot.describe_certificate(certificateId=record["id"])[
                "certificateDescription"
            ]
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                continue
            raise
        if (certificate["certificateId"], certificate["certificateArn"]) != (
            record["id"],
            record["arn"],
        ):
            raise OwnershipError("Certificate identity mismatch")
        # A demo certificate should have at most one thing and one policy. Reject
        # extra attachments (including further pages) before detaching anything.
        policies = iot.list_attached_policies(target=record["arn"], pageSize=250)
        things = iot.list_principal_things(principal=record["arn"], maxResults=250)
        if (
            policies.get("nextMarker")
            or things.get("nextToken")
            or any(
                p["policyName"] != record["policy"]
                for p in policies.get("policies", [])
            )
            or any(t != record["device"] for t in things.get("things", []))
        ):
            raise OwnershipError("Certificate has attachments outside this deployment")


def create_bucket(s3, state, manifest, bucket):
    if state.get("stack_id") or state["stack_delete_requested"]:
        raise OwnershipError("Artifact setup is only allowed before stack registration")
    if not bucket or not re.fullmatch(r"telemetry-artifacts-[a-z0-9-]{3,43}", bucket):
        raise OwnershipError("Supply an unused telemetry-artifacts-* bucket name")
    if state.get("bucket") and state["bucket"] != bucket:
        raise OwnershipError("A different artifact bucket is already recorded")
    if not state.get("bucket"):
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": state["region"]},
        )
        state["bucket"] = bucket
        state.pop("deleted_bucket", None)
        save(manifest, state)  # Record the external resource before any next SDK call.
    params = {"Bucket": bucket, "ExpectedBucketOwner": state["account"]}
    s3.head_bucket(**params)
    if s3.get_bucket_location(**params).get("LocationConstraint") != state["region"]:
        raise OwnershipError("Artifact bucket region mismatch")
    s3.put_bucket_tagging(
        **params,
        Tagging={
            "TagSet": [{"Key": k, "Value": v} for k, v in ownership_tags(state).items()]
        },
    )
    s3.put_public_access_block(
        **params,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_encryption(
        **params,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
            ]
        },
    )


def provision_certificates(iot, stack, state, manifest, directory):
    if (
        stack["StackStatus"] not in ("CREATE_COMPLETE", "UPDATE_COMPLETE")
        or state["stack_delete_requested"]
    ):
        raise OwnershipError(
            "Certificate provisioning requires a completed, active deployment"
        )
    outputs = deployment_outputs(stack)
    for letter in ("A", "B"):
        name = outputs["Device" + letter + "Name"]
        policy = outputs["Device" + letter + "PolicyName"]
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,31}", name):
            raise OwnershipError("Invalid device name in stack outputs")
        device = directory / name
        existing = [r for r in state["certificates"] if r["device"] == name]
        if existing:
            record = existing[0]
            if not all(
                (device / f).is_file() for f in ("certificate.pem", "private.key")
            ):
                raise OwnershipError(
                    "Recorded certificate key is missing; teardown then start a new deployment"
                )
        else:
            device.mkdir(
                mode=0o700
            )  # Fail before allocation if local state already exists.
            response = iot.create_keys_and_certificate(setAsActive=True)
            record = {
                "device": name,
                "policy": policy,
                "arn": response["certificateArn"],
                "id": response["certificateId"],
                "deployment_id": state["deployment_id"],
                "stack_id": state["stack_id"],
            }
            state["certificates"].append(record)
            save(manifest, state)
            for filename, value in (
                ("certificate.pem", response["certificatePem"]),
                ("private.key", response["keyPair"]["PrivateKey"]),
            ):
                fd = os.open(
                    device / filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
                with os.fdopen(fd, "w") as file:
                    file.write(value)
                    file.flush()
                    os.fsync(file.fileno())
        iot.attach_thing_principal(thingName=name, principal=record["arn"])
        iot.attach_policy(policyName=policy, target=record["arn"])


def remove_certificates(iot, state, manifest, directory):
    for record in list(state["certificates"]):
        operations = (
            (
                "detach_policy",
                {"policyName": record["policy"], "target": record["arn"]},
            ),
            (
                "detach_thing_principal",
                {"thingName": record["device"], "principal": record["arn"]},
            ),
            (
                "update_certificate",
                {"certificateId": record["id"], "newStatus": "INACTIVE"},
            ),
            ("delete_certificate", {"certificateId": record["id"]}),
        )
        for operation, params in operations:
            try:
                getattr(iot, operation)(**params)
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
        try:
            iot.describe_certificate(certificateId=record["id"])
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        else:
            raise RuntimeError(
                "Certificate deletion not yet observable; retry teardown"
            )
        state["certificates"].remove(record)
        state["deleted_certificates"].append(record)
        save(manifest, state)
    # Also remove keys on a retry after the preceding manifest update succeeded.
    for record in state["deleted_certificates"]:
        device = directory / record["device"]
        for filename in ("certificate.pem", "private.key"):
            (device / filename).unlink(missing_ok=True)
        if device.exists():
            device.rmdir()


def remove_bucket(s3, state, manifest, exists):
    if not state.get("bucket"):
        return
    params = {"Bucket": state["bucket"], "ExpectedBucketOwner": state["account"]}
    if exists:
        for _ in range(10):
            objects = s3.list_objects_v2(**params, MaxKeys=1000).get("Contents", [])
            if not objects:
                break
            response = s3.delete_objects(
                **params,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects], "Quiet": True},
            )
            if response.get("Errors"):
                raise RuntimeError("Artifact object deletion incomplete; retry cleanup")
        if s3.list_objects_v2(**params, MaxKeys=1).get("Contents"):
            raise RuntimeError("Unexpected artifact volume; inspect before continuing")
        s3.delete_bucket(**params)
    state["deleted_bucket"] = {
        "name": state.pop("bucket"),
        "account": state["account"],
        "region": state["region"],
        "deployment_id": state["deployment_id"],
        "stack_id": state.get("stack_id"),
    }
    save(manifest, state)


def run(args, session):
    manifest = args.directory / "resources.json"
    if args.command == "artifact-bucket" and not manifest.exists():
        state = {
            "schema_version": 1,
            "project": PROJECT,
            "deployment_id": uuid.uuid4().hex,
            "account": args.account,
            "region": args.region,
            "stack": args.stack,
            "project_name": args.project_name,
            "certificates": [],
            "deleted_certificates": [],
            "stack_delete_requested": False,
        }
    else:
        state = load_manifest(
            manifest, args.account, args.stack, args.region, args.project_name
        )
    if session.region_name != args.region:
        raise OwnershipError("SDK session region mismatch")
    if (
        session.client("sts", config=CONFIG).get_caller_identity()["Account"]
        != args.account
    ):
        raise OwnershipError("Active account differs from target account")
    cf = session.client("cloudformation", config=CONFIG)
    s3 = session.client("s3", config=CONFIG)
    iot = session.client("iot", config=CONFIG)
    if args.command == "artifact-bucket":
        if describe_stack(cf, args.stack) is not None:
            raise OwnershipError(
                "Stack name already exists; do not create a new deployment manifest"
            )
        args.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(args.directory, 0o700)
        save(manifest, state)
        create_bucket(s3, state, manifest, args.bucket)
        print(
            "Artifact bucket configured; deployment_id recorded in the private manifest."
        )
    elif args.command == "register-stack":
        stack = describe_stack(cf, args.stack)
        if stack is None or stack["StackStatus"] == "DELETE_COMPLETE":
            raise OwnershipError("No stack available to register")
        check_live_stack(stack, state)
        state["stack_id"] = stack["StackId"]
        save(manifest, state)
        print("StackId and deployment ownership verified and registered.")
    elif args.command == "certificates":
        stack = validate_stack(cf, state)
        validate_certificates(iot, stack, state)
        provision_certificates(iot, stack, state, manifest, args.directory)
        print(
            "Device certificates attached; private keys stay in the runtime directory."
        )
    elif args.command == "cleanup-bucket":
        if (
            state.get("stack_id")
            or state["certificates"]
            or describe_stack(cf, args.stack) is not None
        ):
            raise OwnershipError(
                "Stack exists or was registered; use register-stack and teardown"
            )
        exists = validate_bucket(s3, state)
        remove_bucket(s3, state, manifest, exists)
        print("Pre-deployment artifact bucket cleanup complete.")
    else:
        # Complete read-only preflight before any detach, deactivate or delete.
        stack = validate_stack(cf, state, allow_deleted=True)
        bucket_exists = validate_bucket(s3, state)
        validate_certificates(iot, stack, state)
        remove_certificates(iot, state, manifest, args.directory)
        if stack is not None:
            state["stack_delete_requested"] = True
            save(manifest, state)
            if stack["StackStatus"] != "DELETE_IN_PROGRESS":
                cf.delete_stack(StackName=state["stack_id"])
            cf.get_waiter("stack_delete_complete").wait(
                StackName=state["stack_id"],
                WaiterConfig={"Delay": 5, "MaxAttempts": 60},
            )
        remove_bucket(s3, state, manifest, bucket_exists)
        state["teardown"] = "complete"
        save(manifest, state)
        print("Recorded certificates, stack and artifact bucket cleanup complete.")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=(
            "artifact-bucket",
            "register-stack",
            "certificates",
            "cleanup-bucket",
            "teardown",
        ),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--account", required=True)
    parser.add_argument("--stack", required=True)
    parser.add_argument("--project-name", default="telemetry_demo")
    parser.add_argument("--region", choices=(REGION,), default=REGION)
    parser.add_argument("--bucket")
    parser.add_argument("--directory", type=Path, default=Path(".runtime/cloud"))
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error(
            "No changes made; use --execute after reviewing the target account and deployment scope"
        )
    if not re.fullmatch(r"[0-9]{12}", args.account):
        parser.error("A 12-digit target account ID is required")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{0,127}", args.stack):
        parser.error("Invalid stack name")
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,20}", args.project_name):
        parser.error("Invalid ProjectName")
    os.umask(0o077)
    run(args, boto3.Session(region_name=args.region))


if __name__ == "__main__":
    main()
