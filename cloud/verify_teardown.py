#!/usr/bin/env python3
"""Read-only checks of this telemetry demo's recorded AWS resources.

Run from the project root after manifest-bound teardown. This script makes no
AWS mutations. It writes a result summary locally, without account/resource IDs.
"""
import argparse
from collections import Counter
import datetime
import json
from pathlib import Path
import re


EXPECTED = {
    "AWS::DynamoDB::Table": 1,
    "AWS::SQS::Queue": 1,
    "AWS::Logs::LogGroup": 2,
    "AWS::IAM::Role": 2,
    "AWS::Lambda::Function": 1,
    "AWS::Lambda::EventInvokeConfig": 1,
    "AWS::IoT::TopicRule": 1,
    "AWS::Lambda::Permission": 1,
    "AWS::CloudWatch::Alarm": 1,
    "AWS::IoT::Thing": 2,
    "AWS::IoT::Policy": 2,
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def recorded_artifact_bucket(state, supplied):
    """Bind the absence lookup to the private record retained during cleanup."""
    record = state.get("deleted_bucket")
    require(isinstance(record, dict), "Recorded artifact bucket identity required")
    name = record.get("name")
    require(isinstance(name, str) and re.fullmatch(r"telemetry-artifacts-[a-z0-9-]{3,43}", name),
            "Invalid recorded artifact bucket name")
    require(not state.get("bucket"), "Manifest artifact bucket cleanup incomplete")
    require(all(key in state and record.get(key) == state[key]
                for key in ("account", "region", "deployment_id", "stack_id")),
            "Recorded artifact bucket scope mismatch")
    require(supplied == name, "Artifact bucket differs from recorded identity")
    return name


def topic_rule_absent(iot, name, arn, max_pages=100):
    """Confirm absence using every list page; API errors never prove absence."""
    token = None
    seen_tokens = set()
    for _ in range(max_pages):
        params = {"maxResults": 250}
        if token:
            params["nextToken"] = token
        page = iot.list_topic_rules(**params)
        if any(r.get("ruleName") == name or r.get("ruleArn") == arn
               for r in page.get("rules", [])):
            return False
        token = page.get("nextToken")
        if not token:
            return True
        require(token not in seen_tokens, "Repeated IoT rule pagination token")
        seen_tokens.add(token)
    raise RuntimeError("IoT rule listing exceeded the bounded page limit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True)
    parser.add_argument("--region", default="eu-central-1", choices=["eu-central-1"])
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--directory", type=Path, default=Path(".runtime/cloud"))
    parser.add_argument("--output", type=Path, default=Path("results/aws-teardown.json"))
    args = parser.parse_args()

    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    state = json.loads((args.directory / "resources.json").read_text())
    before = json.loads((args.directory / "stack-before-teardown.json").read_text())
    inventory = json.loads((args.directory / "resources-before-teardown.json").read_text())
    resources = inventory["StackResourceSummaries"]
    require(re.fullmatch(r"[0-9]{12}", args.account), "Invalid target account")
    require((state["account"], state["region"]) == (args.account, args.region), "Manifest scope mismatch")
    require(state.get("teardown") == "complete" and not state["certificates"], "Manifest teardown incomplete")
    require(state["stack_id"] == before["StackId"] and state["stack"] == before["StackName"], "Snapshot identity mismatch")
    require(state["stack_id"].startswith(f"arn:aws:cloudformation:{args.region}:{args.account}:stack/"), "Stack ARN scope mismatch")
    parameters = {p["ParameterKey"]: p.get("ParameterValue") for p in before["Parameters"]}
    tags = {t["Key"]: t["Value"] for t in before["Tags"]}
    require(parameters.get("DeploymentId") == state["deployment_id"] == tags.get("TelemetryDeployment"), "Deployment identifier mismatch")
    require(parameters.get("ProjectName") == state["project_name"] == tags.get("TelemetryProjectName"), "Project identity mismatch")
    require(tags.get("TelemetryProject") == "reliable-telemetry-ingestion", "Project tag mismatch")
    artifact_bucket = recorded_artifact_bucket(state, args.bucket)
    require(Counter(r["ResourceType"] for r in resources) == Counter(EXPECTED), "Unexpected or incomplete saved resource inventory")
    require(len(state["deleted_certificates"]) == 2, "Expected two recorded deleted certificates")
    outputs = {o["OutputKey"]: o["OutputValue"] for o in before["Outputs"]}
    rule_id = next(r["PhysicalResourceId"] for r in resources if r["ResourceType"] == "AWS::IoT::TopicRule")
    rule_name = rule_id.rsplit("/", 1)[-1] if rule_id.startswith("arn:") else rule_id
    require(rule_name == outputs["RuleName"], "Saved IoT rule identity mismatch")
    rule_arn = f"arn:aws:iot:{args.region}:{args.account}:rule/{rule_name}"

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    config = Config(connect_timeout=3, read_timeout=5, retries={"mode": "standard", "total_max_attempts": 2})
    clients = {}

    def client(service):
        if service not in clients:
            clients[service] = session.client(service, config=config)
        return clients[service]

    require(client("sts").get_caller_identity()["Account"] == args.account, "Active AWS account mismatch")
    checks = []

    def run_check(label, operation):
        try:
            result = operation()
            require(result is not False, "Resource still exists or deletion is incomplete")
        except Exception as exc:
            code = exc.response["Error"]["Code"] if isinstance(exc, ClientError) else type(exc).__name__
            checks.append({"check": label, "status": "failed", "reason": code})
            print(f"FAIL: {label} ({code})", flush=True)
        else:
            checks.append({"check": label, "status": "passed"})
            print(f"PASS: {label}", flush=True)

    def absent(service, operation, params, missing):
        try:
            getattr(client(service), operation)(**params)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in missing:
                return True
            raise
        return False

    def deleted_stack():
        try:
            stacks = client("cloudformation").describe_stacks(StackName=state["stack_id"])["Stacks"]
        except ClientError as exc:
            error = exc.response["Error"]
            if error["Code"] == "ValidationError" and "does not exist" in error.get("Message", ""):
                return True
            raise
        return len(stacks) == 1 and stacks[0]["StackStatus"] == "DELETE_COMPLETE"

    run_check("recorded stack deleted", deleted_stack)

    def name_absent():
        try:
            client("cloudformation").describe_stacks(StackName=state["stack"])
        except ClientError as exc:
            error = exc.response["Error"]
            if error["Code"] == "ValidationError" and "does not exist" in error.get("Message", ""):
                return True
            raise
        return False

    run_check("stack name has no active replacement", name_absent)

    def deleted_inventory():
        current = {}
        for page in client("cloudformation").get_paginator("list_stack_resources").paginate(StackName=state["stack_id"]):
            current.update({r["LogicalResourceId"]: r for r in page["StackResourceSummaries"]})
        return all(
            r["LogicalResourceId"] in current
            and current[r["LogicalResourceId"]].get("PhysicalResourceId") == r.get("PhysicalResourceId")
            and current[r["LogicalResourceId"]]["ResourceStatus"] == "DELETE_COMPLETE"
            for r in resources
        )

    run_check("all 15 recorded stack resources DELETE_COMPLETE", deleted_inventory)
    run_check("artifact bucket absent", lambda: absent("s3", "head_bucket", {"Bucket": artifact_bucket, "ExpectedBucketOwner": args.account}, {"404", "NoSuchBucket"}))

    for number, record in enumerate(state["deleted_certificates"], 1):
        require(record["stack_id"] == state["stack_id"] and record["deployment_id"] == state["deployment_id"], "Certificate scope mismatch")
        require(record["arn"] == f"arn:aws:iot:{args.region}:{args.account}:cert/{record['id']}", "Certificate ARN mismatch")
        run_check(f"device certificate {number} absent", lambda r=record: absent("iot", "describe_certificate", {"certificateId": r["id"]}, {"ResourceNotFoundException"}))

    specifications = {
        "AWS::DynamoDB::Table": ("dynamodb", "describe_table", "TableName", {"ResourceNotFoundException"}),
        "AWS::Lambda::Function": ("lambda", "get_function", "FunctionName", {"ResourceNotFoundException"}),
        "AWS::Logs::LogGroup": ("logs", "filter_log_events", "logGroupName", {"ResourceNotFoundException"}),
        "AWS::IAM::Role": ("iam", "get_role", "RoleName", {"NoSuchEntity"}),
        "AWS::IoT::Thing": ("iot", "describe_thing", "thingName", {"ResourceNotFoundException"}),
        "AWS::IoT::Policy": ("iot", "get_policy", "policyName", {"ResourceNotFoundException"}),
    }
    for resource in resources:
        kind = resource["ResourceType"]
        physical = resource["PhysicalResourceId"]
        label = resource["LogicalResourceId"] + " absent"
        if kind in specifications:
            service, operation, key, missing = specifications[kind]
            params = {key: physical}
            if service == "logs":
                params["limit"] = 1
            run_check(label, lambda s=service, op=operation, p=params, m=missing: absent(s, op, p, m))
        elif kind == "AWS::IoT::TopicRule":
            run_check(label, lambda: topic_rule_absent(client("iot"), rule_name, rule_arn))
        elif kind == "AWS::SQS::Queue":
            run_check(label, lambda p=physical: absent("sqs", "get_queue_attributes", {"QueueUrl": p, "AttributeNames": ["QueueArn"]}, {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}))
        elif kind == "AWS::CloudWatch::Alarm":
            def alarm_absent(name=physical):
                result = client("cloudwatch").describe_alarms(AlarmNames=[name])
                return not result.get("MetricAlarms") and not result.get("CompositeAlarms")
            run_check(label, alarm_absent)
        # Lambda permission and async config are owned by the function. Their
        # deletion is covered by its absence and the exact CloudFormation IDs.

    result = {
        "status": "passed" if all(c["status"] == "passed" for c in checks) else "failed",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "region": args.region,
        "stack_resources": 15,
        "external_certificates": 2,
        "artifact_buckets": 1,
        "checks": checks,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, separators=(",", ":")))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
