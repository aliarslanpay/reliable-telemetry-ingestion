# AWS adapter runbook

IoT Core receives gateway events over mutual TLS. An IoT Rule forwards a bounded
base64 payload and the authorized topic device to Lambda. Lambda validates the
shared contract, conditionally inserts into DynamoDB, then publishes a QoS 1
application ACK. Retries preserve the original identity. Conditional conflicts
never overwrite stored data. No TTL or deletion participates in deduplication.

DynamoDB uses `pk = device_id#stream_id` and a 16-digit sequence `sk`. Exact reads
and bounded range/count queries use strong consistency; at most 100 items/page
and 10 pages. `truncated=true` identifies partial results. PostgreSQL and DynamoDB
share semantics, not performance or availability guarantees. Lambda failures
propagate to asynchronous retries and a failure queue. IoT dispatch failures use
a separate log group. Offline tests cannot verify service permissions or live TLS.

## Offline checks

Use a Python 3.12 environment without inherited Python path overrides:

```sh
python3 -I -m venv .venv-cloud
.venv-cloud/bin/python -I -m pip install -r cloud/requirements-tools.txt -r ingestion/requirements-cloud.txt -r ingestion/requirements.txt
.venv-cloud/bin/python -I -m pip check
PYTHONPATH=ingestion .venv-cloud/bin/python tests/cloud_unit.py
PYTHONPATH=ingestion .venv-cloud/bin/python tests/cloud_resources_unit.py
.venv-cloud/bin/python tests/cloud_teardown_unit.py
AWS_EC2_METADATA_DISABLED=true SAM_CLI_TELEMETRY=0 .venv-cloud/bin/sam validate --lint -t cloud/template.yaml --region eu-central-1
SAM_CLI_TELEMETRY=0 .venv-cloud/bin/sam build -t cloud/template.yaml --build-dir .aws-sam/build --region eu-central-1
```

The tooling file pins SAM 1.145.1. A separately installed SAM can be used after checking its version.
The package contains only the shared contract, AWS adapter and pinned SDK.

## Prepare the deployment scope

Read [the resource and cost plan](DEPLOYMENT_PLAN.md), complete native verification,
and check the intended account's identity, plan, credits, service access and quotas.
Use a normal deployment IAM identity and Frankfurt (`eu-central-1`). Set concrete
values locally; the names below are examples, not account details:

```sh
export AWS_PROFILE=YOUR_PROFILE
export AWS_REGION=eu-central-1
export TARGET_ACCOUNT=YOUR_12_DIGIT_ACCOUNT_ID
export STACK_NAME=telemetry-demo
export PROJECT_NAME=telemetry_demo
export ARTIFACT_BUCKET=telemetry-artifacts-YOUR_UNIQUE_SUFFIX
aws sts get-caller-identity --no-cli-pager
IOT_ENDPOINT=$(aws iot describe-endpoint --endpoint-type iot:Data-ATS --query endpointAddress --output text)
```

The helper requires `--execute` and checks STS against `--account`. Its private
manifest in `.runtime/cloud/resources.json` binds account, region, stack name,
project and a generated deployment identifier. Back up this directory securely;
it includes private keys and must never be tracked or published. Run one helper
at a time and do not edit the manifest to bypass a rejected ownership check.

## Bucket, SAM deployment and stack registration

```sh
.venv-cloud/bin/python cloud/resources.py artifact-bucket --execute \
  --account "$TARGET_ACCOUNT" --stack "$STACK_NAME" --project-name "$PROJECT_NAME" \
  --bucket "$ARTIFACT_BUCKET"
DEPLOYMENT_ID=$(.venv-cloud/bin/python -c 'import json; print(json.load(open(".runtime/cloud/resources.json"))["deployment_id"])')
.venv-cloud/bin/sam deploy --template-file .aws-sam/build/template.yaml \
  --stack-name "$STACK_NAME" --region "$AWS_REGION" --s3-bucket "$ARTIFACT_BUCKET" \
  --capabilities CAPABILITY_IAM \
  --tags "TelemetryProject=reliable-telemetry-ingestion TelemetryDeployment=$DEPLOYMENT_ID TelemetryProjectName=$PROJECT_NAME" \
  --parameter-overrides "ProjectName=$PROJECT_NAME DeploymentId=$DEPLOYMENT_ID IotDataEndpoint=$IOT_ENDPOINT DeviceA=sensor-a DeviceB=sensor-b"
.venv-cloud/bin/python cloud/resources.py register-stack --execute \
  --account "$TARGET_ACCOUNT" --stack "$STACK_NAME" --project-name "$PROJECT_NAME"
```

Registration verifies the CloudFormation StackId ARN, stack tags and template
parameters before recording the actual ID. Certificate provisioning and teardown
require that ID; a stack name alone never authorizes deletion. `--resolve-s3` is
unnecessary because this deployment has its own recorded artifact bucket.

## Certificates and bounded verification

```sh
.venv-cloud/bin/python cloud/resources.py certificates --execute \
  --account "$TARGET_ACCOUNT" --stack "$STACK_NAME" --project-name "$PROJECT_NAME"
curl --fail --silent --show-error https://www.amazontrust.com/repository/AmazonRootCA1.pem \
  -o .runtime/cloud/root-ca.pem
PYTHONPATH=ingestion .venv-cloud/bin/python cloud/verify.py --execute \
  --account "$TARGET_ACCOUNT" --stack "$STACK_NAME" --project-name "$PROJECT_NAME" \
  --gateway build/gateway --output results/aws-live.json
```

Certificates are recorded immediately after allocation, before writing the key or
attaching the thing/policy. Directories use mode 0700; certificates and private keys
use 0600. IoT returns the private key only at creation. Gateway TLS paths use
`MQTT_CA_FILE`, `MQTT_CERT_FILE`, `MQTT_KEY_FILE`; clear local username/password
variables for certificate connections. Preserve hostname/certificate validation.

The live verifier uses the registered stack and runs 10 gateway events/device,
one duplicate and conflict per device, cross-device topic denial, and one malformed
asynchronous Lambda invocation. Each gateway has a 20 s deadline/window 2; the
failure queue and alarm checks have bounded waits. It does not inject an IoT Rule
dispatch error. Use `cloud/query.py --help` for exact/range/count inspection.

## Teardown and interrupted-run recovery

Save the deployed stack and exact resource inventory before teardown. These
private snapshots let the independent checker query the recorded identities
after the stack has been deleted:

```sh
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query 'Stacks[0]' --output json > .runtime/cloud/stack-before-teardown.json
aws cloudformation list-stack-resources --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --output json > .runtime/cloud/resources-before-teardown.json
.venv-cloud/bin/python cloud/resources.py teardown --execute \
  --account "$TARGET_ACCOUNT" --stack "$STACK_NAME" --project-name "$PROJECT_NAME"
.venv-cloud/bin/python cloud/verify_teardown.py \
  --account "$TARGET_ACCOUNT" --region "$AWS_REGION" --bucket "$ARTIFACT_BUCKET" \
  --directory .runtime/cloud --output results/aws-teardown.json
```

The cleanup helper retains `deleted_bucket` in the private manifest, binding the
name to account, region, deployment identifier and StackId. The checker requires
that record and matches `--bucket` to it before creating an AWS session. The record
is never included in public results. A resumed teardown preserves the identity;
a new artifact bucket clears an earlier pre-deployment cleanup record.

The checker makes no AWS mutations and omits account/resource identifiers from
its output. It requires this template's 15-resource inventory and two recorded
certificates. It checks CloudFormation deletion status and service-level absence,
including both execution roles, logs, queue, table, function, alarm, IoT things,
policies and rule, certificates and artifact bucket. Lambda permission/async
configuration are covered by their exact CloudFormation deletion records and
the absence of the parent function. API/permission errors fail the check.
Rule absence is determined by a full paginated `ListTopicRules` lookup of the
recorded name/ARN, not by interpreting a failed `GetTopicRule` call as absence.

Before any destructive call, teardown validates the manifest, active account,
region, stored StackId and the current name-to-ID mapping, stack ownership tags and
parameters, bucket owner/location/tags/versioning, and all certificate identities
and attachments. Unexpected policy/thing attachments reject cleanup. Then it
detaches policies/things, deactivates and deletes recorded certificates, removes
local keys, deletes the stack by **StackId**, waits for deletion, and empties/deletes
the owned artifact bucket. Bucket cleanup is capped at 10,000 current objects;
versioned buckets need manual inspection. This is a demo guard backed by private
local state and mutable AWS tags, not a security boundary against account admins.

| Interruption | Recovery |
| --- | --- |
| Bucket created; tagging/encryption failed | Repeat `artifact-bucket` with the same manifest/name to finish setup. The bucket name was saved before configuration calls. Cleanup requires the ownership tags to be present. |
| SAM failed but a stack exists, including rollback states | Run `register-stack` with the original deployment tags/parameters, then `teardown`. Outputs are not required to register a failed stack. |
| SAM created no stack | Use `cleanup-bucket --execute --account "$TARGET_ACCOUNT" --stack "$STACK_NAME" --project-name "$PROJECT_NAME"`. It refuses if any stack exists or a StackId was registered. |
| Certificate allocated; key/attach failed | Keep the recorded ID. If both key files exist, repeating `certificates` reattaches the existing ID and provisions the remaining device. If the private key is lost/incomplete, use teardown and a fresh deployment; no duplicate certificate is allocated to replace a missing key. |
| Certificate deletion or stack wait failed | Repeat `teardown` using the same manifest. Remaining IDs stay recorded; completed certificate deletions and a requested stack deletion are recorded atomically. |
| Stack gone; artifact cleanup failed | Repeat `teardown`. An absent stack is accepted only after a recorded delete request with no remaining certificates. A replacement stack under the old name is rejected. |
| Manifest missing/corrupt or ownership differs | Stop automated cleanup. Restore the private backup and reconcile exact IDs with CloudFormation/IoT/S3 records. Do not synthesize state from a name. |

A process failure between a successful create response and the local manifest
write can still leave an unrecorded external resource. Resolve that narrow window
using service audit records and exact IDs; never infer ownership from a name alone.
An empty device directory left by a failed create can be removed after confirming
that no certificate was allocated. Do not remove a directory containing keys.

## Results

The live verifier writes `results/aws-live.json` only after its checks complete.
The teardown checker writes `results/aws-teardown.json` with per-resource outcomes;
API errors fail the check. Both use actual UTC observation timestamps and omit
account IDs, endpoints, certificate IDs, keys and bucket names.

Keep private resource snapshots under `.runtime/cloud/`. After a successful run,
review the two result summaries before adding them under `docs/results/`.
IoT Rule dispatch-error injection is not covered.

## API references

- [DescribeStacks: names, IDs and deleted stacks](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_DescribeStacks.html)
- [Stack IDs, tags and parameters](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_Stack.html)
- [DeleteStack by unique ID](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_DeleteStack.html)
- [SAM deployment options and tags](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-command-reference-sam-deploy.html)
- [IoT certificate creation and one-time private key](https://docs.aws.amazon.com/iot/latest/apireference/API_CreateKeysAndCertificate.html)
- [IoT certificate deletion prerequisites](https://docs.aws.amazon.com/iot/latest/apireference/API_DeleteCertificate.html)
- [S3 expected bucket owner](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html)
