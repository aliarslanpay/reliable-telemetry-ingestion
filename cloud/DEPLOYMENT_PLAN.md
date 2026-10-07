# AWS deployment scope and cost review

Use one short-lived telemetry deployment in Frankfurt (`eu-central-1`). Complete
native checks, select the target IAM identity, and verify account/service access
before execution. Review the actual account plan, credit balance/expiry, quotas and
pricing. A credit balance is not a spending cap or a zero-cost guarantee. Do not
change account plans or add services as part of this demo.

## Resource scope

| Resource | Cost driver and removal |
| --- | --- |
| One on-demand DynamoDB table | Conditional writes, consistent reads, small storage; no TTL, replicas, backups or secondary indexes; deleted with stack |
| One Python 3.12 Lambda, 128 MiB, 10 s timeout | Invocations/duration; asynchronous age 60 s, up to two retries; deleted with stack |
| One IoT Rule | Connections, messages, rule/action executions; bounded raw payload plus controlled topic metadata; deleted with stack |
| Two IoT Things/policies | Exact client/event/ACK scopes; deleted with stack |
| Two external active certificates | Allocated by helper and immediately recorded; detached/deactivated/deleted before stack |
| Two CloudWatch log groups | Ingest/storage, one-day retention; deleted with stack |
| One Lambda Errors alarm | Standard metric alarm, no SNS action; deleted with stack |
| One SQS asynchronous failure queue | Requests and failed invocation envelopes, one-hour retention; deleted with stack |
| Two IAM execution roles and scoped invocation permission | Owned table/device ACK/log/queue scopes; deleted with stack |
| One external S3 artifact bucket | Deployment archives, requests/storage; blocked public access, SSE-S3; emptied/deleted by helper |

The failure queue receives Lambda failures after asynchronous retries; Rule
dispatch errors use a separate log group. IoT dispatch success does not prove a
successful Lambda write. Producer application retries remain necessary. There is
no queue consumer, second ingestion architecture or unattended load service.
No EC2, RDS, NAT gateway, Kubernetes or extra observability stack is included.

## Ownership and operational review

Record a concrete account ID, region, stack/project names, unused bucket name,
two distinct device names and ATS endpoint. The helper creates a private deployment
identifier before bucket allocation. SAM must receive that identifier as both a
template parameter and stack tag. Register the actual StackId before creating
certificates. Follow [the runbook](README.md) for commands and failure recovery.

Keep the private manifest and keys backed up outside Git. `--execute` is mandatory
and STS must match the target account. A missing/invalid manifest, changed StackId,
wrong account/region, or ownership mismatch fails before destructive calls.
Do not replace these checks with name-only cleanup or account-wide resource scans.
Budget notifications, if separately configured, alert on spending but do not cap it.

## Bounded verification and cleanup

Run at most 10 unique gateway events/device, one identical retry and one conflict
per device, cross-device publish/subscribe denials, and one malformed asynchronous
Lambda invocation. Gateway deadline is 20 s/window 2; queue/alarm waits are bounded.
Additional retransmissions can occur. Check usage and logs after the run; do not
infer a maximum cost from the unique event count alone.

Verify real TLS/hostname, topic identity, durable ACKs/stored originals,
duplicates/conflicts, IoT transform, asynchronous errors, CloudWatch alarm/logs and
the failure queue. SDK tests provide no live IAM or service assurance. The bounded
verifier does not inject a Rule dispatch failure; record that scope separately.

Delete recorded certificates before the registered stack, then the owned artifact
bucket. Check certificate IDs, StackId, bucket and owned table/log/queue resources
for absence. Retain sanitized result summaries with actual observation times. IoT Rule
dispatch-error injection, host power loss and sustained loads need separate tests.

## Cost references

Read current account-specific terms and prices before deployment:
[account plans](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html),
[Free Tier](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier.html),
[IoT](https://aws.amazon.com/iot-core/pricing/),
[DynamoDB](https://aws.amazon.com/dynamodb/pricing/on-demand/),
[Lambda](https://aws.amazon.com/lambda/pricing/),
[CloudWatch](https://aws.amazon.com/cloudwatch/pricing/),
[S3](https://aws.amazon.com/s3/pricing/),
[SQS](https://aws.amazon.com/sqs/pricing/).
