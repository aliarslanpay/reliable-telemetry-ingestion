"""Conditional immutable storage and bounded per-stream queries."""

import json
from botocore.exceptions import ClientError
from .core import canonical


def keys(device, stream, sequence):
    return {"pk": {"S": device + "#" + stream}, "sk": {"S": f"{sequence:016d}"}}


class DynamoStore:
    def __init__(self, client, table):
        self.client = client
        self.table = table

    def store(self, event):
        key = keys(event["device_id"], event["stream_id"], event["sequence"])
        item = {
            **key,
            "fingerprint": {"S": event["fingerprint"]},
            "data": {"S": canonical(event)},
        }
        try:
            self.client.put_item(
                TableName=self.table,
                Item=item,
                ConditionExpression="attribute_not_exists(pk)",
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
            return "stored"
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            original = exc.response.get("Item")
            if original is None:
                original = self.client.get_item(
                    TableName=self.table, Key=key, ConsistentRead=True
                ).get("Item")
            if original is None:
                raise RuntimeError("conditional-write original unavailable") from exc
            return (
                "duplicate"
                if original.get("fingerprint") == item["fingerprint"]
                and original.get("data") == item["data"]
                else "conflict"
            )

    def get(self, device, stream, sequence):
        item = self.client.get_item(
            TableName=self.table,
            Key=keys(device, stream, sequence),
            ConsistentRead=True,
        ).get("Item")
        return json.loads(item["data"]["S"]) if item else None

    def query(self, device, stream, first, last, limit=100, max_pages=5, count=False):
        if not 1 <= limit <= 100 or not 1 <= max_pages <= 10:
            raise ValueError("query pagination bound")
        parameters = {
            "TableName": self.table,
            "KeyConditionExpression": "pk=:pk AND sk BETWEEN :first AND :last",
            "ExpressionAttributeValues": {
                ":pk": {"S": device + "#" + stream},
                ":first": {"S": f"{first:016d}"},
                ":last": {"S": f"{last:016d}"},
            },
            "ConsistentRead": True,
            "Limit": limit,
        }
        if count:
            parameters["Select"] = "COUNT"
        items = []
        total = 0
        continuation = None
        for _ in range(max_pages):
            response = self.client.query(**parameters)
            total += response.get("Count", 0)
            items += [
                json.loads(item["data"]["S"]) for item in response.get("Items", [])
            ]
            continuation = response.get("LastEvaluatedKey")
            if not continuation:
                break
            parameters["ExclusiveStartKey"] = continuation
        return {"items": items, "count": total, "truncated": bool(continuation)}
