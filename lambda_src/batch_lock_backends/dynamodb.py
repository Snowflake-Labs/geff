import os
from typing import Dict, Text

import boto3

AWS_REGION = os.environ[
    "AWS_REGION"
]  # Placeholder while in dev TODO: change as variable/header
DYNAMODB_TABLE = "geff-request-locking"
DYNAMODB_RESOURCE = boto3.resource("dynamodb", region_name=AWS_REGION)
table = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE)
TTL = 1800


def close_lock(batch_id: Text, response: Dict):
    """
    Write to the request-locking backend table, a batch id, a response and a TTL
    """
    table.put_item(Item={"batch_id": batch_id, "response": response, "ttl": TTL})


def open_lock(batch_id: Text):
    """
    Initialize an item in the requests table with a null response
    """
    table.put_item(Item={"batch_id": batch_id, "response": "-1", "ttl": TTL})


def get_data_from_lock(batch_id: Text):
    """
    Retreive response for a batch id
    """
    item = table.get_item(Key={"batch_id": batch_id})
    try:
        response = item["Item"]["response"]
    except KeyError:
        return None
    return response
