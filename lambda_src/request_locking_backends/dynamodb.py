import os
from typing import Dict, Text
import boto3

AWS_REGION = os.environ.get(
    "AWS_REGION", "us-west-2"
)  # Placeholder while in dev TODO: change as variable/header
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE_NAME"]
DYNAMODB_RESOURCE = boto3.resource("dynamodb", region_name=AWS_REGION)
table = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE)
TTL = 1800


def close_lock(batch_id: Text, response: Dict):
    """
    Write to the request-locking backend table, a batch id, a response and a TTL
    """
    table.put_item(Item={"batch_id": batch_id, "response": response, "ttl": 1800})


def open_lock(batch_id: Text):
    """
    Initialize an item in the requests table with a null response
    """
    table.put_item(Item={"batch_id": batch_id, "response": "-1", "ttl": 1800})


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
