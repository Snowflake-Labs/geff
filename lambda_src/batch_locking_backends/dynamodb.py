import os
from typing import Dict, Text
import boto3

AWS_REGION = os.environ.get(
    "AWS_REGION", "us-west-2"
)  # Placeholder while in dev TODO: change as variable/header
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE_NAME")
DYNAMODB_RESOURCE = boto3.resource("dynamodb", region_name=AWS_REGION)
if DYNAMODB_TABLE:
    table = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE)

TTL = 1800
LOCKED = '-1'


def finish_batch_processing(batch_id: Text, response: Dict):
    """
    Write to the batch-locking table, a batch id, response and TTL
    """
    table.put_item(Item={"batch_id": batch_id, "response": response, "ttl": 1800})


def initialize_batch(batch_id: Text):
    """
    Initialize an item in the batch-locking table with a null response
    """
    table.put_item(Item={"batch_id": batch_id, "response": "-1", "ttl": 1800})


def get_response_for_batch(batch_id: Text):
    """
    Retreive response for a batch id
    """
    item = table.get_item(Key={"batch_id": batch_id})
    try:
        response = item["Item"]["response"]
    except KeyError:
        return None
    return response


def is_batch_processing(batch_id: Text):
    """
    Check if a batch id is being processed already, i.e is locked.
    """
    return get_response_for_batch(batch_id) == LOCKED


def is_batch_initialized(batch_id: Text):
    """
    Check if a batch id has been initialized in the batch-locking table.
    """
    return get_response_for_batch(batch_id) is not None
