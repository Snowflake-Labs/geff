import os
from typing import Dict, Text, List, Any
import boto3
from json import dumps
from hashlib import md5

from botocore.exceptions import ClientError
from ..utils import LOG, ResponseType

AWS_REGION = os.environ.get(
    "AWS_REGION", "us-west-2"
)  # Placeholder while in dev TODO: change as variable/header
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE_NAME")
DYNAMODB_RESOURCE = boto3.resource("dynamodb", region_name=AWS_REGION)
if DYNAMODB_TABLE:
    table = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE)

TTL = 1800
LOCKED = '-1'


def finish_batch_processing(
    batch_id: Text, response: ResponseType, res_data: List[List[Any]] = None
):
    """
    Write to the batch-locking table, a batch id, response and TTL
    """

    try:
        table.put_item(Item={"batch_id": batch_id, "response": response, "ttl": 1800})
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ValidationException' and res_data:
            LOG.error(ce)
            error_dumps = dumps(
                {
                    'data': [
                        [
                            row_num,
                            {
                                'error': f"Response size for batch ID: {batch_id} is too large to be stored in the backend. {len(res_data)} row(s) and {len(dumps(response))} bytes (gzipped). This row's size: {len(dumps(row_res))} bytes. Decreasing MAX_BATCH_ROWS might help.",
                                'response_hash': md5(
                                    dumps(response, sort_keys=True).encode()
                                ).hexdigest(),
                            },
                        ]
                        for row_num, row_res in res_data
                    ]
                }
            )
            size_exceeded_response = {
                'statusCode': 200,
                'body': error_dumps,
            }
            table.put_item(
                Item={
                    "batch_id": batch_id,
                    "response": size_exceeded_response,
                    "ttl": 1800,
                }
            )


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