'''
This backend helps lock batches so that they are not processed twice when the API Gateway times out and Snowflake retries the request.

Each batch has a lock that goes through three states:

1. Un-initialized batch has not yet been seen by GEFF and so a lock does not exist
2. Initialized batch is "processing" and has a "locked" lock while the call driver is running
3. Finished batch means a call driver has responded or timed out and the lock is "unlocked" and stores the response

In dynamodb, this will correspond to Items like:
1. No item
2. {"batch_id": "558c5ffb-08a7-4b15-aba7-b7f68edd567f", "locked": true}
3. {"batch_id": "558c5ffb-08a7-4b15-aba7-b7f68edd567f", "locked": false, "response": ...}
'''

import os
from typing import Dict, Text, List, Any, Tuple, Union, Optional
import boto3
from json import dumps
from hashlib import md5

from botocore.exceptions import ClientError
from ..utils import LOG, ResponseType

AWS_REGION = os.environ.get(
    'AWS_REGION', 'us-west-2'
)  # Placeholder while in dev TODO: change as variable/header
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE_NAME')
TTL = os.environ.get('DYNAMODB_TABLE_TTL', 86400)

if DYNAMODB_TABLE:
    table = boto3.resource('dynamodb', region_name=AWS_REGION).Table(DYNAMODB_TABLE)
    BATCH_LOCKING_ENABLED = True
else:
    BATCH_LOCKING_ENABLED = False


def finish_batch_processing(
    batch_id: Text, response: ResponseType, res_data: List[List[Union[int, Dict]]]
):
    """
    Write to the batch-locking table, a batch ID, response and TTL.

    Args:
        batch_id (Text): Batch ID for which the response is to be written.
        response (ResponseType): Response to be written.
        res_data (List[List[Union[int, Dict]]]): For calculating the no. of rows in the response.

    Returns:
        None
    """

    try:
        table.put_item(
            Item={
                'batch_id': batch_id,
                'locked': False,
                'response': response,
                'ttl': TTL,
            }
        )
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ValidationException' and res_data:
            LOG.error(ce)
            size_exceeded_response = {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': dumps(
                    {
                        'data': [
                            [
                                row_num,
                                {
                                    'error': (
                                        f'Response size for batch ID {batch_id} is too large to be stored in the backend. '
                                        f'{len(res_data)} row(s) and {len(dumps(response))} bytes (gzipped). '
                                        f'This row\'s size: {len(dumps(row_res))} bytes. Decreasing MAX_BATCH_ROWS might help.'
                                    ),
                                    'response_hash': md5(
                                        dumps(response, sort_keys=True).encode()
                                    ).hexdigest(),
                                },
                            ]
                            for row_num, row_res in res_data
                        ]
                    }
                ),
            }
            table.put_item(
                Item={
                    'batch_id': batch_id,
                    'locked': False,
                    'response': size_exceeded_response,
                    'ttl': TTL,
                }
            )


def initialize_batch(batch_id: Text):
    """
    Initialize an item in the batch-locking table with a null response.

    Args:
        batch_id (Text): The batch ID for which an item will be initialized.

    Returns:
        None
    """
    table.put_item(Item={'batch_id': batch_id, 'locked': True, 'ttl': TTL})


def _get_lock(batch_id: Text) -> Optional[bool]:
    """
    Retreive lock for a batch ID.

    Args:
        batch_id (Text): The batch ID for which the 'locked' key's value will be retrieved.

    Returns:
        Optional[bool]: Value of the locked key. None if absent.
    """
    item = table.get_item(Key={'batch_id': batch_id})

    return item['Item']['locked'] if 'Item' in item else None


def is_batch_processing(batch_id: Text) -> bool:
    """
    Check if a batch ID is being processed already, i.e is locked.

    Args:
        batch_id (Text): The batch ID to be checked.

    Returns:
        bool: Boolean representing if the batch is still being processed.
    """
    return _get_lock(batch_id) is True


def is_batch_initialized(batch_id: Text) -> bool:
    """
    Check if a batch ID has been initialized in the batch-locking table.

    Args:
        batch_id (Text): The batch ID to be checked.

    Returns:
        bool: Boolean representing if the batch has been initialized.
    """
    return _get_lock(batch_id) is not None


def get_response_for_batch(batch_id: Text) -> Optional[ResponseType]:
    """
    Retreive response for a batch ID.

    Args:
        batch_id (Text): The batch ID for which the response is to be retrieved.

    Returns:
        Optional[ResponseType]: Dictionary representing the response for a batch ID. None if absent.

    """
    item = table.get_item(Key={'batch_id': batch_id})

    return item['Item']['response'] if 'Item' in item else None
