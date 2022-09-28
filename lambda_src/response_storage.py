import os
from typing import Dict, Text

import boto3

AWS_REGION = os.environ[
    'AWS_REGION'
]
DYNAMODB_TABLE = 'geff-requests'
DYNAMODB_RESOURCE = boto3.resource('dynamodb', region_name=AWS_REGION)
table = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE)


def write_dynamodb_item(batch_id: Text, response: Dict):
    """
    Write to the requests table, a batch id and a response
    """
    table.put_item(Item={'batch_id': batch_id, 'response': response})


def initialize_dynamodb_item(batch_id: Text):
    """
    Initialize an item in the requests table with a null response
    """
    table.put_item(Item={'batch_id': batch_id, 'response': None})


def get_response(batch_id: Text):
    """
    Retreive response for a batch id
    """
    item = table.get_item(Key={'batch_id': batch_id})
    response = item['Item']['response']
    return response


def check_if_initialized(batch_id: Text):
    """
    Check if a response for a batch id exists in the table
    """
    item = table.get_item(Key={'batch_id': batch_id})
    try:
        response = item['Item']['response']
    except KeyError:
        return False

    return True
