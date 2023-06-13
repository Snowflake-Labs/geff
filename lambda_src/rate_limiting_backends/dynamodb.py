import os
from typing import Text
import boto3

from ..utils import ResponseType

AWS_REGION = os.environ.get(
    'AWS_REGION', 'us-west-2'
)  # Placeholder while in dev TODO: change as variable/header
RATE_LIMITING_TABLE = os.environ.get('RATE_LIMITING_TABLE_NAME')
TTL = os.environ.get('RATE_LIMITING_TABLE_TTL', 86400)

RATE_LIMITING_ENABLED = False
if RATE_LIMITING_TABLE:
    table = boto3.resource('dynamodb', region_name=AWS_REGION).Table(
        RATE_LIMITING_TABLE
    )
    RATE_LIMITING_ENABLED = True

RATE_LIMIT = 10


def get_hit_count(url: Text) -> int:
    """
    Retreive count for a url.

    Args:
        batch_id (Text): The batch ID for which the 'locked' key's value will be retrieved.

    Returns:
        Optional[bool]: Value of the locked key. None if absent.
    """
    item = table.get_item(Key={'url': url})
    if 'Item' in item:
        return item['Item']['hit_count']
    else:
        _initialize_url(url)
        return 0


def _initialize_url(url: Text):
    """
    Initialize an item in the rate-limiting table.

    Args:
        batch_id (Text): The batch ID for which an item will be initialized.

    Returns:
        None
    """
    table.put_item(Item={'url': url, 'hit_count': 0, 'ttl': TTL})


def increment_count(url):
    table.update_item(
        Key={'url': url},
        UpdateExpression='SET hit_count = hit_count + :inc',
        ExpressionAttributeValues={':inc': 1},
        ReturnValues='UPDATED_NEW',
    )
