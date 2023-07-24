import os
import time
from typing import Text, Tuple
from ..utils import LOG
import boto3

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


def get_hit_count(url: Text, rate_limit_window: int) -> Tuple[int, int]:
    """
    Retreive hit count for a url.

    Args:
        url (Text): The url to which the rate limit will apply.
        rate_limit_window (int): Time window for how long the rate
                    limit is valid after the initial request.

    Returns:
        Optional[bool]: Value of the locked key. None if absent.
    """
    item = table.get_item(Key={'url': url})
    if 'Item' in item:
        return item['Item']['hit_count'], item['Item']['expiry']
    else:
        initialize_url(url, rate_limit_window)
        item = table.get_item(Key={'url': url})
        return item['Item']['hit_count'], item['Item']['expiry']


def initialize_url(url: Text, rate_limit_window: int) -> Tuple[int, int]:
    """
    Initialize an item in the rate-limiting table.

    Args:
        url (Text):  The url to which the rate limit will apply.
        rate_limit_window (int): Time window for how long the rate
                    limit is valid after the initial request.

    Returns:
        Tuple[int, int]: The initialized hit count and the expiry time.
    """
    hit_count = 1
    expiry = int(time.time()) + rate_limit_window * 60

    table.put_item(
        Item={
            'url': url,
            'hit_count': hit_count,
            'ttl': TTL,
            'expiry': expiry,
        }
    )


def increment_and_get_hit_count(
    url: Text, rate_limit: int, rate_limit_window: int
) -> Tuple[int, int]:
    """
    Increment hit count for a URL and retrieve the updated count.

    Args:
        url (Text): The URL for which the count will be incremented and returned.
        rate_limit (int): The maximum number of requests that should be made to a URL in a time window.
        rate_limit_window (int): Time window for how long the rate limit is valid after the initial request.

    Returns:
        Tuple[int, int]: The updated hit count and the expiry time.
    """
    try:
        table.update_item(
            Key={'url': url},
            UpdateExpression='SET hit_count = hit_count + :inc',
            ConditionExpression='hit_count < :limit',
            ExpressionAttributeValues={':inc': 1, ':limit': rate_limit},
            ReturnValues="UPDATED_NEW",
        )

    except boto3.exceptions.botocore.exceptions.ClientError as ce:
        if ce.response['Error']['Code'] == 'ConditionalCheckFailedException':
            LOG.info(ce.response['Error']['Message'])

        elif ce.response['Error']['Code'] == "ValidationException":
            initialize_url(url, rate_limit_window)

        else:
            raise

    item = table.get_item(Key={'url': url})
    return item['Item']['hit_count'], item['Item']['expiry']
