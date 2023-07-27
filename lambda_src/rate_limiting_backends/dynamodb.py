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


def reset_rate_limit(url: Text, rate_limit_window: int) -> Tuple[int, int]:
    """
    Retreive hit count for a url.

    Args:
        url (Text): The url to which the rate limit will apply.
        rate_limit_window (int): Time window for how long the rate
                    limit is valid after the initial request.

    Returns:
        None
    Returns:
        Optional[bool]: Value of the locked key. None if absent.
    """
    window_start = int(time.time())
    hit_count = 1
    window_end = window_start + rate_limit_window

    item = table.get_item(Key={'url': url})
    if 'Item' in item:
        table.update_item(
            Key={'url': url},
            UpdateExpression='SET hit_count = :new_hit_count, window_start = :new_window_start, window_end = :new_window_end',
            ExpressionAttributeValues={
                ':new_hit_count': hit_count,
                ':new_window_start': window_start,
                ':new_window_end': window_end,
            },
        )
    else:
        initialize_url(url, rate_limit_window)


def initialize_url(url: Text, rate_limit_window: int):
    """
    Initialize an item in the rate-limiting table.

    Args:
        url (Text):  The url to which the rate limit will apply.
        rate_limit_window (int): Time window for how long the rate
                    limit is valid after the initial request.

    Returns:
        None
    """
    window_start = int(time.time())
    hit_count = 1
    window_end = window_start + rate_limit_window

    table.put_item(
        Item={
            'url': url,
            'hit_count': hit_count,
            'window_start': window_start,
            'window_end': window_end,
            'ttl': TTL,
        }
    )


def increment_and_get_hit_count(
    url: Text, rate_limit: int, rate_limit_window: int
) -> Tuple[int, int, int]:
    """
    Increment hit count for a URL and retrieve the updated count and window_start, window_end.

    Args:
        url (Text): The URL for which the count will be incremented and returned.
        rate_limit (int): The maximum number of requests that should be made to a URL in a time window.
        rate_limit_window (int): Time window for how long the rate limit is valid after the initial request.

    Returns:
        Tuple[int, int]: The updated hit count and window_end.
    """
    current_time = int(time.time())
    item = table.get_item(Key={'url': url})

    if 'Item' in item:
        window_start = item['Item']['window_start']
        window_end = item['Item']['window_end']
        hit_count = item['Item']['hit_count']

        if current_time < window_start:
            window_start = current_time
            hit_count = 1

        elif hit_count >= rate_limit:
            LOG.info('Rate limit for URL "%s" exceeded.', url)
            return hit_count, window_end

        else:
            hit_count += 1

        try:
            table.update_item(
                Key={'url': url},
                UpdateExpression='SET hit_count = :new_hit_count, window_start = :new_window_start, window_end = :new_window_end',
                ConditionExpression='hit_count < :limit',
                ExpressionAttributeValues={
                    ':limit': rate_limit,
                    ':new_hit_count': hit_count,
                    ':new_window_start': window_start,
                    ':new_window_end': window_end,
                },
            )
        except boto3.exceptions.botocore.exceptions.ClientError as ce:
            if ce.response['Error']['Code'] == 'ConditionalCheckFailedException':
                LOG.info('Rate limit for URL "%s" exceeded.', url)
            else:
                raise

    else:
        initialize_url(url, rate_limit_window)

    item = table.get_item(Key={'url': url})
    return (
        item['Item']['hit_count'],
        item['Item']['window_end'],
    )
