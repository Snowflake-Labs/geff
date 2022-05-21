import json
import logging
import os
import re
from codecs import encode
from json import dumps
from logging import Logger
from typing import Any, Dict, Optional, Text, Tuple

import boto3
import sentry_sdk
from sentry_sdk.client import Client

from .log import setup_logger

ULILS_LOGGER = setup_logger('utils', logging.DEBUG)


def pick(path: str, d: dict):
    # path e.g. "a.b.c"
    retval: Optional[Any] = d
    for p in path.split('.'):
        if p and retval:
            retval = retval.get(p)
    return retval


# from https://requests.readthedocs.io/en/master/_modules/requests/utils/
def parse_header_links(value):
    """Return a list of parsed link headers proxies.

    i.e. Link: <http:/.../front.jpeg>; rel=front; type="image/jpeg",<http://.../back.jpeg>; rel=back;type="image/jpeg"

    :rtype: list
    """
    links = []
    replace_chars = ' \'"'

    value = value.strip(replace_chars)
    if not value:
        return links

    for val in re.split(', *<', value):
        try:
            url, params = val.split(';', 1)
        except ValueError:
            url, params = val, ''

        link = {'url': url.strip('<> \'"')}

        for param in params.split(';'):
            try:
                key, value = param.split('=')
            except ValueError:
                break

            link[key.strip(replace_chars)] = value.strip(replace_chars)

        links.append(link)

    return links


def zip(s, chunk_size=1_000_000):
    """zip in pieces, as it is tough to inflate large chunks in
    Snowflake per UDF mem limits

    Args:
        s ([type]): [description]
        chunk_size ([type], optional): [description]. Defaults to 1_000_000.
    """

    def do_zip(s):
        return encode(encode(s.encode(), encoding='zlib'), 'base64').decode()

    if len(s) > chunk_size:
        return [do_zip(s[:chunk_size])] + zip(s[chunk_size:], chunk_size)
    return [do_zip(s)]


def format(s, ps):
    """format string s with params ps, preserving type of singular references

    >>> format('{0}', [{'a': 'b'}])
    {'a': 'b'}

    >>> format('{"z": [{0}]}', [{'a': 'b'}])
    """

    def replace_refs(s, ps):
        for i, p in enumerate(ps):
            old = '{' + str(i) + '}'
            new = dumps(p) if isinstance(p, (list, dict)) else str(p)
            s = s.replace(old, new)
        return s

    m = re.match(r'{(\d+)}', s)
    return ps[int(m.group(1))] if m else replace_refs(s, ps)


def create_response(code: int, msg: Text) -> Dict[Text, Any]:
    return {'statusCode': code, 'body': msg}


def invoke_process_lambda(event: Any, lambda_name: Text) -> Dict[Text, Any]:
    """Helper method to invoke a child lambda.

    Args:
        event (Any): The event as received by the base lambda
        lambda_name (Text): The lambda function name

    Returns:
        Dict[Text, Any]: This is a 202 status with empty body in our usage.
    """
    # Create payload to be sent to lambda
    invoke_payload = json.dumps(event)

    # We call a child lambda to do the sync_flow and return a 202 to prevent timeout.
    lambda_client = boto3.client(
        'lambda',
        region_name=os.environ['AWS_REGION'],
    )
    lambda_response = lambda_client.invoke(
        FunctionName=lambda_name, InvocationType='Event', Payload=invoke_payload
    )

    # Returns 202 on success if InvocationType = 'Event'
    return lambda_response


def setup_sentry(
    geff_dsn: Optional[str],
    sentry_driver_dsn: Optional[str],
) -> Tuple[Logger, Logger, Logger]:
    """Sets up the sentry SDK clients for GEFF exceptions to be sent
    along with any external errors to be sent to Sentry from the Sentry driver.

    Args:
        geff_dsn (Optional[str]): The DSN URL for the geff Sentry project.
        sentry_driver_dsn (Optional[str]): The DSN URL for the snowflake-errors Sentry project.
    """
    ULILS_LOGGER.debug('Setting up sentry_sdk.')

    if geff_dsn and sentry_driver_dsn:
        ULILS_LOGGER.debug('Both DSNs were provided. Initializing Sentry clients and loggers.')

        geff_client = Client(dsn=geff_dsn)
        sentry_client = Client(dsn=sentry_driver_dsn)
        ULILS_LOGGER.debug(f'Clients {geff_client} and {sentry_client} have both been instantiated.')

        def send_event(event):
            if  event.get('logger') == 'sentry_driver':
                sentry_client.capture_event(event)
            else:
                geff_client.capture_event(event)

        sentry_sdk.init(
            transport=send_event,
            max_breadcrumbs=10,
        )
        ULILS_LOGGER.debug('sentry_sdk has been initialized.')

    return (
        setup_logger(logger_name='console', level=logging.DEBUG),
        setup_logger(logger_name='geff', level=logging.WARNING),
        setup_logger(logger_name='sentry_driver', level=logging.ERROR),
    )
