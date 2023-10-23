from collections import namedtuple
import json
import logging
import os
import re
import sys
from codecs import encode
from json import dumps
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Text,
    TypedDict,
    Union,
    get_type_hints,
    get_origin,
    get_args,
)
from urllib.parse import urlparse, urlunparse, urlencode


import boto3

logging.basicConfig(stream=sys.stdout)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


DataMetadata = namedtuple('DataMetadata', ['data', 'metadata'])


class ResponseType(TypedDict, total=False):
    """
    Type constructor for responses to be returned
    """

    statusCode: int
    body: Text
    isBase64Encoded: bool
    headers: Dict[Text, Any]
    response: Dict[Text, Any]
    uri: str


def pick(path: str, d: dict):
    # path e.g. "a.b.c"
    retval: Optional[Any] = d
    for p in path.split('.'):
        if p and retval:
            retval = retval.get(p)
    return retval


def set_value(d: Dict[Any, Any], path: str, value: Any) -> Dict[Any, Any]:
    """
    Set a value in a nested dictionary based on a dot-separated path.
    Creates new dictionaries if the path does not exist.
    """
    keys = path.split('.')
    for key in keys[:-1]:
        d = d.setdefault(key, {})
    d[keys[-1]] = value
    return d


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


def create_response(code: int, msg: Text) -> ResponseType:
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


def cast_parameters(params: Dict[str, Any], func: Callable) -> Dict[str, Any]:
    type_hints = get_type_hints(func)
    casted_params = {}

    for name, param_type in type_hints.items():
        value = params.get(name)

        if value is None:
            continue

        origin = get_origin(param_type)
        args = get_args(param_type)

        actual_type = args[0] if origin is Union else param_type

        if isinstance(actual_type, type):
            casted_params[name] = actual_type(value)

    return {**params, **casted_params}


def add_param_to_url(url, param_name, param_value):
    parsed_url = urlparse(url)
    query_dict = (
        {k: v for k, v in (kv.split('=') for kv in parsed_url.query.split('&'))}
        if parsed_url.query
        else {}
    )
    query_dict[param_name] = param_value
    return urlunparse(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            urlencode(query_dict),
            parsed_url.fragment,
        )
    )
