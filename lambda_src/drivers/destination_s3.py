import json
import os
from random import sample
from typing import Any, AnyStr, Dict, Generator, List, Optional, Text, Tuple, Union
from urllib.parse import urlparse
from time import strftime
import re
from hashlib import sha256

import boto3
from botocore.exceptions import ClientError
from ..utils import LOG, lazy_format

SAMPLE_SIZE: int = 10
MAX_JSON_FILE_SIZE: int = 15 * 1024 * 1024 * 1024
AWS_REGION = os.environ[
    'AWS_REGION'
]  # Placeholder while in dev TODO: change as variable/header
S3_CLIENT = boto3.client('s3', region_name=AWS_REGION)
MANIFEST_FILENAME = 'MANIFEST.json'
MANIESTS_FOLDER_NAME = 'meta'


def parse_destination_uri(destination: Text) -> Tuple[Text, Text]:
    """Parses the URL into bucket and prefix

    Args:
        destination (Text): [description]

    Returns:
        Tuple[Text, Text]: [description]
    """
    LOG.debug(f'destination from header is {destination}')
    parsed_url = urlparse(destination)
    return (parsed_url.netloc, strftime(parsed_url.path[1:]))  # remove leading slash


def estimated_record_size(records: List[Dict[Text, Any]]) -> float:
    """A helper utility to get a rough (really rough) estimate of
    the size of a single record in a list of dictionary objects.
    We assume it is json serializable for writing to a file.

    Args:
        records (List[Dict[Text, Any]]): [description]

    Returns:
        float: [description]
    """
    sample_list = sample(records, SAMPLE_SIZE)
    sample_json_str = json.dumps({'data': sample_list})
    return len(sample_json_str) / SAMPLE_SIZE


def chunker(records: List[Any], chunk_size: int) -> Generator[List[Any], None, None]:
    """
    Use to paginate a list of objects.
    >>> a = [1,2,3,4,5]
    >>> for chunk in chunker(a, 2):
    ...     LOG.debug(chunk)
    ...
    [1, 2]
    [3, 4]
    [5]

    Args:
        records (List[Any]): List of records we want to paginate
        chunk_size (int): Represents the size of chunk we want to return

    Yields:
        List[Any]: Returns a list of objects chunk size at a time
    """
    for pos in range(0, len(records), chunk_size):
        yield records[pos : pos + chunk_size]


def write_to_s3(bucket: Text, filename: Text, content: AnyStr) -> Dict[Text, Any]:
    return S3_CLIENT.put_object(
        Bucket=bucket,
        Body=content,
        Key=filename,
    )


def initialize(destination: Text, batch_id: Text):
    bucket, prefix = parse_destination_uri(destination)
    content = ''  # We use empty body for creating a folder
    # Regex captures characters after and including the rightmost '/' in a path,
    # which are then replaced with a '/', e.g. '/a/b/c' -> '/a/b/'
    prefix_folder = re.sub(r'/[^/]*$', '/', prefix) if '/' in prefix else ''
    prefix_folder = re.sub(r'/{[^}].*', '/', prefix_folder)
    if prefix_folder:
        write_to_s3(bucket, prefix_folder, content)


def write(
    destination: Text,
    batch_id: Text,
    datum: Union[Dict, List],
    row_index: int,
) -> Dict[Text, Any]:
    bucket, prefix = parse_destination_uri(destination)
    if isinstance(datum, bytes):
        encoded_datum = datum
        encoded_datum_hash = sha256(encoded_datum).hexdigest()
        prefixed_filename = lazy_format(prefix, hash=lambda: encoded_datum_hash)
    else:
        encoded_datum = (
            '\n'.join(json.dumps(d) for d in datum)
            if isinstance(datum, list)
            else json.dumps(datum, default=str)
        )
        prefixed_filename = f'{prefix}{batch_id}_row_{row_index}.data.json'

    s3_uri = f's3://{bucket}/{prefixed_filename}'

    response = {
        'response': write_to_s3(
            bucket,
            prefixed_filename,
            encoded_datum,
        ),
        'uri': s3_uri,
    }
    if isinstance(encoded_datum, bytes):
        response['sha256_hash'] = encoded_datum_hash
    return response


def finalize(
    destination: Text,
    batch_id: Text,
    datum: Dict,
) -> Dict[Text, Any]:
    bucket, _ = parse_destination_uri(destination)
    encoded_datum = json.dumps(datum)
    prefixed_filename = f'{MANIESTS_FOLDER_NAME}/{batch_id}_{MANIFEST_FILENAME}'
    s3_uri = f's3://{bucket}/{prefixed_filename}'

    return {
        'response': write_to_s3(
            bucket,
            prefixed_filename,
            encoded_datum,
        ),
        'uri': s3_uri,
    }


def check_status(destination: Text, batch_id: Text) -> Optional[Text]:
    bucket, _ = parse_destination_uri(destination)
    prefixed_filename = f'{MANIESTS_FOLDER_NAME}/{batch_id}_{MANIFEST_FILENAME}'
    try:
        response_obj = S3_CLIENT.get_object(Bucket=bucket, Key=prefixed_filename)
        content = response_obj['Body']
        json_object = json.loads(content.read().decode('utf-8'))
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'NoSuchKey':
            LOG.debug('No manifest file found returning None.')
            return None
    else:
        LOG.debug(f'Manifest file found returning contents. {json_object}')
        return json.dumps({'data': json_object})

    return None
