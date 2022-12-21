import os
import os.path
import sys
from base64 import b64encode
from gzip import compress
from importlib import import_module
from json import dumps, loads
from typing import Any, Dict, Text, Optional, List
from types import ModuleType
from urllib.parse import urlparse
from timeit import default_timer as timer
from hashlib import md5

from botocore.exceptions import ClientError
from .log import format_trace
from .utils import LOG, create_response, format, invoke_process_lambda, ResponseType
from .batch_locking_backends.dynamodb import (
    initialize_batch,
    is_batch_initialized,
    is_batch_processing,
    get_response_for_batch,
    finish_batch_processing,
)


# pip install --target ./site-packages -r requirements.txt
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, 'site-packages'))

BATCH_ID_HEADER = 'sf-external-function-query-batch-id'
DESTINATION_URI_HEADER = 'sf-custom-destination-uri'


def async_flow_init(event: Any, context: Any) -> ResponseType:
    """
    Handles the async part of the request flows.

    Args:
        event (Any): Has the event as received by the lambda_handler()
        context (Any): Has the function context. Defaults to None.

    Returns:
        ResponseType: Represents the response state and data.
    """
    LOG.debug('Found a destination header and hence using async_flow_init().')

    headers = event['headers']
    batch_id = headers[BATCH_ID_HEADER]
    destination = headers['write-uri'] = headers.pop(DESTINATION_URI_HEADER)
    lambda_name = context.function_name
    LOG.debug(f'async_flow_init() received destination: {destination}.')

    destination_driver = import_module(
        f'geff.drivers.destination_{urlparse(destination).scheme}'
    )
    # Ignoring style due to dynamic import
    destination_driver.initialize(destination, batch_id)  # type: ignore

    LOG.debug('Invoking child lambda.')
    lambda_response = invoke_process_lambda(event, lambda_name)
    if lambda_response['StatusCode'] != 202:
        LOG.debug('Child lambda returned a non-202 status.')
        return create_response(400, 'Error invoking child lambda.')
    else:
        LOG.debug('Child lambda returned 202.')
        return {'statusCode': 202}


def async_flow_poll(destination: Text, batch_id: Text) -> ResponseType:
    """Repeatedly checks on the status of the batch, and returns
    the result after the processing has been completed.

    Args:
        destination (Text): This is the destination parsed
        batch_id (Text):

    Returns:
        ResponseType: This is the return value with the status code of 200 or 202
        as per the status of the write.
    """
    LOG.debug('async_flow_poll() called as destination header was not found in a GET.')
    destination_driver = import_module(
        f'geff.drivers.destination_{urlparse(destination).scheme}'
    )

    # Ignoring style due to dynamic import
    status_body = destination_driver.check_status(destination, batch_id)  # type: ignore
    if status_body:
        LOG.debug(f'Manifest found return status code 200.')
        return {'statusCode': 200, 'body': status_body}
    else:
        LOG.debug(f'Manifest not found return status code 202.')
        return {'statusCode': 202}


def process_batch(
    event: Any,
    destination_driver: Optional[ModuleType],
) -> List[List[Any]]:
    """
    Processes a request and returns the result data.

    Args:
        event (Any): This is the event object as received by the lambda_handler().
        destination_driver (Optional[Text]): The destination driver such as S3. Defaults to None.

    Returns:
        List[List[Any]]: Result data returned after the request is processed.
    """
    res_data = []
    headers = event['headers']
    req_body = loads(event['body'])
    write_uri = headers.get('write-uri')
    batch_id = headers[BATCH_ID_HEADER]

    processed_headers = {
        k.replace('sf-custom-', '').replace('-', '_'): v
        for k, v in headers.items()
        if k.startswith('sf-custom-')
    }

    for row_number, *args in req_body['data']:
        row_result = []
        process_row_params = {k: format(v, args) for k, v in processed_headers.items()}

        try:
            driver, *path = event['path'].lstrip('/').split('/')
            driver = driver.replace('-', '_')
            driver_module = f'geff.drivers.process_{driver}'
            process_row = import_module(
                driver_module, package=None
            ).process_row  # type: ignore

            LOG.debug(f'Invoking process_row for the driver {driver_module}.')
            row_result = process_row(*path, **process_row_params)
            LOG.debug(f'Got row_result for URL: {process_row_params.get("url")}.')

            if write_uri:
                # Write s3 data and return confirmation
                row_result = destination_driver.write(  # type: ignore
                    write_uri, batch_id, row_result, row_number
                )

        except Exception as e:
            row_result = [{'error': repr(e), 'trace': format_trace(e)}]

        res_data.append(
            [
                row_number,
                row_result,
            ]
        )

    # Write data to s3 or return data synchronously
    if write_uri:
        response = destination_driver.finalize(  # type: ignore
            write_uri, batch_id, res_data
        )
    else:
        data_dumps = dumps({'data': res_data}, default=str)
        response = {
            'statusCode': 200,
            'body': b64encode(compress(data_dumps.encode())).decode(),
            'isBase64Encoded': True,
            'headers': {'Content-Encoding': 'gzip'},
        }
        write_dynamodb_item(batch_id, response)

    if len(response) > 6_000_000:
        response = dumps(
            {
                'data': [
                    [
                        rn,
                        {
                            'error': (
                                f'Response size ({len(response)} bytes) will likely'
                                'exceeded maximum allowed payload size (6291556 bytes).'
                            )
                        },
                    ]
                    for rn, *args in req_body['data']
                ]
            }
        )
    return response


def sync_flow(event: Any, context: Any = None) -> ResponseType:
    """
    Handles the synchronous part of the generic lambda flows.

    Args:
        event (Any): This the event object as received by the lambda_handler()
        context (Any): Has the function context. Defaults to None.

    Returns:
        ResponseType: Represents the response status and data.
    """
    LOG.debug('Destination header not found in a POST and hence using sync_flow().')
    headers = event['headers']
    req_body = loads(event['body'])
    start_time = timer()

    destination_driver = None
    batch_id = headers[BATCH_ID_HEADER]
    write_uri = headers.get('write-uri')
    destination_driver = (
        import_module(f'geff.drivers.destination_{urlparse(write_uri).scheme}')
        if write_uri
        else None
    )

    LOG.debug(f'sync_flow() received destination: {write_uri}.')

    if not destination_driver:
        if not is_batch_initialized(batch_id):
            initialize_batch(batch_id)
        else:
            while is_batch_processing(batch_id):
                if (timer() - start_time) > 30:
                    return create_response(202, 'API Gateway timed out.')
            return get_response_for_batch(batch_id)

    res_data = process_batch(event, destination_driver)

    # Write data to s3 or return data synchronously
    if destination_driver:
        response = destination_driver.finalize(  # type: ignore
            write_uri, batch_id, res_data
        )
    else:
        data_dumps = dumps({'data': res_data}, default=str)
        response = {
            'statusCode': 200,
            'body': b64encode(compress(data_dumps.encode())).decode(),
            'isBase64Encoded': True,
            'headers': {'Content-Encoding': 'gzip'},
        }
        end_time = timer()
        if response and (end_time - start_time) > 20:
            LOG.debug('Storing the response in lock cache.')
            try:
                finish_batch_processing(batch_id, response)  # write the response
            except ClientError as ce:
                if ce.response['Error']['Code'] == 'ValidationException':
                    LOG.error(ce)
                    error_dumps = dumps(
                        {
                            'data': [
                                [
                                    rn,
                                    {
                                        'error': f'Response size ({len(dumps(response))} bytes) too large to be stored in the backend.',
                                        'response_hash': md5(
                                            dumps(response, sort_keys=True).encode()
                                        ).hexdigest(),
                                    },
                                ]
                                for rn in req_body['data']
                            ]
                        }
                    )
                    size_exceeded_response = {
                        'statusCode': 202,
                        'body': error_dumps,
                    }
                    finish_batch_processing(batch_id, size_exceeded_response)

    if len(response) > 6_000_000:
        response = construct_size_error_response(response, req_body)
    return response


def construct_size_error_response(
    size_exceeded_response: ResponseType, req_body: Dict[Text, Any]
) -> ResponseType:
    """
    Creates a new response object with an error message,
    for when the response size is likely to exceed the allowed payload size.

    Args:
        size_exceeded_response (ResponseType): Response object to calculate the size.
        req_body (Dict[Text, Any]): Body of the request, obtained from the events object.

    Returns:
        ResponseType: Represents the response with the error message.
    """
    error_dumps = dumps(
        {
            'data': [
                [
                    rn,
                    {
                        'error': (
                            f'Response size ({len(size_exceeded_response)} bytes) will likely '
                            'exceeded maximum allowed payload size (6291556 bytes).'
                        )
                    },
                ]
                for rn in req_body['data']
            ]
        }
    )
    return {
        'statusCode': 202,
        'body': error_dumps,
    }


def lambda_handler(event: Any, context: Any) -> ResponseType:
    """
    Implements the asynchronous function on AWS as described in the Snowflake docs here:
    https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws.html

    Args:
        event (Any): Event received from AWS
        context (Any): Function context received from AWS

    Returns:
        ResponseType: Returns the response body.
    """
    method = event.get('httpMethod')
    headers = event['headers']
    LOG.debug(f'lambda_handler() called.')

    destination = headers.get(DESTINATION_URI_HEADER)
    batch_id = headers.get(BATCH_ID_HEADER)

    # httpMethod doesn't exist implies caller is base lambda.
    # This is required to break an infinite loop of child lambda creation.
    if not method:
        return sync_flow(event, context)

    # httpMethod exists implies caller is API Gateway
    if method == 'POST' and destination:
        return async_flow_init(event, context)
    elif method == 'POST':
        return sync_flow(event, context)
    elif method == 'GET':
        return async_flow_poll(destination, batch_id)

    return create_response(400, 'Unexpected Request.')
