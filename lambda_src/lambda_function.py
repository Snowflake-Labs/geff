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
from botocore.exceptions import ClientError

from .log import format_trace
from .utils import LOG, create_response, format, invoke_process_lambda

# pip install --target ./site-packages -r requirements.txt
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, 'site-packages'))

BATCH_ID_HEADER = 'sf-external-function-query-batch-id'
DESTINATION_URI_HEADER = 'sf-custom-destination-uri'
LOCKED = '-1'


def async_flow_init(event: Any, context: Any) -> Dict[Text, Any]:
    """
    Handles the async part of the request flows.

    Args:
        event (Any): Has the event as received by the lambda_handler()
        context (Any): Has the function context. Defaults to None.

    Returns:
        Dict[Text, Any]: Represents the response state and data.
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


def async_flow_poll(destination: Text, batch_id: Text) -> Dict[Text, Any]:
    """Repeatedly checks on the status of the batch, and returns
    the result after the processing has been completed.

    Args:
        destination (Text): This is the destination parsed
        batch_id (Text):

    Returns:
        Dict[Text, Any]: This is the return value with the status code of 200 or 202
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


def sync_flow(event: Any, context: Any = None) -> Dict[Text, Any]:
    """
    Handles the synchronous part of the generic lambda flows.

    Args:
        event (Any): This the event object as received by the lambda_handler()
        context (Any): Has the function context. Defaults to None.

    Returns:
        Dict[Text, Any]: Represents the response status and data.
    """
    LOG.debug('Destination header not found in a POST and hence using sync_flow().')
    headers = event['headers']
    req_body = loads(event['body'])
    start_time = timer()

    destination_driver = None
    batch_id = headers[BATCH_ID_HEADER]

    write_uri = headers.get('write-uri')

    LOG.debug(f'sync_flow() received destination: {write_uri}.')

    if write_uri:
        destination_driver = import_module(
            f'geff.drivers.destination_{urlparse(write_uri).scheme}'
        )

    request_locking_backend = import_module('geff.request_locking_backends.dynamodb')
    while True:
        data = request_locking_backend.get_data_from_lock(batch_id)
        if data is None:  # request hasn't been initialized
            break
        elif data != LOCKED:  # if false, the response is yet to be written
            return data

    request_locking_backend.open_lock(batch_id)  # initilaze the request

    res_data = process_request(
        req_body, headers, event, batch_id, write_uri, destination_driver
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
        end_time = timer()
        if response and (end_time - start_time) > 20:
            LOG.debug(end_time - start_time)
            try:
                request_locking_backend.close_lock(
                    batch_id, response
                )  # write the response
            except ClientError as ce:
                if ce.response['Error']['Code'] == 'ValidationException':
                    LOG.error(ce)
                    pass

    if len(response) > 6_000_000:
        response = response_size_error(response, req_body)
    return response


def response_size_error(
    response_input: Dict[Text, Any], req_body: Dict[Text, Any]
) -> str:
    """
    Creates a new response object with an error message,
    for when the response size is likely to exceed the allowed payload size.

    Args:
        response (Dict[Text, Any]): Response object to calculate the size.
        req_body (Any): Body of the request, obtained from the events object.

    Returns:
        Dict[Text, Any]: Represents the response with the error message.
    """
    response = dumps(
        {
            'data': [
                [
                    rn,
                    {
                        'error': (
                            f'Response size ({len(response_input)} bytes) will likely'
                            'exceeded maximum allowed payload size (6291556 bytes).'
                        )
                    },
                ]
                for rn, *args in req_body['data']
            ]
        }
    )
    return response


def process_request(
    req_body: Dict[Text, Any],
    headers: Dict[Text, Any],
    event: Any,
    batch_id: Text,
    write_uri: Optional[Text] = None,
    destination_driver: Optional[ModuleType] = None,
) -> List[List[Any]]:
    """
    Processes a request and returns the result data.

    Args:
        req_body (Dict[Text, Any]): Body of the request, obtained from the event object.
        headers (Dict[Text, Any]): Headers in the request, obtained from the event object.
        event (Any): This is the event object as received by the lambda_handler().
        batch_id (Text): Batch ID from a request.
        write_uri (Optional[Text]): The path where the response should be stored. Defaults to None.
        destination_driver (Optional[Text]): The destination driver such as S3. Defaults to None.

    Returns:
        Dict[Text, Any]: Result data returned after the request is processed.
    """
    res_data = []

    for row_number, *args in req_body['data']:
        row_result = []
        process_row_params = {
            k.replace('sf-custom-', '').replace('-', '_'): format(v, args)
            for k, v in headers.items()
            if k.startswith('sf-custom-')
        }

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
                    format(write_uri, args), batch_id, row_result, row_number
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


def lambda_handler(event: Any, context: Any) -> Dict[Text, Any]:
    """
    Implements the asynchronous function on AWS as described in the Snowflake docs here:
    https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws.html

    Args:
        event (Any): Event received from AWS
        context (Any): Function context received from AWS

    Returns:
        Dict[Text, Any]: Returns the response body.
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
