from json import loads

import boto3
from botocore.response import StreamingBody

from utils import pick

DISALLOWED_CLIENTS = {'kms', 'secretsmanager'}


def process_row(
    client_name,
    method_name,
    assume_role_arns=None,
    role_session_name=None,
    results_path=None,
    region='us-west-2',
    **kwargs,
):
    if client_name in DISALLOWED_CLIENTS:
        return
    if assume_role_arns:
        creds = None
        for arn in loads(assume_role_arns):
            access_key = creds['Credentials']['AccessKeyId'] if creds else None
            secret_key = creds['Credentials']['SecretAccessKey'] if creds else None
            aws_session_token = creds['Credentials']['SessionToken'] if creds else None
            assume_role_params = arn if type(arn) is dict else {"RoleArn": arn}
            creds = boto3.client(
                'sts',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=aws_session_token,
            ).assume_role(
                RoleSessionName='geff'
                if role_session_name is None
                else f'geff_{role_session_name}',
                **assume_role_params
            )
        client = boto3.client(
            client_name,
            region_name=region,
            aws_access_key_id=creds['Credentials']['AccessKeyId'],
            aws_secret_access_key=creds['Credentials']['SecretAccessKey'],
            aws_session_token=creds['Credentials']['SessionToken'],
        )
    else:
        client = boto3.client(client_name, region)
    method = getattr(client, method_name)
    result = method(**kwargs)
    if results_path:
        if isinstance(result.get('Body'), StreamingBody):
            result = loads(result['Body'].read())
        result = pick(results_path, result)
    return result
