from json import loads

import boto3
from botocore.response import StreamingBody


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
        for assume_role_arn in loads(assume_role_arns):
            access_key = creds['Credentials']['AccessKeyId'] if creds else None
            secret_key = creds['Credentials']['SecretAccessKey'] if creds else None
            aws_session_token = creds['Credentials']['SessionToken'] if creds else None
            creds = boto3.client('sts').assume_role(
                RoleArn=assume_role_arn,
                RoleSessionName=f'geff_{role_session_name}'
                if role_session_name is None
                else 'geff',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=aws_session_token,
            )
        client = boto3.client(
            client_name,
            region=region,
            aws_access_key_id=creds['Credentials']['AccessKeyId'],
            aws_secret_access_key=creds['Credentials']['SecretAccessKey'],
            aws_session_token=creds['Credentials']['SessionToken'],
        )
    else:
        client = boto3.client(client_name, region)
    method = getattr(client, method_name)
    result = method(**kwargs)
    if (
        results_path
        and result.get('ContentType') == 'application/json'
        and isinstance(result.get('Body'), StreamingBody)
    ):
        result = pick(results_path, loads(result.get('Body')).read())
    return result
