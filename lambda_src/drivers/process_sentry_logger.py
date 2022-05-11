from typing import Dict
from urllib.parse import urlencode

import sentry_sdk
from sentry_sdk import push_scope

from ..log import get_loggers

CONSOLE_LOGGER, GEFF_SENTRY_LOGGER, SENTRY_DRIVER_LOGGER = get_loggers()


def get_snowsight_url(
    database: str,
    schema: str,
    region: str,
    account: str,
    name: str,
    history_type: str,
    query_id: str,
):
    type_filter = 'type=relative&relative={"tense":"past","value":28,"unit":"day","excludePartial":false,"exclusionSize":"day","exclusionSizeParam":""}'
    db_schema_filter = f"database={database}&schema={schema}"

    history_url = (
        (
            f"https://app.snowflake.com/{region}/{account}/compute/history/tasks?" +
            type_filter + 
            f"&task={name}&{db_schema_filter}&status=Failed"
        )
        if history_type == "task"
        else (
            f"https://app.snowflake.com/{region}/{account}/compute/history/copies?" +
            type_filter +
            f"&pipe={name}&{db_schema_filter}&status=LOAD_FAILED"
        )
        if history_type == "pipe"
        else (
            f"https://app.snowflake.com/{region}/{account}/compute/history/queries/" +
            query_id +
            f"/detail?autoRefreshInSeconds=0"
        )
    )
    return urlencode(history_url)


def process_row(
    error: str,
    query_id: str,
    database: str,
    schema: str,
    name: str,
    ts: str,
    history_type: str,
    region: str,
    account: str,
):
    """Each row is sent to Sentry via the SENTRY_DRIVER_LOGGER

    Args:
        error (str): The actual error message.
        query_id (str): The query ID of the failed query.
        database (str): The DB.
        schema (str): The schema.
        name (str): The object name.
        ts (str): The timestamp of the error.
        history_type (str): pipe or task or query.
        region (str): Region of the Snowflake account.
        account (str): The Snowflake account name.
    """
    history_url = get_snowsight_url(
        database,
        schema,
        region,
        account,
        name,
        history_type,
        query_id,
    )

    try:
        with push_scope() as scope:
            scope.set_extra('history_url', history_url)
            sentry_sdk.set_tag(
                (
                    'PIPE'
                    if history_type in ('pipe', 'PIPE')
                    else 'TASK'
                    if history_type in ('task', 'TASK')
                    else 'QUERY'
                ),
                name
            )
            sentry_sdk.set_tag('error', error)
            sentry_sdk.set_tag('query_id', query_id)
            sentry_sdk.set_tag('error_time', ts)
            sentry_sdk.set_tag('history_type', history_type)
            SENTRY_DRIVER_LOGGER.exception(error)
    except Exception as e:
        GEFF_SENTRY_LOGGER.exception(e)
