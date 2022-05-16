from urllib.parse import urlencode
from typing import Optional

import sentry_sdk
from sentry_sdk import push_scope

from ..log import get_loggers

CONSOLE_LOGGER, GEFF_SENTRY_LOGGER, SENTRY_DRIVER_LOGGER = get_loggers()


def get_snowsight_url(
    account: str,
    region: str,
    database: str,
    schema: str,
    name: str,
    history_type: str,
) -> str:
    """Generate a URL for the erroring object on Snowsight.

    Args:
        account (str): Snowflake account name.
        region (str): Snowflake account region.
        database (str): Snowflake database.
        schema (str): Snowflake Schema.
        name (str): Task name or Pipe name (or query ID for query history).
        history_type (str): copy, pipe or query.

    Returns:
        str: Returns the generated URL of the erroring object.
    """

    relative_filter = {
        "tense": "past",
        "value": 1,
        "unit": "day",
        "excludePartial": False,
        "exclusionSize": "day",
        "exclusionSizeParam": "",
    }
    type_filter = f'type=relative&relative={urlencode(relative_filter)}'
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
        if history_type == "copy"
        else (
            f"https://app.snowflake.com/{region}/{account}/compute/history/queries/" +
            name +
            f"/detail?autoRefreshInSeconds=0"
        )
    )
    return history_url


def process_row(
    account: str,
    region: str,
    database: str,
    schema: str,
    name: str,
    history_type: str,
    error: str,
    ts: str,
):
    """Each row is sent to Sentry via the SENTRY_DRIVER_LOGGER

    Args:
        account (str): The Snowflake account name.
        region (str): Region of the Snowflake account.
        database (str): The DB.
        schema (str): The schema.
        name (str): The object name.
        history_type (str): pipe or task or query.
        error (str): The actual error message.
        ts (str): The timestamp of the error.
    """
    CONSOLE_LOGGER.info(f'sentry_logger driver invoked.')

    history_url = get_snowsight_url(
        account,
        region,
        database,
        schema,
        name,
        history_type
    )
    CONSOLE_LOGGER.info(f'Constructed history_url: {history_url}.')
    status: Optional[str] = None

    try:
        with push_scope() as scope:
            scope.set_extra('history_url', history_url)
            sentry_sdk.set_tag(
                (
                    'PIPE_NAME'
                    if history_type in ('copy', 'COPY')
                    else 'TASK_NAME'
                    if history_type in ('task', 'TASK')
                    else 'QUERY_ID'
                ),
                name
            )
            sentry_sdk.set_tag('error', error)
            sentry_sdk.set_tag('error_time', ts)
            sentry_sdk.set_tag('history_type', history_type)
            SENTRY_DRIVER_LOGGER.exception(error)
        status = f'Captured {name} error at {ts}.'
    except Exception as e:
        GEFF_SENTRY_LOGGER.exception(e)
        status = f'Failed to captured {name} error at {ts}.'
    return status
