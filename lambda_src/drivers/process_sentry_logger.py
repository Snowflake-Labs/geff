import logging

import sentry_sdk
from sentry_sdk import push_scope

CONSOLE_LOGGER = logging.getLogger('console')
GEFF_SENTRY_LOGGER = logging.getLogger('geff')
SENTRY_DRIVER_LOGGER = logging.getLogger('sentry_driver')



def process_row(
    name: str,
    history_type: str,
    error: str,
    ts: str,
    history_url: str,
) -> str:
    """Each row is sent to Sentry via the SENTRY_DRIVER_LOGGER

    Args:
        name (str): The object name.
        history_type (str): pipe or task or query.
        error (str): The actual error message.
        ts (str): The timestamp of the error.
        history_url (str): URL of the erroring object in history.
    """
    CONSOLE_LOGGER.info(f'sentry_logger driver invoked.')

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
        return f'Captured {name} error at {ts}.'
    except Exception as e:
        GEFF_SENTRY_LOGGER.exception(e)
        return f'Failed to captured {name} error at {ts}.'
