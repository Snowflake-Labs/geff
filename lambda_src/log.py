import logging
import traceback
from logging import Logger, StreamHandler
from os import getpid
from os.path import relpath
from typing import Any, Tuple


def fmt(fs) -> str:
    return (
        './'
        + relpath(fs.filename)
        + f':{fs.lineno}'
        + f' in {fs.name}\n'
        + f'    {fs.line}\n'
    )


def format_exception(e) -> str:
    return ''.join(traceback.format_exception(type(e), e, e.__traceback__))


def format_exception_only(e) -> str:
    return ''.join(traceback.format_exception_only(type(e), e)).strip()


def format_trace(e: Exception) -> str:
    trace: Any = traceback.extract_tb(e.__traceback__)
    fmt_trace: str = ''.join(fmt(f) for f in trace)
    stack: Any = traceback.extract_stack()

    for i, f in enumerate(reversed(stack)):
        if (f.filename, f.name) == (trace[0].filename, trace[0].name):
            stack = stack[:-i]
            break  # skip the log.py part of stack
    for i, f in enumerate(reversed(stack)):
        if 'site-packages' in f.filename:
            stack = stack[-i:]
            break  # skip the flask part of stack
    fmt_stack = ''.join(fmt(f) for f in stack)

    a: str = (
        fmt_stack
        + '--- printed exception w/ trace ---\n'
        + fmt_trace
        + format_exception_only(e)
    )

    pid = getpid()
    return f'[{pid}] {a}'


def setup_logger(logger_name: str, level: int = logging.INFO) -> Logger:
    """Sets up the logger object.

    Args:
        logger_name (str): Name to use to retreive the logger instance.
        level (int, optional): Level of logging. Defaults to logging.INFO.
        stdout (bool, optional): Bool whether to print to stdout or not. Defaults to False.

    Returns:
        Logger: Returns the logger object.
    """
    l = logging.getLogger(logger_name)
    l.setLevel(level)
    return l


def get_loggers() -> Tuple[Logger, Logger, Logger]:
    """Returns 3 loggers:
    1. Console only logger
    2. Sentry + Console logger
    3. Sentry driver logger

    Returns:
        Tuple[Any]: Returns the 3 Logger objects.
    """
    return (
        logging.getLogger(name='console'),
        logging.getLogger(name='geff'),
        logging.getLogger(name='sentry_driver'),
    )
