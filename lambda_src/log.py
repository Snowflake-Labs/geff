import logging
import sys
import traceback
from logging import StreamHandler
from os import getpid
from os.path import relpath
from typing import Any


def fmt(fs):
    return (
        './'
        + relpath(fs.filename)
        + f':{fs.lineno}'
        + f' in {fs.name}\n'
        + f'    {fs.line}\n'
    )


def format_exception(e):
    return ''.join(traceback.format_exception(type(e), e, e.__traceback__))


def format_exception_only(e):
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


def setup_logger(logger_name, level=logging.INFO, stdout=False):
    l = logging.getLogger(logger_name)
    l.setLevel(level)
    l.addHandler(StreamHandler(sys.stdout)) if stdout else None


def get_loggers():
    return (
        logging.getLogger(logger_name='console', level=logging.DEBUG, stdout=True),
        logging.getLogger(logger_name='geff', level=logging.WARNING),
        logging.getLogger(logger_name='sentry_driver', level=logging.WARNING),
    )
