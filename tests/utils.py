from pytest import fixture, mark
from unittest.mock import patch, Mock, call

from email.message import EmailMessage


@fixture
def mock_urlopen(request):
    with patch('urllib.request.urlopen') as mock_urlopen:
        mock_urlopen.side_effect = request.param
        yield mock_urlopen


def fixture_params(fixture, args):
    return mark.parametrize(fixture, [args], indirect=True)


def mock_response(headers: dict, body: bytes) -> Mock:
    mock_response = Mock()
    mock_response.read.return_value = body

    mock_response.headers = EmailMessage()
    for key, value in headers.items():
        mock_response.headers[key] = value

    return mock_response


def mock_urlopen_with_responses(*responses):
    return lambda test: fixture_params(
        "mock_urlopen",
        responses,
    )(test)
