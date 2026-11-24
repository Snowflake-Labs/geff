from unittest.mock import Mock, patch
from urllib.request import Request

from pytest import mark, raises
from utils import mock_response, mock_urlopen, mock_urlopen_with_responses

from lambda_src.drivers import process_https


def assert_urlopen_made_requests(mock_urlopen: Mock, expected_requests: list):
    """
    Assert that urlopen was called with Request objects having the expected attributes.

    Parameters:
    - mock_urlopen (Mock): The mock urlopen object.
    - expected_requests (list): List of expected Request objects.
    """
    call_args_list = mock_urlopen.call_args_list
    assert len(call_args_list) == len(expected_requests), "Number of calls do not match"

    for i, expected_request in enumerate(expected_requests):
        actual_request = call_args_list[i][0][0]  # Request arg

        assert (
            actual_request.full_url == expected_request.full_url
        ), f"Mismatch in full_url for call {i+1}"
        assert (
            actual_request.data == expected_request.data
        ), f"Mismatch in data for call {i+1}"
        # Add any other attribute checks you need
        assert (
            actual_request.headers == expected_request.headers
        ), f"Mismatch in headers for call {i+1}"
        # Add any other attribute checks you need


def get_request_headers(req):
    return {key.lower(): value for key, value in req.header_items()}


@mock_urlopen_with_responses(
    mock_response({'Content-Type': 'application/json'}, b'{"items": [4], "next": "1"}'),
    mock_response({'Content-Type': 'application/json'}, b'{"items": [2]}'),
)
def test_process_row_destination_metadata(mock_urlopen):
    result = process_https.process_row(
        base_url='https://api.eg.com',
        url='/items?from=0',
        cursor='next:from',
        results_path='items',
        auth='{"host": "api.eg.com", "authorization": "{{query}}"}',
    )

    assert mock_urlopen.call_count == 2
    assert result == [4, 2]

    assert_urlopen_made_requests(
        mock_urlopen,
        [
            Request(
                'https://api.eg.com/items?from=0',
                headers={
                    'authorization': 'from=0',
                    'Accept-encoding': 'gzip',
                    'User-agent': 'GEFF 1.0',
                },
            ),
            Request(
                'https://api.eg.com/items?from=1',
                headers={
                    'authorization': 'from=1',
                    'Accept-encoding': 'gzip',
                    'User-agent': 'GEFF 1.0',
                },
            ),
        ],
    )


@mock_urlopen_with_responses(
    mock_response({'Content-Type': 'application/json'}, b'[]'),
)
def test_process_row_auth_template_uses_request_method(mock_urlopen):
    process_https.process_row(
        url='https://api.eg.com/items',
        method='get',
        auth='{"host": "api.eg.com", "authorization": "{{method}}"}',
    )

    req = mock_urlopen.call_args[0][0]
    assert get_request_headers(req)['authorization'] == 'GET'


@mock_urlopen_with_responses(
    mock_response(
        {
            'Content-Type': 'application/json',
            'link': '<https://other.eg.com/items>;rel="next"',
        },
        b'[1]',
    ),
    mock_response({'Content-Type': 'application/json'}, b'[2]'),
)
def test_process_row_cross_origin_pagination_strips_headers(mock_urlopen):
    result = process_https.process_row(
        url='https://api.eg.com/items',
        headers='{"X-Api-Key": "header-secret"}',
        auth='{"host": "api.eg.com", "bearer": "auth-secret"}',
    )

    first_req = mock_urlopen.call_args_list[0][0][0]
    second_req = mock_urlopen.call_args_list[1][0][0]

    assert result == [1, 2]
    assert get_request_headers(first_req)['authorization'] == 'Bearer auth-secret'
    assert get_request_headers(first_req)['x-api-key'] == 'header-secret'
    assert get_request_headers(second_req) == {
        'accept-encoding': 'gzip',
        'user-agent': 'GEFF 1.0',
    }


@mock_urlopen_with_responses(
    mock_response(
        {
            'Content-Type': 'application/json',
            'link': '<https://other.eg.com/items>;rel="next"',
        },
        b'[1]',
    ),
    mock_response({'Content-Type': 'application/json'}, b'[2]'),
)
def test_process_row_cross_origin_pagination_strips_auth_body(mock_urlopen):
    process_https.process_row(
        url='https://api.eg.com/items',
        auth='{"host": "api.eg.com", "body": "body-secret"}',
    )

    first_req = mock_urlopen.call_args_list[0][0][0]
    second_req = mock_urlopen.call_args_list[1][0][0]

    assert first_req.data == b'body-secret'
    assert second_req.data is None


@mark.parametrize(
    'next_url',
    [
        'http://api.example.test/next',
        'ftp://api.example.test/next',
        'file:///example-next-page',
    ],
)
def test_process_row_rejects_non_https_pagination(next_url):
    response = mock_response(
        {'Content-Type': 'application/json', 'Link': f'<{next_url}>; rel="next"'},
        b'[1]',
    )
    with patch.object(process_https, 'open_url', return_value=response) as opener:
        with raises(ValueError, match='URL scheme must be HTTPS'):
            process_https.process_row(url='https://api.example.test/items')

    opener.assert_called_once()


@mark.parametrize(
    'location', ['http://api.example.test/next', 'ftp://api.example.test/next']
)
def test_redirect_handler_rejects_non_https_targets(location):
    response = mock_response({'Location': location}, b'')
    req = Request('https://api.example.test/items')
    req.timeout = 1
    opener = Mock()
    handler = process_https.CredentialStrippingRedirectHandler()
    handler.add_parent(opener)

    with raises(ValueError, match='URL scheme must be HTTPS'):
        handler.http_error_302(req, response, 302, 'Found', response.headers)

    opener.open.assert_not_called()


@mark.parametrize(
    'location', ['https://other.eg.com/items', 'https://api.eg.com:8443/items']
)
def test_redirect_handler_strips_credentials_on_origin_change(location):
    req = Request(
        'https://api.eg.com/items',
        headers={
            'Authorization': 'Bearer auth-secret',
            'X-Api-Key': 'header-secret',
            'User-Agent': 'GEFF 1.0',
            'Accept-Encoding': 'gzip',
        },
    )

    redirected_req = (
        process_https.CredentialStrippingRedirectHandler().redirect_request(
            req, None, 302, 'Found', {}, location
        )
    )

    assert get_request_headers(redirected_req) == {
        'accept-encoding': 'gzip',
        'user-agent': 'GEFF 1.0',
    }


@mark.parametrize('location', ['https://api.eg.com:443/next', '/next'])
def test_redirect_handler_preserves_credentials_on_same_origin(location):
    req = Request(
        'https://api.eg.com/items',
        headers={'Authorization': 'Bearer auth-secret'},
    )
    req.timeout = 1
    response = mock_response({'Location': location}, b'')
    opener = Mock()
    handler = process_https.CredentialStrippingRedirectHandler()
    handler.add_parent(opener)
    handler.http_error_302(req, response, 302, 'Found', response.headers)

    redirected_req = opener.open.call_args.args[0]
    assert get_request_headers(redirected_req)['authorization'] == 'Bearer auth-secret'
