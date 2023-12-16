from utils import mock_urlopen_with_responses, mock_response, mock_urlopen, Mock

from urllib.request import Request

from lambda_src.drivers.process_https import process_row


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


@mock_urlopen_with_responses(
    mock_response({'Content-Type': 'application/json'}, b'{"items": [4], "next": "1"}'),
    mock_response({'Content-Type': 'application/json'}, b'{"items": [2], "next": "2"}'),
)
def test_process_row_pagination_cursor_json(mock_urlopen):
    result = process_row(
        base_url='https://api.eg.com',
        url='/items',
        page_limit=2,
        cursor='{"path":"next", "body":"from"}',
        results_path='items',
        json='{}',
    )

    assert mock_urlopen.call_count == 2
    assert result == [4, 2]

    assert_urlopen_made_requests(
        mock_urlopen,
        [
            Request('https://api.eg.com/items', headers={}, data=b'{}'),
            Request(
                'https://api.eg.com/items',
                data=b'{"from": "1"}',
            ),
        ],
    )


@mock_urlopen_with_responses(
    mock_response({'Content-Type': 'application/json'}, b'{"items": [4], "next": null}')
)
def test_process_row_pagination_cursor_json_null(mock_urlopen):
    result = process_row(
        base_url='https://api.eg.com',
        url='/items',
        page_limit=2,
        cursor='{"path":"next", "body":"from"}',
        results_path='items',
        json='{}',
    )

    assert mock_urlopen.call_count == 1
    assert result == [4]

    assert_urlopen_made_requests(
        mock_urlopen,
        [
            Request('https://api.eg.com/items', headers={}, data=b'{}'),
        ],
    )


@mock_urlopen_with_responses(
    mock_response({'Content-Type': 'application/json'}, b'{"items": [4], "next": "1"}'),
    mock_response({'Content-Type': 'application/json'}, b'{"items": [2], "next": "2"}'),
)
def test_process_row_pagination_cursor_string(mock_urlopen):
    result = process_row(
        base_url='https://api.eg.com',
        url='/items',
        page_limit=2,
        cursor='next:p',
        results_path='items',
        json='{"a":1}',
    )

    assert mock_urlopen.call_count == 2
    assert result == [4, 2]

    assert_urlopen_made_requests(
        mock_urlopen,
        [
            Request('https://api.eg.com/items', data=b'{"a": 1}'),
            Request('https://api.eg.com/items?p=1', data=b'{"a": 1}'),
        ],
    )


@mock_urlopen_with_responses(
    mock_response(
        {'Content-Type': 'application/json'}, b'{"items": [4], "next": null}'
    ),
)
def test_process_row_pagination_cursor_null(mock_urlopen):
    result = process_row(
        base_url='https://api.eg.com',
        url='/items',
        page_limit=2,
        cursor='next:p',
        results_path='items',
        json='{"a":1}',
    )

    assert mock_urlopen.call_count == 1
    assert result == [4]

    assert_urlopen_made_requests(
        mock_urlopen,
        [
            Request('https://api.eg.com/items', data=b'{"a": 1}'),
        ],
    )


@mock_urlopen_with_responses(
    mock_response(
        {
            'Content-Type': 'application/json',
            'link': '<https://api.eg.com/items?p=2>;rel="next"',
        },
        b'{"items": [4]}',
    ),
    mock_response({'Content-Type': 'application/json'}, b'{"items": [2]}'),
)
def test_process_row_pagination_links(mock_urlopen):
    result = process_row(
        base_url='https://api.eg.com',
        url='/items',
        page_limit=2,
        results_path='items',
    )

    assert mock_urlopen.call_count == 2
    assert result == [4, 2]

    assert_urlopen_made_requests(
        mock_urlopen,
        [
            Request('https://api.eg.com/items', data=None),
            Request('https://api.eg.com/items?p=2', data=None),
        ],
    )
