from utils import mock_urlopen_with_responses, mock_response, mock_urlopen, Mock

from urllib.request import Request

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
