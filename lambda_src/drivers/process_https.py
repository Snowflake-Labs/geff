from base64 import b64encode
from email.utils import parsedate_to_datetime
from gzip import decompress
from hashlib import sha256
from hmac import new as new_hmac
from io import BytesIO
from json import JSONDecodeError, dumps, loads
from re import match
from time import time
from typing import Any, Dict, List, Optional, Text, Union
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlparse
from urllib.request import Request, urlopen

from ..utils import LOG, parse_header_links, pick
from ..vault import decrypt_if_encrypted


def make_basic_header(auth):
    return b'Basic ' + b64encode(auth.encode())


def parse_header_dict(value):
    return {k: v for k, v in parse_qsl(value)}


def render_jinja_template(template, params, global_functions):
    import jinja2

    e = jinja2.Environment()
    e.globals.update(global_functions)
    return e.from_string(template).render(params)


def process_row(
    data: Optional[Text] = None,
    base_url: Text = '',
    url: Text = '',
    json: Optional[Text] = None,
    method: Text = 'get',
    headers: Text = '',
    auth: Text = None,
    params: Text = '',
    verbose: bool = False,
    cursor: Text = '',
    results_path: Text = '',
):
    if not base_url and not url:
        raise ValueError('Missing required parameter. Need one of url or base-url.')

    req_url = url if url.startswith(base_url) else base_url + url

    req_params: str = params
    if req_params:
        req_url += f'?{req_params}'

    parsed_url = urlparse(req_url)
    if parsed_url.scheme != 'https':
        raise ValueError('URL scheme must be HTTPS.')

    req_host = parsed_url.hostname
    req_headers = (
        loads(headers)
        if headers.startswith('{')
        else parse_header_dict(headers)
        if headers
        else {}
    )

    req_headers.setdefault('User-Agent', 'GEFF 1.0')
    req_headers.setdefault('Accept-Encoding', 'gzip')

    # We look for an auth header and if found, we parse it from its encoded format
    if auth:
        auth = render_jinja_template(
            decrypt_if_encrypted(auth),
            {
                'path': parsed_url.path,
                'query': parsed_url.query,
                'method': method,
                'unixtime': int(time()),
            },
            {
                'time': time,
                'hmac_sha256_base64': lambda secret_key, signature_string: (
                    b64encode(
                        new_hmac(
                            secret_key.encode(),
                            signature_string.encode(),
                            sha256,
                        ).digest()
                    ).decode()
                ),
            },
        )

        req_auth = (
            loads(auth)
            if auth and auth.startswith('{')
            else parse_header_dict(auth)
            if auth
            else {}
        )
        auth_host = req_auth.get('host')

        # We reject the request if the 'auth' is present but doesn't match the pinned host.
        if auth_host and req_host and auth_host != req_host:
            raise ValueError(
                "Requests can only be made to host provided in the auth header."
            )
        # If the URL is missing a hostname, use the host from the auth dictionary
        elif auth_host and not req_host:
            req_host = auth_host
        # We make unauthenticated request if the 'host' key is missing.
        elif not auth_host:
            raise ValueError(f"'auth' missing the 'host' key.")
        elif 'basic' in req_auth:
            req_headers['Authorization'] = make_basic_header(req_auth['basic'])
        elif 'bearer' in req_auth:
            req_headers['Authorization'] = f"Bearer {req_auth['bearer']}"
        elif 'authorization' in req_auth:
            req_headers['authorization'] = req_auth['authorization']
        elif 'headers' in req_auth:
            req_headers.update(req_auth['headers'])
        elif 'body' in req_auth:
            if json:
                raise ValueError(f"auth 'body' key and json param are both present")
            else:
                json = (
                    req_auth['body']
                    if isinstance(req_auth['body'], str)
                    else dumps(req_auth['body'])
                )

    # query, nextpage_path, results_path
    req_results_path: str = results_path
    req_cursor: str = cursor
    req_method: str = method.upper()

    if json:
        req_data: Optional[bytes] = (
            json if json.startswith('{') else dumps(parse_header_dict(json))
        ).encode()
        req_headers['Content-Type'] = 'application/json'
    else:
        req_data = None if data is None else data.encode()

    next_url: Optional[str] = req_url
    row_data: List[Any] = []

    LOG.debug('Starting pagination.')
    while next_url:
        LOG.debug(f'next_url is {next_url}.')
        req = Request(next_url, method=req_method, headers=req_headers, data=req_data)
        links_headers = None

        try:
            LOG.debug(f'Making request with {req}')
            res = urlopen(req)
            links_headers = parse_header_links(
                ','.join(res.headers.get_all('link', []))
            )
            response_headers = dict(res.getheaders())
            res_body = res.read()
            LOG.debug(f'Got the response body with length: {len(res_body)}')

            raw_response = (
                decompress(res_body)
                if res.headers.get('Content-Encoding') == 'gzip'
                else res_body
            )
            response_date = (
                parsedate_to_datetime(response_headers['Date']).isoformat()
                if 'Date' in response_headers
                else None
            )
            response_body = (
                loads(raw_response)
                if response_headers.get('Content-Type', '').startswith(
                    'application/json'
                )
                else BytesIO(raw_response).getbuffer().tobytes()
            )
            LOG.debug('Extracted data from response.')

            response = (
                {
                    'body': response_body,
                    'headers': response_headers,
                    'responded_at': response_date,
                }
                if verbose
                else response_body
            )
            result = pick(req_results_path, response)
        except HTTPError as e:
            response_body = (
                decompress(e.read())
                if e.headers.get('Content-Encoding') == 'gzip'
                else e.read()
            ).decode()
            content_type = e.headers.get('Content-Type', '')
            result = {
                'error': 'HTTPError',
                'url': next_url,
                'status': e.code,
                'reason': e.reason,
                'body': (
                    loads(response_body)
                    if content_type and content_type.startswith('application/json')
                    else response_body
                ),
            }
        except URLError as e:
            result = {
                'error': f'URLError',
                'reason': str(e.reason),
                'host': req_host,
            }
        except JSONDecodeError as e:
            result = {
                'error': 'JSONDecodeError' if raw_response else 'No Content',
                'body': raw_response.decode(),
                'status': res.status,
                'responded_at': response_date,
            }

        if req_cursor and isinstance(result, list):
            row_data += result

            if ':' in req_cursor:
                cursor_path, cursor_param = req_cursor.rsplit(':', 1)
            else:
                cursor_path = req_cursor
                cursor_param = cursor_path.split('.')[-1]

            cursor_value = pick(cursor_path, response)

            next_url = (
                cursor_value
                if cursor_value
                and isinstance(cursor_value, str)
                and cursor_value.startswith('https://')
                else f'{req_url}&{cursor_param}={cursor_value}'
                if cursor_value
                else None
            )
        elif links_headers and isinstance(result, list):
            row_data += result
            link_dict: Dict[Any, Any] = next(
                (l for l in links_headers if l['rel'] == 'next'), {}
            )
            nu: Optional[str] = link_dict.get('url')
            next_url = nu if nu != next_url else None
        else:
            row_data = result
            next_url = None

    LOG.debug(f'Returning row_data with count: {len(row_data)}')
    return row_data
