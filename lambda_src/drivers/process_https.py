from base64 import b64encode
from email.utils import parsedate_to_datetime
from gzip import decompress
from hashlib import sha256
from hmac import new as new_hmac
from io import BytesIO
from json import JSONDecodeError, dumps, loads
from re import match
from time import time
from typing import Any, Dict, List, Optional, Union
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlparse
from urllib import request


from ..utils import (
    LOG,
    parse_header_links,
    set_value,
    pick,
    DataMetadata,
    add_param_to_url,
)
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
    base_url: str = '',
    url: str = '',
    data: Optional[str] = None,
    json: Optional[str] = None,
    method: str = 'get',
    headers: str = '',
    auth: Optional[str] = None,
    params: str = '',
    verbose: bool = False,
    cursor: str = '',
    page_limit: Optional[int] = None,
    results_path: str = '',
    destination_metadata: str = '',
):
    if not base_url and not url:
        raise ValueError('Missing required parameter. Need one of url or base-url.')

    if data and json:
        raise ValueError('parameters data and json cannot both be present')

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
            if data:
                raise ValueError(f"auth 'body' key and data param are both present")
            else:
                data = (
                    req_auth['body']
                    if isinstance(req_auth['body'], str)
                    else dumps(req_auth['body'])
                )

    # query, nextpage_path, results_path
    req_results_path: str = results_path
    req_cursor: str = cursor
    req_page_count: int = 0
    req_method: str = method.upper()
    req_data: Optional[bytes] = None if data is None else data.encode()
    if json:
        req_json = loads(json) if json.startswith('{') else parse_header_dict(json)
        req_headers['Content-Type'] = 'application/json'
    else:
        req_json = None

    next_url: Optional[str] = req_url
    row_data: List[Any] = []
    metadata: Optional[Any] = None

    LOG.debug('Starting pagination.')
    while next_url:
        LOG.debug(f'~> {req_method} {next_url}')
        req = request.Request(
            next_url,
            method=req_method,
            headers=req_headers,
            data=req_data or (None if req_json is None else dumps(req_json).encode()),
        )
        links_headers = None

        try:
            res = request.urlopen(req)
            links_headers = parse_header_links(
                ','.join(res.headers.get_all('link', []))
            )
            res_headers = dict(res.headers.items())
            res_body = res.read()
            res_encoding = res_headers.get('Content-Encoding')
            res_type = res_headers.get('Content-Type', '')
            LOG.debug(f'<~ {len(res_body)} bytes [{res_type}] [{res_encoding}]')

            raw_response = decompress(res_body) if res_encoding == 'gzip' else res_body
            response_date = (
                parsedate_to_datetime(res_headers['Date']).isoformat()
                if 'Date' in res_headers
                else None
            )
            response_body = (
                loads(raw_response)
                if res_type.startswith('application/json')
                else BytesIO(raw_response).getbuffer().tobytes()
            )

            response = (
                {
                    'body': response_body,
                    'headers': res_headers,
                    'responded_at': response_date,
                }
                if verbose
                else response_body
            )
            result = pick(req_results_path, response)
            if destination_metadata:
                metadata = pick(destination_metadata, response)

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
            req_page_count += 1

            cursor_body = None
            if req_cursor.startswith('{'):
                c = loads(req_cursor)
                cursor_path = c.get('path')
                cursor_body = c.get('body')
                cursor_param = c.get('param')
            elif ':' in req_cursor:
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
                else add_param_to_url(req_url, cursor_param, cursor_value)
                if cursor_param and cursor_value
                else next_url
                if cursor_body and cursor_value and isinstance(req_json, dict)
                else None
            )
            if cursor_body:
                if isinstance(req_json, dict):
                    set_value(req_json, cursor_body, cursor_value)
                else:
                    raise ValueError('cursor.body present without json param')

            if page_limit == req_page_count:
                next_url = None

        elif links_headers and isinstance(result, list):
            row_data += result
            req_page_count += 1
            link_dict: Dict[Any, Any] = next(
                (l for l in links_headers if l['rel'] == 'next'), {}
            )
            nu: Optional[str] = link_dict.get('url')
            next_url = nu if nu != next_url else None

            if page_limit == req_page_count:
                next_url = None

        elif isinstance(result, list):
            row_data += result
            next_url = None

        else:
            row_data = result
            next_url = None

    LOG.debug(f'<- len(row_data)={len(row_data)}')
    return row_data if metadata is None else DataMetadata(row_data, metadata)
