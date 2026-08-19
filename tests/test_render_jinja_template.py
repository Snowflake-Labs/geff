from base64 import b64encode
from urllib.parse import urlparse
from time import time
from hashlib import sha256
from hmac import new as new_hmac

import pytest
from jinja2.exceptions import SecurityError

from lambda_src.drivers.process_https import render_jinja_template


def test_render_jinja_template():
    u = urlparse('s3://asdf/asdf')
    req_path = u.path
    method = 'GET'
    decrypt_if_encrypted = lambda x: x

    auth = '{"Timestamp": "{{unixtime}}", "Authorization": "TC 1234:{{hmac_sha256_base64("querty", [path, method, unixtime]|join(":"))}}"}'

    render_jinja_template(
        auth,
        {'path': req_path, 'method': method, 'unixtime': int(time())},
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


def test_render_jinja_template_blocks_unsafe_attribute_access():
    def registered_function():
        return None

    template = "{{ registered_function.__globals__.__builtins__.eval('40 + 2') }}"

    with pytest.raises(SecurityError):
        render_jinja_template(
            template, {}, {'registered_function': registered_function}
        )


def test_render_jinja_template_does_not_expose_default_globals():
    assert render_jinja_template('{{ cycler is undefined }}', {}, {}) == 'True'
