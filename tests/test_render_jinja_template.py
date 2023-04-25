from base64 import b64encode
from urllib.parse import urlparse
from time import time
from hmac import new as new_hmac

from lambda_src.drivers.process_https import render_jinja_template


def test_render_jinja_template():
    u = urlparse('s3://asdf/asdf')
    req_path = u.path
    method = 'GET'
    decrypt_if_encrypted = lambda x: x

    auth = '{"Timestamp": "{{unixtime}}", "Authorization": "TC 1234:{{hmac_sha256_base64("querty", [path, method, unixtime]|join(":"))}}"}'

    render_jinja_template(
        decrypt_if_encrypted(auth),
        {'path': req_path, 'method': method, 'unixtime': int(time())},
        {
            'time': time,
            'hmac_sha256_base64': lambda secret_key, signature_string: (
                b64encode(
                    new_hmac(
                        secret_key.encode(),
                        signature_string.encode(),
                        hashlib.sha256,
                    ).digest()
                ).decode()
            ),
        },
    )
