import json
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Tuple, Any

from ..vault import decrypt_if_encrypted


def parse_smtp_creds(auth_dict: Dict) -> Tuple[Any, Any, Any]:
    user = next(
        value
        for value in map(
            auth_dict.get,
            [
                'SMTP_USERNAME',
                'SMTP_USER',
                'SMTP_USER_NAME',
                'smtp_user_name',
                'smtp_username',
                'smtp_user',
                'username',
                'user_name',
                'user',
            ],
        )
        if value is not None
    )
    password = next(
        value
        for value in map(
            auth_dict.get,
            [
                'SMTP_PASSWORD',
                'SMTP_PASS',
                'smtp_password',
                'smtp_pass',
                'password',
                'pass',
            ],
        )
        if value is not None
    )
    host = next(
        value
        for value in map(
            auth_dict.get,
            [
                'SMTP_SERVER',
                'SMTP_HOST',
                'smtp_server',
                'smtp_host',
                'host',
                'server',
                'mail_server',
            ],
        )
        if value is not None
    )
    return user, password, host


def process_row(
    recipient_email,
    text,
    user=None,
    password=None,
    auth=None,
    sender_email=None,
    html=None,
    subject=None,
    reply_to=None,
    cc=None,
    bcc=None,
    host='smtp.gmail.com',
    port=587,
    use_ssl=True,
    use_tls=True,
):

    user = decrypt_if_encrypted(user)
    password = decrypt_if_encrypted(password)
    sender_email = sender_email or user

    if auth:
        auth_json = decrypt_if_encrypted(auth)
        auth_dict = json.loads(auth_json)
        user, password, host = parse_smtp_creds(auth_dict)

    # Create the base MIME message.
    if html is None:
        message = MIMEMultipart()
    else:
        message = MIMEMultipart('alternative')

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first

    # Turn these into plain/html MIMEText objects
    textPart = MIMEText(text, 'plain')
    message.attach(textPart)

    if html is not None:
        htmlPart = MIMEText(html, 'html')
        message.attach(htmlPart)

    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = recipient_email

    recipients = recipient_email.split(',')

    if cc is not None:
        message['Cc'] = cc
        recipients = recipients + cc.split(',')

    if bcc is not None:
        recipients = recipients + bcc.split(',')

    if reply_to is not None:
        message.add_header('reply-to', reply_to)

    if use_ssl is True:
        context = ssl.create_default_context()
        if use_tls is True:
            smtpserver = smtplib.SMTP(host, port)
            smtpserver.starttls(context=context)
        else:
            smtpserver = smtplib.SMTP_SSL(host, port, context=context)
    else:
        smtpserver = smtplib.SMTP(host, port)

    if user and password:
        smtpserver.login(user, password)

    try:
        result = smtpserver.sendmail(sender_email, recipients, message.as_string())
    except ValueError as e:
        result = {
            'error': 'ValueError',
            'reason': str(e),
        }
    except smtplib.SMTPDataError as e:
        result = {
            'error': 'SMTPDataError',
            'smtp_code': e.smtp_code,
            'smtp_error': e.smtp_error.decode(),
        }
    finally:
        smtpserver.close()

    return result
