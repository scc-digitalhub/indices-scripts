import pandas as pd
import boto3
import botocore
import io
import json
import os
import requests
import base64
import gzip
import time
import re
import pytz

from botocore.client import Config
from datetime import datetime
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session

S3_ENDPOINT = os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET')
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')

BASE_PATH = 'devart/dailydev/'

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_DAILY_URL = "https://www.deviantart.com/api/v1/oauth2/browse/dailydeviations"
DEVART_METADATA_URL = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"


SESSION = False


class BytesIOWrapper(io.BufferedReader):
    """Wrap a buffered bytes stream over TextIOBase string stream."""

    def __init__(self, text_io_buffer, encoding=None, errors=None, **kwargs):
        super(BytesIOWrapper, self).__init__(text_io_buffer, **kwargs)
        self.encoding = encoding or text_io_buffer.encoding or 'utf-8'
        self.errors = errors or text_io_buffer.errors or 'strict'

    def _encoding_call(self, method_name, *args, **kwargs):
        raw_method = getattr(self.raw, method_name)
        val = raw_method(*args, **kwargs)
        return val.encode(self.encoding, errors=self.errors)

    def read(self, size=-1):
        return self._encoding_call('read', size)

    def read1(self, size=-1):
        return self._encoding_call('read1', size)

    def peek(self, size=-1):
        return self._encoding_call('peek', size)

# client credentials


def client_auth():

    client = BackendApplicationClient(client_id=CLIENT_ID)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=OAUTH_TOKEN_URL,
                              client_id=CLIENT_ID,
                              client_secret=CLIENT_SECRET)

    return oauth


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def api_call_get(url, params):
    global SESSION
    if not SESSION:
        SESSION = client_auth()

    try:
        r = SESSION.get(url, params=params)
        data = r.json()
        return data

    except TokenExpiredError:
        SESSION = client_auth()
        return api_call_get(url, params)

    except Exception as ex:
        print(
            f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")
        pass


def handler(context, event):
    try:
        context.logger.info('download datasets from daily devart')

        context.logger.error('Done')
        return context.Response(body='Done',
                                headers={},
                                content_type='text/plain',
                                status_code=200)
    except Exception as e:
        context.logger.error('Error: '+str(e))
        return context.Response(body='Error '+str(e),
                                headers={},
                                content_type='text/plain',
                                status_code=500)
