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

# Function to unpack and merge the daily deviation
# response and the metadata response


def unpack_data(js):

    dd_js = js[0]
    md_js = js[1]
    d = {}
    d["deviationid"] = [dd_js["deviationid"]]
    d["printid"] = [dd_js["printid"]]
    d["published_time"] = [pd.to_datetime(
        dd_js["published_time"], unit="s", origin="unix")]
    d["day_of_daily_deviation"] = [
        pd.to_datetime(dd_js["daily_deviation"]["time"])]
    d["url"] = [dd_js["url"]]
    d["title"] = [dd_js["title"]]
    d["category"] = [dd_js["category"]]
    d["category_path"] = [dd_js["category_path"]]
    d["author_userid"] = [dd_js["author"]["userid"]]
    d["author_username"] = [dd_js["author"]["username"]]
    d["author_type"] = [dd_js["author"]["type"]]
    d["allows_comments"] = [dd_js["allows_comments"]]
    d["is_mature"] = [dd_js["is_mature"]]
    d["is_downloadable"] = [dd_js["is_downloadable"]]
    d["description"] = [md_js["description"]]
    d["license"] = [md_js["license"]]
    d["tags"] = [[i["tag_name"] for i in md_js["tags"]]]
    return d


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

        start_date = datetime.today().strftime("%Y-%m-%d")
        end_date = False
        msg = {}

        # params - expect json
        if(event.content_type == 'application/json'):
            msg = event.body
        else:
            jsstring = event.body.decode('utf-8').strip()
            if jsstring:
                msg = json.loads(jsstring)

        context.logger.info(msg)

        # fetch params
        if 'start_date' in msg:
            start_date = datetime.strptime(msg['start_date'], '%Y-%m-%d')

        if 'end_date' in msg:
            end_date = datetime.strptime(msg['end_date'], '%Y-%m-%d')

        # init s3 client
        s3 = boto3.client('s3',
                          endpoint_url=S3_ENDPOINT,
                          aws_access_key_id=S3_ACCESS_KEY,
                          aws_secret_access_key=S3_SECRET_KEY,
                          config=Config(signature_version='s3v4'),
                          region_name='us-east-1')

        if not end_date:
            end_date = start_date
        daterange = pd.date_range(start=start_date, end=end_date, freq="d")
        datelist = daterange.to_pydatetime().tolist()

        for cur_date in datelist:
            day = cur_date.strftime("%Y-%m-%d")
            date_path = cur_date.strftime("year=%Y/month=%m/")
            ddf_filename = 'daily-dev-{}'.format(day)

            context.logger.info('download for '+day)

            # dailydev
            ddev_params = {
                "date": day,
                "mature_content": "true"
            }

            ddev_data = api_call_get(DEVART_DAILY_URL, ddev_params)

            # write to S3 as StringIO json
            jsonio = io.StringIO()
            json.dump(ddev_data, jsonio, ensure_ascii=False)
            # seek to start otherwise upload will be 0
            jsonio.seek(0)

            # wrap as byte with reader
            wrapjsonio = BytesIOWrapper(jsonio)

            context.logger.info('upload to s3 as '+ddf_filename+'.json')
            json_key = BASE_PATH + 'json/'+date_path+ddf_filename+'.json'

            s3.upload_fileobj(wrapjsonio, S3_BUCKET, json_key)

            # fetch ids
            id_list = [dev["deviationid"] for dev in ddev_data["results"]]
            idf = pd.DataFrame(id_list, columns=["ids"])

            # write to S3 as csv
            csvio = io.StringIO()
            idf.to_csv(csvio, header=True, index=False)

            # seek to start otherwise upload will be 0
            csvio.seek(0)

            # wrap as byte with reader
            wrapcsvio = BytesIOWrapper(csvio)

            context.logger.info('upload to s3 as '+ddf_filename+'.csv')
            csv_key = BASE_PATH + 'csv/'+date_path+ddf_filename+'-ids.csv'

            s3.upload_fileobj(wrapcsvio, S3_BUCKET, csv_key)
            # download metadata for every deviations
            ddev_list = [dev for dev in ddev_data["results"]]
            limit = 50
            mdev_list = []
            for id_dev in chunks(id_list, limit):
                mdev_params = {
                    "deviationids[]": id_dev,
                    "mature_content": "true"
                }

                mdata_response = api_call_get(DEVART_METADATA_URL, mdev_params)
                for dev in mdata_response["metadata"]:
                    mdev_list.append(dev)

            # merge data and metadata and export to parquet
            ddev_list = sorted(ddev_list, key=lambda x: x["deviationid"])
            mdev_list = sorted(mdev_list, key=lambda x: x["deviationid"])
            coupled_json = [[i, j] for i, j in zip(
                ddev_list, mdev_list) if i["deviationid"] == j["deviationid"]]

            list_pandas = []
            for couple in coupled_json:
                d = unpack_data(couple)
                list_pandas.append(pd.DataFrame.from_dict(d))

            # daily dev + metadata
            ddf = pd.concat(list_pandas)

            # store parquet to S3
            parquetio = io.BytesIO()
            ddf.to_parquet(parquetio, engine='pyarrow')
            # seek to start otherwise upload will be 0
            parquetio.seek(0)

            context.logger.info('upload to s3 as '+ddf_filename+'.parquet')
            ddf_key = BASE_PATH + 'daily/'+date_path+ddf_filename+'.parquet'
            s3.upload_fileobj(parquetio, S3_BUCKET, ddf_key)

            # cleanup
            del parquetio
            del wrapcsvio
            del wrapjsonio
            del csvio
            del jsonio
            del idf
            del ddf

            # rate limit
            time.sleep(2)

        ##
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
