import ast
import boto3
import botocore
import io
import json
import os
import pandas as pd
import requests
import time


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

BASE_PATH = 'devart/popular/'
#eventually change this
PATH_CSV_CATEGORY = "devart/popular/category/"

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_POULAR_URL = "https://www.deviantart.com/api/v1/oauth2/browse/popular"
DEVART_METADATA_URL = "https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"

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

# Function to unpack and merge the popular deviation
# response and the metadata response
def unpack_data(js,today):

    dd_js = js[0]
    md_js = js[1]
    d = {}
    d["deviationid"] = [dd_js["deviationid"]]
    d["printid"] = [dd_js["printid"]]
    d["published_time"] = [pd.to_datetime(
        dd_js["published_time"], unit="s", origin="unix")]
    d["poular_24hr_time"] = [pd.to_datetime(today)]
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

        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        date_path = today.strftime("year=%Y/month=%m/day=%d/")\

        timerange = '24hr'

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
        if 'timerange' in msg:
            timerange = msg['timerange']


        #set base filename and storing directories
        if timerange == '24hr':

            filename = f'popular-{timerange}-{today_str}'
            csv_path = f"{BASE_PATH}{timerange}/csv/{date_path}"
            json_path = f"{BASE_PATH}{timerange}/json/{date_path}"
            parquet_path = f"{BASE_PATH}{timerange}/parquet/{date_path}"

        elif timerange == 'alltime':
            #TODO eventually
            pass

        # init s3 client
        s3 = boto3.client('s3',
                          endpoint_url=S3_ENDPOINT,
                          aws_access_key_id=S3_ACCESS_KEY,
                          aws_secret_access_key=S3_SECRET_KEY,
                          config=Config(signature_version='s3v4'),
                          region_name='us-east-1')

        context.logger.info(f'download popular deviation with timerange {timerange}')

        
        #reading csv from minio - the path/filename must be rewritten
        key = f"{PATH_CSV_CATEGORY}category_list.csv"
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        dataio = io.BytesIO(obj['Body'].read())
        df = pd.DataFrame()
        context.logger.info(f'read {key} into pandas dataframe')
        df = pd.read_csv(dataio)
        #eventually change filename
        category_list = df['catpath'].values.tolist()


        pdev_list = []
        for cat in category_list:

            cat_name = cat.lstrip("/").replace("/","_")

            limit = 24
            offset = 0
            has_more = True
            len_array = 1

            #cycle over offset until there's new results
            while has_more==True and len_array>0:

                params_pop = {
                        "category_path":cat,
                        "timerange":timerange,
                        "offset":offset,
                        "limit":limit,
                        "mature_content":"true"}

                pdev_data = api_call_get(DEVART_POULAR_URL,params_pop)

                len_array = len(pdev_data["results"])
                has_more = pdev_data["has_more"]

                #if the response doesn't contain data the json is not saved
                if len_array>0:

                    pdev_list.extend([i for i in pdev_data["results"]])

                    # write to S3 as StringIO json
                    jsonio = io.StringIO()
                    json.dump(pdev_data, jsonio, ensure_ascii=False)
                    # seek to start otherwise upload will be 0
                    jsonio.seek(0)

                    # wrap as byte with reader
                    wrapjsonio = BytesIOWrapper(jsonio)

                    context.logger.info(f'upload to s3 as {filename}-{cat_name}-{offset}.json')
                    json_key = f"{json_path}{filename}-{cat_name}-{offset}.json"

                    s3.upload_fileobj(wrapjsonio, S3_BUCKET, json_key)

                    offset += 24

                time.sleep(1)


        #remove duplicated deviations
        pdev_list = set([str(x) for x in pdev_list])
        pdev_list = [ast.literal_eval(i) for i in pdev_list]


        # fetch and store ids of popular deviation
        id_list = [dev["deviationid"] for dev in pdev_list]
        idf = pd.DataFrame(id_list, columns=["ids"])

        # write to S3 as csv
        csvio = io.StringIO()
        idf.to_csv(csvio, header=True, index=False)

        # seek to start otherwise upload will be 0
        csvio.seek(0)

        # wrap as byte with reader
        wrapcsvio = BytesIOWrapper(csvio)

        context.logger.info(f'upload to s3 as {filename}-ids.csv')
        csv_key = f"{csv_path}{filename}-ids.csv"

        s3.upload_fileobj(wrapcsvio, S3_BUCKET, csv_key)


        # download metadata for every deviations
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

            time.sleep(1)

        # merge data and metadata and export to parquet
        pdev_list = sorted(pdev_list, key=lambda x: x["deviationid"])
        mdev_list = sorted(mdev_list, key=lambda x: x["deviationid"])
        coupled_json = [[i, j] for i, j in zip(
            pdev_list, mdev_list) if i["deviationid"] == j["deviationid"]]

        list_pandas = []
        for couple in coupled_json:
            d = unpack_data(couple, today)
            list_pandas.append(pd.DataFrame.from_dict(d))

        # popular dev + metadata
        pdf = pd.concat(list_pandas)
        pdf.reset_index(drop=True, inplace=True)

        # store parquet to S3
        parquetio = io.BytesIO()
        pdf.to_parquet(parquetio, engine='pyarrow')
        # seek to start otherwise upload will be 0
        parquetio.seek(0)

        context.logger.info(f'upload to s3 as {filename}.parquet')
        parquet_key = f"{parquet_path}{filename}.parquet"
        s3.upload_fileobj(parquetio, S3_BUCKET, parquet_key)

        # cleanup
        del parquetio
        del wrapcsvio
        del wrapjsonio
        del csvio
        del jsonio
        del idf
        del pdf
        del pdev_list
        del mdev_list
        del coupled_json

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
