import pandas as pd
import boto3
import botocore
import io
import json
import os
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

BASE_PATH = 'devart/dailydev/'

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_METADATA_URL = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"

TODAY = datetime.today()
DAILY_PATH = TODAY.strftime("year=%Y/month=%m/day=%d/")

SESSION = False

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

#Function to unpack the stats in the metadata response
def unpack_stats(js):
    d = {}
    d["deviationid"] = [js["deviationid"]]
    d["data"] = [pd.to_datetime(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))]
    d["views"] = [js["stats"]["views"]]
    d["views_today"] = [js["stats"]["views_today"]]
    d["favourites"] = [js["stats"]["favourites"]]
    d["comments"] = [js["stats"]["comments"]]
    d["downloads"] = [js["stats"]["downloads"]]
    d["downloads_today"] = [js["stats"]["downloads_today"]]
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
            ddf_filename = f'stats-daily-dev-of-{day}'
 
            #reading csv from minio
            key = f"{BASE_PATH}csv/{date_path}daily-dev-{day}-ids.csv"
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            dataio = io.BytesIO(obj['Body'].read())
            df = pd.DataFrame()
            context.logger.info('read csv into pandas dataframe')
            df = pd.read_csv(dataio)                        
            id_list = df['ids'].values.tolist()

            limit = 10
            stat_list = []
            for id_dev in chunks(id_list, limit):
                params = {
                        "deviationids[]":id_dev,
                        "ext_stats":"true",
                        "mature_content":"true"
                        }
                mdata_response = api_call_get(DEVART_METADATA_URL,params)
                for dev in mdata_response['metadata']:
                    d = unpack_stats(dev)
                    stat_list.append(pd.DataFrame.from_dict(d))


            # daily dev stats dataframe 
            ddf = pd.concat(stat_list)

            # store parquet to S3
            parquetio = io.BytesIO()
            ddf.to_parquet(parquetio, engine='pyarrow')
            # seek to start otherwise upload will be 0
            parquetio.seek(0)

            context.logger.info('upload to s3 as '+ddf_filename+'.parquet')
            ddf_key = BASE_PATH + 'stats/' + DAILY_PATH + ddf_filename + '.parquet'
            s3.upload_fileobj(parquetio, S3_BUCKET, ddf_key)

            # cleanup
            del parquetio
            del ddf
            del dataio
            del df

            # rate limit
            time.sleep(2)


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
