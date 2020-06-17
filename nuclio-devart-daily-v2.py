import pandas as pd
import io
import json
import os
import requests
import base64
import gzip
import time
import re
import pytz

from datetime import datetime
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
AZURE_CONTAINER = os.environ.get('AZURE_CONTAINER')
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')

BASE_PATH = 'daily/'
STATS_PATH = 'stats/'
IDS_PATH = 'ids/'

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_DAILY_URL = "https://www.deviantart.com/api/v1/oauth2/browse/dailydeviations"
DEVART_METADATA_URL = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"

SLEEP_AMOUNT = 0.5
SESSION = False

# max working is 64MB
AZURE_CHUNK_SIZE = 64 * 1024 * 1024

##
# DeviantArt client
##


def client_auth():

    client = BackendApplicationClient(client_id=CLIENT_ID)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=OAUTH_TOKEN_URL,
                              client_id=CLIENT_ID,
                              client_secret=CLIENT_SECRET)

    return oauth


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


#
# helpers
#

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Function to unpack and merge the daily deviation
# response and the metadata response


def unpack_data(dd_js, md_js):
    if dd_js["deviationid"] != md_js["deviationid"]:
        raise Exception("data mismatch")

    d = {}
    d["deviationid"] = [dd_js["deviationid"]]
    d["printid"] = [dd_js["printid"]]
    d["published_time"] = [pd.to_datetime(
        dd_js["published_time"], unit="s", origin="unix")]
    d["daily_time"] = [
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


# Function to unpack the stats in the metadata response
def unpack_stats(js, today):
    d = {}
    d["deviationid"] = [js["deviationid"]]
    d["stats_time"] = [pd.to_datetime(today)]
    d["views"] = [js["stats"]["views"]]
    d["views_today"] = [js["stats"]["views_today"]]
    d["favourites"] = [js["stats"]["favourites"]]
    d["comments"] = [js["stats"]["comments"]]
    d["downloads"] = [js["stats"]["downloads"]]
    d["downloads_today"] = [js["stats"]["downloads_today"]]
    return d

#
# azure
#


def upload_azure_blob(blob_client, data, fileSize=-1, overwrite=True):
    exists = False

    if (overwrite and fileSize > 0) or (overwrite == False):
        # check if raw files already exist
        # https://github.com/Azure/azure-sdk-for-python/issues/9507
        try:
            # One of the very few methods which do not mutate state
            properties = blob_client.get_blob_properties()
            if (fileSize > 0) and (int(properties['size']) == fileSize):
                # skip only if complete
                exists = True
            if(fileSize <= 0):
                exists = True
        except ResourceNotFoundError:
            # Not found
            exists = False
            pass

    if exists == True and overwrite == False:
        # do not overwrite existing obj
        return -1

    else:
        # upload with automatic chunking
        properties1 = blob_client.upload_blob(data, overwrite=overwrite)
        # we need to re-read properties to access size
        properties2 = blob_client.get_blob_properties()
        return properties2['size']
    # done


def handler(context, event):
    try:
        context.logger.info('download datasets from daily devart')

        today = datetime.today()
        start_date = today
        end_date = False

        msg = {}

        # params - expect json
        if(event.content_type == 'application/json'):
            msg = event.body
        else:
            jsstring = event.body.decode('utf-8').strip()
            if jsstring:
                msg = json.loads(jsstring)

        # fetch params
        if 'start_date' in msg:
            start_date = datetime.strptime(msg['start_date'], '%Y-%m-%d')

        if 'end_date' in msg:
            end_date = datetime.strptime(msg['end_date'], '%Y-%m-%d')

        if not end_date:
            end_date = start_date

        # init blob client
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(
            AZURE_CONTAINER)

        # derive days list and iterate
        daterange = pd.date_range(start=start_date, end=end_date, freq="d")
        datelist = daterange.to_pydatetime().tolist()

        for cur_date in datelist:
            day = cur_date.strftime("%Y-%m-%d")
            day_str = cur_date.strftime("%Y%m%d")
            date_path = cur_date.strftime("%Y/%m/%d/")
            partition_path = cur_date.strftime("year=%Y/month=%m/day=%d/")

            # set base filename and paths
            filename = f'daily-dev-{day_str}'
            json_path = f"{BASE_PATH}json/{date_path}"
            parquet_path = f"{BASE_PATH}parquet/{partition_path}"

            context.logger.info(
                f'download daily deviation for {day}')

            # dailydev
            call_params = {
                "date": day,
                "mature_content": "true"
            }

            ddev_data = api_call_get(DEVART_DAILY_URL, call_params)

            # write as gzipped json
            jsonio = io.BytesIO()
            with gzip.GzipFile(fileobj=jsonio, mode="wb") as gzio:
                gzio.write(json.dumps(
                    ddev_data, ensure_ascii=False).encode())

            # seek to start otherwise upload will be 0
            jsonio.seek(0)

            # upload
            json_key = f"{json_path}{filename}.json.gz"
            context.logger.debug(f'upload json to azure as {json_key}')
            blob_client = container_client.get_blob_client(json_key)
            wsize = upload_azure_blob(blob_client, jsonio)
            context.logger.debug(
                f'wrote {wsize} to azure as {json_key}')

            # cleanup
            del jsonio
            del blob_client

            # parse result
            ddev_list = {}
            for dev in ddev_data["results"]:
                ddev_list[dev['deviationid']] = dev

            # fetch ids
            id_list = [i for i in ddev_list.keys()]
            idf = pd.DataFrame(id_list, columns=["ids"])

            # write to azure as gzipped csv
            csvio = io.BytesIO()
            with gzip.GzipFile(fileobj=csvio, mode="wb") as gzio:
                gzio.write(idf.to_csv(header=True, index=False).encode())

            # seek to start otherwise upload will be 0
            csvio.seek(0)

            csv_key = f"{IDS_PATH}{date_path}{filename}-ids.csv.gz"
            context.logger.info(f'upload csv to azure as {csv_key}')
            blob_client = container_client.get_blob_client(csv_key)
            wsize = upload_azure_blob(blob_client, csvio)
            context.logger.debug(f'wrote {wsize} to azure as {csv_key}')

            # cleanup
            del csvio
            del blob_client

            # download metadata for every deviations
            context.logger.debug(
                f'download daily {day} metadata for {len(id_list)} entries')

            # limit is 50, reduced to 10 when using ext_stats
            limit = 10
            mdev_list = {}
            for id_dev in chunks(id_list, limit):
                mdev_params = {
                    "deviationids[]": id_dev,
                    "ext_stats": "true",
                    "mature_content": "true"
                }

                mdata_response = api_call_get(DEVART_METADATA_URL, mdev_params)
                for dev in mdata_response["metadata"]:
                    mdev_list[dev['deviationid']] = dev

                # rate limit
                time.sleep(SLEEP_AMOUNT)

            context.logger.debug(
                f'downloaded daily {day} metadata for {len(mdev_list)} entries')
            context.logger.debug(
                f'process daily {day} entries + metadata for {len(id_list)} entries')

            list_pandas = []
            for i in id_list:
                if i in ddev_list and i in mdev_list:
                    d = unpack_data(ddev_list[i], mdev_list[i])
                    list_pandas.append(pd.DataFrame.from_dict(d))

            context.logger.debug(
                f'unpacked daily {day} entries + metadata for {len(list_pandas)} entries')

            # daily dev + metadata
            ddf = pd.concat(list_pandas)
            ddf.reset_index(drop=True, inplace=True)

            context.logger.debug(
                f'result daily {day} entries + metadata for {len(ddf)} entries')

            # fix fields precision
            ddf['published_time'] = ddf['published_time'].astype(
                "datetime64[ms]")
            # also drop timezone, we need a naive timestamp and we care about day
            ddf["daily_time"] = ddf["daily_time"].dt.tz_localize(None)
            ddf['daily_time'] = ddf['daily_time'].astype(
                "datetime64[ms]")

            # dump
            parquetio = io.BytesIO()
            ddf.to_parquet(parquetio, engine='pyarrow')
            # seek to start otherwise upload will be 0
            parquetio.seek(0)

            # upload
            parquet_key = f"{parquet_path}{filename}.parquet"
            context.logger.info(f'upload df to azure as {parquet_key}')
            blob_client = container_client.get_blob_client(parquet_key)
            wsize = upload_azure_blob(blob_client, parquetio)
            context.logger.debug(f'wrote {wsize} to azure as {parquet_key}')

            # cleanup
            del parquetio
            del blob_client

            # write stats from meta
            stat_list = []
            for dev in mdev_list.values():
                d = unpack_stats(dev, today)
                stat_list.append(pd.DataFrame.from_dict(d))

            # stats dataframe
            sdf = pd.concat(stat_list)
            sdf.reset_index(drop=True, inplace=True)

            # fix fields precision
            sdf['stats_time'] = sdf['stats_time'].astype(
                "datetime64[ms]")

            # dump
            statsio = io.BytesIO()
            sdf.to_parquet(statsio, engine='pyarrow')
            # seek to start otherwise upload will be 0
            statsio.seek(0)

            # upload
            stats_path = today.strftime("year=%Y/month=%m/day=%d/")
            stats_key = f"{STATS_PATH}{stats_path}{filename}-stats.parquet"
            context.logger.info(f'upload stats to azure as {stats_key}')
            blob_client = container_client.get_blob_client(stats_key)
            wsize = upload_azure_blob(blob_client, statsio)
            context.logger.debug(f'wrote {wsize} to azure as {stats_key}')

            # cleanup
            del statsio
            del blob_client
            del idf
            del ddf
            del sdf

            # rate limit
            time.sleep(SLEEP_AMOUNT)

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
