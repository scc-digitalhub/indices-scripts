import pandas as pd
import io
import json
import os
import requests
import time
import gzip


from datetime import datetime, timedelta
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
AZURE_CONTAINER = os.environ.get('AZURE_CONTAINER')
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
DEVART_STATS_INTERVAL = os.environ.get('DEVART_STATS_INTERVAL')


BASE_PATH = 'stats/'
IDS_PATH = 'ids/'
IDS_SUFFIX = '-ids.csv.gz'

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_METADATA_URL = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"

SLEEP_AMOUNT = 0.5
SESSION = False

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

        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        today_path = today.strftime("year=%Y/month=%m/day=%d/")

        context.logger.info(f'download metadata stats of {today_str}')

        interval = int(DEVART_STATS_INTERVAL)
        delta = timedelta(days=interval)
        start_date = today - delta
        end_date = today - timedelta(days=1)

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

        # derive days list and iterate
        daterange = pd.date_range(start=start_date, end=end_date, freq="d")
        datelist = daterange.to_pydatetime().tolist()

        context.logger.info(
            f"retrieve metadata for deviation from {start_date} to {end_date}")

        # init blob client
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(
            AZURE_CONTAINER)

        # iterate over days
        for cur_date in datelist:
            day = cur_date.strftime("%Y-%m-%d")
            date_path = cur_date.strftime("%Y/%m/%d/")

            context.logger.debug(f"process ids for {day}")

            # read ids
            csv_list = []
            ids_prefix = f"{IDS_PATH}{date_path}"
            blobs_list = container_client.list_blobs(
                name_starts_with=ids_prefix)
            for blob in blobs_list:
                if blob.name.endswith(IDS_SUFFIX):
                    csv_list.append(blob.name)
            # iterate  id lists
            for csv_key in csv_list:
                context.logger.debug(f"read ids from {csv_key} for {day}")
                # strip suffix and extract name
                filename = os.path.basename(csv_key[:-(len(IDS_SUFFIX))])
                # read from azure
                blob_client = container_client.get_blob_client(csv_key)
                csvio = io.BytesIO()
                csvio.write(blob_client.download_blob().readall())
                csvio.seek(0)
                idf = pd.read_csv(csvio, compression='gzip')

                # cleanup
                del csvio

                id_list = idf['ids'].values.tolist()

                context.logger.debug(
                    f'download {today_str} metadata for {len(id_list)} entries')

                # limit is 50, reduced to 10 when using ext_stats
                limit = 10
                mdev_list = {}
                for id_dev in chunks(id_list, limit):
                    mdev_params = {
                        "deviationids[]": id_dev,
                        "ext_stats": "true",
                        "mature_content": "true"
                    }

                    mdata_response = api_call_get(
                        DEVART_METADATA_URL, mdev_params)
                    for dev in mdata_response["metadata"]:
                        mdev_list[dev['deviationid']] = dev

                    # rate limit
                    time.sleep(SLEEP_AMOUNT)

                context.logger.debug(
                    f'downloaded {today_str} metadata for {len(mdev_list)} entries')

                stat_list = []
                for i in id_list:
                    if i in mdev_list:
                        d = unpack_stats(mdev_list[i], today)
                        stat_list.append(pd.DataFrame.from_dict(d))

                context.logger.debug(
                    f'unpacked {today_str} metadata for {len(stat_list)} entries')

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
                stats_key = f"{BASE_PATH}{today_path}{filename}-stats.parquet"
                context.logger.info(f'upload stats to azure as {stats_key}')
                blob_client = container_client.get_blob_client(stats_key)
                wsize = upload_azure_blob(blob_client, statsio)
                context.logger.debug(f'wrote {wsize} to azure as {stats_key}')

                # cleanup
                del statsio
                del sdf
                del blob_client
                del stat_list
                del mdev_list
                del id_list

                # rate limit
                time.sleep(SLEEP_AMOUNT)

            # done day

        # done days
        del container_client
        del blob_service_client

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
