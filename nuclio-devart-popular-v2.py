import ast
import io
import json
import os
import pandas as pd
import requests
import time


from datetime import datetime
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
AZURE_CONTAINER = os.environ.get('AZURE_CONTAINER')
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
DEVART_POPULAR_TIMERANGE = os.environ.get('DEVART_POPULAR_TIMERANGE')
DEVART_POPULAR_CATEGORIES = [e.strip() for e in os.environ.get(
    'DEVART_POPULAR_CATEGORIES').split(',')]

BASE_PATH = 'popular/'
STATS_PATH = 'stats/'
IDS_PATH = 'ids/'

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_POPULAR_URL = "https://www.deviantart.com/api/v1/oauth2/browse/popular"
DEVART_METADATA_URL = "https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"

DEVART_VALID_TIMERANGE = ['24hr', '3days', '1week', '1month', 'alltime']

PAGE_SIZE = 24
MAX_OFFSET = 5000
SLEEP_AMOUNT = 0.5
SESSION = False

# max working is 64MB
AZURE_CHUNK_SIZE = 64 * 1024 * 1024


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


def unpack_data(js, today):

    dd_js = js[0]
    md_js = js[1]
    d = {}
    d["deviationid"] = [dd_js["deviationid"]]
    d["printid"] = [dd_js["printid"]]
    d["published_time"] = [pd.to_datetime(
        dd_js["published_time"], unit="s", origin="unix")]
    d["popular_category"] = [dd_js["popular_category"]]
    d["popular_time"] = [pd.to_datetime(today)]
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

        today = datetime.today()
        today_str = today.strftime("%Y%m%d")
        date_path = today.strftime("%Y/%m/%d/")
        partition_path = today.strftime("year=%Y/month=%m/day=%d/")

        # call params from default
        category_list = DEVART_POPULAR_CATEGORIES
        timerange = DEVART_POPULAR_TIMERANGE

        msg = {}
        # params - expect json
        if(event.content_type == 'application/json'):
            msg = event.body
        else:
            jsstring = event.body.decode('utf-8').strip()
            if jsstring:
                msg = json.loads(jsstring)

        # fetch params
        if 'timerange' in msg:
            timerange = msg['timerange']

        if 'categories' in msg:
            category_list = [c for c in msg['categories']]

        if timerange not in DEVART_VALID_TIMERANGE:
            raise Exception("Invalid timerange")

        # set base filename and paths
        filename = f'popular-dev-{timerange}-{today_str}'
        json_path = f"{BASE_PATH}{timerange}/json/{date_path}"
        parquet_path = f"{BASE_PATH}{timerange}/parquet/{partition_path}"

        # init blob client
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(
            AZURE_CONTAINER)

        context.logger.info(
            f'download popular deviation for range {timerange}')

        # store ids,meta for subsequent requests
        id_list = []
        meta_list = {}

        # iterate over categories
        for cat in category_list:
            cat_name = cat.lstrip("/").replace("/", "_")
            context.logger.info(
                f'download popular {cat_name} for range {timerange}')

            pcat_list = []

            # pagination
            limit = PAGE_SIZE
            offset = 0
            has_more = True
            len_array = 1

            context.logger.debug(
                f'download popular {cat_name} json responses')

            # cycle over offset until there's no new results up to max
            while has_more == True and len_array > 0 and offset < MAX_OFFSET:

                call_params = {
                    "category_path": cat,
                    "timerange": timerange,
                    "offset": offset,
                    "limit": limit,
                    "mature_content": "true"}

                # call for page - rate limited
                pdev_data = api_call_get(DEVART_POPULAR_URL, call_params)

                len_array = len(pdev_data["results"])
                has_more = pdev_data["has_more"]

                # if the response doesn't contain data the json is not saved
                if len_array > 0:

                    pcat_list.extend([i for i in pdev_data["results"]])

                    # write as StringIO json
                    jsonio = io.StringIO()
                    json.dump(pdev_data, jsonio, ensure_ascii=False)
                    # seek to start otherwise upload will be 0
                    jsonio.seek(0)

                    # wrap as byte with reader
                    wrapjsonio = BytesIOWrapper(jsonio)

                    # upload
                    fok = format(offset, '05d')
                    json_key = f"{json_path}{cat_name}/{filename}-{cat_name}-{fok}.json"
                    context.logger.debug(f'upload json to azure as {json_key}')
                    blob_client = container_client.get_blob_client(json_key)
                    wsize = upload_azure_blob(blob_client, wrapjsonio)
                    context.logger.debug(
                        f'wrote {wsize} to azure as {json_key}')

                    # cleanup
                    del wrapjsonio
                    del jsonio
                    del blob_client

                    offset += PAGE_SIZE

                # rate limit
                time.sleep(SLEEP_AMOUNT)

            # add request category to results
            for pc in pcat_list:
                pc['popular_category'] = cat_name

            # remove duplicated deviations - not needed in single cat
            #pcat_list = set([str(x) for x in pcat_list])
            #pcat_list = [ast.literal_eval(i) for i in pcat_list]

            idcat_list = [dev["deviationid"] for dev in pcat_list]
            id_list.extend(idcat_list)

            # download metadata for every deviations
            context.logger.debug(
                f'download popular {cat_name} metadata for {len(idcat_list)} entries')

            # limit is 50, rediced to 10 when using ext_stats
            limit = 10
            mdev_list = []
            for id_dev in chunks(idcat_list, limit):
                mdev_params = {
                    "deviationids[]": id_dev,
                    "ext_stats": "true",
                    "mature_content": "true"
                }

                mdata_response = api_call_get(DEVART_METADATA_URL, mdev_params)
                for dev in mdata_response["metadata"]:
                    mdev_list.append(dev)

                # rate limit
                time.sleep(SLEEP_AMOUNT)

            context.logger.debug(
                f'process popular {cat_name} entries + metadata for {len(idcat_list)} entries')

            # merge data and metadata and export to parquet
            pcat_list = sorted(pcat_list, key=lambda x: x["deviationid"])
            mdev_list = sorted(mdev_list, key=lambda x: x["deviationid"])
            for m in mdev_list:
                meta_list[m['deviationid']] = m

            coupled_json = [[i, j] for i, j in zip(
                pcat_list, mdev_list) if i["deviationid"] == j["deviationid"]]

            list_pandas = []
            for couple in coupled_json:
                d = unpack_data(couple, today)
                list_pandas.append(pd.DataFrame.from_dict(d))

            # popular dev + metadata
            pdf = pd.concat(list_pandas)
            pdf.reset_index(drop=True, inplace=True)

            # fix fields precision
            pdf['published_time'] = pdf['published_time'].astype(
                "datetime64[ms]")
            pdf['popular_time'] = pdf['popular_time'].astype("datetime64[ms]")

            # dump
            parquetio = io.BytesIO()
            pdf.to_parquet(parquetio, engine='pyarrow')
            # seek to start otherwise upload will be 0
            parquetio.seek(0)

            # upload
            parquet_key = f"{parquet_path}cat={cat_name}/{filename}-{cat_name}.parquet"
            context.logger.info(f'upload df to azure as {parquet_key}')
            blob_client = container_client.get_blob_client(parquet_key)
            wsize = upload_azure_blob(blob_client, parquetio)
            context.logger.debug(f'wrote {wsize} to azure as {parquet_key}')

            # cleanup
            del parquetio
            del pdf
            del blob_client
            del pcat_list
            del mdev_list
            del coupled_json
            # end category

        # fetch and store ids of popular deviation
        id_list = [x for x in set(id_list)]
        idf = pd.DataFrame(id_list, columns=["ids"])

        # write to azure as csv
        csvio = io.StringIO()
        idf.to_csv(csvio, header=True, index=False)
        # seek to start otherwise upload will be 0
        csvio.seek(0)

        # wrap as byte with reader
        wrapcsvio = BytesIOWrapper(csvio)

        csv_key = f"{IDS_PATH}{date_path}{filename}-ids.csv"
        context.logger.info(f'upload csv to azure as {csv_key}')
        blob_client = container_client.get_blob_client(csv_key)
        wsize = upload_azure_blob(blob_client, wrapcsvio)
        context.logger.debug(f'wrote {wsize} to azure as {csv_key}')

        # write stats from meta
        stat_list = []
        for dev in meta_list.values():
            d = unpack_stats(dev, today)
            stat_list.append(pd.DataFrame.from_dict(d))

        # daily dev stats dataframe
        ddf = pd.concat(stat_list)
        ddf.reset_index(drop=True, inplace=True)

        # fix fields precision
        ddf['stats_time'] = ddf['stats_time'].astype(
            "datetime64[ms]")

        # dump
        statsio = io.BytesIO()
        ddf.to_parquet(statsio, engine='pyarrow')
        # seek to start otherwise upload will be 0
        statsio.seek(0)

        # upload
        stats_key = f"{STATS_PATH}{partition_path}{filename}-stats.parquet"
        context.logger.info(f'upload stats to azure as {stats_key}')
        blob_client = container_client.get_blob_client(stats_key)
        wsize = upload_azure_blob(blob_client, statsio)
        context.logger.debug(f'wrote {wsize} to azure as {stats_key}')

        # cleanup
        del wrapcsvio
        del csvio
        del statsio
        del ddf
        del idf

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
