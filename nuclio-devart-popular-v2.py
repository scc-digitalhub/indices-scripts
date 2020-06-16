import ast
import io
import json
import os
import pandas as pd
import requests
import time
import gzip


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

DUMP_JSON = True
PAGE_SIZE = 24
MAX_OFFSET = 5000
MAX_RETRIES = 3
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


def unpack_data(dd_js, md_js, today):
    if dd_js["deviationid"] != md_js["deviationid"]:
        raise Exception("data mismatch") 

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

            pcat_list = {}

            # pagination
            limit = PAGE_SIZE
            offset = 0
            has_more = True
            len_array = 1
            retries = 0
            estimates = 0    
            can_write = False        

            context.logger.debug(
                f'download popular {cat_name} json responses')

            # cycle over offset until there's no new results up to max
            while has_more == True and len_array > 0 and offset < MAX_OFFSET and retries < MAX_RETRIES:

                context.logger.debug(f'call API for popular {cat_name} offset {offset} retry {retries}')

                call_params = {
                    "category_path": cat,
                    "timerange": timerange,
                    "offset": offset,
                    "limit": limit,
                    "mature_content": "true"}

                # call for page - rate limited
                pdev_data = api_call_get(DEVART_POPULAR_URL, call_params)

                estimates = int(pdev_data['estimated_total'])
                len_array = len(pdev_data["results"])
                has_more = pdev_data["has_more"]

                if len_array == PAGE_SIZE:
                    # complete page
                    for i in pdev_data["results"]:
                        pcat_list[i['deviationid']] = i
                    offset += PAGE_SIZE
                    can_write = True
                    
                elif len_array < PAGE_SIZE and offset < (estimates - PAGE_SIZE):
                    # truncated page, not last, retry
                    retries += 1
                    can_write = False

                    if retries == MAX_RETRIES:
                        # accept truncated and correct offset
                        for i in pdev_data["results"]:
                            pcat_list[i['deviationid']] = i
                        offset += len_array
                        retries = 0
                        can_write = True
                    
                elif len_array < PAGE_SIZE and offset >= (estimates - PAGE_SIZE):
                    # last page
                    for i in pdev_data["results"]:
                        pcat_list[i['deviationid']] = i
                    offset += len_array   
                    can_write = True 


                if can_write and DUMP_JSON:
                    # write as gzipped json
                    jsonio = io.BytesIO()
                    with gzip.GzipFile(fileobj=jsonio, mode="wb") as gzio:
                        gzio.write(json.dumps(
                            pdev_data, ensure_ascii=False).encode())

                    # seek to start otherwise upload will be 0
                    jsonio.seek(0)

                    # upload
                    fok = format(offset, '05d')
                    json_key = f"{json_path}{cat_name}/{filename}-{cat_name}-{fok}.json.gz"
                    context.logger.debug(f'upload json to azure as {json_key}')
                    blob_client = container_client.get_blob_client(json_key)
                    wsize = upload_azure_blob(blob_client, jsonio)
                    context.logger.debug(
                        f'wrote {wsize} to azure as {json_key}')

                    # cleanup
                    del jsonio
                    del blob_client                    


                del pdev_data

                # rate limit
                time.sleep(SLEEP_AMOUNT)

            # add request category to results
            for pc in pcat_list.values():
                pc['popular_category'] = cat_name


            idcat_list = [i for i in pcat_list.keys()]
            id_list.extend(idcat_list)

            # download metadata for every deviations
            context.logger.debug(
                f'download popular {cat_name} metadata for {len(idcat_list)} entries')

            # limit is 50, reduced to 10 when using ext_stats
            limit = 10
            mdev_list = {}
            for id_dev in chunks(idcat_list, limit):
                mdev_params = {
                    "deviationids[]": id_dev,
                    "ext_stats": "true",
                    "mature_content": "true"
                }

                mdata_response = api_call_get(DEVART_METADATA_URL, mdev_params)
                for dev in mdata_response["metadata"]:
                    mdev_list[dev['deviationid']] = dev
                    meta_list[dev['deviationid']] = dev

                # rate limit
                time.sleep(SLEEP_AMOUNT)

            context.logger.debug(
                f'downloaded popular {cat_name} metadata for {len(mdev_list)} entries')
            context.logger.debug(
                f'process popular {cat_name} entries + metadata for {len(pcat_list)} entries')

            list_pandas = []
            for i in idcat_list:
                if i in pcat_list and i in mdev_list:
                    d = unpack_data(pcat_list[i], mdev_list[i], today)                
                    list_pandas.append(pd.DataFrame.from_dict(d))

            context.logger.debug(
                f'unpacked popular {cat_name} entries + metadata for {len(list_pandas)} entries')

            # popular dev + metadata
            pdf = pd.concat(list_pandas)
            pdf.reset_index(drop=True, inplace=True)

            context.logger.debug(
                f'result popular {cat_name} entries + metadata for {len(pdf)} entries')

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
            del idcat_list
            del pcat_list
            del mdev_list
            # end category

        # fetch and store ids of popular deviation
        id_list = [x for x in set(id_list)]
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

        # write stats from meta
        stat_list = []
        for dev in meta_list.values():
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
        stats_key = f"{STATS_PATH}{partition_path}{filename}-stats.parquet"
        context.logger.info(f'upload stats to azure as {stats_key}')
        blob_client = container_client.get_blob_client(stats_key)
        wsize = upload_azure_blob(blob_client, statsio)
        context.logger.debug(f'wrote {wsize} to azure as {stats_key}')

        # cleanup
        del csvio
        del statsio
        del sdf
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
