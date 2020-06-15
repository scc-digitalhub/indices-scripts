import gzip
import io
import json
import os
import pandas as pd
import requests
import time

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session


AZURE_CONNECTION_STRING = os.environ.get("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER")

BASE_PATH = "devart/dailydev/"

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_DAILY_URL = "https://www.deviantart.com/api/v1/oauth2/browse/dailydeviations"
DEVART_METADATA_URL = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"

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

# Function to unpack and merge the daily deviation
# response and the metadata response

def unpack_data(js):

    dd_js = js[0] # dailydev response
    md_js = js[1] # metadata response
    
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
        context.logger.info("download datasets from daily devart")

        start_date = datetime.today().strftime("%Y-%m-%d")
        end_date = False
        msg = {}

        # params - expect json
        if(event.content_type == "application/json"):
            msg = event.body
        else:
            jsstring = event.body.decode("utf-8").strip()
            if jsstring:
                msg = json.loads(jsstring)

        context.logger.info(msg)

        # fetch params
        if "start_date" in msg:
            start_date = datetime.strptime(msg["start_date"], "%Y-%m-%d")

        if "end_date" in msg:
            end_date = datetime.strptime(msg["end_date"], "%Y-%m-%d")

        # init blob client
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER)

        if not end_date:
            end_date = start_date
        daterange = pd.date_range(start=start_date, end=end_date, freq="d")
        datelist = daterange.to_pydatetime().tolist()

        for cur_date in datelist:
            day = cur_date.strftime("%Y-%m-%d")
            date_path = cur_date.strftime("year=%Y/month=%m/")
            ddf_filename = "daily-dev-{}".format(day)

            context.logger.info("download for "+day)

            # dailydev
            ddev_params = {
                "date": day,
                "mature_content": "true"
            }


            # api call
            ddev_data = api_call_get(DEVART_DAILY_URL, ddev_params)

            # STORE JSON
            # write to azure as BytesIO json
            jsonio = io.BytesIO()

            # write as gzip
            with gzip.GzipFile(fileobj=jsonio, mode="wb") as gzio:
                gzio.write(json.dumps(ddev_data, ensure_ascii=False).encode())

            # check cursor position over buffer
            if jsonio.tell() > 0:
                # seek to start otherwise upload will be 0
                jsonio.seek(0)

            context.logger.info("upload to azure as "+ddf_filename+".json")
            json_key = BASE_PATH + "json/"+date_path+ddf_filename+".json"

            # get blob client for json and upload resources
            blob_client_json = container_client.get_blob_client(json_key)
            blob_client_json.upload_blob(jsonio)

            # close jsonio
            jsonio.close()


            # STORE CSV
            # fetch ids
            id_list = [dev["deviationid"] for dev in ddev_data["results"]]
            idf = pd.DataFrame(id_list, columns=["ids"])

            # write to S3 as csv
            csvio = io.StringIO()
            idf.to_csv(csvio, header=True, index=False)

            # seek to start otherwise upload will be 0
            if csvio.tell() > 0:
                csvio.seek(0)

            # convert stringio to bytesio
            csvio = io.BytesIO(csvio.read().encode("utf8"))
            if csvio.tell() > 0:
                csvio.seek(0)

            context.logger.info("upload to azure as "+ddf_filename+".csv")
            csv_key = BASE_PATH + "csv/"+date_path+ddf_filename+"-ids.csv"

            # get blob client for csv and upload resources
            blob_client_csv = container_client.get_blob_client(csv_key)
            blob_client_csv.upload_blob(csvio)

            # close csvio
            csvio.close()
            

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


            # STORE PARQUET
            parquetio = io.BytesIO()
            ddf.to_parquet(parquetio, engine="pyarrow")
            
            # seek to start otherwise upload will be 0
            if parquetio.tell() > 0:
                parquetio.seek(0)

            context.logger.info("upload to azure as "+ddf_filename+".parquet")
            parq_key = BASE_PATH + "daily/"+date_path+ddf_filename+".parquet"
            
            # get blob client for parquet and upload resources
            blob_client_parq = container_client.get_blob_client(parq_key)
            blob_client_parq.upload_blob(parquetio)
            
            parquetio.close()

            # cleanup
            del blob_client_json
            del blob_client_parq
            del blob_client_csv
            del parquetio
            del csvio
            del jsonio
            del ddf

            # rate limit
            time.sleep(2)


        context.logger.error("Done")
        return context.Response(body="Done",
                                headers={},
                                content_type="text/plain",
                                status_code=200)

    except Exception as e:
        context.logger.error("Error: "+str(e))
        return context.Response(body="Error "+str(e),
                                headers={},
                                content_type="text/plain",
                                status_code=500)
