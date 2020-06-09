import ast
import pandas as pd
import json
import os
import requests
import time

from datetime import datetime
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session


CLIENT_ID = ""
CLIENT_SECRET = ""

BASE_PATH = 'devart/popular/'
PATH_CSV_CATEGORY = "devart/popular/categorylist/"

OAUTH_AUTHORIZE_URL = "https://www.deviantart.com/oauth2/authorize"
OAUTH_TOKEN_URL = "https://www.deviantart.com/oauth2/token"

DEVART_POULAR_URL = "https://www.deviantart.com/api/v1/oauth2/browse/popular"
DEVART_METADATA_URL = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"

SESSION = False


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


def api_call_get(url,params):

    global SESSION
    if not SESSION:
        SESSION=client_auth()

    try:
        r = SESSION.get(url, params=params)
        data = r.json()
        return data
    except TokenExpiredError:
        SESSION = client_auth()
        return api_call_get(url,params)
    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")
        pass


def download_data(timerange=None):

        today = datetime.today()
        date_path = today.strftime("year=%Y/month=%m/day=%d/")\

        if not timerange:
            timerange = '24hr'

        #just an example of category list instead of read csv path
        category_list = ["/anthro","/anthro/digital"]
        pdev_list = []

        #set base filename and storing directories
        if timerange == '24hr':

            filename = f'popular-{timerange}-{today.strftime("%Y-%m-%d")}'
            csv_path = f"{BASE_PATH}{timerange}/csv/{date_path}"
            json_path = f"{BASE_PATH}{timerange}/json/{date_path}"
            parquet_path = f"{BASE_PATH}{timerange}/parquet/{date_path}"

        elif timerange == 'alltime':
            #TODO
            pass

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

                data = api_call_get(DEVART_POULAR_URL,params_pop)

                len_array = len(data["results"])
                has_more = data["has_more"]

                #if the response doesn't contain data the json is not saved
                if len_array>0:

                    pdev_list.extend([i for i in data["results"]])

                    #storing json file
                    json_key = f"{json_path}{filename}-{cat_name}-{offset}.json"

                    if not os.path.exists(json_path):
                        os.makedirs(json_path)

                    with open(json_key, "w") as file:
                        json.dump(data, file, ensure_ascii=False)

                    offset += 24

                time.sleep(1)


        #remove duplicated deviations
        pdev_list = set([str(x) for x in pdev_list])
        pdev_list = [ast.literal_eval(i) for i in pdev_list]


        # fetch and store ids of popular deviation
        id_list = [dev["deviationid"] for dev in pdev_list]
        idf = pd.DataFrame(id_list, columns=["ids"])

        csv_key = f"{csv_path}{filename}.csv"

        if not os.path.exists(csv_path):
            os.makedirs(csv_path)

        idf.to_csv(csv_key,index=False)


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
        ddf = pd.concat(list_pandas)

        parquet_key = f"{parquet_path}{filename}.parquet"

        if not os.path.exists(parquet_path):
            os.makedirs(parquet_path)

        ddf.to_parquet(parquet_key, index=False, engine="pyarrow")

        # rate limit
        time.sleep(2)