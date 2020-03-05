import json
import os
import pandas as pd
import pyarrow
import requests
import time


from datetime import datetime
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session


CLIENT_ID = ""
CLIENT_SECRET = ""
URL_AUT = "https://www.deviantart.com/oauth2/authorize"
URL_TKN = "https://www.deviantart.com/oauth2/token"
SESSION = False

TODAY = datetime.today()
DAILY_PATH = TODAY.strftime("year=%Y/month=%m/day=%d/")

PATH_CSV_IDS = "./dev_art/ddev_csv_ids/"
PATH_PARQUET_STAT = "./dev_art/ddev_stat_parquet/"

#client credentials
def client_auth():

    client = BackendApplicationClient(client_id=CLIENT_ID)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=URL_TKN,
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

#metadata API call
def metadata(id_dev):

    global SESSION
    if not SESSION:
        SESSION=client_auth()

    url = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"
    params = {}
    if isinstance(id_dev, list):
        params["deviationids[]"]=id_dev
    else:
        params["deviationids[]"]=[id_dev]
    params["ext_stats"]="true"
    params["mature_content"] = "true"
    try:
        r = SESSION.get(url, params=params)
        data = r.json()
        return data
    except TokenExpiredError:
        SESSION = client_auth()
        return metadata(id_dev)
    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")
        pass

#metadata stats download function
def download_stats(start,end=None):

    if not end:
        end = start
    datelist = pd.date_range(start=start,end=end,freq="d").to_pydatetime().tolist()
    datelist = [(x.strftime("%Y-%m-%d"), x.strftime("year=%Y/month=%m/")) for x in datelist]

    for day, date_path in datelist:
        #read id list
        try:
            id_list = pd.read_csv(f"{PATH_CSV_IDS}{date_path}{day}.csv").values.tolist()
            limit = 10
            stat_list = []
            for id_dev in chunks(id_list, limit):
                mdata_response = metadata(id_dev)
                for dev in mdata_response['metadata']:
                    d = unpack_stats(dev)
                    stat_list.append(pd.DataFrame.from_dict(d))
                
                        
            directory = f"{PATH_PARQUET_STAT}{DAILY_PATH}"
            filename = f"{directory}Stats_DDEV_{day}.parquet"
            if not os.path.exists(directory):
                os.makedirs(directory)
            df = pd.concat(stat_list)
            df.to_parquet(filename, index=False, engine='pyarrow')
        
            time.sleep(2)
        
        except Exception as ex:
            print(f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")
            pass