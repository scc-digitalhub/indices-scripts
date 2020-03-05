import json
import os
import pandas as pd
import pyarrow
import requests
import time


from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session


CLIENT_ID = ""
CLIENT_SECRET = ""
URL_AUT = "https://www.deviantart.com/oauth2/authorize"
URL_TKN = "https://www.deviantart.com/oauth2/token"
SESSION = False

PATH_DDEV = "./dev_art/ddev_json/"
PATH_CSV_IDS = "./dev_art/ddev_csv_ids/"
PATH_PARQUET_DEV = "./dev_art/ddev_parquet/"

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

#Function to unpack and merge the daily deviation 
#response and the metadata response
def unpack_data(js):
    
    dd_js = js[0]
    md_js = js[1]
    d = {}
    d["deviationid"] = [dd_js["deviationid"]]
    d["printid"] = [dd_js["printid"]]
    d["published_time"] = [pd.to_datetime(dd_js["published_time"], unit="s", origin="unix")]
    d["day_of_daily_deviation"] = [pd.to_datetime(dd_js["daily_deviation"]["time"])]
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

#site aired "2000-08-07"
def api_call(endpoint,date=None,id_dev=None):

    global SESSION
    if not SESSION:
        SESSION=client_auth()

    if endpoint == "daily_dev":
        url = "https://www.deviantart.com/api/v1/oauth2/browse/dailydeviations"
        params = {"date":date,
                  "mature_content":"true"}

    elif endpoint == "metadata":
        url = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata/"
        params = {"deviationids[]":id_dev,
                  "mature_content":"true"}

    try:
        r = SESSION.get(url, params=params)
        data = r.json()
        return data

    except TokenExpiredError:
        SESSION = client_auth()
        return api_call(endpoint,date=date,id_dev=id_dev)
        
    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")
        pass

def download_data(start_date,end_date=None):
    
    if not end_date:
        end_date = start_date
    datelist = pd.date_range(start=start_date,end=end_date,freq="d").to_pydatetime().tolist()
    datelist = [(x.strftime("%Y-%m-%d"), x.strftime("year=%Y/month=%m/")) for x in datelist]

    for day, date_path in datelist:
        
        #dailydev
        data_ddev = api_call("daily_dev",date=day)
        
        #storeing daily dev json
        directory = f"{PATH_DDEV}{date_path}"
        filename_ddev = f"{directory}{day}.json"
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(filename_ddev, "w") as file:
            json.dump(data_ddev, file, ensure_ascii=False)
        
        #save ids file
        id_list = [dev["deviationid"] for dev in data_ddev["results"]]
        directory = f"{PATH_CSV_IDS}{date_path}"
        filename_id = f"{directory}{day}.csv"
        if not os.path.exists(directory):
            os.makedirs(directory)
        pd.DataFrame(id_list, columns=["ids"]).to_csv(filename_id, index=False)

        #download metadata for every deviations
        ddev_list = [dev for dev in data_ddev["results"]]
        limit = 50
        mdev_list = []
        for id_dev in chunks(id_list, limit):
            mdata_response = api_call("metadata",id_dev=id_dev)
            for dev in mdata_response["metadata"]:
                mdev_list.append(dev)

        #merge data and metadata and export to parquet
        ddev_list = sorted(ddev_list, key = lambda x: x["deviationid"])
        mdev_list = sorted(mdev_list, key = lambda x: x["deviationid"])
        coupled_json = [[i,j] for i,j in zip(ddev_list,mdev_list) if i["deviationid"]==j["deviationid"]]
        list_pandas = []
        for couple in coupled_json:
            d = unpack_data(couple)
            list_pandas.append(pd.DataFrame.from_dict(d))
        
        #store parquet divided by month
        directory = f"{PATH_PARQUET_DEV}{date_path}"
        filename_parquet = f"{directory}{day}.parquet"
        if not os.path.exists(directory):
            os.makedirs(directory)
        pd.concat(list_pandas).to_parquet(filename_parquet, index=False, engine="pyarrow")
        
        time.sleep(2)