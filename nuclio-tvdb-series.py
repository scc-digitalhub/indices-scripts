import boto3
import botocore
import gzip
import io
import json
import numpy as np
import os
import pandas as pd
import requests
import time


from botocore.client import Config
from datetime import datetime, timedelta
from requests.exceptions import ReadTimeout


S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
S3_BUCKET = os.environ.get("S3_BUCKET")

BASE_URL = "https://api.thetvdb.com"
LOGIN_URL = f"{BASE_URL}/login"

PATH = "tvdb/series/"
PATH_JSON_ID = f"{PATH}id/"
PATH_JSON_EP = f"{PATH}episodes/"
PATH_CSV = f"{PATH}csv/"
PATH_PARQUET = f"{PATH}parquet/"

RANGE = 100

TOKEN = None
APIKEY = os.environ.get("APIKEY")
USERKEY = os.environ.get("USERKEY")
USERNAME = os.environ.get("USERNAME")
LOG_PARAM = {
    "apikey": APIKEY,
    "userkey": USERKEY,
    "username": USERNAME
    }

def authentication():
    """
    Function that return an header with an autentication token
    """

    try:

        r = requests.post(LOGIN_URL, json=LOG_PARAM)
        r_data = r.json()
        authToken = r_data["token"]
        
        return {"Authorization": "Bearer "+authToken}

    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")


def api_call_get(url, params=None, timeout=None):

    
    global TOKEN
    if not TOKEN:
        TOKEN = authentication()
    
    try:
        r = requests.get(url, headers=TOKEN, params=params, timeout=timeout)
        data = r.json()
              
        if "Error" in data:
            if data["Error"]=="Not authorized":
                TOKEN = authentication()
                api_call_get(url, params=params, timeout=timeout)
            elif data["Error"]=="No results for your query" or data["Error"]=="Resource not found":
                return "Empty"
        else:
            return data
    
    except ReadTimeout:
        return "Timeout"

    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. \nArguments:\n{ex.args}")

def unpack_js(js, timestamp):
    
    data = js["data"]

    d = {
        "id":[data["id"]],
        "seriesId":[data["seriesId"]],
        "seriesName":[data["seriesName"]],
        "aliases":[[i for i in data["aliases"]]],
        "season":[data["season"]],
        "status":[data["status"]],
        "firstAired":[pd.to_datetime(data["firstAired"])],
        "network":[data["network"]],
        "networkId":[data["networkId"]],
        "runtime":[data["runtime"]],
        "language":[data["language"]],
        "genre":[[j for j in data["genre"]]],
        "lastUpdated":[data["lastUpdated"]],
        "airsDayOfWeek":[data["airsDayOfWeek"]],
        "airsTime":[data["airsTime"]],
        "rating":[data["rating"]],
        "imdbId":[data["imdbId"]],
        "added":[pd.to_datetime(data["added"])],
        "siteRating":[data["siteRating"]],
        "siteRatingCount":[data["siteRatingCount"]],
        "slug":[data["slug"]],
        "scrapedate":[pd.to_datetime(timestamp, utc=True)]
        }

    for key in d:
        if d[key] == []:
            d[key].append(np.nan)

    return d

def process_image_number(js):

    data = js["data"]

    d = {"fanart": [],
        "poster": [], 
        "season": [], 
        "seasonwide": [], 
        "series": []}

    for k in d.keys():
        if k in data:
            d[k].append(data[k])
        else:
            d[k].append(0)
    return d

def handler(context, event):

    try:

        today = datetime.today()
        today_str = today.strftime("%Y%m%d")
        date_path = today.strftime("year=%Y/month=%m/day=%d/")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # init s3 client
        s3 = boto3.client("s3",
                            endpoint_url=S3_ENDPOINT,
                            aws_access_key_id=S3_ACCESS_KEY,
                            aws_secret_access_key=S3_SECRET_KEY,
                            config=Config(signature_version="s3v4"),
                            region_name="us-east-1")

        #fetch serie_list
        #reading csv from minio - the path/filename must be rewritten
        key = f"{PATH}id_series.csv"
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        dataio = io.BytesIO(obj["Body"].read())
        df_id = pd.DataFrame()
        context.logger.info(f"read {key} into pandas dataframe")
        df_id = pd.read_csv(dataio)
        id_list = df_id["ser_id"].values.tolist()

        strt, c = 0, 0
        series_list = []
        for title_id in id_list:
            context.logger.info(f"Call the API for series {str(title_id)}")
            try:
                #API urls (for series metadata, number of images, episodes)
                url_id = f"{BASE_URL}/series/{str(title_id)}"
                url_im = f"{BASE_URL}/series/{str(title_id)}/images" 
                url_ep = f"{BASE_URL}/series/{str(title_id)}/episodes"
                
                #first section: downloads tv series metadata
                js = api_call_get(url_id, timeout=10)
                
                if js not in [None,"Empty","Timeout"]:
                    filename_id = f"series-{str(title_id)}-{today_str}.json.gz"
                    jsonio_id = io.BytesIO()                 
                    with gzip.GzipFile(fileobj=jsonio_id, mode="wb") as gzio:
                        gzio.write(json.dumps(js, ensure_ascii=False).encode())
                    jsonio_id.seek(0)
                    key_id = f"{PATH_JSON_ID}{date_path}{filename_id}"
                    context.logger.info(f"upload to s3 as {key_id}")
                    s3.upload_fileobj(jsonio_id, S3_BUCKET, key_id)
                    jsonio_id.close()

                    #unpack JSON
                    upk_data = unpack_js(js,timestamp)
                    df = pd.DataFrame(upk_data)

                    #second section: download images numbers   
                    context.logger.info(f"Call {url_im}")
                    js_img = api_call_get(url_im, timeout=10)
                    
                    if js_img not in [None,"Empty","Timeout"]:
                        context.logger.info(f"Process images number json for {str(title_id)}")
                        num = process_image_number(js_img)
                        df_num = pd.DataFrame(num)
                    else:      
                        df_num = pd.DataFrame({"fanart":[0],"poster":[0],"season":[0],"seasonwide":[0],"series": [0]})

                    df_num.rename(columns={"fanart":"img_fanart",
                                        "poster":"img_poster",
                                        "season":"img_season",
                                        "seasonwide":"img_seasonwide",
                                        "series":"img_series"},
                                inplace=True)
                                    

                    #Include the numbers in the dataframe
                    df = pd.concat([df,df_num], axis=1)

                    #third section: downloads episodes metadata           
                    page, last_page = 1, 1
                    c_ep = 0
                    dict_ep = {"data":[]}

                    #Loop over paginated response
                    while page <= last_page:
                        
                        context.logger.info(f"Call {url_ep} for page {page}")
                        
                        params = {"page":page}
                        js_ep = api_call_get(url_ep, params=params, timeout=10)
                        
                        if js_ep not in [None,"Empty","Timeout"]:
                            dict_ep["data"].extend(js_ep["data"])
                            c_ep += len(js_ep["data"])
                            
                            if js_ep["links"]["last"]>1:
                                last_page = js_ep["links"]["last"]

                        page+=1
                    
                        time.sleep(1)
                    
                    #Store a unique episode"s JSON for every series
                    if dict_ep["data"]!=[]:
                        filename_ep = f"series-episodes-{str(title_id)}-{today_str}.json.gz"
                        # write to S3 as bytesIO json
                        jsonio_ep = io.BytesIO()                 
                        with gzip.GzipFile(fileobj=jsonio_ep, mode="wb") as gzio:
                            gzio.write(json.dumps(dict_ep, ensure_ascii=False).encode())
                        jsonio_ep.seek(0)

                        key_ep = f"{PATH_JSON_EP}{date_path}{filename_ep}"
                        context.logger.info(f"upload to s3 as {key_ep}")
                        s3.upload_fileobj(jsonio_ep, S3_BUCKET, key_ep)
                        jsonio_ep.close()
                        del jsonio_ep
                        del dict_ep 
                    
                    #store the total numbers of episode for series in the dataframe
                    df["num_episodes"] = c_ep
                    
                    c+=1
                    series_list.append(df)

                    #Store chunk parquet
                    if (c == RANGE) or (0 < c < RANGE and title_id == id_list[-1]):
        
                        pdf = pd.concat(series_list)
                        pdf.reset_index(inplace=True, drop=True)

                        # store chunk parquet to S3
                        parquetio = io.BytesIO()
                        df.to_parquet(parquetio, engine="pyarrow")
                        # seek to start otherwise upload will be 0
                        parquetio.seek(0)

                        filename_parq = f"series-{strt+1}-{strt+c}"
                        key_parquet = f"{PATH_PARQUET}{date_path}{filename_parq}.parquet"
                        context.logger.info(f"upload to s3 as {key_parquet}")
                        s3.upload_fileobj(parquetio, S3_BUCKET, key_parquet)
                        parquetio.close()

                        series_list = []
                        c = 0
                        strt+=RANGE

                        del parquetio
                        del pdf 

                    # cleanup
                    del jsonio_id   
            
            #catch
            except Exception as e:
                context.logger.error("Error: "+str(e))

        # cleanup
        del dataio
        del df_id

        context.logger.info("Done")
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