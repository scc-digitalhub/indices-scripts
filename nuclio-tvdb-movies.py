import boto3
import botocore
import io
import json
import numpy as np
import os
import pandas as pd
import requests
import time
import gzip


from botocore.client import Config
from datetime import datetime
from requests.exceptions import ReadTimeout



S3_ENDPOINT = os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET')

BASE_URL = "https://api.thetvdb.com"
LOGIN_URL = f"{BASE_URL}/login"


PATH = "tvdb/movies/"
PATH_JSON = f"{PATH}json/"
PATH_PARQUET = f"{PATH}parquet/"
PATH_CSV = f"{PATH}csv/"

RANGE = 500

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
        authToken = r_data['token']
        
        return {'Authorization': 'Bearer '+authToken}

    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")



def unpack_data(js, timestamp):

    n_bac, n_pos, n_ico = 0, 0, 0
    for art in js['artworks']:
        if art['artwork_type'] == "Background":
            n_bac += 1
        elif art['artwork_type'] == "Poster":
            n_pos += 1
        elif art['artwork_type'] == "Icon":
            n_ico += 1

    d = {
        "id_tvdb":[js["id"]],
        "runtime":[js["runtime"]],
        "title":[title["name"] for title in js["translations"] if title["is_primary"]==True],
        "release_date":[pd.to_datetime(min(date["date"] for date in js["release_dates"]))],
        "language":[title["language_code"] for title in js["translations"] if title["is_primary"]==True],
        "id_imdb":[ids["id"] for ids in js["remoteids"] if ids["id"].startswith("tt")],
        "genres":[[gen["name"] for gen in js["genres"]]],
        "url":[js["url"].replace("https://api.thetvdb.com/","https://thetvdb.com/movies/")],
        "num_genres":[len(js["genres"]) if "genres" in js else 0],
        "num_translation":[len(js["translations"]) if "translations" in js else 0],
        "num_artworks":[len(js["artworks"]) if "artworks" in js else 0],
        "num_background":[n_bac],
        "num_poster":[n_pos],
        "num_icon":[n_ico],
        "num_actors":[len(js["people"]["actors"]) if "actors" in js["people"] else 0],
        "num_directors":[len(js["people"]["directors"]) if "directors" in js["people"] else 0],
        "num_writers":[len(js["people"]["writers"]) if "writers" in js["people"] else 0],
        "num_producers":[len(js["people"]["producers"]) if "producers" in js["people"] else 0],
        "scrapedate":[pd.to_datetime(timestamp, utc=True)]
        }
    
    for key in d:
        if d[key] == []:
            d[key].append(np.nan)

    return d



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


def handler(context, event):

    try:

        today = datetime.today()
        today_str = today.strftime("%Y%m%d")
        date_path = today.strftime("year=%Y/month=%m/day=%d/")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # init s3 client
        s3 = boto3.client('s3',
                          endpoint_url=S3_ENDPOINT,
                          aws_access_key_id=S3_ACCESS_KEY,
                          aws_secret_access_key=S3_SECRET_KEY,
                          config=Config(signature_version='s3v4'),
                          region_name='us-east-1')

        #fetch movie_list
        #reading csv from minio - the path/filename must be rewritten
        key = f"{PATH}id_movies.csv"
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        dataio = io.BytesIO(obj['Body'].read())
        df_id = pd.DataFrame()
        context.logger.info(f'read {key} into pandas dataframe')
        df_id = pd.read_csv(dataio)
        id_list = df_id["mov_id"].values.tolist()
        
        strt, c = 0, 0
        movie_list=[]
        for title_id in id_list:
            
            context.logger.info(f"Call the API for movie {str(title_id)}")
            try:
                url_api = f"{BASE_URL}/movies/{str(title_id)}"
                js = api_call_get(url_api, timeout=10)
                
                #if json is empty or there is a timeout error, justpass to the next id
                if js not in [None,"Empty","Timeout"]:
                    
                    filename_js = f"movie-{str(title_id)}-{today_str}"                

                    # write to S3 as bytesIO json
                    jsonio = io.BytesIO()
                    # #plain text
                    # jsonio.write(json.dumps(js, ensure_ascii=False).encode())
                    # write as gzip
                    with gzip.GzipFile(fileobj=jsonio, mode="wb") as gzio:
                        gzio.write(json.dumps(js, ensure_ascii=False).encode())


                    json_len = jsonio.tell()
                    context.logger.info(f'json dump written {json_len}')

                    #check length
                    if jsonio.tell() > 0:
                        # seek to start otherwise upload will be 0
                        jsonio.seek(0)
                        
                        # upload to minio
                        json_key = f"{PATH_JSON}{date_path}{filename_js}.json.gz"
                        context.logger.info(f'upload to s3 as {json_key}')
                        s3.upload_fileobj(jsonio, S3_BUCKET, json_key)
                        #cleanup now
                        jsonio.close()


                        movie_list.append(js["data"])
                        c+=1

                        #store chunks parquet
                        if (c == RANGE) or (0 < c < RANGE and title_id == id_list[-1]):
                            
                            #unpack data into a dataframe
                            df_list = []
                            for movie in movie_list:
                                try:
                                    d = unpack_data(movie,timestamp)
                                    df_list.append(pd.DataFrame.from_dict(d))
                                except Exception as ex:
                                    print(f"An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args}")

                            df = pd.concat(df_list)
                            df.reset_index(inplace=True, drop=True)

                            # store chunk parquet to S3
                            parquetio = io.BytesIO()
                            df.to_parquet(parquetio, engine='pyarrow')
                            # seek to start otherwise upload will be 0
                            parquetio.seek(0)

                            filename_parq = f"movies-{strt+1}-{strt+c}-{today_str}"
                            parquet_key = f"{PATH_PARQUET}{date_path}{filename_parq}.parquet"
                            context.logger.info(f'upload to s3 as {parquet_key}')
                            s3.upload_fileobj(parquetio, S3_BUCKET, parquet_key)
                            #cleanup now
                            parquetio.close()

                            movie_list = []
                            c = 0
                            strt+=RANGE

                            # cleanup
                            del parquetio
                            del df

                    del jsonio                    

            except Exception as e:
                context.logger.error('Error: '+str(e))

            # wait before next call
            time.sleep(1)

        # cleanup
        del dataio
        del df_id

        context.logger.info('Done')
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