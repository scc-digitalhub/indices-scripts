import pandas as pd
import boto3
import botocore
import io
import json
import os
import requests
import base64
import gzip
import time
import re
import pytz

from botocore.client import Config
from datetime import datetime


S3_ENDPOINT = os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET')

BASE_URL = 'https://datasets.imdbws.com/'
DATASETS = ["title.basics", "title.episode", "title.ratings",
            "title.akas", "title.crew", "title.principals", "name.basics"]


def download_file(url, context):
    context.logger.info("read from "+url)
    response = requests.get(url, stream=True)
    context.logger.info("response code "+str(response.status_code))
    if(response.status_code == 200):
        return response.raw
    else:
        raise Exception('error downloading from {}'.format(url))


def handler(context, event):
    try:
        context.logger.info('download datasets from '+BASE_URL)

        now = datetime.today().astimezone(pytz.UTC)
        day = now.strftime('%Y%m%d')
        today = now.strftime('%Y-%m-%d')

        # init s3 client
        s3 = boto3.client('s3',
                          endpoint_url=S3_ENDPOINT,
                          aws_access_key_id=S3_ACCESS_KEY,
                          aws_secret_access_key=S3_SECRET_KEY,
                          config=Config(signature_version='s3v4'),
                          region_name='us-east-1')

        for dataset in DATASETS:
            context.logger.info('download dataset '+dataset)
            filename = 'imdb/datasets/{}/imdb-{}-{}-{}.{}'.format(
                today, day, dataset, now.strftime('%Y%m%dT%H%M%S'), 'tsv.gz')

            # check if raw files already exist
            exists = False
            try:
                s3.head_object(Bucket=S3_BUCKET, Key=filename)
                exists = True
            except botocore.exceptions.ClientError:
                # Not found
                exists = False
                pass

            if exists:
                context.logger.info('files for '+filename +
                                    ' already exists in bucket, skip.')
                continue

            # download and save via stream
            uri = BASE_URL+dataset+'.tsv.gz'
            context.logger.info('download from '+uri +
                                ' and save to '+filename)
            fileio = download_file(uri,context)
            s3.upload_fileobj(fileio, S3_BUCKET, filename)

            context.logger.info('dataset '+dataset + ' done.')
            del fileio

    except Exception as e:
        context.logger.error('Error: '+str(e))
        return context.Response(body='Error '+str(e),
                                headers={},
                                content_type='text/plain',
                                status_code=500)
