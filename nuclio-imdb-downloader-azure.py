import pandas as pd
import io
import json
import os
import requests
import base64
import gzip
import time
import re
import pytz

from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError


AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
AZURE_CONTAINER = os.environ.get('AZURE_CONTAINER') 

BASE_URL = 'https://datasets.imdbws.com/'
DATASETS = ["title.basics", "title.episode", "title.ratings",
            "title.akas", "title.crew", "title.principals", "name.basics"]



def handler(context, event):
    try:
        context.logger.info('download datasets from '+BASE_URL)

        now = datetime.today().astimezone(pytz.UTC)
        day = now.strftime('%Y%m%d')
        today = now.strftime('%Y-%m-%d')

        # init blob client
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER)

        for dataset in DATASETS:
            context.logger.info('download dataset '+dataset)
            filename = 'datasets/{}/imdb-{}-{}-{}.{}'.format(
                today, day, dataset, now.strftime('%Y%m%dT%H%M%S'), 'tsv.gz')

            #get blob client for this op
            blob_client = container_client.get_blob_client(filename)

            # check if raw files already exist
            # https://github.com/Azure/azure-sdk-for-python/issues/9507
            exists = False
            try:
                # One of the very few methods which do not mutate state
                blob_client.get_blob_properties()
                exists = True
            except ResourceNotFoundError:
                # Not found
                exists = False
                pass

            if exists:
                context.logger.info('files for '+filename +
                                    ' already exists in container, skip.')
                continue

            # download and save via stream
            uri = BASE_URL+dataset+'.tsv.gz'
            context.logger.info('download from '+uri +
                                ' and save to '+filename)

            context.logger.info("read from "+uri)
            with requests.get(uri, stream=True) as response:
                context.logger.info("response code "+str(response.status_code))
                if(response.status_code != 200):             
                    raise Exception('error downloading from {}'.format(uri))
                
                idx = 0
                blist = []
                #stream with chunk 4MB
                for chunk in response.iter_content(chunk_size=1024*1024*4):
                    if chunk:
                        idx += 1
                        #note: block ids should be consistent in length
                        bid = format(idx, '05d')
                        blob_client.stage_block(bid, chunk, len(chunk))
                        blist.append(BlobBlock(block_id=bid))

                #sync via commit
                blob_client.commit_block_list(blist)

                context.logger.info('dataset '+dataset + ' done.')
            
            del blob_client

    except Exception as e:
        context.logger.error('Error: '+str(e))
        return context.Response(body='Error '+str(e),
                                headers={},
                                content_type='text/plain',
                                status_code=500)
