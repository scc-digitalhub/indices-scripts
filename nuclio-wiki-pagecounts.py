import io
import json
import os
import requests
import base64
import gzip
import time
import re
import pandas as pd

from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

from datetime import datetime, timedelta, time
from dateutil.relativedelta import *
from bs4 import BeautifulSoup
import re
import requests


AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
AZURE_CONTAINER = os.environ.get('AZURE_CONTAINER')

BASE_URL = 'http://cricca.disi.unitn.it/datasets/pagecounts-raw-sorted/'
LANGUAGES = ['it','es','en']
DATE_START = datetime(2008, 5, 1, 0, 0, 0)
DATE_END = datetime(2016, 7, 1, 0, 0, 0)

#CHUNK_SIZE = 128*1024*1024
CHUNK_SIZE = 64 * 1024 * 1024


def count_chunks(fileSize, chunkSize):
    parts = int(fileSize / chunkSize)
    reminder = fileSize % chunkSize
    if(reminder > 0):
        parts += 1
    return parts


def getLinks(url):
    r = requests.get(url)
    raw_html = r.content
    soup = BeautifulSoup(raw_html)
    links = []
    excludes = re.compile("^(\?.*|\/.*)")
    for link in soup.find_all('a'):
        hr = link.get('href')
        if not excludes.match(hr):
            links.append(hr)

    return links


def getParts(uri, includes):
    #idxdf = pd.read_csv(uri, header=None, delimiter=r"\s+")
    #read skipping errors with quote disabled, fixes bad index files
    idxdf = pd.read_csv(uri,header=None, error_bad_lines=False, delimiter=r"\s+", warn_bad_lines=False, quotechar=None, quoting=3)
    return idxdf[idxdf[1].isin(includes)][0].tolist()


def handler(context, event):
    try:
        context.logger.info('download pagecounts from '+BASE_URL)

        # init blob client
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(
            AZURE_CONTAINER)
        # date loop
        start_date = DATE_START
        end_date = DATE_END
        interval = relativedelta(months=+1)

        cur_date = start_date
        while cur_date <= end_date:
            day = cur_date.strftime("%Y-%m")
            context.logger.info('process '+day)

            # build urls and get links
            idx_uri = "{}{}/index.{}.txt".format(BASE_URL, day, day)
            links = getLinks("{}{}".format(BASE_URL, day))

            # divide languages
            for lang in LANGUAGES:
                # get matching parts
                keeps = getParts(idx_uri, [lang])
                for link in links:
                    #print("process {}".format(link))
                    if(link not in keeps):
                        # skip file
                        continue

                    uri = "{}{}/{}".format(BASE_URL, day, link)
                    dest = "pagecounts-raw-sorted/{}/{}/{}".format(
                        lang, day, link)

                    try:
                        response = requests.head(uri)

                        if(response.status_code == 200):
                            fileSize = int(response.headers['Content-Length'])
                            context.logger.debug(
                                "import {} content length {}".format(uri, fileSize))

                            # create blob
                            blob_client = container_client.get_blob_client(
                                dest)

                            # count chunks
                            parts = count_chunks(fileSize, CHUNK_SIZE)
                            context.logger.debug("upload to {} in {} chunks of size {}".format(
                                dest, parts, CHUNK_SIZE))

                            blist = []
                            for idx in range(0, parts):
                                bid = format(idx, '05d')
                                olength = CHUNK_SIZE
                                ostart = CHUNK_SIZE*idx
                                if(ostart >= fileSize):
                                    break
                                oend = ostart+CHUNK_SIZE
                                if(oend > fileSize):
                                    oend = fileSize
                                    olength = oend - ostart
                                if(olength <= 0):
                                    break
                                #print("chunk {} start {} length {}".format(bid, ostart, olength))

                                blob_client.stage_block_from_url(
                                    bid, uri, source_offset=ostart, source_length=olength)
                                blist.append(BlobBlock(block_id=bid))

                            # sync
                            blob_client.commit_block_list(blist)

                            # read meta
                            properties = blob_client.get_blob_properties()
                            context.logger.debug("wrote to {} size {}".format(
                                properties['name'], properties['size']))

                            # cleanup
                            del blob_client
                            del properties
                            del blist

                        else:
                            context.logger.error(
                                "error source, skip {}".format(uri))

                        # close
                        response.close()

                    except Exception as e:
                        context.logger.error(
                            "error with {} : {}".format(uri, str(e)))
            # end lang loop
            cur_date = cur_date + interval
        # end while
        context.logger.info("done.")
    except Exception as e:
        context.logger.error('Error: '+str(e))
        return context.Response(body='Error '+str(e),
                                headers={},
                                content_type='text/plain',
                                status_code=500)
