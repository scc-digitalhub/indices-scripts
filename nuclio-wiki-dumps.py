import io
import json
import os
import requests
import base64
import gzip
import time
import re

from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

from datetime import datetime, timedelta, time
from bs4 import BeautifulSoup
import re
import requests


AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
AZURE_CONTAINER = os.environ.get('AZURE_CONTAINER')

#BASE_URL = 'https://dumps.wikimedia.org'
BASE_URL='http://ftp.acc.umu.se/mirror/wikimedia.org/dumps'
LANGUAGES = ['it', 'es', 'en']
DATE = datetime(2020, 5, 1, 0, 0, 0)
TYPES = ['metahistory7zdump']

#CHUNK_SIZE = 128*1024*1024
CHUNK_SIZE = 64 * 1024 * 1024


def count_chunks(fileSize, chunkSize):
    parts = int(fileSize / chunkSize)
    reminder = fileSize % chunkSize
    if(reminder > 0):
        parts += 1
    return parts



def handler(context, event):
    try:
        context.logger.info('download dumps from '+BASE_URL)

        # init blob client
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(
            AZURE_CONTAINER)

        day = DATE.strftime("%Y%m%d")

        # walk languages
        for lang in LANGUAGES:
            wiki = lang+'wiki'
            context.logger.info('process {} for date {}'.format(wiki, day))

            # build urls and get links
            idx_url = "{}/{}/{}/dumpstatus.json".format(BASE_URL, wiki, day)

            r = requests.get(idx_url)
            json = r.json()
            jobs = json['jobs']
            r.close()
            
            for dtyp in TYPES:
                context.logger.info('download {}/{} for date {}'.format(wiki, dtyp, day))

                files = jobs[dtyp]['files']
                for f in files:
                    try:
                        fileName = f
                        furi = BASE_URL+files[f]['url']
                        ##resolve redirect
                        rr = requests.get(furi, stream=True)
                        uri = rr.url
                        rr.close()
                        fileSize = files[f]['size']
                        context.logger.debug(
                            "import {} size {}".format(uri, fileSize))

                        dest = "dumps/{}/{}/{}/{}".format(lang,
                                                          day, dtyp, fileName)

                        # create blob
                        blob_client = container_client.get_blob_client(dest)

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

                    except Exception as e:
                        context.logger.error(
                            "error with {} : {}".format(f, str(e)))

                # end files for lang
            # end dtyp
        # end lang
        context.logger.info("done.")
    except Exception as e:
        context.logger.error('Error: '+str(e))
        return context.Response(body='Error '+str(e),
                                headers={},
                                content_type='text/plain',
                                status_code=500)
