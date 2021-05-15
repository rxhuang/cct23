import json
import logging

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from ffmpy import FFmpeg
import subprocess
import os


def main(event: func.EventGridEvent, context: func.Context):

    ur = os.environ.get('VIDEO_STORAGE_ACCOUNT_URL')
    key = os.environ.get('VIDEO_STORAGE_ACCOUNT_API_KEY')

    longname = event.subject.split('/')[-1]
    name = longname.split('.')[0]

    if name:
        blob_service_client = BlobServiceClient(account_url=ur, credential=key)
        blob_client = blob_service_client.get_blob_client\
            ('video-input-container', longname)

        video_path = '/tmp/'
        video_file = os.path.join(video_path, name)
        thumbnail_path = '/tmp/thumbnails/'
        ff_exe = os.path.join(context.function_directory, 'ffmpeg')

        if not os.path.exists(video_path):
            os.mkdir(video_path)
        if not os.path.exists(thumbnail_path):
            os.mkdir(thumbnail_path)

        with open(video_file, "wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())

        subprocess.call(['chmod', '777', 'ffmpeg'])
        ff = FFmpeg(executable=ff_exe, inputs={video_file: None},
                    outputs={thumbnail_path+name+'_%d.png': '-y -vf "fps=1"'})
        ff.run()

        for filename in os.listdir(thumbnail_path):
            if name in filename:
                with open(os.path.join(thumbnail_path, filename), 'rb') as f:
                    blob_client = blob_service_client.get_blob_client\
                        ('thumbnail-container', filename)
                    blob_client.upload_blob(f, blob_type="BlockBlob")

        logging.info('Success')

    logging.info('End of request')
