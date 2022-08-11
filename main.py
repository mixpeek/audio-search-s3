import boto3
from opensearchpy import OpenSearch
from config import *
import sys
import speech_recognition as sr


# elasticsearch object
os = OpenSearch(opensearch_uri)

s3_file_name="audio.mp3"
bucket_name="mixpeek-demo"


def download_file():
    """Download the file
    :param str s3_file_name: name of s3 file
    :param str bucket_name: bucket name of where the s3 file is stored
    """

    # s3 boto3 client instantiation
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    # open in memory
    with open(s3_file_name, 'wb') as file:
        s3_client.download_fileobj(
            bucket_name,
            s3_file_name,
            file
        )
        print("file downloaded")
        # parse the file
        transcribed_audio = extract_text(s3_file_name)
        print("file contents extracted")
        # insert parsed pdf content into elasticsearch
        insert_into_search_engine(s3_file_name, transcribed_audio)
        print("file contents inserted into search engine")


def extract_text(s3_file_name):
    """Perform audio transcription on the file
    :param str s3_file_name: name of s3 file
    """
    recognizer = sr.Recognizer()
    with sr.AudioFile(s3_file_name) as source:
        recorded_audio = recognizer.listen(source)
        print("Done recording")

    print("Recognizing the text")
    # we're using CMUSphinx here, but you can use any
    # audio transcription library here:
    # https://github.com/Uberi/speech_recognition
    raw_text = recognizer.recognize_sphinx(
        recorded_audio,
        language="en-US"
    )
    return raw_text



def insert_into_search_engine(s3_file_name, transcribed_audio):
    """Download the file
    :param str s3_file_name: name of s3 file
    :param str transcribed_audio: extracted contents of audio file
    """
    doc = {
        "filename": s3_file_name,
        "full_text": transcribed_audio
    }
    # insert
    resp = os.index(
        index = index_name,
        body = doc,
        id = 1,
        refresh = True
    )
    print('\nAdding document:')
    print(resp)


def create_index():
    """Create the index
    """
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 1
            }
        }
    }
    response = os.indices.create(index_name, body=index_body)
    print('\nCreating index:')
    print(response)


if __name__ == '__main__':
    globals()[sys.argv[1]]()
