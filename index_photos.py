from __future__ import print_function

import boto3

import json
import logging
import urllib
from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)


rekognition = boto3.client('rekognition')



def detect_labels(bucket, key):

    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})

    labels = []

    for l in response.get('Labels'):
        labels.append(l.get('Name'))


    return labels


def add_to_elasticsearch(object_key, bucket, time_created, labels):

    credentials = boto3.Session().get_credentials()
    region = 'us-east-1'
    service = 'es'
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    host = ''  # host information
    es = Elasticsearch(hosts=[{"host": host, 'port': 443}],
                       http_auth=awsauth,
                       use_ssl=True,
                       verify_certs=True,
                       connection_class=RequestsHttpConnection)

    doc = {
        'objectKey': object_key,
        'bucket': bucket,
        'createdTimestamp': time_created,
        'labels': labels
    }

    res = es.index(index="photos-index", body=doc)


def lambda_handler(event, context):
    
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    time_created = event['Records'][0]['eventTime']
    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']


    try:
        # Calls rekognition DetectLabels API to detect labels in S3 object
        labels = detect_labels(bucket, key)

    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e

    add_to_elasticsearch(object_key, bucket, time_created, labels)

