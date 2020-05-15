import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from botocore.vendored import requests
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lex_handler(event):
    q = event["q"]
    lex = boto3.client('lex-runtime')
    print('apple')
    lex_response = lex.post_text(
        botName='SearchForPhotos',
        botAlias='searchbot',
        userId="search-photos",
        inputText=q
    )
    
    keywords = []
    keyWordOne = lex_response['slots']['Object_one']
    keyWordTwo = lex_response['slots']['Object_two']
    if keyWordOne is not None:
        keywords.append(keyWordOne)
    if keyWordTwo is not None:
        keywords.append(keyWordTwo)
        
    return keywords
    
def elasticsearch_handler(keywords):
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
    
    results = []
    for keyword in keywords:
        query_body = {
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "labels": keyword
                        }
                    }
                }
            }
        }
    
    
        search_result = es.search(index="photos-index", body=query_body)
        for hit in search_result['hits']['hits']:
            print(hit)
            _source = hit["_source"]
            objectKey = _source["objectKey"]
            bucket = _source["bucket"]
            labels = _source["labels"]
            result = {"url": "https://s3.amazonaws.com/" + bucket + "/" + objectKey}
            results.append(result)
            
    return results
def lambda_handler(event, context):

    # Disambiguate the query using Lex
    keywords = lex_handler(event)
    
    if len(keywords)>2:  # Something wrong inside Lex
        return {
            'statusCode': 400,
            'body': lex_response["message"]
        }
    # Search the keywords in ElasticSearch
    results = elasticsearch_handler(keywords)
    
    return {
        'statusCode': 200,
        'body': json.dumps({"results": results})
    }
    