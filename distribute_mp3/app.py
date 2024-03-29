import json
import os
import urllib.parse

import boto3

s3 = boto3.resource('s3')
dynamodb = boto3.resource("dynamodb")
DESTINATION_BUCKET_NAME = os.environ["DESTINATION_BUCKET_NAME"]
USERS_TABLE_NAME = os.environ["USERS_TABLE_NAME"]


def lambda_handler(event, context):
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    dynamodb_table = dynamodb.Table(USERS_TABLE_NAME)
    mp3_to_distribute_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    mp3_to_distribute_source = {
        'Bucket': source_bucket_name,
        'Key': mp3_to_distribute_key
    }
    try:
        destination_bucket_directories = [
            user.get('id') for user in dynamodb_table.scan()['Items']
        ]
        destination_bucket = s3.Bucket(DESTINATION_BUCKET_NAME)
        for destination_bucket_directory in destination_bucket_directories:
            distributed_mp3 = destination_bucket.Object(
                f'{destination_bucket_directory}/{mp3_to_distribute_key}')
            distributed_mp3.copy(mp3_to_distribute_source)
        return {
            "statusCode": 201,
            "body": json.dumps({
                "message": "New mp3 successfully distributed to all Black Magic users.",
                "mp3Name": mp3_to_distribute_key,
                "userIds": destination_bucket_directories
            }),
        }
    except Exception as error:
        print(error)
        print(
            f'Error copying {mp3_to_distribute_key} between {source_bucket_name} and {DESTINATION_BUCKET_NAME} s3 Buckets.')
        raise error
