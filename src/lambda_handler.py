import json
import boto3


def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]

    glue = boto3.client('glue')

    response = glue.start_job_run(
        JobName='pyspark-cdc',
        Arguments={
            '--s3_target_path_key': file_name,
            '--s3_target_path_bucket': bucket_name
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f'triggered glue job with bucket {bucket_name} and object {file_name}')
    }
