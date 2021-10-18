from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(
    sys.argv, ['s3_target_path_key', 's3_target_path_bucket'])

bucket = args['s3_target_path_bucket']
key = args['s3_target_path_key']
