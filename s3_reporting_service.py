import boto3
import os
import pandas as pd
from setup import load_env_variables

load_env_variables()

def create_s3_client():
    # Create s3 client
    s3 = boto3.client('s3',
                      region_name='us-east-1',
                      aws_access_key_id=os.environ['AWS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET'])
    return s3


def list_all_objects(s3_client, bucket_name, prefix):
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)

    return response


def upload_file_to_s3(local_file_path, bucket_name, key, acl_value='public-read'):
    s3.upload_file(Filename=local_file_path,
                   Key=key, Bucket=bucket_name,
                   ExtraArgs={'ACL': acl_value})


def convert_to_dataframe(bucket_list):
    object_df = pd.DataFrame(bucket_list['Contents'])
    return object_df


def add_link_column_to_df(bucket_name, object_df):
    base_url = "http://{}.s3.amazonaws.com/".format(bucket_name)
    object_df['Link'] = base_url + object_df['Key']

    object_df.head()

    return object_df


print("Creating client...")
s3 = create_s3_client()

bucket_name = 'dailyreportingdata'
prefix = "WHO/"

print("Obtaining list of all objects inside {} with the prefix of {}".format(bucket_name, prefix))
objects_list = list_all_objects(s3, bucket_name, prefix)

print("Converting to dataframe")
df = convert_to_dataframe(objects_list)

print("Adding Link Column")
final_df = add_link_column_to_df(bucket_name, df)

print("Final DF")
print(final_df.head())
