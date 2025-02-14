import json
import boto3
import os

# defines strings to search for in filenames to identify lookup data and manifest data
lookup_data = "lookup_data"
manifest = ".txt"

# bucket to copy files to; set in env var
new_bucket = os.environ['NEW_BUCKET']

s3 = boto3.resource('s3')
s3_cli = boto3.client('s3')

def generate_new_filename(name):
    split_name = name.split('/')
    delim = "/"
    name = split_name[-1]
    prefix = delim.join(split_name[:-1])
    prefix = prefix.strip('/')
    if lookup_data in name:
        day = f"day={name[-21:-19]}"
        month = f"month={name[-24:-22]}"
        year = f"year={name[-29:-25]}"
        name = f"lookup_data/{prefix}{year}/{month}/{day}/{name}"
        return name
    else:
        day = f"day={name[-9:-7]}"
        month = f"month={name[-12:-10]}"
        year = f"year={name[-17:-13]}"
        name = f"hit_data/{prefix}/{year}/{month}/{day}/{name}"
        return name

def copy_file(original_bucket,
              new_bucket,
              original_name,
              new_name):
    copy_source = {
        'Bucket': original_bucket,
        'Key': original_name
    }
    s3.meta.client.copy(copy_source, new_bucket, new_name)

def check_etags(original_bucket,
              new_bucket,
              original_name,
              new_name):
    original_s3_resp = s3_cli.head_object(Bucket=original_bucket, Key=original_name)
    new_s3_resp = s3_cli.head_object(Bucket=new_bucket, Key=new_name)
    if original_s3_resp['ETag'] == new_s3_resp['ETag']:
        print('Ok! ETags match')
    else:
        print('WARNING: ETags do not match')

def lambda_handler(event,context):
    for record in event['Records']:
        try: 
            original_name = record['s3']['object']['key']
            original_bucket = record['s3']['bucket']['name']
            if manifest in original_name:
                print('Manifest file, skipping')
                break
            else:
                new_name = generate_new_filename(original_name)
                copy_file(original_bucket=original_bucket,
                          original_name=original_name,
                          new_bucket=new_bucket,
                          new_name=new_name)
                check_etags(original_bucket=original_bucket,
                            original_name=original_name,
                            new_bucket=new_bucket,
                            new_name=new_name)
        except KeyError:
            print('Not an S3 Event')
            break



