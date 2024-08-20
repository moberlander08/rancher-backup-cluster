#!/usr/bin/env python3
import os
import requests
import boto3
import time
import json
import yaml
from datetime import datetime
from botocore.exceptions import ClientError



def main():

  REGION=os.environ.get("AWS_REGION")
  TIMESTAMP = datetime.now().isoformat()
  rancher_bucket = "XXXXXXXXXXXXXXX"


  # get the aws account number
  awsaccount = account_id(REGION)

  verify_connectivity()

  backup_cluster_config(REGION, awsaccount, rancher_bucket)

  backup_etcd(REGION, awsaccount, rancher_bucket)

# find the account id
def account_id(REGION: str):

  #get the account id
  sts_client = boto3.client('sts', region_name=REGION)
  aws_account = sts_client.get_caller_identity()
  awsaccount = aws_account['Account']

  #return the account id
  return awsaccount

# make sure the server is up and running
def verify_connectivity():

  RANCHER_URL = str(os.environ.get("RANCHER_URL"))

  server_offline = True
  loop_counter = 0

  while server_offline and loop_counter < 5:
    response = requests.get(RANCHER_URL+'/ping')
    status = response.content

    if status.decode("utf-8") == 'pong':
      server_offline = False
    else:
      print("The Rancher Server is not up and responding, waiting 1m before attempting again...")
      time.sleep(60)
    loop_counter +=1

  if server_offline:
    print ("The Rancher Server is not able to be contacted, unable to proceed with deployment.")
    exit(1)
  else:
    print ("The Rancher Server is up and responding, proceeding with backups.")


def backup_cluster_config(REGION: str, awsaccount: str, rancher_bucket: str):


  RANCHER_URL = str(os.environ.get("RANCHER_URL"))
  TOKEN = 'Bearer '+str(os.environ.get("RANCHER_API_TOKEN"))
  CLUSTER_ID = os.environ.get("CLUSTER_ID")

  rancher_url=f"{RANCHER_URL}/v3/clusters/{CLUSTER_ID}?action=exportYaml"

  # make your request
  response = requests.post(rancher_url, headers={'Authorization': TOKEN})
                          
  # make sure that the request was successfull 
  if str(response.status_code) >= '400': 
    print (f"Unable to access Rancher API, exiting with a {str(response.status_code)} status code!")
    exit(1)

  data = json.loads(response.content)

  # load resonse to dict 
  cluster_yaml = data['yamlOutput']

  # load response in to a json string
  cluster_json = str(response.json())

  # send both docs to s3 for backup
  backup_config_s3(REGION, awsaccount, rancher_bucket, cluster_yaml, filename='cluster.yaml')
  backup_config_s3(REGION, awsaccount, rancher_bucket, cluster_json, filename='cluster.json')

  
def backup_etcd(REGION: str, awsaccount: str, rancher_bucket: str):

  RANCHER_URL = str(os.environ.get("RANCHER_URL"))
  TOKEN = 'Bearer '+str(os.environ.get("RANCHER_API_TOKEN"))
  CLUSTER_ID = os.environ.get("CLUSTER_ID")

  rancher_url=f"{RANCHER_URL}/v3/clusters/{CLUSTER_ID}?action=backupEtcd"

  # grab the number of backups in s3/current state
  before_num_backups = num_s3_objects(REGION, awsaccount, rancher_bucket)

  # make your request
  response = requests.post(rancher_url, headers={'Authorization': TOKEN})

  print (response)

  # make sure that the request was successfull 
  if str(response.status_code) >= '400': 
    print (f"Unable to access Rancher API, exiting with a {str(response.status_code)} status code!")
    exit(1)

  now = datetime.now()
  current_time = now.strftime("%H:%M:%S")
  
  print (f"Backup request was submited as of: {str(current_time)}, sleeping for 60 sec to allow transfer to s3")

  control = 1
  while control <= 3:

    # sleep to allow backup to be created and moved to s3
    time.sleep(60)

    after_num_backups = num_s3_objects(REGION, awsaccount, rancher_bucket)

    if after_num_backups > before_num_backups:
      print ("The Rancher ETCD backup has been completed successfully")
      break
    elif control <= 3:
      print (f"Backup has not been validated after attempt {str(control)}")
      control += 1
    else:
      print ("Unable to verify the ETCD backup status, please verify manually")
      exit(1)



def num_s3_objects(REGION: str, awsaccount: str, rancher_bucket: str):

  
  s3_client = boto3.client('s3', region_name=REGION)

  # get the number of objects in a particular prefix
  response = s3_client.list_objects_v2(
    Bucket = rancher_bucket+'-'+str(awsaccount),
    MaxKeys = 123,
    Prefix = 'etcd-backup/'
  )

  numberofbackups = len(response['Contents'])

  return numberofbackups

def backup_config_s3(REGION: str, awsaccount: str, rancher_bucket: str, config:str, filename: str):

  s3_client = boto3.client('s3', region_name=REGION)

  print('--------------------------------')
  print(f'Sending {filename} backup to s3.')

  # ceate the s3 client, and write the results file to s3.
  s3_client = boto3.client('s3', region_name=REGION)
  response = s3_client.put_object(
    Body=config, 
    Bucket=rancher_bucket + "-"+ awsaccount, 
    Key='deployments/rancher/'+ filename,
    
    ServerSideEncryption='aws:kms'
    )

  time.sleep(10)

# main function call
if __name__ == '__main__':
  main()
