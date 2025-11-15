import boto3
import os

class S3Api:
    def __init__(self,configPath,chunkSize):
        self.chunkSize = chunkSize

        with open(configPath,"r") as f:
            keyId = f.readline()
            secretKey = f.readline

        self.s3 = boto3.client('s3',aws_access_key_id=keyId,aws_secret_access_key=secretKey,region_name='us-east-1')

    def uploadFile(localPath,bucketName):
        pass

    

