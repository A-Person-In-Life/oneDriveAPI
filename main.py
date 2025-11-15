import os
import boto3
import asyncio
import aiohttp
import aiofiles
import math
import requests
from xml.etree.ElementTree import *
import time

class S3Api:
    def __init__(self, authPath):
        with open(authPath, "r") as f:
            self.accessKey = f.readline().strip()
            self.secretKey = f.readline().strip()
            self.bucketName = f.readline().strip()

        self.region = "us-east-2"
        self.client = boto3.client("s3", aws_access_key_id=self.accessKey, aws_secret_access_key=self.secretKey, region_name=self.region)

    async def downloadFile(self, localDestination, s3Path):
        url = self.client.generate_presigned_url(ClientMethod="get_object", Params={"Bucket": self.bucketName, "Key": s3Path}, ExpiresIn=30)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                async with aiofiles.open(localDestination, "wb") as f:
                    await f.write(await response.read())
                    print(f"DownloadFile, {s3Path}, {response.status}")

    async def uploadFile(self, localFile, s3Path):
        url = self.client.generate_presigned_url(ClientMethod="put_object", Params={"Bucket": self.bucketName, "Key": s3Path}, ExpiresIn=30)
        async with aiofiles.open(localFile, "rb") as f:
            data = await f.read()
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=data) as response:
                print(f"UploadFile, {s3Path}, {response.status}")

    async def uploadPart(self,localFile, partNumber, url, data, session, totalParts):
        async with session.put(url, data=data) as response:
            print(f"Uploaded Part {partNumber} out of {totalParts} for {os.path.basename(localFile)}")
            return {"ETag": response.headers["ETag"], "PartNumber": partNumber}

    async def uploadMultipart(self, localFile, s3Path):
        partSize = 5242880
        fileSize = os.path.getsize(localFile)
        totalParts = math.ceil(fileSize / partSize)
        parts_data = []
        partUrls = []

        if not s3Path:
            s3Path = os.path.basename(localFile)

        response = self.client.create_multipart_upload(Bucket=self.bucketName, Key=s3Path)
        uploadId = response["UploadId"]

        for i in range(1, totalParts + 1):
            url = self.client.generate_presigned_url(ClientMethod="upload_part", Params={"Bucket": self.bucketName, "Key": s3Path, "UploadId": uploadId, "PartNumber": i}, ExpiresIn=3600)
            partUrls.append(url)

        async with aiofiles.open(localFile, "rb") as f:
            for interation in range(totalParts):
                part = await f.read(partSize)
                parts_data.append(part)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(totalParts):
                task = self.uploadPart(localFile,i + 1, partUrls[i], parts_data[i], session, totalParts)
                tasks.append(task)
            endData = await asyncio.gather(*tasks)

        self.client.complete_multipart_upload(Bucket=self.bucketName, Key=s3Path, UploadId=uploadId, MultipartUpload={"Parts": endData})
        print(f"Multipart upload completed for {s3Path}")

    def listDir(self, s3Folder, operation):
        url = self.client.generate_presigned_url(ClientMethod="list_objects_v2", Params={"Bucket": self.bucketName, "Prefix": s3Folder, "Delimiter": "/"}, ExpiresIn=30)
        response = requests.get(url)
        subfolders = []
        xml = fromstring(response.content)

        filenames = []
        for file in xml.findall("Contents"):
            filenames.append(file.find("Key").text)

        for subfolder in xml.findall("CommonPrefixes"):
            subfolders.append(subfolder.find("Prefix").text)

        if operation == "folders":
            return subfolders
        elif operation == "files":
            return filenames
        else:
            return [subfolders, filenames]

    def getMetaData(self, s3File, operation):
        url = self.client.generate_presigned_url(ClientMethod="head_object", Params={"Bucket": self.bucketName, "Key": s3File}, ExpiresIn=30)
        response = requests.head(url)
        return response.headers[operation]

async def main(api):
    uploadTasks = []
    filesToUpload = [
        ("/home/gavin/onedrive/test/20MB-Test1.pdf", "20mb1"),
        ("/home/gavin/onedrive/test/20MB-Test1.pdf", "20mb2"),
        ("/home/gavin/onedrive/test/20MB-Test1.pdf", "20mb3"),
        ("/home/gavin/onedrive/test/20MB-Test1.pdf", "20mb4"),
        ("/home/gavin/onedrive/test/20MB-Test1.pdf", "20mb5")]

    for localFile, s3Key in filesToUpload:
        uploadTasks.append(api.uploadMultipart(localFile, s3Key))

    await asyncio.gather(*uploadTasks)


if __name__ == "__main__":
    startTime = time.time()
    api = S3Api("/home/gavin/desktop/python_projects/onedriveApi/config/aws_auth.txt")
    asyncio.run(main(api))
    endTime = time.time()
    print(f"Total runtime: {endTime - startTime} seconds")