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

    async def uploadFile(self, localFolder, s3Path):
        url = self.client.generate_presigned_url(ClientMethod="put_object", Params={"Bucket": self.bucketName, "Key": s3Path}, ExpiresIn=30)
        async with aiofiles.open(localFolder, "rb") as f:
            data = await f.read()
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=data) as response:
                print(f"UploadFile, {s3Path}, {response.status}")

    async def uploadPart(self,localFolder, partNumber, url, data, session, totalParts):
        async with session.put(url, data=data) as response:
            print(f"Uploaded Part {partNumber} out of {totalParts} for {os.path.basename(localFolder)}")
            return {"ETag": response.headers["ETag"], "PartNumber": partNumber}

    async def uploadMultipart(self, localFolder, s3Path):
        partSize = 5242880
        fileSize = os.path.getsize(localFolder)
        totalParts = math.ceil(fileSize / partSize)
        parts_data = []
        partUrls = []

        if not s3Path:
            s3Path = os.path.basename(localFolder)

        response = self.client.create_multipart_upload(Bucket=self.bucketName, Key=s3Path)
        uploadId = response["UploadId"]

        for i in range(1, totalParts + 1):
            url = self.client.generate_presigned_url(ClientMethod="upload_part", Params={"Bucket": self.bucketName, "Key": s3Path, "UploadId": uploadId, "PartNumber": i}, ExpiresIn=3600)
            partUrls.append(url)

        async with aiofiles.open(localFolder, "rb") as f:
            for interation in range(totalParts):
                part = await f.read(partSize)
                parts_data.append(part)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(totalParts):
                task = self.uploadPart(localFolder,i + 1, partUrls[i], parts_data[i], session, totalParts)
                tasks.append(task)
            endData = await asyncio.gather(*tasks)

        self.client.complete_multipart_upload(Bucket=self.bucketName, Key=s3Path, UploadId=uploadId, MultipartUpload={"Parts": endData})
        print(f"Multipart upload completed for {s3Path}")

    def listDir(self, s3Folder, operation=None):
        url = self.client.generate_presigned_url(
            ClientMethod="list_objects_v2", 
            Params={"Bucket": self.bucketName, "Prefix": s3Folder, "Delimiter": "/"}, 
            ExpiresIn=30
        )
        response = requests.get(url)
        subfolders = []
        filenames = []
        
        nameSpace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        xml = fromstring(response.content)

        for file in xml.findall("s3:Contents", nameSpace):
            filenames.append(file.find("s3:Key", nameSpace).text)

        for subfolder in xml.findall("s3:CommonPrefixes", nameSpace):
            subfolders.append(subfolder.find("s3:Prefix", nameSpace).text)
        
        print(filenames, subfolders)

        if operation == "folders":
            return subfolders
        elif operation == "files":
            return filenames
        elif operation == None:
            return [filenames, subfolders]
    
    def getMetaData(self, s3File, operation):
        url = self.client.generate_presigned_url(ClientMethod="head_object", Params={"Bucket": self.bucketName, "Key": s3File}, ExpiresIn=30)
        response = requests.head(url)
        print(response.headers)
        return response.headers[operation]

class Executor:
    def __init__(self,api):
        self.api = api
        pass

    async def push(self, localFolder, s3Folder):
        files = []
        folders = []
        s3Filenames, s3Subfolders = self.api.listDir(s3Folder, operation=None)
        s3Basenames = []
        s3FolderBasenames = []
        tasks = []
        outputPairs = {}
        
        for filePath in s3Filenames:
            s3Basenames.append(os.path.basename(filePath))
        for subfolderPath in s3Subfolders:
            s3FolderBasenames.append(os.path.basename(subfolderPath.rstrip('/')))

        for entry in os.listdir(localFolder):
            entry_path = os.path.join(localFolder, entry)
            if os.path.isfile(entry_path):
                if entry not in s3Basenames:
                    files.append(entry)
                    
            elif os.path.isdir(entry_path):
                if entry not in s3FolderBasenames:
                    folders.append(entry)
        
        for file in files:
            local_path = os.path.join(localFolder, file)
            outputPairs[local_path] = file
        
        for local_path, filename in outputPairs.items():
            s3_path = os.path.join(s3Folder, filename)
            tasks.append(self.api.uploadMultipart(local_path, s3_path))
        await asyncio.gather(*tasks)

        for folder in folders:
            local_subfolder = os.path.join(localFolder, folder)
            s3_subfolder = os.path.join(s3Folder, folder)
            await self.push(local_subfolder, s3_subfolder)

startTime = time.time()
api = S3Api("/home/gavin/desktop/python_projects/onedriveApi/config/aws_auth.txt")
function = Executor(api)
asyncio.run(function.push("/home/gavin/test/", "test/"))
endTime = time.time()
print(f"Runtime: {endTime-startTime}")


