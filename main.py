from concurrent.futures import *
import os
import requests
from msal import PublicClientApplication, SerializableTokenCache
import webbrowser
class OneDriveApi:
    def __init__(self, tenantId, clientId, scopes, cachePath):
        self.tenantId = tenantId
        self.clientId = clientId
        self.scopes = scopes
        self.accessToken = None
        self.cachePath = cachePath
        
        authority = f"https://login.microsoftonline.com/{tenantId}"
        tokenCache = SerializableTokenCache()
        with open(cachePath, "r") as f:
            tokenCache.deserialize(f.read())
        self.app = PublicClientApplication(client_id=clientId, authority=authority, token_cache=tokenCache)

        accounts = self.app.get_accounts()
        if accounts:
            result = self.app.acquire_token_silent(scopes, account=accounts[0])
        else:
            flow = self.app.initiate_device_flow(scopes=scopes)
            if "error" in flow:
                raise ValueError(f"Device flow error: {flow['error_description']}")
            print(flow["message"])
            webbrowser.open(flow["verification_uri"])
            result = self.app.acquire_token_by_device_flow(flow)

        if "access_token" in result:
            print("Access token acquired!")
            print(result["access_token"][:100] + "...")
            self.accessToken = result["access_token"]
        else:
            raise ValueError(f"Error acquiring token: {result.get('error_description')}")

        if tokenCache.has_state_changed:
            with open(cachePath, "w") as f:
                f.write(tokenCache.serialize())
                
    def downloadFile(self, oneDriveFolder, localDestination):
        version = "v1.0"
        urlSafePath = requests.utils.quote(oneDriveFolder, safe="/")
        url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}"
        headers = {"Authorization": f"Bearer {self.accessToken}"}

        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Failed to get file info: {response.status_code}")
            return
            
        data = response.json()
        downloadUrl = data.get("@microsoft.graph.downloadUrl")
        fileName = data.get("name")
        if not downloadUrl or not fileName:
            print("Missing download URL or fileName")
            return
            
        print(f"Download URL: {downloadUrl}")
                
        file = requests.get(downloadUrl)
        localPath = os.path.join(localDestination, fileName)
        with open(localPath, "wb") as f:
            f.write(file.content)
        
    def uploadFile(self, oneDriveFolder, localFilePath):
        cutoffSize = 4000000
        version = "v1.0"
        fileName = os.path.basename(localFilePath)
        urlSafePath = requests.utils.quote(f"{oneDriveFolder}/{fileName}", safe="")

        if os.path.getsize(localFilePath) <= cutoffSize:
            url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}:/content"
            headers = {"Authorization": f"Bearer {self.accessToken}", "Content-Type": "application/octet-stream"}
            print(headers)

            with open(localFilePath, "rb") as f:
                response = requests.put(url=url, headers=headers, data=f)
                print(f"Status Code: {response.status_code}")

            if response.status_code not in (200, 201):
                print("Upload failed:")
            else:
                print("Small upload succeeded")
            return
        #needs an upload session for files above 4mb
        createSessionUrl = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}:/createUploadSession"
        sessionHeaders = {"Authorization": f"Bearer {self.accessToken}"}
        sessionBody = {"item": {"@microsoft.graph.conflictBehavior": "replace", "name": fileName}}

        print("Creating upload session...")
        sessionResp = requests.post(createSessionUrl, headers=sessionHeaders, json=sessionBody)
        print(f"Create session status: {sessionResp.status_code}")
        if sessionResp.status_code not in (200, 201):
            print("Failed to create upload session:")
            print(sessionResp.text)
            return

        uploadUrl = sessionResp.json().get("uploadUrl")
        if not uploadUrl:
            print("No uploadUrl returned by createUploadSession.")
            return

        fileSize = os.path.getsize(localFilePath)
        chunkSize = 10485760
        uploaded = 0

        print(f"Starting chunked upload: {fileSize} bytes total, chunk size {chunkSize} bytes")

        with open(localFilePath, "rb") as f:
            while uploaded < fileSize:
                start = uploaded
                end = min(uploaded + chunkSize, fileSize) - 1
                chunkLength = end - start + 1

                f.seek(start)
                chunkData = f.read(chunkLength)

                headers = {"Content-Length": str(chunkLength), "Content-Range": f"bytes {start}-{end}/{fileSize}"}

                response = requests.put(uploadUrl, headers=headers, data=chunkData)

                if response.status_code in (200, 201):
                    uploaded = fileSize
                    print("Upload complete.")
                    break
                elif response.status_code == 202:
                    uploaded = end + 1
                    continue
                else:
                    return

        print("Large file upload finished.")
    
    def listDir(self, oneDrivePath):
        version = "v1.0"
        urlSafePath = requests.utils.quote(oneDrivePath)
        url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}:/children"
        headers = {"Authorization": f"Bearer {self.accessToken}"}

        response = requests.get(url=url, headers=headers)
        if response.status_code != 200:
            print(f"listDir failed for {oneDrivePath}: {response.status_code}")
            return []

        data = response.json()
        items = data["value"]
        results = []
        for item in items:
            if not isinstance(item, dict):
                continue
                
            name = item.get("name")
            results.append(name)
                
        return results
    
    def getMetaData(self, oneDrivePath, output):
        version = "v1.0"
        urlSafePath = requests.utils.quote(oneDrivePath)
        url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}"
        headers = {"Authorization": f"Bearer {self.accessToken}"}

        response = requests.get(url=url, headers=headers)
        if response.status_code not in (200, 201):
            print("getMetaData failed")
            print(response.status_code)
            return
        
        data = response.json()
        if output == None:
            return data
        else:
            return data.get(output)
    
    def makeDir(self, oneDrivePath, oneDriveFolderName):
        version = "v1.0"
        urlSafePath = requests.utils.quote(oneDrivePath)
        parentId = self.getMetaData(oneDrivePath, output="id")
        url = f"https://graph.microsoft.com/{version}/me/drive/items/{parentId}/children"
        headers = {"Authorization": f"Bearer {self.accessToken}", "Content-Type": "application/json"}
        jsonData = {"name": f"{oneDriveFolderName}", "folder": {}, "@microsoft.graph.conflictBehavior": "rename"}

        response = requests.post(url=url, headers=headers, json=jsonData)
        print(response.status_code)


class Execution: 
    def __init__(self, workers, api):
        self.api = api
        self.workers = workers
        
    def checkNames(self, names, localFolderPath):
        filteredNames = []
        for name in names:
            if not os.path.exists(os.path.join(localFolderPath, name)):
                filteredNames.append(name)
        return filteredNames

    def checkLocalFiles(self, names, oneDriveFolder):
        oneDriveItems = self.api.listDir(oneDriveFolder)
        filteredNames = []
        for name in names:
            if name not in oneDriveItems:
                filteredNames.append(name)
        return filteredNames

    def push(self, localFolderPath, oneDriveFolder, executor=None):
        print(f"Scanning local folder: {localFolderPath}")
        
        if executor ==  None:
            firstCall = True
        if firstCall:
            executor = InterpreterPoolExecutor(max_workers=self.workers)
            print(f"Created a pool of {self.workers} threads!")
        
        files = []
        folders = []

        for name in os.listdir(localFolderPath):
            if os.path.isfile(os.path.join(localFolderPath, name)):
                files.append(name)
            elif os.path.isdir(os.path.join(localFolderPath, name)):
                folders.append(name)

        files = self.checkLocalFiles(files, oneDriveFolder)
        print(f"Found {len(files)} files to upload in {localFolderPath}!")
        
        futures = []
        for file in files:
            print(f"Scheduling upload for {file}")
            future = executor.submit(self.api.uploadFile, oneDriveFolder, os.path.join(localFolderPath, file))
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An upload failed: {e}")
        
        listedDir = self.api.listDir(oneDriveFolder)
        
        for folder in folders:
            if folder not in listedDir:
                print(f"Creating folder: {folder}")
                self.api.makeDir(oneDriveFolder, folder)
            self.push(os.path.join(localFolderPath, folder), os.path.join(oneDriveFolder, folder), executor)

        if firstCall:
            executor.shutdown(wait=True)
            print(f"All uploads finished")
    
    def pull(self, localFolderPath, oneDriveFolder, executor=None):
        print(f"Scanning OneDrive folder: {oneDriveFolder}")
        
        if executor ==  None:
            firstCall = True
        if firstCall:
            executor = InterpreterPoolExecutor(max_workers=self.workers)
            print(f"Created a pool of {self.workers} threads!")
        
        items = self.api.listDir(oneDriveFolder)
        files = []
        folders = []
        
        for name in items:            
            metaData = self.api.getMetaData(os.path.join(oneDriveFolder, name), None)
            if "file" in metaData:
                files.append(name)
            elif "folder" in metaData:
                folders.append(name)
        
        files = self.checkNames(files, localFolderPath)
        print(f"Found {len(files)} files to download from {oneDriveFolder}!")
        print(files)

        futures = []
        for file in files:
            print(f"Scheduling download for {file}")
            future = executor.submit(self.api.downloadFile, os.path.join(oneDriveFolder, file), localFolderPath)
            futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"A download failed: {e}")

        for folder in folders:
            localSubFolder = os.path.join(localFolderPath, folder)
            if not os.path.exists(localSubFolder):
                print(f"Creating local folder {folder}")
                os.makedirs(localSubFolder)
            self.pull(localSubFolder, os.path.join(oneDriveFolder, folder), executor)
        
        if firstCall:
            executor.shutdown(wait=True)
            print(f"All downloads finished")


if __name__ == "__main__":
    baseDir = "/home/gavin/downloads/icloud_api_config/"
    oneDriveAuth = os.path.join(baseDir, "onedrive_auth.txt")
    oneDriveAuthCache = os.path.join(baseDir, "onedrive_auth_cache.json")

    with open(oneDriveAuth, "r") as f:
        clientId = f.readline().strip()
        tenantId = f.readline().strip()
        scopes = f.readline().strip().split(",")

    print("Enter the local path:")
    localPath = input("")
    print("Enter the oneDrivePath:")
    oneDrivePath = input("")
    print("Enter the desired operation:")
    operation = input("")

    api = OneDriveApi(tenantId, clientId, scopes, oneDriveAuthCache)
    function = Execution(6, api)
    if str.lower(operation) == "pull":
        function.pull(localPath, oneDrivePath)
    elif str.lower(operation) == "push":
        function.push(localPath, oneDrivePath)
    else:
        print("invalid operation")
