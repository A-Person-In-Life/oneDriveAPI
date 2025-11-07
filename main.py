from concurrent.futures import *
import os
import requests
from msal import PublicClientApplication, SerializableTokenCache
import webbrowser


class oneDriveApi:
    def __init__(self, tenantId, clientId, scopes, cache_path):
        self.tenantId = tenantId
        self.clientId = clientId
        self.scopes = scopes
        self.accessToken = None
        self.cache_path = cache_path
        
        authority = f"https://login.microsoftonline.com/{tenantId}"
        token_cache = SerializableTokenCache()
        with open(cache_path, "r") as f:
            token_cache.deserialize(f.read())
        self.app = PublicClientApplication(client_id=clientId,authority=authority,token_cache=token_cache)

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

        if token_cache.has_state_changed:
            with open(cache_path, "w") as f:
                f.write(token_cache.serialize())
                
    def downloadFile(self, onedriveFolder, localDestination):
        version = "v1.0"
        urlSafePath = requests.utils.quote(onedriveFolder, safe="/")
        url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}"
        headers = {"Authorization": f"Bearer {self.accessToken}"}

        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Failed to get file info: {response.status_code}")
            return
            
        data = response.json()
        download_url = data.get("@microsoft.graph.downloadUrl")
        filename = data.get("name")
        if not download_url or not filename:
            print("Missing download URL or filename")
            return
            
        print(f"Download URL: {download_url}")
        
        # Create subdirectories if needed
        os.makedirs(localDestination, exist_ok=True)
        
        file = requests.get(download_url)
        localPath = os.path.join(localDestination, filename)
        with open(localPath, "wb") as f:
            f.write(file.content)
        
    def uploadFile(self, onedriveFolder, localFilePath):
        cuttoffSize = 262144000
        version = "v1.0"
        filename = os.path.basename(localFilePath)
        urlSafePath = requests.utils.quote(f"{onedriveFolder}/{filename}", safe="")

        if os.path.getsize(localFilePath) <= cuttoffSize:
            url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}:/content"
            headers = {"Authorization": f"Bearer {self.accessToken}", "Content-Type": "application/octet-stream"}
            print(headers)

            with open(localFilePath, "rb") as f:
                response = requests.put(url=url, headers=headers, data=f)
                print(f"Status Code: {response.status_code}")

            if response.status_code not in (200, 201):
                print("Upload failed:")
                print(response.text)
            else:
                print("Upload succeeded (simple upload).")
            return

        create_session_url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}:/createUploadSession"
        session_headers = {"Authorization": f"Bearer {self.accessToken}"}
        session_body = {"item": {"@microsoft.graph.conflictBehavior": "replace", "name": filename}}

        print("Creating upload session...")
        session_resp = requests.post(create_session_url, headers=session_headers, json=session_body)
        print(f"Create session status: {session_resp.status_code}")
        if session_resp.status_code not in (200, 201):
            print("Failed to create upload session:")
            print(session_resp.text)
            return

        upload_url = session_resp.json().get("uploadUrl")
        if not upload_url:
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

                headers = {"Content-Length": str(chunkLength),"Content-Range": f"bytes {start}-{end}/{fileSize}"}

                response = requests.put(upload_url, headers=headers, data=chunkData)

                if response.status_code in (200, 201):
                    uploaded = fileSize
                    print("Upload complete.")
                    break
                elif response.status_code == 202:
                    uploaded = end + 1
                    progress = (uploaded / fileSize) * 100
                    continue
                else:
                    print(response.text)
                    return

        print("Large file upload finished.")
    
    def listOneDriveDir(self, onedrivePath):
        version = "v1.0"
        urlSafePath = requests.utils.quote(onedrivePath)
        url = f"https://graph.microsoft.com/{version}/me/drive/root:/{urlSafePath}:/children"
        headers = {"Authorization": f"Bearer {self.accessToken}"}

        response = requests.get(url=url, headers=headers)
        if response.status_code != 200:
            print(f"listOneDriveDir failed for {onedrivePath}: {response.status_code}")
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
    
    def getMetaData(self, onedrivePath, output):
        version = "v1.0"
        urlSafePath = requests.utils.quote(onedrivePath)
        url = f"https://graph.microsoft.com/{version}/driveItem/drive/root:/{urlSafePath}"
        headers = {"Authorization": f"Bearer {self.accessToken}"}

        response = requests.get(url=url,headers=headers)
        if response != 200:
            print("getMetaData failed")
            return
        
        data = response.json()
        print(data[output])
        return data[output]

class execution: 
    def __init__(self,workers,api):
        self.api = api
        self.workers = workers

    def differ(self, localPath, onedrivePath):
        onedriveSize = self.oneDriveApi.getMetaData(onedrivePath,"size")
        onedriveDate = self.oneDriveApi.getMetaData(onedrivePath, "lastModifiedDateTime")

        localSize = os.path.getsize(localPath)
        localDate = os.path.getmtime(localPath)
        
    def checkNames(self, names,localFolderPath):
        filteredNames = []
        for name in names:
            if not os.path.exists(os.path.join(localFolderPath, name)):
                filteredNames.append(name)
        return filteredNames

    def checkLocalFiles(self, names, onedriveFolder):
        onedriveNames = self.api.listOneDriveDir(onedriveFolder)
        filteredNames = []
        for name in names:
            if name not in onedriveNames:
                filteredNames.append(name)
        return filteredNames
                

    def push(self,localFolderPath, onedriveFolder):
        print("Scanning local folder:")
        files = []

        for file in os.listdir(localFolderPath):
            if os.path.isfile(os.path.join(localFolderPath, file)):
                files.append(file)

        files = self.checkLocalFiles(files, onedriveFolder)
        print(f"Found {len(files)} files to upload!")
        
        executer = ThreadPoolExecutor(max_workers=4)
        print("Created a pool of 4 threads!") 
        
        futures = []
        for file in files:
            print(f"Scheduling upload for {file}")
            future = executer.submit(self.api.uploadFile, onedriveFolder, os.path.join(localFolderPath, file))
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An upload failed: {e}")
        
        executer.shutdown(wait=True)
        print("All uploads finished!")
    
    def pull(self,localFolderPath, onedriveFolder):
        print("Scanning OneDrive folder:")
        files = []
        for name in self.api.listOneDriveDir(onedriveFolder):
            files.append(name)
        files = self.checkNames(files,localFolderPath)
        print(f"Found {len(files)} files to dzownload!")
        print(files)
        
        executer = ThreadPoolExecutor(max_workers=4)
        print("Created a pool of 4 threads!") 

        futures = []
        for file in files:
            print(f"Scheduling download for {file}")
            future = executer.submit(self.api.downloadFile, os.path.join(onedriveFolder,file), localFolderPath)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"A download failed: {e}")


if __name__ == "__main__":
    base_dir = "/home/gavin/downloads/icloud_api_config/"
    onedrive_auth = os.path.join(base_dir, "onedrive_auth.txt")
    onedriveAuthCache = os.path.join(base_dir, "onedrive_auth_cache.json")

    with open(onedrive_auth, "r") as f:
        clientId = f.readline().strip()
        tenantId = f.readline().strip()
        scopes = f.readline().strip().split(",")

    api = oneDriveApi(tenantId, clientId, scopes, onedriveAuthCache)
    function = execution(6,api)
    function.push(r"/home/gavin/onedrive/test",r"test")