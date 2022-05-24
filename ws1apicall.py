import requests
import json
import pandas as pd
from azure.storage.blob import BlobClient
import time
import datetime
start_time = time.time()
wsdate = str(datetime.date.today())
def uploadcsv(df, filename, overwrite):
    containerName = "github"
    blobName = wsdate + "/" + filename + ".csv"
    output = df.to_csv(index_label="idx", encoding="utf-8")
    connection_string = "DefaultEndpointsProtocol=https;AccountName=***;AccountKey=****"
    blob = BlobClient.from_connection_string(conn_str=connection_string, container_name=containerName,
                                          blob_name=blobName)
    if overwrite == "0":
        blob.upload_blob(output,overwrite=True)
    else:
        blob.upload_blob(output)




def getDevices(pagenumber):
    devices_api_url = "https://****.awmdm.com/api/mdm/devices/search?Page="+ str(pagenumber)
    r = requests.get(devices_api_url, headers={'Content-Type': 'application/json',
                                               'Authorization': '***',
                                               'aw-tenant-code': '***'})
    jsonObject = r.json()
    return jsonObject

def getapps(appid):
    devices_api_url = "https://as1106.awmdm.com/api/mdm/devices/"+ appid + "/apps/search"
    r = requests.get(devices_api_url, headers={'Content-Type': 'application/json',
                                               'Authorization': 'Basic Z3V3dy5uZXRcUmFzaG1pLk1BQGRpYWdlby5jb206QmFuZ2Fsb3JlQDIwMjE=',
                                               'aw-tenant-code': 'n1AkDs+X+3lsAHFqqVnCxuFT7EiiNSOZr4FyfpeOGY0='})
    jsonObject = r.json()
    return jsonObject["app_items"]

error = 0
devices_uuid = set()
jsonObject = getDevices(0)
PageSize = int(jsonObject["PageSize"])
TotalRecords = int (jsonObject["Total"])
pages =  int(TotalRecords/PageSize)+1

# Create the pandas DataFrame
appslist = list()
logs = list()
head = ["Messages"]
counter=0



for pagecount in range(pages):
    logs.append ('Processing Page ' + str(pagecount))
    jsonObject = getDevices(pagecount)
    value = jsonObject['Devices']
    for key in value:
        newvalue = key
        for key in newvalue:
            if key == 'Uuid':
                try:
                    displayitem = str(newvalue[key])
                    lastappdetails = getapps(str(newvalue[key]))
                    for key in lastappdetails:
                        apps = key
                        appslist.append([apps["name"], apps["assignment_status"], apps["assigned_version"], apps["installed_status"], apps["installed_version"], apps["latest_uem_action"], apps["latest_uem_action_time"],displayitem])
                        counter = counter + 1
                        logs.append (str(counter) + ">>>>>>" + displayitem)
                        print(str(counter) + ">>>>>>" + displayitem)
                except ValueError:
                    logs.append ("Error occured while fetching from  >>>>>> " + str(newvalue[key]))

    if pagecount==0:
        df = pd.DataFrame(appslist, columns = ['Name', 'assignment_status','assigned_version','installed_status','installed_version','latest_uem_action','latest_uem_action_time','DeviceId'])
        uploadcsv(df, "page" + str(pagecount), "0")
        logs.append("Took" + str((time.time() - start_time)) + "seconds to complete the page " + str(pagecount))
        logsdf = df = pd.DataFrame (logs, columns = head)
        uploadcsv(logsdf, "logs", "0")
        exit()











