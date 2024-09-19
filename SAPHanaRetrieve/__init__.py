from datetime import datetime 
import logging
import pandas as pd
import WherescapeSQLFunction as SQL
import SAPHANAConfig as con
import azure.functions as func
from dotenv import load_dotenv, dotenv_values
load_dotenv()
from azure.identity import DefaultAzureCredential
from datetime import timedelta
import time
#from azure.storage.blob import ContainerClient
import pyodbc as p
import os
import logging

#----------------------Initiation
Logtxt=[]
connection_string=os.getenv("blobconstring")
container_name="saphanalog"
VesselBFULineItemURL=con.VesselBFULineItemURL
VesselBFUAggregatedURL=con.VesselBFUAggregatedURL
OpeningBalance=con.OpeningBalance
BalanceSheet=con.BalanceSheet

#---------------------Require Function-----------------------------------------
def Datetimenow():
    return datetime.utcnow()+timedelta(hours=8)
utc_timestamp = datetime.utcnow()+timedelta(hours=8)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    filename = 'Log.txt',
    datefmt='%Y-%m-%d %H:%M:%S')
auth_server_url=con.auth_server_url

# def upload_files(file, filename, connection_string, container_name):#Blob storage to store import time. Hide since it is not use and potential unhide for other application
#     path = filename
#     #container_client  = ContainerClient.from_connection_string(connection_string, container_name)
#     print("Uploading files to blob storage.................")
#     #blob_client = container_client.get_blob_client(path)
#     blob_client.upload_blob(file, overwrite=True)
#     print(f'{path} uploaded to blob storage')


def compile(TableName,URL):# A compile of loops to run for an API
    logging.info(f"working on {TableName}")
    Header=[]
    Line=[]
    MasterHead=[]#Initiate List
    token=con.get_new_token()#get new token 
    URLPage=URL+con.gettimeURL(TableName)#get last import date time to get the latest data.
    print(f"Running {TableName},URL: {URLPage} ") 
    Data=con.getData(token,URLPage)
    if isinstance(Data, str):#Data return is string, indicate it is an error
        logging.info(Data)
        return
    elif Data["d"]["results"]==[]:#data is dict but return empty list
        logging.info("Empty Result")
        print("Empty Result")
        return
    [head,lin,mastHeader]=con.Retrieve(Data)#transform data into header line and Master header
    SQL.CreateTable(mastHeader,TableName)#precaution to perform 
    MasterHead=con.appendIfNot(MasterHead,mastHeader)#append master header if master header is not in list
    Header.extend(head)#append  header if header is not in list
    Line.extend(lin)#append  line if line is not in list
    for i in MasterHead:
        SQL.AddColumn(i,TableName)
    SQL.InsertDataEQHL(Header,Line,TableName)
    logging.info(f"{TableName}:{len(Line)} inserted")
    
    return len(Line)
#----------------------Main Function---------------------------------------------

def main(mytimer: func.TimerRequest) -> None:
    try:
        startime=Datetimenow()#get start time
        logging.info(f"--- start at {startime} seconds ---")#print start time
        Line=""
        Agg=""
        Line=compile("SAPHANA_API_LineItem",VesselBFULineItemURL)# SAPHANA_API_VesselBFULineItem
        Agg=compile("SAPHANA_API_Aggregated",VesselBFUAggregatedURL)# SAPHANA_API_VesselBFUAggregated
       
    

    except (Exception,TypeError,NameError)as Err:
        logging.exception(f"error {Err}")#print error
    finally:
        logging.info("done")#print complete statement
        print("done")
        Endtime=Datetimenow()
        timeconsume=Endtime - startime
        logging.info(f"--- {timeconsume} seconds ---")
        



