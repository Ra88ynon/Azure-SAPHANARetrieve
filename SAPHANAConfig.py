import requests
import os
import WherescapeSQLFunction as SQL
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from datetime import datetime,timedelta
import os
import logging
VesselBFULineItemURL=os.getenv("SAPxVesselBFULineItemURL")
VesselBFUAggregatedURL=os.getenv("SAPxVesselBFUAggregatedURL")
OpeningBalance=os.getenv("SAPxOpeningBalanceURL")
BalanceSheet=os.getenv("SAPxBalanceSheetURL")
auth_server_url=os.getenv("SAPxAuthServer_url")

def get_new_token():
    OAuthClientID=os.getenv("SAPHANAOAuthClientID")
    OAuthClientSecret=os.getenv("SAPHANAOAuthClientSecret")
    token_req_payload = {'grant_type': 'client_credentials'}

    token_response = requests.post(auth_server_url,
                                   data=token_req_payload, verify=False, allow_redirects=False,
                                   auth=(OAuthClientID, OAuthClientSecret))
    if token_response.status_code != 200:
        token_response.text
        logging.info(token_response.text)
    else:
        token=token_response.json()
        return token['access_token']
    
def Retrieve(Data):
    Header=[]
    Line=[]
    MastHeader=[]
    for i in Data["d"]["results"]:
        header=[]
        line=[]
        for j in i:
            if j not in MastHeader:
                MastHeader.append(j)
            header.append(j)
            line.append(i[j])
        #print(line[0])
        Header.append(header)
        Line.append(line)
    return [Header,Line,MastHeader]
def appendIfNot(Master, part):
    if part in Master:
        return Master
    for i in part:
        if i not in Master:
            Master.append(i)
    return Master
def getData(Token,URL):
    header = {
        'Authorization': f'Bearer {Token}'
    }
    Response=requests.get(URL,headers=header,timeout=300)
    
    if Response.status_code != 200:
        return Response.text
    else:
        return Response.json()
    
def appendIfNot(Master, part):
    if part in Master:
        return Master
    for i in part:
        if i not in Master:
            Master.append(i)
    return Master
def GetNumberOfLine(URL):
    token=get_new_token()
    URLPage=URL+f"&$top=1&$inlinecount=allpages"
    Datacount=getData(token,URLPage)
    return Datacount
def ExtractData(Df):
    Header=[]
    Line=[]
    MasterHead=[]
    print("extracting")
    for index,Data in enumerate(Df):
        print(str(type(Data)),":",index)
        if Data is None:
            raise Exception("None Datatype")

        elif type(Data) == str:
            raise Exception(Data)
        elif Data["d"]["results"]==[]:
            raise Exception("Empty Result")
        [head,lin,mastHeader]=Retrieve(Data)
        MasterHead=appendIfNot(MasterHead,mastHeader)
        Header.extend(head)
        Line.extend(lin)
    return MasterHead,Header,Line
def GetNumberOfLine(URL):
    token=get_new_token()
    URLPage=URL+f"&$top=1&$inlinecount=allpages"
    Datacount=getData(token,URLPage)
    return Datacount
def gettimeURL(Tablename):
    Lastdate=SQL.selectMax(Tablename,"lastchangedatetime")
    minb4=datetime.strptime(Lastdate,"%Y%m%d%H%M%S")+timedelta(seconds=1)
    minb4=minb4.strftime("%Y%m%d%H%M%S")
    requeststring=f"&$filter=lastchangedatetime ge '{minb4}'"
    return requeststring