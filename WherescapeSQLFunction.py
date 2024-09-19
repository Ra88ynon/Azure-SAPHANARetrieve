import json
import datetime as d
import pyodbc as p
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()
connStr =os.getenv("WherescapeconnStr")
conn = p.connect(connStr)
mycursor = conn.cursor()
def CreateTable(header,TableName):
    TableCreate=f"IF OBJECT_ID(N'{TableName}', N'U') IS NULL CREATE TABLE {TableName}("+",".join([i+" Varchar(MAX)" for i in header])+")"
    print(TableCreate)
    mycursor.execute(TableCreate)
    conn.commit()
    print(f"Table {TableName} Created")
def DeleteTable(TableName):
    TableDelete=f"IF OBJECT_ID(N'{TableName}', N'U') IS NOT NULL DELETE from {TableName}"

    mycursor.execute(TableDelete)
    count=mycursor.rowcount
    conn.commit()
    
    return count
def InsertData(header,Data,TableName):
    tem2 = ",".join([i for i in header])
    for line in Data:
        tem=",".join(["NULL" if i is None else "'"+str(i).replace("'","''")+"'" for i in line])
        print(tem2,"--")
        print(tem,"--")
        mycursor.execute(f"""INSERT INTO {TableName} ({tem2}) VALUES ({tem})""")
    conn.commit()
def InsertDataEQHL(header,line,TableName):

    for  Header ,Line in zip(header,line):
        tem2 = ",".join([i for i in Header])
        tem=",".join(["NULL" if i is None else "'"+str(i).replace("'","''")+"'" for i in Line])
        mycursor.execute(f"""INSERT INTO {TableName} ({tem2}) VALUES ({tem})""")
    
    conn.commit()
    
    print(f"table {TableName} Created")
    
def selectTable (tableName,Key="*",Unique=0,condition=""):
    distinct=""
    if Unique==1:
        distinct="distinct"
    if condition:
        condition=f"where {condition}"
    mycursor.execute(f"""Select {distinct} {Key} from {tableName} {condition}""")

    return mycursor.fetchall()
def InsertDataDetail(header,Data,TableName):
    for Header, Line in zip(header,Data):
        Variable=",".join([i for i in Header])
        Value=",".join(["'"+str(i).replace("\n","").replace("'","")+"'" for i in Line])
        mycursor.execute(f"""INSERT INTO {TableName} ({Variable}) VALUES ({Value})""")
    print("complete")
    conn.commit()
def AddColumn(column, Table):
    ColumnCreate=f'''IF COL_LENGTH('{Table}', '{column}') IS NULL
    BEGIN
    ALTER TABLE {Table}
    ADD {column} Varchar(Max)
    END'''
    mycursor.execute(ColumnCreate)
    conn.commit()
    
def InsertIfColumnNotExist(TableName,ColumnName):
    mycursor.execute(f"""IF NOT EXISTS (
        SELECT * FROM sys.columns 
        WHERE object_id = OBJECT_ID(N'{TableName}') AND name = '{ColumnName}'
        )
        BEGIN
            ALTER TABLE {TableName} ADD  {ColumnName} varchar(MAX) ;
        END;""")
    
    conn.commit()
# def closeSQL():
#     mycursor.close()
#     conn.close()
def selectMax(Tablename,column):
    SelectTable=f"select top 1 {column} from {Tablename} order by {column} desc"
    mycursor.execute(SelectTable)
    Maxval = mycursor.fetchall()[0][0]
    return Maxval
