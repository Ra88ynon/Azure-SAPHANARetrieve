import snowflake.connector as sf
# import json
# import datetime as d
# import pyodbc as p
import os
from dotenv import load_dotenv

load_dotenv()
# connStr = os.getenv("WherescapeconnStr")
snowflake_user = os.getenv("snowflake_user")
snowflake_password = os.getenv("snowflake_password")
snowflake_account = os.getenv("snowflake_account")
snowflake_warehouse = os.getenv("snowflake_warehouse")
snowflake_database = os.getenv("snowflake_database")
snowflake_schema = os.getenv("snowflake_schema")
snowflake_role = os.getenv("snowflake_role")
conn = sf.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse,
    role=snowflake_role
)

# conn = p.connect(connStr)
mycursor = conn.cursor()


def CreateTable(header, TableName):
    TableCreate = f"CREATE TABLE IF NOT EXIST {TableName}(" + ",".join(
        [i + " Varchar(4000)" for i in header]) + ")"
    print(TableCreate)
    mycursor.execute(TableCreate)
    # conn.commit()
    print(f"Table {TableName} Created")


def DeleteTable(TableName):
    TableDelete = f"TRUNCATE TABLE IF EXISTS {TableName}"

    mycursor.execute(TableDelete)
    count = mycursor.rowcount  # Snowflake does not support "number of rows deleted/truncated" at the momoent. Always -1
    # conn.commit()

    return count


def InsertData(header, Data, TableName):
    tem2 = ",".join([i for i in header])
    for line in Data:
        tem = ",".join(["NULL" if i is None else "'" + str(i).replace("'", "''") + "'" for i in line])
        print(tem2, "--")
        print(tem, "--")
        mycursor.execute(f"""INSERT INTO {TableName} ({tem2}) VALUES ({tem})""")
    # conn.commit()


def InsertDataEQHL(header, line, TableName):
    for Header, Line in zip(header, line):
        tem2 = ",".join([i for i in Header])
        tem = ",".join(["NULL" if i is None else "'" + str(i).replace("'", "''") + "'" for i in Line])
        mycursor.execute(f"""INSERT INTO {TableName} ({tem2}) VALUES ({tem})""")

    # conn.commit()

    print(f"table {TableName} Created")


def selectTable(tableName, Key="*", Unique=0, condition=""):
    distinct = ""
    if Unique == 1:
        distinct = "distinct"
    if condition:
        condition = f"where {condition}"
    mycursor.execute(f"""Select {distinct} {Key} from {tableName} {condition}""")

    return mycursor.fetchall()


def InsertDataDetail(header, Data, TableName):
    for Header, Line in zip(header, Data):
        Variable = ",".join([i for i in Header])
        Value = ",".join(["'" + str(i).replace("\n", "").replace("'", "") + "'" for i in Line])
        mycursor.execute(f"""INSERT INTO {TableName} ({Variable}) VALUES ({Value})""")
    print("complete")
    # conn.commit()


# The following two functions are identical
def AddColumn(column, Table):
    ColumnCreate = f"ALTER TABLE {Table} ADD COLUMN IF NOT EXISTS {column} Varchar(4000)"
    mycursor.execute(ColumnCreate)
    # conn.commit()

def InsertIfColumnNotExist(TableName, ColumnName):
    ColumnCreate = f"ALTER TABLE {TableName} ADD COLUMN IF NOT EXISTS {ColumnName} Varchar(4000)"
    mycursor.execute(ColumnCreate)


# def closeSQL():
#     mycursor.close()
#     conn.close()
def selectMax(Tablename, column):
    SelectTable = f"SELECT {column} FROM {Tablename} ORDER BY {column} DESC LIMIT 1"
    mycursor.execute(SelectTable)
    Maxval = mycursor.fetchall()[0][0]
    return Maxval
