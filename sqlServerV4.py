import pandas as pd
import pyodbc
import os

def sqlServerConnection(servername, databasename):
    connectionString = "DRIVER={" + "sql server};" + \
                       "SERVER={};DATABASE={};Trusted_Connection=yes".format(servername,databasename)
    print(connectionString)
    connection = pyodbc.connect(connectionString)
    return connection

def getColumns():
    ColumnsQuery = """SELECT
                    tbl.name 'Table Name',
                    c.name 'Column Name',
                    t.Name 'Data Type',
                    c.max_length 'Max Length' ,
                    c.precision ,
                    c.scale ,
                    c.is_nullable,
                    ISNULL(i.is_primary_key, 0) 'Primary Key'
                FROM    
                    sys.columns c
                INNER JOIN 
                    sys.types t ON c.user_type_id = t.user_type_id
               LEFT OUTER JOIN 
                    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                LEFT OUTER JOIN 
                    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                inner join sys.tables tbl on tbl.object_id=c.object_id 
                inner join sys.schemas s on tbl.schema_id = s.schema_id and s.name='dbo' and tbl.lob_data_space_id=0
                """
    Columns = pd.read_sql(ColumnsQuery, conn)
    return Columns

def getTableSize():
    TableSizeQuery="""SELECT
s.Name AS SchemaName,
t.Name AS TableName,
c.Name AS Columnname,
p.rows AS RowCounts,
CAST(ROUND((SUM(a.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Used_MB,
CAST(ROUND((SUM(a.total_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Total_MB
FROM sys.tables t
INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id and s.name='dbo' and t.lob_data_space_id=0
INNER JOIN sys.columns c ON c.OBJECT_ID = t.object_id
GROUP BY t.Name, s.Name, p.Rows,c.Name
ORDER BY s.Name, t.Name, c.Name"""

    TableSize = pd.read_sql(TableSizeQuery, conn)

    return TableSize

if __name__ == '__main__':
    try:
        servername=input("Provide ServerName : ") #"VMT7246\MSSQLSERVER01",
        databasename = input("Please enter Database Name : ")
        conn = sqlServerConnection(servername,databasename)
    except Exception as e:
        print("Runtime ERROR: {}".format(e))
#ske
    print("\nGetting Tables Information")
    try:
        TableSize = getTableSize()
        print(TableSize)
    except Exception as e:
        print("Runtime ERROR: {}".format(e))
    print("\nGetting Columns Information")
    try:
        Columns = getColumns()
        print(Columns)
    except Exception as e:
        print("Runtime ERROR: {}".format(e))




