# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import snowflake.connector as sf

# Function To Convert the Datatype from Excel "Name" cell
def convert_name_cell(cell):
    if cell.lower() in ("char","nvarchar","varchar","nchar","ntext","text" ):        
        return 'TEXT'
    elif cell.lower() in("bit","tinyint","smallint","int","bigint","numeric","decimal","money","smallmoney","real","float"):
        return 'NUMBER'
    elif cell.lower() in("date","time","datetime","datetime2"):
        return 'DATETIME'
    elif cell.lower() in ("binary","varbinary","image"):
        return 'BINARY'
    elif cell.lower()in ("boolean"):
        return 'BOOLEAN'
    
    return cell

def sfConnectivity():
    sf_credentials=dict(pd.read_excel(srcpath,sheet_name='properties').to_dict('r')[0])
    conn=sf.connect(user=sf_credentials.get('user'),\
           password=sf_credentials.get('password')\
           ,account=sf_credentials.get('account')\
           ,warehouse=sf_credentials.get('warehouse')\
           ,database=sf_credentials.get('database')\
           ,schema=sf_credentials.get('schema'))
    return conn

def schemaCretaion():    
    ex_df=pd.read_excel(srcpath,sheet_name='Tables', converters={'name':convert_name_cell})
    val2= '('+ ex_df['actual_precision'].map(str)+','+ex_df['actual_scale'].map(str)+')'
    val3= '('+ ex_df['actual_length'].map(str)+')'   
    # adding a new column in existing dataframe **
    ex_df["concatenated_value"]=np.where(ex_df["name"]=="NUMBER",val2,val3)
    ex_df["nullable_value"]=np.where(ex_df["is_nullable"]==1,'NULL','NOT_NULL')      
    tables=set(ex_df['Table_Name'])
    final_sql=''
    for tbl in tables:
        # concatenating every required field to make a snowflake DDL Schema **
        df=ex_df.where(ex_df['Table_Name']==tbl).dropna()
        formatted=df['COL_Name'] + ' ' + df['name'] + df['concatenated_value'] + ' ' + df['nullable_value']
        sql_str=''
        for lines in formatted:
            sql_str += lines +","
        #Constructing create table statement
        new_sql='CREATE OR REPLACE TABLE ' + tbl + ' (' + sql_str.rstrip(",") +');'
       
        final_sql+= new_sql
                
    try:
       conn.execute_string(final_sql)      
    except Exception as e:
        print("Runtime ERROR: {}".format(e))

def awsCredentials():
    sf_credentials=dict(pd.read_excel(srcpath,sheet_name='properties').to_dict('r')[0])
    url=sf_credentials.get('url')
    key_id=sf_credentials.get('aws_key_id')
    secret_key=sf_credentials.get('aws_secret_key')
    
    return url,key_id,secret_key

def stageDataLoad():
    url,key_id,secret_key=awsCredentials()
    
    stage="CREATE OR REPLACE STAGE copy_stage url='" + url \
    + "' credentials=(aws_key_id='"+key_id+"' aws_secret_key='" + \
    secret_key +"') file_format = (type = csv field_delimiter = ',' skip_header = 1) "
    
    ex_df=pd.read_excel(srcpath,sheet_name='Tables',usecols='A')
    tables=set(ex_df['Table_Name'])
    copy_load_cmds=''
    for tbl in tables:
        load_cmds='COPY INTO ' + tbl + " from @copy_stage pattern='.*" + tbl +"*.csv' on_error='skip_file' force=TRUE;"
        copy_load_cmds += load_cmds
               
    try:
        conn.execute_string(stage)
    except Exception as e:
        print("Runtime ERROR: {}".format(e))
        
    try:
        conn.execute_string(copy_load_cmds)
    except Exception as e:
        print("Runtime ERROR: {}".format(e))

def directDataLoad():
    url,key_id,secret_key=awsCredentials()
    ex_df=pd.read_excel(srcpath,sheet_name='Tables',usecols='A')
    tables=set(ex_df['Table_Name'])
    copy_load_cmds=''
    for tbl in tables:
        load_cmds='COPY INTO ' + tbl + " from " + url + tbl + "/ credentials=(aws_key_id='"+key_id+"' aws_secret_key='" + secret_key +"') file_format = (type = csv field_delimiter = ',' skip_header = 1) on_error='skip_file' force=TRUE;"
        copy_load_cmds += load_cmds  
        
    try:
        conn.execute_string(copy_load_cmds)
    except Exception as e:
        print("Runtime ERROR: {}".format(e))
        
if __name__ == '__main__':
    option=input("Provide the Option\n(schema/stage/direct) : ")
    context="Provided input {}".format(option)
    if option not in ('schema','stage','direct'):
        print(context + " invalid")
        exit(-1)
    else:
        print(context)
    
        srcpath=input("Input file path\n(C:\\Users\\<>\\<>\\input.xlsx) : ")
        sheetValidation=pd.ExcelFile(srcpath)
        sheetValidation=pd.ExcelFile(srcpath)#,sheet_name='Tables',usecols='A')

        for sheetName in sheetValidation.sheet_names:
            if sheetName not in ('Tables', 'DataTypes', 'properties'):
                print("Required {} are not exists".format(sheetName))
                exit(-1)
        else: 
            conn=sfConnectivity()
            if option.lower() == 'schema':
                schemaCretaion()
            elif option.lower() == 'stage':
                stageDataLoad()
            elif option.lower() =='direct':
                directDataLoad()    