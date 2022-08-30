import dataiku
import sys, os
import re
import sqlite3
import pandas as pd
import time
import RAAlib.__utils__ as Utils
from datetime import datetime, timedelta

def create_load_request(SQLiteObj,filters,variables):
    '''
    This function is creating the LOAD_REQUEST table which is used to run the flow.
    Parameters:
     -SQLiteObj: the connection object to the SQLite DB
     
     -filters: scope of the load request
     -variables: string containing the variables to be executed
            
    '''
    blockPrint()
    enablePrint()
    
    exec(variables,globals())
    print(dim_metric_family_id)
    timestamp_now=int((datetime.now() + timedelta(0,3600)).strftime('%Y%m%d%H%M%S'))
    blockPrint()
    SQLiteObj.run_query('''
    INSERT INTO
    DIM_LOAD_REQUEST
    (
     DIM_LOAD_ID,
     DIM_METRIC_FAMILY_ID,
     DIM_SOURCE_ID,
     DIM_DATE_MONTH_ID,
     DIM_LAYER_ID,
     DIM_LAYER_NAME,
     DIM_STEP_ID,
     DIM_STEP_NAME,
     DIM_STEP_CODE,
     DIM_STATUS,
     DIM_SOURCE_APPLICABILITY,
     DIM_STEP_DESCRIPTION,
     DIM_RECIPE_LINK,
     VARIABLES,
     STEP_COMPLETE,
     START_TIME,
     END_TIME,
     RUN_TIME
    )

    SELECT 
    {timestamp_now} AS DIM_LOAD_ID
    ,"{dim_metric_family_id}" AS DIM_METRIC_FAMILY_ID
    ,"{dim_source_id}" AS DIM_SOURCE_ID
   ,"{dim_date_month_id}" AS DIM_DATE_MONTH_ID
    ,*
    ,'{variables}' AS VARIABLES
    ,0 AS STEP_COMPLETE
    ,'' AS START_TIME
    ,'' AS END_TIME
    ,0 AS  RUN_TIME
    FROM DIM_LOAD_MASTER 
    WHERE 1=1 
    AND DIM_STATUS=1
    {filters}

    ORDER BY DIM_STEP_ID ASC

    '''.format(dim_metric_family_id=dim_metric_family_id,
               dim_source_id=dim_source_id,
               dim_date_month_id=dim_date_month_id,                   
               filters=filters,
               variables=variables,
               timestamp_now=timestamp_now
              ))
    enablePrint()
    return(timestamp_now)

    # Disable
def blockPrint():
    sys._jupyter_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')    

# Restore
def enablePrint():
    sys.stdout = sys._jupyter_stdout 
    
def run_load_request(SQLiteObj,filter_load_request=''):
    blockPrint()
    df_load_request=SQLiteObj.run_query('''
    SELECT *  FROM DIM_LOAD_REQUEST

    WHERE 1=1 
    AND STEP_COMPLETE=0
    {0}

    ORDER BY DIM_STEP_ID ASC
    '''.format(filter_load_request)
    )                       
    #return (df_load_request)
    
    for index,row in df_load_request.iterrows():
        #dim_source_id=row.DIM_SOURCE_ID
        #dim_date_month_id=row.DIM_DATE_MONTH_ID
        #dim_metric_family_id=row.DIM_METRIC_FAMILY_ID
        enablePrint()
        print('LAYER NAME:',row.DIM_LAYER_NAME,'\tSTEP_NAME:',row.DIM_STEP_NAME,'\tSTEP_ID',row.DIM_STEP_ID,'\tLOAD_ID',row.DIM_LOAD_ID)
        blockPrint()
        exec(row.VARIABLES)
        #print(row.DIM_STEP_CODE)
        try:
            SQLiteObj.run_query('''UPDATE DIM_LOAD_REQUEST SET START_TIME=DATETIME('now')
                                            WHERE DIM_LOAD_ID={0} and DIM_STEP_ID={1}'''.format(row.DIM_LOAD_ID,row.DIM_STEP_ID))
            exec(Utils.frombits(row.DIM_STEP_CODE).format(dim_source_id=dim_source_id,
                                                       dim_date_month_id=dim_date_month_id,
                                                       dim_metric_family_id=dim_metric_family_id))
            SQLiteObj.run_query('''UPDATE DIM_LOAD_REQUEST SET STEP_COMPLETE=1, END_TIME=DATETIME('now'), RUN_TIME=(JULIANDAY(DATETIME('now'))-JULIANDAY(START_TIME))*86400
                                            WHERE DIM_LOAD_ID={0} and DIM_STEP_ID={1}'''.format(row.DIM_LOAD_ID,row.DIM_STEP_ID))
            
        except Exception as e:
            enablePrint()
            print(e)
            break
    enablePrint()

def complete_all_steps(SQLiteObj):
    SQLiteObj.run_query(''' UPDATE DIM_LOAD_REQUEST SET STEP_COMPLETE=1 WHERE STEP_COMPLETE=0 ''')
    print('All steps are now set as completed')