import sqlite3
import dataiku
import os
import pandas as pd

#Create connection to SQLite STAGE DB.
#using the SQLite_Helper will not work bcause of circular references while using the variables in __utils__
handle = dataiku.Folder('SQLiteDB',ignore_flow=True)
paths = handle.list_paths_in_partition()
path = handle.get_path()
conn = sqlite3.connect(os.path.join(path,"STAGE.db"))
conn_raw = sqlite3.connect(os.path.join(path,"RAW.db"))

##################################
# Country codes static variables #
##################################

country_mapping=pd.read_sql('''SELECT distinct ALPHA3_CODE as CODES, ALPHA2_CODE from REF_COUNTRY_CODES
                                 UNION 
                                 SELECT distinct COUNTRY_NAME as CODES, ALPHA2_CODE from REF_COUNTRY_CODES
                                 ''', con=conn)
ALPHA2_COUNTRY_LIST=list(country_mapping.ALPHA2_CODE.unique())
CODE_COUNTRY_MAP_DICT=dict(zip(list(country_mapping.CODES),list(country_mapping.ALPHA2_CODE)))
    


##################
# Hashed columns #
##################

col_to_hash=pd.read_sql(''' SELECT * FROM DIM_SOURCE ''', con=conn)