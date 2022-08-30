import dataiku
import os
import re
import sqlite3
import os
import pandas as pd
import time
import RAAlib.__utils__ as Utils
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName



class Hyper_helper():
    
    def __init__(self, DB_FOLDER="OUTPUTS",HYPER_NAME="STAGE"):
        self.BASE_HYPER_DIR=DB_FOLDER
        self.STD_HYP=HYPER_NAME
        self.handle = dataiku.Folder(self.BASE_HYPER_DIR,ignore_flow=True)
        self.paths = self.handle.list_paths_in_partition()
        self.path = self.handle.get_path()
        self.HYP_PATH=os.path.join(self.path,(self.STD_HYP+'.hyper'))
        self.gpn = int(Utils.get_login_info()['associatedDSSUser'][0])
        self.global_variables=Utils.get_global_variables()
        self.fernet=Utils.fernet_key(password='Zan',salt='Davide')
    
    def add_doub_quotes(self,string):
        string='"'+string+'"'
        return(string)
    
    def __add_quotes_hyper_query__(self,string):
        string=re.sub(" +"," ",string)
        query_elements=string.split(" ")
        regexp1=re.compile(r'\.')
        regexp2=re.compile(r'\=')
        regexp3=re.compile(r'_')
        regexp4=re.compile(r',')
        regexp5=re.compile(r'\(')
        query=""
        for item in query_elements:
            print(item)
            if (regexp1.search(item)):
                items=item.split('.')
                items=list(map(self.add_doub_quotes,items))
                items='''.'''.join(items)
                query=query +" "+ items

            elif bool((regexp2.search(item)))&(len(item)>1)&(not item=='1=1'):
                items=item.split('=')
                items[0]='"'+items[0]+'"'
                items='''='''.join(items)
                query=query +" "+ items
            elif bool(regexp4.search(item))&(not regexp5.search(item)):
                items=item.split(',')
                items[1]='"'+items[1]+'"'
                items=''','''+items[1]
                query=query +" "+ items
            elif (regexp3.search(item)):
                query=query +' "'+ item +'" '
            
                
            else:
                query= query +" " +item
        query=re.sub(" +"," ",query)
        print(query)        
        return(query)
    
    
 

    def run_query(self,query,ATTACH_HYP=0):
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            #  Connect to an existing .hyper file (CreateMode.NONE)
            with Connection(hyper.endpoint,self.HYP_PATH,CreateMode.CREATE_IF_NOT_EXISTS) as connection: 
                if ATTACH_HYP==1:
                    for hyp in self.paths:
                        regexp=re.compile(r'hyper')
                        regexp_name=re.compile(self.STD_HYP)
                        if bool(regexp.search(hyp)) & bool(not regexp_name.search(hyp)):
                            hyp_name=hyp.split('.')[0].split('/')[1]
                            try:                                
                                connection.catalog.attach_database(os.path.join(self.path,(hyp_name + '.hyper')))
                                print('Hyper Attached:', hyp_name)
                            except:
                                pass
                result=connection.execute_query(query=self.__add_quotes_hyper_query__(query)) 
                lst=[]        
                for row in result:
                    lst.append(row)
                columns=[]  
                for c in result.schema.columns:
                    columns.append(str(c.name).replace('"',''))
                df = pd.DataFrame(lst,  columns = columns)
        return(df)
    def run_query_raw(self,query,ATTACH_HYP=0):
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            #  Connect to an existing .hyper file (CreateMode.NONE)
            with Connection(hyper.endpoint,self.HYP_PATH,CreateMode.CREATE_IF_NOT_EXISTS) as connection: 
                if ATTACH_HYP==1:
                    for hyp in self.paths:
                        regexp=re.compile(r'hyper')
                        regexp_name=re.compile(self.STD_HYP)
                        if bool(regexp.search(hyp)) & bool(not regexp_name.search(hyp)):                            
                            hyp_name=hyp.split('.')[0].split('/')[1]
                            try:                                
                                connection.catalog.attach_database(os.path.join(self.path,(hyp_name + '.hyper')))
                                print('Hyper Attached:', hyp_name)
                            except:
                                pass
                result=connection.execute_query(query=query) 
                lst=[]        
                for row in result:
                    lst.append(row)
                columns=[]  
                for c in result.schema.columns:
                    columns.append(str(c.name).replace('"',''))
                df = pd.DataFrame(lst,  columns = columns)
        return(df)
        
    def get_column_name_raw(self, query):
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            #  Connect to an existing .hyper file (CreateMode.NONE)
            with Connection(hyper.endpoint,self.HYP_PATH) as connection: 
                result=connection.execute_query(query=query) 
                lst=[]        
                for row in result:
                    lst.append(row)
                columns=[]  
                for c in result.schema.columns:
                    columns.append(str(c.name).replace('"',''))                
            return(columns)
        
    def describe_object(self,hyper_path=None,hyper_name=None):
        if (hyper_path is None)&(hyper_name is None):
            hyper_path=self.path
            hyper_name=self.STD_HYP
        else:
            print('Hyper currently in use:\n',hyper_path,hyper_name)
            
            
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            #  Connect to an existing .hyper file (CreateMode.NONE)
            with Connection(hyper.endpoint,os.path.join(hyper_path,(hyper_name+'.hyper'))) as connection: 
                schemas=connection.catalog.get_schema_names()
                print(schemas) 
        
    def SQLite_to_Hyper(self, SQLiteObj,SQLiteSource,HyperTarget,Filter):
        '''
        The function allows to insert data, with delta logic, from SQlite to Hyper
        Parameters:
             -SQLiteConn: the connection object to the SQLite DB
             -SQLiteSource: the string representing the name of the object from where the data should be copied 
             -HyperTarget: a list representing the name of the schema and the object where the data should be copied into
                           ex. ['CORE','CDD_CORE']
             -Filter: a string including the Filters for the delta logic 
             
                      ex. 
                          "and DIM_SOURCE_ID in ('AMUS','CHCRR') and DIM_DATE_MONTH_ID in (202206)"
        
        '''
        
        columns=self.get_column_name_raw(r''' SELECT * from {0} limit 1   '''.format(TableName(HyperTarget[0], HyperTarget[1])))
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        #  Connect to an existing .hyper file (CreateMode.NONE)
            with Connection(hyper.endpoint,self.HYP_PATH,CreateMode.CREATE_IF_NOT_EXISTS) as connection:
                
                #Delete from Hyper using Delta logic
                command=r"DELETE FROM {0} WHERE 1=1 {1}".format(TableName(HyperTarget[0], HyperTarget[1]),self.__add_quotes_hyper_query__(Filter))
                print(command)
                row_count = connection.execute_command(command)
                print('deleted:', row_count,'rows')
                
                #Insert into Hyper querying from SQLite
                with Inserter(connection, TableName(HyperTarget[0], HyperTarget[1])) as inserter:
                    
                    col_str=','.join(columns)
                    # instead of * inject fields of Hyper table
                    sql_query='SELECT {0} FROM {1} where 1=1 {2} '.format(col_str,SQLiteSource,Filter)
                    print(sql_query)
                    print('Quering from', SQLiteSource,'..')
                    for a in SQLiteObj.Conn.cursor().execute(sql_query).fetchall():                
                        inserter.add_row(                    
                            list(a)
                            )
                    try:
                        inserter.execute() 
                        print('Successfully inserted', Filter.replace('and','-'))
                    except Exception as e:
                        print(e)
            #self.defragment_hyper(hyper)
                
    def defragment_hyper(self, hyper):
        path=os.path.join(self.path,self.STD_HYP +'.hyper')
        path_output=os.path.join(self.path,'defr_'+ self.STD_HYP +'.hyper')

        with Connection(hyper.endpoint) as connection:
            # Connect to the input and output databases
            # Create the output Hyper file or overwrite it
            catalog = connection.catalog
            catalog.drop_database_if_exists(path_output)
            catalog.create_database(path_output)
            catalog.attach_database(path_output, alias="defragmented_database")
            catalog.attach_database(path, alias='input_database')

            # Process all tables of all schemas of the input Hyper file and copy them into the output Hyper file
            for input_schema_name in catalog.get_schema_names("input_database"):
                for input_table_name in catalog.get_table_names(input_schema_name):
                    output_table_name = TableName("defragmented_database", input_schema_name.name, input_table_name.name)
                    output_table_definition = TableDefinition(output_table_name, catalog.get_table_definition(input_table_name).columns)
                    catalog.create_schema_if_not_exists(output_table_name.schema_name)
                    catalog.create_table(output_table_definition)
                    connection.execute_command(f"INSERT INTO {output_table_name} (SELECT * FROM {input_table_name})")
                    print(f"Successfully converted table {input_table_name}")
            print(f"Successfully defragmented ")
            os.remove(path)
            os.rename(path_output, path)
            
    def create_hyper_table(self, SQLiteObj, SQLiteSource, HyperTarget):
        '''
        the function is creating tables in hyper
        
        Parameters:
             -SQLiteConn: the connection object to the SQLite DB
             -SQLiteSource: the string representing the name of the object from where the data should be copied 
             -HyperTarget: a list representing the name of the schema and the object where the data should be copied into
                           ex. ['CORE','CDD_CORE']
             -Filter: a string including the Filters for the delta logic 
             
                      ex. 
                          "and DIM_SOURCE_ID in ('AMUS','CHCRR') and DIM_DATE_MONTH_ID in (202206)"
        
        '''
        if SQLiteSource[:2].upper()=='V_':
            SQLiteObj.run_query('''DROP TABLE IF EXISTS {0}_TEMPLATE '''.format(HyperTarget[1]))
            SQLiteObj.run_query('''CREATE TABLE {0}_TEMPLATE as SELECT * from {1} limit 100'''.format(HyperTarget[1],SQLiteSource))
            print('created temporary table for getting schema and format')
            fields=SQLiteObj.describe_object('{0}_TEMPLATE'.format(HyperTarget[1]))            
            SQLiteObj.run_query('''DROP TABLE IF EXISTS {0}_TEMPLATE '''.format(HyperTarget[1]))
            print('temporary table dropped')
        else:
            fields=SQLiteObj.describe_object(SQLiteSource)
            
        type_dict={'INT':'big_int()','TEXT':'text()','INTEGER':'big_int()','REAL':'double()','':'text()','NUM':'big_int()'}
        columns=[]
        path=os.path.join(self.path,self.STD_HYP +'.hyper')
        for index,row in fields.iterrows():
            #print('''TableDefinition.Column('{0}', SqlType.{1}),'''.format(row['name'],type_dict[row['type']]))
            columns.append('''TableDefinition.Column('{0}', SqlType.{1}),'''.format(row['name'],type_dict[row['type']]))
        columns="".join(columns)
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            print('The Hyper ')
            with Connection(hyper.endpoint, path, CreateMode.CREATE_IF_NOT_EXISTS) as connection:    
                print("The connection to the Hyper file is open.")
                connection.catalog.create_schema_if_not_exists(HyperTarget[0])
                print(HyperTarget[0], ': Schema already exists and it will not be recreated.')
                table = TableDefinition(TableName(HyperTarget[0],HyperTarget[1]),list(eval(columns)))
                print(table)
                print("The table {0} is now defined.".format(HyperTarget[1]))
                connection.catalog.create_table_if_not_exists(table)
                print("The connection to the Hyper extract file is closed.")
            print("The HyperProcess has shut down.")