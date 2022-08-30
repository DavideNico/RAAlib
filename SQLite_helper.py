import dataiku
import sqlite3
import os
import pandas as pd
import time
import RAAlib.__utils__ as Utils

def log_queries(func):
    def inner(self,SQLquery,*args,**kwargs):
        start_time = time.time() +3600
        s=time.localtime(start_time)        
        print("--- Started : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", s)))
        LogInInfo=Utils.get_login_info()
        now=Utils.get_current_time()
        df= func(self,SQLquery,*args,**kwargs)
        end_time = time.time() +3600
        e=time.localtime(end_time)
        print("--- Completed : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", e)))
        run_time=round(end_time-start_time,2)
        print("--- %s seconds ---" % (run_time))
        data_to_insert={'USER':int(LogInInfo['associatedDSSUser'][0]),
                        'DATAIKU_PROJECT':LogInInfo['via'][0],
                        'AUTH_SOURCE':LogInInfo['authSource'][0],
                        'QUERY':SQLquery,
                        'TIMESTAMP':now,
                        'RUN_TIME':run_time}
        
        self.write_df_to_db(df=pd.DataFrame(data=data_to_insert),table_name='QUERY_LOG',if_exists='append',index=False)
        return(df)
    return inner



class SQLite():
    '''
    *** MA CHI E' QUEL MONA CHE SBATTE LA PORTA E LA CHIUDE URLANDO?? ***
    !!Class which wraps together the most amazing functions for querying an SQLite DB!!
    
    Example:
    
        import RAAlib.SQLite_helper as SqLite
    
        #connect to STAGE DB
            SQL=SqLite.SQLite()    

        #connect to another DB
            SQL_IB=SqLite.SQLite(DB_NAME='STAGE_IB')

        #See qhat a DB contains
            #all objects
            SQL.all_objects()
            #tables
            SQL.all_objects('table')
        #get connection string
            SQL.Conn()
        #Run a query
            SQL.run_query('SELECT * FROM v_FACT_STAGE LIMIT 5')
    
    
    '''
    
    SQL_TEMPLATES={'All Objects':"SELECT * FROM sqlite_schema",
                   'All Objects Filter':"SELECT * FROM sqlite_schema WHERE type ='{}'",
                   'Object info':"PRAGMA table_info({})",
                   'Delete by date':"DELETE FROM {} WHERE DIM_DATE_MONTH_ID in ({})",
                   'Count rows':"SELECT count(*) from {} where DIM_DATE_MONTH_ID in ({})"}     
        
    
    def __init__(self,DB_FOLDER="SQLiteDB",DB_NAME='STAGE', LOG_DB_NAME='LOG_DB', ATTACH_DBs=True):   
        '''
        This function initiate some parameters that will be used in othe functions 
        within the class
        
        Parameters
        DB_FOLDER="FINANCIALCRIMEPREVENTIONANALYTICS.SQL" - the predefined folder where all DBs are saved
        DB_NAME='STAGE' - the main DB containing CDD data
        
        self.handle is reading the content of a dataiku folder -> BASE_DB_DIR
        self.paths is retrieving all the file names in the selected dataiku folder
        self.path is retrieving the path of the dataiku folder to be able to read files from it
        self.gpn is the gpn login of the user
        '''
        self.BASE_DB_DIR=DB_FOLDER 
        self.STD_DB=DB_NAME  
        self.LOG_DB=LOG_DB_NAME
        self.handle = dataiku.Folder(self.BASE_DB_DIR,ignore_flow=True)
        self.paths = self.handle.list_paths_in_partition()
        self.path = self.handle.get_path()
        self.gpn = int(Utils.get_login_info()['associatedDSSUser'][0])
        self.global_variables=Utils.get_global_variables()
        self.AttachDB=ATTACH_DBs
        self.Conn=self.__Conn__()
        self.LogConn=self.__LogConn__()
        self.fernet=Utils.fernet_key(password='Zan',salt='Davide')
    
    def __Conn__(self):
        '''
        The function connects to the main database in the BASE_DB_DIR (SQL Dataiku folder) 
        and returns a connection string to the database
        
        also is attaching all DBs to the main - i.e. DB link
        '''
        conn = sqlite3.connect(os.path.join(self.path,"{}.db".format(self.STD_DB)))
        conn.text_factory = str
        
        cur=conn.cursor()
        if self.AttachDB==True:
            for db in self.paths:                       
                DB_name=db.split('.')[0].split('/')[1] 
                try:
                    attachDatabaseSQL        = "ATTACH DATABASE ? AS {0}".format(DB_name)
                    dbSpec=(os.path.join(self.path,DB_name+'.db'),)
                    cur.execute(attachDatabaseSQL,dbSpec)
                    
                except:
                    pass
            
        conn.create_function("COUNTRY_REGEXP", 2, Utils.country_list_regexp)
        conn.create_function("REGEXP", 2, Utils.regexp)
        conn.create_function("ENCRYPT",1, Utils.SQL_ENCRYPT)
        conn.create_function("DECRYPT",1, Utils.SQL_DECRYPT)
        conn.create_function("DO_NOTHING",1, Utils.do_nothing)
        conn.create_function("COUNTRY_MAPPING",1, Utils.country_fields_mapping)
        conn.create_function("CP_CO_VLAD",2, Utils.RE_COUNTRY_VLAD)
        
            
        return(conn) 
    
   
        
        return()
    
    def __LogConn__(self):
        '''
        The function connects to the main database in the BASE_DB_DIR (SQL Dataiku folder) 
        and returns a connection string to the database
        '''
        conn = sqlite3.connect(os.path.join(self.path,"{}.db".format(self.LOG_DB)))
        conn.text_factory = str
        return(conn)
    
    @log_queries
    def run_query(self,SQLquery,*args,**kwargs):
        '''
        Helper function which run a query against the selected DB
        '''
        if (('SELECT' in SQLquery.upper())|('PRAGMA' in SQLquery.upper()))&('CREATE ' not in SQLquery.upper())&('UPDATE ' not in SQLquery.upper())&('INSERT ' not in SQLquery.upper())&('DELETE ' not in SQLquery.upper()):
            df=pd.read_sql_query(sql=''' {} '''.format(SQLquery), con=self.Conn,*args,**kwargs)
            
            return(df)
        elif (self.gpn in [43642257, #davide
                           43641666 #zan 
                            ]):
            #cur = self.__Conn__().cursor()
            #cur.execute(SQLquery)
            #self.__Conn__().commit()
            try:
                self.Conn.execute('''{0}'''.format(SQLquery))
                self.Conn.commit()
                print('Executed: {0}'.format(SQLquery))
            except Exception as e:
                self.Conn.rollback()
                print('You did not get that one!')
                raise Exception (e)
                
        else:
            raise Exception ('User Not allowed to perform such queries, please contact Davide or Zan')
            
                
    
    def write_df_to_db(self, df, table_name,*args,**kwargs):    
        '''
        Function whihc allows to write into tables/create new tables from pandas dataframes
        '''
        
        
        if (table_name=='QUERY_LOG'):
            df['DATABASE']=self.STD_DB
            df.to_sql(name=table_name,con=self.__LogConn__(),*args,**kwargs)
            self.LogConn.commit()
        elif(self.gpn in [43642257, #davide
                          43641666 #zan 
                         ]):
            start_time = time.time() +3600
            s=time.localtime(start_time)        
            print("--- Started : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", s)))
            df.to_sql(name=table_name,con=self.Conn,*args,**kwargs)
            self.Conn.commit()
            end_time = time.time() +3600
            e=time.localtime(end_time)
            print("--- Completed : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", e)))
            run_time=round(end_time-start_time,2)
            print("--- %s seconds ---" % (run_time))
           
        elif table_name!='QUERY_LOG':
                print('{} Table successfully created/ amended'.format(table_name))
        else:
            raise Exception ('User Not allowed to perform such queries, please contact Davide or Zan')
        
            
        
    def all_objects(self,obj=None): 
        '''
        Function which returns what is contained in a DB
        
        parameters
        obj -> None is the predefined value and will return all objects in a table
               table, will return all tables
               view, will return all views
               index, will return all indexes
        '''
        if obj==None:
            print("{} Database includes the following objects:".format(self.STD_DB))
            return(self.run_query(SQLquery=self.SQL_TEMPLATES['All Objects']))
        else:
            print("{0} Database includes the following {1}(s):".format(self.STD_DB,obj))
            return(self.run_query(SQLquery=self.SQL_TEMPLATES['All Objects Filter'].format(obj)))
        print("{} Database includes the following tables:".format(self.STD_DB))
        return(self.run_query(SQLquery=self.SQL_TEMPLATES['All Tables'])) 
    
    def describe_object(self, obj_name):  
        '''
        function which returns the content of an object (table, view)
        obj_name - is the name of the object for which details are needed
        '''
        print("{} object includes the following fields:".format(obj_name))
        return(self.run_query(SQLquery=self.SQL_TEMPLATES['Object info'].format(obj_name)))
    
    def partial_refresh(self, data, table_name, DIM_DATE_MONTH_ID, DIM_DIVISION_ID, *args,**kwargs):
        
        '''
        The function will partially refresh the table (table_name) with the data included in (data)
        
        Example:
        
            SQL_DEV.partial_refresh(data=sampleDF,
                                    table_name='FACT_STAGE_SAMPLE',
                                    DIM_DATE_MONTH_ID="'202109','202106'",
                                    DIM_DIVISION_ID='IB')
        
        Parameters:
        
        data - the pandas dataframe with the same format as the table in which data will be inserted
        table_name - The name of the table in the selected DB
        DIM_DATE_MONTH_ID - a list identifying the timeframe for whihc the data needs to be refreshed
                            it is used in the SQL query as a filter on the different quarters.
                            Example: "'202109','202106'"
        DIM_DIVISION_ID - 'all' if all the divisons should be refreshed. Of course the DF should contain all the division data ;)
                          'NAMEOFDIVISION' if only one division needs to be refreshed
                          Example: "IB"
        '''
    
        
        if DIM_DIVISION_ID=='all':
                SQLquery_del=self.SQL_TEMPLATES['Delete by date'].format(table_name,DIM_DATE_MONTH_ID)
                SQLquery_count=self.SQL_TEMPLATES['Count rows'].format(table_name,DIM_DATE_MONTH_ID)                    
                n_row_deleted=self.run_query(SQLquery_count)                    
                self.run_query(SQLquery_del)
                print('{} rows deleted'.format(n_row_deleted.iloc[0][0]))
                self.write_df_to_db(df=data,table_name=table_name,if_exists='append',index=False )
                print('{} has been refreshed for {} and {} divisions. '.format(table_name,DIM_DATE_MONTH_ID,DIM_DIVISION_ID))
                    
        elif DIM_DIVISION_ID in ['GWM','P&C','AM','IB']:
                SQLquery_del=self.SQL_TEMPLATES['Delete by date'].format(table_name,DIM_DATE_MONTH_ID)
                SQLquery_del= SQLquery_del + ' AND DIM_DIVISION_ID in ("{}")'.format(DIM_DIVISION_ID)
                SQLquery_count=self.SQL_TEMPLATES['Count rows'].format(table_name,DIM_DATE_MONTH_ID)
                SQLquery_count= SQLquery_count + ' AND DIM_DIVISION_ID in ("{}")'.format(DIM_DIVISION_ID)
                n_row_deleted=self.run_query(SQLquery_count) 
                self.run_query(SQLquery_del)
                print('{} rows deleted'.format(n_row_deleted.iloc[0][0]))
                self.write_df_to_db(df=data,table_name=table_name,if_exists='append',index=False )
                print('{} has been refreshed for {} and {} divisions. '.format(table_name,DIM_DATE_MONTH_ID,DIM_DIVISION_ID))