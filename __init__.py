import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import os
import re
import time
import sqlite3
import RAAlib.SQLite_helper
import RAAlib.__utils__ as Utils
import RAAlib.UBSTableau as UBSTableau
from datetime import datetime, timedelta
from shutil import copyfile
import RAAlib.__static_data__ as SD
import importlib
importlib.reload(RAAlib.__static_data__)

def read_files_from_managed_folder(folder_name,
                                   file_name='all',
                                   datefield=None,
                                   csv_sep=',',
                                   filt_pattern=None,
                                   print_file_name=False,
                                   print_DF_shape=False,
                                   print_column_names=False,
                                   add_file_info=False,
                                   func_to_apply=Utils.do_nothing,
                                   sheet_number=0,
                                   header=0,
                                   regex_group=1,
                                   csv_enc="ISO-8859-1",
                                   **kwargs):
    
    """
    EXAMPLE:
    
    result=RAAlib.read_files_from_managed_folder("[RAW] Landing Folder",
                                         file_name='ABC_GESAP_20210201_1.csv',
                                         csv_sep=';',
                                         filt_pattern="ABC_GESAP",
                                         print_file_name=True,
                                         print_DF_shape=True,
                                         add_file_info=True,
                                         func_to_apply=Utils.do_nothing)
    
    
    
    Helper function for reading files from Managed Folders in Dataiku (i.e. from a data lake)
    
    Params:
    folder_name (str)        -> Name of the Managed folder 
    file_name   (str)        -> default is 'all' - read all files in the folder
                                specific file name with extension
    datafield   (list)       -> default is None
                                list of column names
    csv_sep     (str)        -> default is a comma ','
    filt_pattern(str)        -> this is a regex pattern to filter files in the selected managed folder
                                default is None 
                                regex pattern (i.e. '[1-9]{2}[A-Z].*)
    print_file_name (bool)   -> print file name while running the function
    print_DF_shape  (bool)   -> print DF shape (number of columns and rows) while running the function
    print_column_names (bool)-> print column name while running the function
    add_file_info (bool)     -> default False
                                if True, add to the output dataframe FILENAME, METRIC FAMILY, METRIC SOURCE, REFERENCE DATE, VERSION 
    func_to_apply (function) -> a function that can be applied on each single file. it can be an ad hoc function or an already existent 
                                function.
                                default 'do nothing function' (i.e. Utils.do_nothing) 
    sheet_number (int, string) -> number or name of the sheet to read if an excel file is read.
                                  default is 0
    header (int)             -> row in whihc headers are
                                default 0
    regex_group (int)        -> number of the group to take into account for matching purposes in regex
                                default is 1, usually do not need to modify this parameter
    **kwargs                 -> all additional paramaters for pandas read_csv and read_excel functions
                             
    """
    #internal parameters
    Excel=['xlsx','xls']
    Csv=['csv']
    Txt=['html','txt']
    pattern=r'\.(.*)'
    
    
    folder=dataiku.Folder(folder_name,ignore_flow=True)
    paths = folder.list_paths_in_partition()
    path_filenames = [os.path.join(folder.get_path(),os.path.basename(path)) for path in paths]
    filenames = [os.path.basename(path) for path in paths]    
    
    if filt_pattern!=None:
        r = re.compile(filt_pattern)
        filenames = list(filter(r.match, filenames))
        path_filenames=[os.path.join(folder.get_path(),os.path.basename(filename)) for filename in filenames]
    
    if file_name=='all':
        
        Df=pd.DataFrame()
        for file in path_filenames:
            
            
            if Utils.get_substr_from_file_name(pattern,file,group=regex_group) in Excel:
                res=pd.read_excel(file,parse_dates=datefield,sheet_name=sheet_number,header=header,**kwargs)
               
            elif Utils.get_substr_from_file_name(pattern,file,group=regex_group) in Csv:
                res=pd.read_csv(file,parse_dates=datefield,sep=csv_sep,header=header,encoding = csv_enc,**kwargs)
            
            res=func_to_apply(res) 
        
            if add_file_info==True:
                res['FILENAME']=os.path.basename(file)
                add_info=Utils.split_string(str(os.path.basename(file)))
                res['METRIC_FAMILY']=add_info[0]
                res['METRIC_SOURCE']=add_info[1]
                res['REFERENCE_DATE']=add_info[2]
                res['VERSION']=Utils.split_string(add_info[3],sep='.')[0]
        
        #print statements - create a decorator
            if print_file_name==True:
                print(os.path.basename(file))        
            if print_DF_shape==True:
                print("num of rows,columns: {}".format(res.shape))
            if print_column_names==True:
                print(list(res.columns))
          
                
            Df=Df.append(res)
            
        return(Df)
    else: 
        if Utils.get_substr_from_file_name(pattern,file_name,group=regex_group) in Excel:
            res=pd.read_excel(os.path.join(folder.get_path(),os.path.basename(file_name)),parse_dates=datefield,sheet_name=sheet_number,header=header,**kwargs)
        elif Utils.get_substr_from_file_name(pattern,file_name,group=regex_group) in Csv:
            res=pd.read_csv(os.path.join(folder.get_path(),os.path.basename(file_name)),parse_dates=datefield,sep=csv_sep,header=header, encoding = csv_enc,**kwargs)            
        elif Utils.get_substr_from_file_name(pattern,file_name,group=regex_group) in Txt:
            with open(os.path.join(folder.get_path(),os.path.basename(file_name)),encoding = csv_enc) as f:
                res = f.read()
        res=func_to_apply(res)
        
        #print statements - create a decorator
        if print_file_name==True:
                print(file_name)        
        if print_DF_shape==True:
                print("num of rows,columns: {}".format(res.shape))
        if print_column_names==True:
                print(list(res.columns))
                    
        if add_file_info==True:
                res['FILENAME']=os.path.basename(file_name)
                add_info=Utils.split_string(str(os.path.basename(file_name)))
                res['METRIC_FAMILY']=add_info[0]
                res['METRIC_SOURCE']=add_info[1]
                res['REFERENCE_DATE']=add_info[2]
                res['VERSION']=Utils.split_string(add_info[3],sep='.')[0]
            
        
        return(res)
    
      
def read_insert_files_to_DB(        folder_name
                                   ,db_name
                                   ,sql_conn
                                   ,filt_pattern
                                   ,if_exists='replace'
                                   ,file_name='all'
                                   ,regex_group=1
                                   ,csv_enc="ISO-8859-1"
                                   ,**kwargs):
    
    #internal parameters
    Excel=['xlsx','xls']
    Csv=['csv']
    Txt=['html','txt']
    pattern=r'\.(.*)'
    
    
    folder=dataiku.Folder(folder_name,ignore_flow=True)
    paths = folder.list_paths_in_partition()
    path_filenames = [os.path.join(folder.get_path(),os.path.basename(path)) for path in paths]
    filenames = [os.path.basename(path) for path in paths]    
    
    if filt_pattern!=None:
        r = re.compile(filt_pattern)
        filenames = list(filter(r.match, filenames))
        path_filenames=[os.path.join(folder.get_path(),os.path.basename(filename)) for filename in filenames]
    
    if file_name=='all':
        
        for file in path_filenames:
            print(file)
            extension=Utils.get_substr_from_file_name(pattern,file,group=regex_group)
            
            csv_sep_type=os.path.basename(file).replace('.'+extension,'').split('_')[-1]
            csv_sep_type=csv_sep_type.replace("P","|").replace("C",",").replace("S",";").replace("T","\t")
            
            
            table_name='_'.join(os.path.basename(file).replace('.'+extension,'').split('_')[0:-1])
            source_id=os.path.basename(file).replace('.'+extension,'').split('_')[1]
            
            
            
            if extension in Excel:
                res=pd.read_excel(file,**kwargs)
               
            elif extension in Csv:
                res=pd.read_csv(file,sep=csv_sep_type,encoding = csv_enc,**kwargs)
                
            elif extension in Txt:
                with open(os.path.join(folder.get_path(),os.path.basename(file_name)),encoding = csv_enc) as f:
                    res = f.read() 
            
            res=Utils.trim_col_spaces(res)
            
            
            start_time = time.time() +3600
            s=time.localtime(start_time)        
            print("--- Started : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", s)))
            
            
            
            #hashing columns
            print('--- Start hashing ---')
            col_to_hash=SD.col_to_hash[SD.col_to_hash['DIM_SOURCE_ID']==source_id].reset_index()['HASH_COLUMNS'][0].split("|")
            df_cols=list(res.columns)
            
            print('df-cols', df_cols)
            print('col_to_hash',col_to_hash)
            
            for col in df_cols:
                if col in col_to_hash: 
                    print('col',col)
                    res[col]=res[col].apply(lambda x: Utils.SQL_ENCRYPT(expr=str(x)))
            print('--- Finish hashing ---')
            
            print('--- Start inserting ---')
            #write to DB
            res.to_sql(table_name,sql_conn,if_exists=if_exists,index=False)
            print('--- Finish inserting ---')
            
            #indexing columns
            col_to_index=SD.col_to_hash[SD.col_to_hash['DIM_SOURCE_ID']==source_id][['INDEX_COLUMNS']].reset_index(drop=True)['INDEX_COLUMNS'][0]
            if len(col_to_index)>0:
                print('--- Start indexing ---')
                Utils.create_DB_index(table_name,col_to_index)
                print('--- Finish indexing ---')
            else:
                print('--- No Columns to index ---')
            
                
            end_time = time.time() +3600
            e=time.localtime(end_time)
            print("--- ",db_name ,': ',table_name, " Created : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", e)))
            run_time=round(end_time-start_time,2)
            print("--- in %s seconds ---" % (run_time))
            
        
    else: 
        if extension in Excel:
            res=pd.read_excel(os.path.join(folder.get_path(),os.path.basename(file_name)),**kwargs)
        
        elif extension in Csv:
            res=pd.read_csv(os.path.join(folder.get_path(),os.path.basename(file_name)),sep=csv_sep_type,encoding = csv_enc,**kwargs)            
        
        elif extension in Txt:
            with open(os.path.join(folder.get_path(),os.path.basename(file_name)),encoding = csv_enc) as f:
                res = f.read()
            start_time = time.time() +3600
            s=time.localtime(start_time)        
            print("--- Started : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", s)))
            
            #hashing columns
            print('--- Start hashing ---')
            col_to_hash=SD.col_to_hash[SD.col_to_hash['DIM_SOURCE_ID']==source_id]['HASH_COLUMNS'][0].split("|")
            df_cols=list(res.columns)
            print('df-cols', df_cols)
            print('col_to_hash',col_to_hash)
            for col in df_cols:                
                if col in col_to_hash:
                    print('col',col)
                    
                    res[col]=res[col].apply(lambda x: Utils.SQL_ENCRYPT(expr=str(x)))
            print('--- Finish hashing  ---')
            print('--- Start inserting ---')
            
            #write to DB
            res.to_sql(table_name,sql_conn,if_exists=if_exists,index=False)
            
            print('--- Finish inserting ---')
            
            #indexing columns
            
            col_to_index=SD.col_to_hash[SD.col_to_hash['DIM_SOURCE_ID']==source_id][['INDEX_COLUMNS']].reset_index(drop=True)['INDEX_COLUMNS'][0]
            if len(col_to_index)>0:
                print('--- Start indexing ---')
                Utils.create_DB_index(table_name,col_to_index)
                print('--- Finish indexing ---')
            else:
                print('--- No Columns to index ---')
            
            end_time = time.time() +3600
            e=time.localtime(end_time)
            print("---",db_name ,': ',table_name, "Created : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", e)))
            run_time=round(end_time-start_time,2)
            print("--- in %s seconds ---" % (run_time))  
    
    
    
    
    
def write_file_to_managed_folder(data,
                                 folder_name,
                                 file_name, 
                                 regex_group=1, 
                                 index_TF=False, 
                                 csv_sep=',',
                                 sheet_name=None,
                                 **kwargs):
    
    Excel=['xlsx','xls']
    Csv=['csv','txt']
    pattern=r'\.(.*)'
    
    Output_Folder= dataiku.Folder(folder_name,ignore_flow=True)
    
    
    if Utils.get_substr_from_file_name(pattern,file_name,group=regex_group) in Excel:
            if sheet_name!=None:
                try:
                    path=os.path.join(Output_Folder.get_path(),file_name)
                    print(path)
                    book = openpyxl.load_workbook(path)
                    writer= pd.ExcelWriter(path, engine='openpyxl',mode='a')
                    writer.book = book
                    data.to_excel(writer, index=index_TF, sheet_name=sheet_name,**kwargs)
                    writer.save()
                    writer.close()
                except:
                    data.to_excel(os.path.join(Output_Folder.get_path(),file_name), index=index_TF,**kwargs)
            else:
                data.to_excel(os.path.join(Output_Folder.get_path(),file_name), index=index_TF,**kwargs)
            print('{} correctly saved in the managed folder "{}"'.format(file_name,folder_name))    
    elif Utils.get_substr_from_file_name(pattern,file_name,group=regex_group) in Csv:
            data.to_csv(os.path.join(Output_Folder.get_path(),file_name), index=index_TF, sep=csv_sep)
            print('{} correctly saved in the managed folder "{}"'.format(file_name,folder_name))
    else:
        raise ValueError('Extension of file not recognised. Please check.')
            
            
def Delete_file_from_managed_folder(folder_name,path):
    
    """
    Function which deletes files from managed folders
    folder_name -> name of Dataiku folder, i.e. TestFolder
    path -> file name in the folder
    
    """
    Folder=dataiku.Folder(folder_name,ignore_flow=True)
    Folder.delete_path(path)   
    
    
def RM_4_DataDeletion(folder_name,
                      num_of_days=3650,                       
                      exception = None): 
    """
    This function implements the mandatory requirements for data retention.
    All data older tha 10 years will be deleted as per requirement.
    Exceptions are taken into account.
    It is also possible to suspend information deletion at request of the business, 
    e.g. in legal hold cases
    
    Parameters:
    num_of_days -> days for which a file is retained
    folder_name -> dataiku folder name in which files need to be deleted
    exception   -> None or List of filenames whihc do not need to be deleted under request of the business
    """
    MaxRetention=timedelta(days=num_of_days)
    almost_to_delete=timedelta(days=num_of_days-365)
    res=Utils.get_file_names_from_managed_folder(folder_name, exception=exception)
    deleted_items=[]
    to_be_deleted_next_year=[]
    #print(res)
    for item in range(0,len(res)):
        
        #calculate time difference between today and the creation date of the file
        diff=datetime.today()-datetime.strptime(res[1][item][2],'%Y%m%d')
        #if the diff is more than the max retention period then delete        
        if (diff > MaxRetention) == True:
            Delete_file_from_managed_folder(folder_name,res[0][item])
            deleted_items.append(res[0][item])          
        elif (diff > almost_to_delete) ==True:
            to_be_deleted_next_year.append(res[0][item])
            
    if len(deleted_items)==0:
        if len(to_be_deleted_next_year)==0:
            print("No files deleted. No files to be deleted within the next year")
        else:
            print("No files deleted but the following file(s) will be deleted within 1 year:")
            for file in to_be_deleted_next_year:
                print(file)
    else:
        if len(to_be_deleted_next_year)==0:
            print("The following file(s) have been deleted:")
            for file in deleted_items:
                print(file)
            print("No other files to be deleted within the next year")
        else:
            print("The following file(s) have been deleted:")
            for file in deleted_items:
                print(file)
            
            print("The following file(s) will be deleted within 1 year:")
            for file in to_be_deleted_next_year:
                print(file)    
    
def SQL(df_list,sql_code):
    '''Decommisioned - use SQLite_helper module'''

    #'''RAAlib.SQL([[df_p,"t1"]],"select * from t1")'''
    con = sqlite3.connect(':memory:')
    for df in df_list:

        df[0].to_sql(df[1],
                      con=con,
                      index=False,
                      if_exists='replace',
                      chunksize=10000)

    SQL_Query = pd.read_sql_query(sql_code, con)
    con.close()
    return (SQL_Query)


def joinDFonDS(df,dataset_in="DIM_TIME_LOCATION",how_in="left",on_in=["DIM_TIME_LOCATION_ID","DIM_TIME_LOCATION_ID"]):
    return pd.merge(df, dataiku.Dataset(dataset_in,ignore_flow=True).get_dataframe(), how=how_in, on=on_in)

def lookupMapping(df,metricSource,managed_folder,mapping_file,how_in, left_on_in, right_on_in):

    df_m=read_files_from_managed_folder(managed_folder,file_name=mapping_file)
    df_m=df_m[df_m["METRIC_SOURCE"]==metricSource]
    df_f=pd.merge(df, df_m, how=how_in, left_on=left_on_in, right_on=right_on_in)
    del df_f[left_on_in]
    del df_f[right_on_in]
    return df_f





def fernet_key(password,salt):
    import base64
    import os
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    password = bytes(password, encoding='utf8')
    salt = bytes(salt, encoding='utf8')
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))
    f = Fernet(key)
    return f
#########
# OLD
#########                                            
def encrypt_df_column(df,column,f):
    df[column]=df.apply(lambda row: f.encrypt(bytes(row[column], encoding='utf8')), axis=1)
    return df
def decrypt_df_column(df,column,f):
    df[column]=df.apply(lambda row: f.decrypt(row[column]).decode("utf-8") , axis=1)
    return df
#########
#########
def SQL_ENCRYPT(item,password,salt):
    '''
    It encrypts a string into byte
    '''
    f=fernet_key(password,salt)
    e=f.encrypt(bytes(item, encoding='utf8')) 
    return(e)

def SQL_DECRYPT(item,password,salt):
    '''
    It decrypts a byte string
    '''
    f=fernet_key(password,salt)
    d=f.decrypt(item).decode("utf-8") 
    return(d)


def Map_Country_Codes(Dataframe,country_col_to_map=['DIM_BO_DOMICILE_ID','DIM_SCAP1_COUNTRY_ID','DIM_SCAP2_COUNTRY_ID']):
    '''
    The function maps each columns that in a dataframe contains country names. Each country that it is not 
        mapped will appear with a full name in the output dataframe
        
        paramaters
        Dataframe - a pandas dataframe that contains fields with country names like BO_DOMICILE, SCAP DOMICILE etc
        country_col_to_map - a list of column names that needs to be mapped
                             predefined is ['DIM_BO_DOMICILE_ID','DIM_SCAP1_COUNTRY_ID','DIM_SCAP1_COUNTRY_ID']
                             
        Output
        Dataframe - the same dataframe with mapped columns is output
    '''
    countries=read_files_from_managed_folder('Mapping Files',
                                         file_name='REFERENCE_AND_MAPPING_TABLES.xlsx',
                                        sheet_number = 'COUNTRY_CURRENCY_CODES')
    COUNTRY_A2_CODES=countries[['COUNTRY_NAME','ALPHA2_CODE']].drop_duplicates()
    
    for column in country_col_to_map:
        
        col_name="{}_Transformed".format(column)
        Dataframe[col_name]=Dataframe[column].str.upper().str.strip().replace('NOT USED','').replace('0','').replace('N','').replace('?','').replace(regex=[r'\.'],value='').replace(regex=[r'(?<=\b[A-Z].\b),(?=\b[A-Z].\b)'],value=';').replace(regex=[r'(?!.*BRITISH)(\,.*)'],value='').replace(regex=[r','],value='').replace(regex=[r'\*.*'],value='').replace(regex=[r'(?!.GB)(?!.UK)(?!.US)(?!.\.U\.S)(?!.BRITISH)(\(.*)'],value='').str.rstrip()

        Dataframe[column]=Dataframe.merge(COUNTRY_A2_CODES,how='left',left_on=col_name,
                                          right_on='COUNTRY_NAME')['ALPHA2_CODE'].fillna(Dataframe[col_name]).replace(regex=[r'NAMIBIA'],value='NA').replace(regex=[r'CYPRUS - NORTHERN'],value='CY').replace(regex=[r'RUSIA'],value='RU')
        Dataframe=Dataframe.drop(columns=[col_name])
        print(column + " has been mapped to Alpha2 country code")

    return (pd.DataFrame(Dataframe))


def copy_all_files(INP_FOLDER_NAME, OUT_FOLDER_NAME):
    
    '''
    Copy all files from a managed folder to another
    '''
    
    input_folder = dataiku.Folder(INP_FOLDER_NAME)    
    output_folder = dataiku.Folder(OUT_FOLDER_NAME) 
       
    input_paths = input_folder.list_paths_in_partition()
    
    for FILE_NAME in input_paths:         
        copyfile(os.path.join(input_folder.get_path(),FILE_NAME),os.path.join(output_folder.get_path(),FILE_NAME) )               
        print('saved ' + FILE_NAME)    


def DB_BACKUP(FILE_NAME, INPUT_FOLDER='SQLiteDB', OUTPUT_FOLDER='BACKUP'):
    input_folder = dataiku.Folder(INPUT_FOLDER) 
    output_folder = dataiku.Folder(OUTPUT_FOLDER) 
    FILE_NAME=FILE_NAME.split('.')
    if len(FILE_NAME)!=2:
        raise ValueError('The name of the file is not complete. Probably missing the extension.')
                    
    
    start_time = time.time() +3600
    s=time.localtime(start_time)  
    
    #server is in London and therefore the function returns 1 hour earlier
    timestamp_now=(datetime.now() + timedelta(0,3600)).strftime('%Y%m%d_%H%M%S')
    print("--- Started : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", s)))    
    
    copyfile(os.path.join(input_folder.get_path(),FILE_NAME[0] + '.' + FILE_NAME[1]),
             os.path.join(output_folder.get_path(),FILE_NAME[0] + '_' + timestamp_now + '.' + FILE_NAME[1]))

    end_time = time.time() +3600
    e=time.localtime(end_time)    
    print("--- Completed : %s ---" % (time.strftime("%Y-%m-%d %H:%M:%S", e)))
    run_time=round(end_time-start_time,2)
    print("--- %s seconds ---" % (run_time))























