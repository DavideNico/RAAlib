import dataiku
import pandas as pd
import os
import re
import getpass
import datetime as dt
import time
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import RAAlib.__static_data__ as SD


def get_login_info():
    '''
    The function gets the login information querying it from the dataiku client
    It returns a dataframe with info on who is connected from where in dataiku
    '''
    client = dataiku.api_client()
    df=pd.DataFrame.from_dict(client.get_auth_info(),orient='index').transpose()
    return(df)

def get_current_time():
    '''
    Return the current date and time
    '''
    return(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
def get_file_names_from_managed_folder(folder_name,exception = None,split_len=3,additional_info=True):
    """
    Function which reads from a managed folder 
    
    
    params:
    folder_name -> String containing the name of the folder (i.e. 'TestFolder' )
    exception   -> None or a list of file names to exclude from the computation
    
    returns tuple of 2 lists:
    1.file names included in a managed folder
    2.list of additional info extracted from filename
    3.list of paths which can be used for reading the file
    
    """
    folder=dataiku.Folder(folder_name,ignore_flow=True)
    paths = folder.list_paths_in_partition()
    
    if exception != None:
        for filename in exception:
            paths.remove('/'+ filename)
            
    path_filenames = [os.path.join(folder.get_path(),os.path.basename(path)) for path in paths]
    filenames = [os.path.basename(path) for path in paths]
    if additional_info == False:
        add_info=[]
    else:
        add_info= [ extract_info_from_filename(filename,split_len) for filename in filenames]
    return(filenames,add_info,path_filenames)

def extract_info_from_filename(filename,split_len=3):
    """
    Function that extracts the info from a file name excluding the extension (i.e. .xlsx)
    
    params:
    filename -> string that contains the filename with extension
    
    returns a list
    """
    add_info=split_string(str(os.path.basename(filename)))
    add_info[split_len]= split_string(add_info[split_len],sep='.')[0]
    return(add_info)

def get_names_from_list_of_emails(string,first_sep=';',second_sep=','): 
    """
    Function which extract names from a list of Outlook contacts.
    The list of contacts is expected to be in this format: 'Washington, George <george.washington@ubs.com>; Merkel, Angela <angela.merkel@ubs.com>'
    The list of contacts is extracted directly from Outlook. From the recipients, just copy paste    
        
    params:
    string -> a string containing the concatenated contact names. i.e. Washington,George;Merkel,Angela
    first_sep -> the separator used for separate the names
    second_sep -> the separator used for separating the Surname from the Name
    
    returns a list of 3 items:
    0 -> list of email addresses (list of strings)
    1 -> list of Surname, Name <email address> (list of strings)
    2 -> string of greetings. i.e. "Dear George, Dear Angela,"
    
    """
    name_emails=[]    
    for name in split_string(string, first_sep):
        name_emails.append(name)
    
    email_addresses=[]    
    for name_email in name_emails:
        email=get_substr_from_file_name('(?<=<)(.*?)(?=>)',name_email, None)
        email_addresses.append(email)
        
    names=[]   
    for name_split in name_emails:
        name_surname=get_substr_from_file_name('(.*)( <)',name_split, 1)
        name=split_string(name_surname,second_sep)
        if len(name)==2:
            names.append(name[1])
    #print(name_emails,email_addresses,names)       
    return([email_addresses,name_emails,'{0}{1}{2}'.format('Dear',', Dear'.join(names),",")])


def count_occurrences(string, charachter):
    """
    Count how many times the charachter is encountered in a string
     Params:
     string -> a string 
     charachter -> a string containing the charachter to search for
     
     return the number of occurrencies 
    
    """
    return(string.count(charachter))


def get_version(DataIku_Managed_Folder,file_partial_name,separator='_'):
    """
    extract the max version of a file in a managed foler given a partial file name
    
    Params:
    DataIku_Managed_Folder -> managed folder name
    file_partial_name -> part of ther name identifiying a file in the folder (i.e. ABC_GESAP_20210301)
    separator -> the separator used in the file name used to split the info contained in it. For example '_'
    
    returns the version number to be given to the next file that will be uploaded.
        for example if already a file exists, with version = 1, then the function will return 2
        
     EXAMPLE:
     
     Utils.get_version('[RAW] Landing Folder','ABC_GESAP_20200101')
    
    """
    
    
    Folder=dataiku.Folder(DataIku_Managed_Folder)
    paths = Folder.list_paths_in_partition()
    filenames=[os.path.basename(path) for path in paths]
    r = re.compile(file_partial_name)
    filenames = list(filter(r.match, filenames))
    
    if len(filenames)==0:
        return 1
    else:
        versions=[]
        for filename in filenames:
            versions=split_string(split_string(filename,separator)[count_occurrences(filename,separator)],".")[0]
            return (int(max(versions))+1)


def YQM_from_date(date, dateformat='%d.%m.%Y'):
    """
    Helper function which take a string or a series of date as parameter and return
    a list of String/Series containing:
    -Year
    -Quarter
    -Month
    -Day
    
    Prameters:
    date -> (str or pd.Series) it can be a string representing a date
                               a datetime value
                               a Series containing datetime values
    dateformat -> format of the date in teh single string value. 
                  used for formatting the string in case it is not recognised as date
                  
                  Default: '%d.%m.%Y'
    Return a list
    """
    if isinstance(date, dt.datetime)==True:
        Year=date.year
        Quarter=date.quarter
        Month=date.month
        Month_twodigit=date.strftime("%m")
        
        Day=date.day
        return([Year,Quarter,Month,Day,Month_twodigit])
    
    elif isinstance(date,pd.Series)==True:
        Year=pd.DatetimeIndex(date).year
        Quarter=pd.DatetimeIndex(date).quarter
        Month=pd.DatetimeIndex(date).month
        Month_twodigit=pd.DatetimeIndex(date).strftime("%m")
        
        Day=pd.DatetimeIndex(date).day
        return([Year,Quarter,Month,Day,Month_twodigit])
    else:
        try:
            date=pd.to_datetime(date,format=dateformat)
            Year=date.year
            Quarter=date.quarter
            Month=date.month
            Month_twodigit=date.strftime("%m")
            
            Day=date.day
            return([Year,Quarter,Month,Day,Month_twodigit])
        except:
            raise ValueError('The parsed date has an incorrect format: {}'.format(date))
    


def mapping_file(sheet_name_or_number):
    """
    Function that reads a specific sheet of the excel containing 
    the Metadata
    
    params:
    sheet_name_or_number  -> int or str, the number or the name of the sheet in REFERENCE_AND_MAPPING_TABLES.xlsx
    
    return a Dataframe
    """
    folder=dataiku.Folder('Mapping Files',ignore_flow=True)   
    df=pd.read_excel(os.path.join(folder.get_path(),os.path.basename('REFERENCE_AND_MAPPING_TABLES.xlsx')),sheet_name=sheet_name_or_number)
    return(df)

def get_tableau_auth_key():
    """
    Read Tableau access tokens for the API authentication from the metadata managed folder ('Mapping Files') 
    using the t number of the logged user. 
    if no or multiple associated tokens are found then rais e an error
    from the file REFERENCE_AND_MAPPING_TABLES.xlsx
    
    return a dict of TokenID and TokenKEY
    """
    df=mapping_file('TABLEAU_API_KEYS')
    #folder=dataiku.Folder('Mapping Files')   
    #df=pd.read_excel(os.path.join(folder.get_path(),os.path.basename('REFERENCE_AND_MAPPING_TABLES.xlsx')),sheet_name='TABLEAU_API_KEYS')
    user=getpass.getuser()
    
    df=df[df['USERNAME']==user]
    if len(df)==1:
        return(dict(df.iloc[0,:]))
    elif len(df)==0:
        raise ValueError("No credentials for user {} exists. Please add them to the REFERENCE_AND_MAPPING_TABLES.xlsx.".format(user))
    else:
        raise ValueError("Multiple entries are found. Please check the REFERENCE_AND_MAPPING_TABLES.xlsx")



#function to match patterns in a string
def get_substr_from_file_name(RE_pattern,filename,group):
    """
    Helper function which returns None if the pattern is not matched while
    the function can take as input group of matching patterns ([A-Z])(.*[1-9])
    """
    if group==None:
        return (re.search(RE_pattern,filename).group())
    else:
        return (re.search(RE_pattern,filename).group(group))
    
#function to split
def split_string(string, sep='_', maxsplit=-1):
    """
    Helper function which splits strings given a pattern
    
    params:
    string -> string to be split
    sep -> default separator '_'
    maxsplit -> default -1 - split all
                an integer which determines the number of split that will be applied
    """
    split_str=string.split(sep,maxsplit)
    return (split_str)


def ABC_csv_format_fix(df):
    """
    Dedicated function for formatting files downloaded
    from T&E SAP system
    
    !!! Use only for csv file and T&E SAP files !!!
    """
    r=re.compile('Unnamed')
    cols_to_change=list(filter(r.match,df.columns))
    col_names_to_change=dict(zip(list(df.columns[0:len(cols_to_change)]),list(df.iloc[0,0:len(cols_to_change)].get_values())))
    df=df.rename(columns=col_names_to_change)
    df=df.iloc[1:]
    return(df)

def FX_Rates_fix(df):
    """
    Dedicated function for formatting files downloaded
    from T&E SAP system
    
    !!! Use only for csv file and T&E SAP files !!!
    """
    df['Date']=pd.to_datetime(df['Date'],format='%d.%m.%Y')
    df=df[['Date','Close']]    
    return(df)

def RDS_R13451_harmonisation(df):
    """
    Dedicated function for formatting csv R13451 files
    from RDS whihc contains information about
    Sanctioned, SCAP Tax, heavens countries
    
    !!! Use only for csv and RDS table R13451 files !!!
    """
    
    regex_pat = re.compile(r'^`')
    
    #deleted first row. It includes header description and is not relevant
    df=df.iloc[1:].replace(regex_pat,"")
    
    #trim all whitespaces int he col names
    old_col_names=list(df.columns)
    new_col_names=pd.Series(old_col_names).str.replace(re.compile(" "),"")    
    modified_col_names=dict(zip(old_col_names,new_col_names))
    df=df.rename(columns=modified_col_names)
    #rename some columns
    df=df.rename(columns={'-':'TABLE_INDEX',
                      'COUNTRY_NAME':'COUNTRY_NAME_DEU',
                      'COUNTRY_NAME.1':'COUNTRY_NAME_ENG',
                      'COUNTRY_NAME.2':'COUNTRY_NAME_FRA',
                      'COUNTRY_NAME.3':'COUNTRY_NAME_ITA'})
    return(df)

def get_global_variables(DataIku_project="FCPANALYTICS"):
    '''
    return a dictionary containing the global variables for a Dataiku project
    '''
    client = dataiku.api_client()
    project = client.get_project(DataIku_project)
    v = project.get_variables()
    return(v['standard'])

def set_global_time_variables():
    '''
    function that set the global time variables, useful for many scripts
    '''
    
    
    today=dt.date.today()
        
    client = dataiku.api_client()
    project = client.get_project("FCPANALYTICS")
    v = project.get_variables()
    
    #set current time variables
    v['standard']['DIM_YEAR_ID'] = int(pd.to_datetime(today).year)    
    v['standard']['DIM_QUARTER_ID'] = int(pd.to_datetime(today).quarter)
    v['standard']['DIM_QUARTER_NAME'] = "Q"+ str(v['standard']['DIM_QUARTER_ID'])    
    v['standard']['DIM_YEAR_QUARTER_NAME'] = str(v['standard']['DIM_YEAR_ID']) + " Q"+  str(v['standard']['DIM_QUARTER_NAME'])        
    v['standard']['DIM_MONTH_ID'] = int(pd.to_datetime(today).month) 
    v['standard']['DIM_DATE_MONTH_ID'] = int( str(v['standard']['Current_Year'])+ '{:02d}'.format(v['standard']['DIM_MONTH_ID']))

    #Set past time variables
    v['standard']['DIM_PAST_YEAR_ID'] =  v['standard']['DIM_YEAR_ID']-1                                         
    v['standard']['DIM_PAST_QUARTER_ID'] =  v['standard']['DIM_QUARTER_ID']-1
    v['standard']['DIM_PAST_MONTH_ID'] =  v['standard']['DIM_MONTH_ID']-1
    v['standard']['DIM_PAST_QUARTER_NAME'] = "Q"+ str(v['standard']['DIM_PAST_QUARTER_ID'])
    v['standard']['DIM_YEAR_QUARTER_NAME'] = str(v['standard']['DIM_YEAR_ID']) + " "+  str(v['standard']['DIM_PAST_QUARTER_ID'])
    v['standard']['DIM_DATE_MONTH_ID'] = int( str(v['standard']['Current_Year']) + '{:02d}'.format(v['standard']['DIM_PAST_QUARTER_ID']*3)) 
    project.set_variables(v)

def A2_country_codes():
    Countries = dataiku.Dataset("REF_COUNTRY_CODES")
    Countries_DB = Countries.get_dataframe()
    Countries_DB=Countries_DB['ALPHA2_CODE'].drop_duplicates()
    return(Countries_DB)

def country_list_regexp(expr, item): 
    reg = re.compile(expr)
    a=re.split('\\|',item)
    match=list()
   # A2_codes=list(A2_country_codes())
    
    for code in a:
        if code!='':
            #
            if code in SD.ALPHA2_COUNTRY_LIST:
                b=reg.fullmatch(code)
                #print(code)
                #print(b)

                try:
                    fmatch=b.group(0)
                except:
                    fmatch=None

                if fmatch is None:
                    print(a)
                    return(False)  
            else:
                print('please check ' + str(a) + '. '+ code + ' is not a valid Alpha 2 Country code')
                print('''SQL.run_query(' SELECT * FROM REF_COUNTRY_CODES
                         where COUNTRY_NAME like ''' "'%" + str(a) + "%'" + '''
                         )         
         SQL.run_query(' INSERT INTO REF_COUNTRY_CODES  (COUNTRY_NAME,
                                                   ALPHA2_CODE,
                                                   ALPHA3_CODE,
                                                    COUNTRY_CODE_NUMBER,
                                                    CURRENCY_NAME,
                                                    CURRENCY_CODE,
                                                   CURRENCY_CODE_NUMBER
                                                    )
                                                    VALUES( 'KOREA-SOUTH'
                                                    ,'KR'
                                                    ,'KOR'
                                                    ,410
                                                    ,'Won'
                                                    ,'KRW'
                                                    ,410
                                                    ) )   ''')
                return(False)
            
        return(True)

def regexp(expr, item): 
    reg = re.compile(expr)
    a=re.split('\\|',item)
    match=list()
    
    for code in a:
        b=reg.fullmatch(code)
        #print(code)
        #print(b)
        
        try:
            fmatch=b.group(0)
        except:
            fmatch=None
            
        if fmatch is None:
            #print(a)
            return(False)  
        
def RE_COUNTRY_VLAD(expr, item): 
    Series=pd.Series([item])
    Series=Series.str.replace(r'(?<=|)(.)((?=POA:)|(?=CP:)|(?=BO1:)|(?=BO2:)|(?=CO:))',';',regex=True)
    Series.str.extract(r'(?<=BO1: )(.*?)((?=\;)|(?=$))')[0]
    BO1_DOMICILE=Series.str.extract(r'(?<=BO1: )(.*?)((?=\;)|(?=$))')[0][0]
    BO2_DOMICILE=Series.str.extract(r'(?<=BO2: )(.*?)((?=\;)|(?=$))')[0][0]
    CP_DOMICILE=Series.str.extract(r'(?<=CP: )(.*?)((?=\;)|(?=$))')[0][0]
    CO_DOMICILE=Series.str.extract(r'(?<=CO: )(.*?)((?=\;)|(?=$))')[0][0]

    if expr=='BO':
        if (pd.isna(BO1_DOMICILE)==True)&( pd.isna(BO2_DOMICILE)==False):
            BO_DOMICILE = BO2_DOMICILE
        elif (pd.isna(BO1_DOMICILE)==False)&(pd.isna(BO2_DOMICILE)==True):
            BO_DOMICILE = BO1_DOMICILE
        elif (pd.isna(BO1_DOMICILE)==False)&(pd.isna(BO2_DOMICILE)==False):
            BO_DOMICILE = BO1_DOMICILE + "|" + BO2_DOMICILE
        else:
            BO_DOMICILE=""
        return(BO_DOMICILE)
    elif expr=='CP_CO': 
        if (pd.isna(CP_DOMICILE)==True)&( pd.isna(CO_DOMICILE)==False):
            CO_CP_DOMICILE = CO_DOMICILE
        elif (pd.isna(CP_DOMICILE)==False)&(pd.isna(CO_DOMICILE)==True):
            CO_CP_DOMICILE = CP_DOMICILE
        elif (pd.isna(CP_DOMICILE)==False)&(pd.isna(CO_DOMICILE)==False):
            CO_CP_DOMICILE = str(CP_DOMICILE) + "|" + str(CO_DOMICILE)
        else:
            CO_CP_DOMICILE=""
        return(CO_CP_DOMICILE)
    else:
        raise Exception (expr, 'wrong parameter!')
   
    
def add_quotes(items):
    final_item=[]    
    for item in items:
        if isinstance(item,int):
            item="'" + str(item) + "'"            
            final_item.append(item)
            
        else:
            final_item.append(item)
            
    return(final_item)
    
def Table_Delta_update(dict_param=None, SQL_version='SQLite'): 
    '''
    This function prepare the filter to be used in queries
    both for Sqlite and for Tableau hyper files allowing 
    the delta update
    
    Param:
    - dict_param: dictionary. containing as key the FIELD_NAME and as value a LIST OF VALUES
    - SQL_version: string. it can be SQLite or Tableau Hyper
    '''
    
    filt=''
    if SQL_version=='SQLite':
        for key in dict_param:
            strlist=[str(a) for a in dict_param[key]]
            if len(strlist)>0:
                filt += 'and ' + key + ' in (' + ",".join(strlist) + ')\n'
                
    elif SQL_version=='Tableau Hyper':
         for key in dict_param:
            strlist=[str(a) for a in dict_param[key]]            
            if len(strlist)>0:
                filt += 'and "{0}" in ({1})\n'.format(key,",".join(add_quotes(strlist)))
    return(filt)

def fernet_key(password,salt):
    
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



def encrypt_df_column(df,column,f):
    df[column]=df.apply(lambda row: f.encrypt(bytes(row[column], encoding='utf8')), axis=1)
    return df
def decrypt_df_column(df,column,f):
    df[column]=df.apply(lambda row: f.decrypt(row[column]).decode("utf-8") , axis=1)
    return df

def Get_Secrets():
    
    client = dataiku.api_client() # client is a dataikuapi.dssclient.DSSClient
    auth_info = client.get_auth_info(with_secrets=True) #get all passwords from Other Credentials 

    for secret in auth_info["secrets"]:
        if secret["key"] == "password":
                password = secret["value"]
        if secret["key"] == "salt":
                salt = secret["value"]            
                break 
    try:
        return([password,salt])
    except:
        return(['no','secrets'])

secrets=Get_Secrets()
f=fernet_key(secrets[0],secrets[1])


def SQL_ENCRYPT(expr,fernet=f):
    '''
    It encrypts a string into byte
    '''
    #f=fernet_key(password,salt)
    e=fernet.encrypt(bytes(expr, encoding='utf8')) 
    return(e)

def SQL_DECRYPT(expr,fernet=f):
    '''
    It decrypts a byte string
    '''
    #f=fernet_key(password,salt)
    try:
        d=fernet.decrypt(expr).decode("utf-8") 
    except:
        d=''
    return(d)

def do_nothing(df):
    """
    A function which does not do anything
    just a placeholder
    """
    df=df
    return(df)

def country_fields_mapping(expr):
    '''
    This function is used in SQLite to map 
    list of country names or ALPHA3 or ALPHA2 codes
    to the standard ISO ALPHA2 Code
    
    It returns a string containing the
    list of distinct ALPHA2 codes pipe (|) separated
    
    Parameters:
    expr: string of pipe (|) separated items
          it accepts also 1 country or empty values
            
    
    '''
    
    
    countries_list=list(set(expr.split('|')))
    try:
        if countries_list!=['']:
            countries_list.remove("") 
    except:
        pass
    
    i=0
    for country in countries_list:  
        country=country.upper().strip()
        try:
            if country in SD.ALPHA2_COUNTRY_LIST:
                countries_list[i]=country               
            else:               
                countries_list[i]=SD.CODE_COUNTRY_MAP_DICT[country]                
        except:
                countries_list[i]=country              
        i=i+1   
    return("|".join(countries_list))

def trim_col_spaces(df):
    
    new_col_dict=dict(zip(list(df.columns),[item.strip().replace('?','').replace('/','').replace(':','').replace(';','').replace('(','').replace(')','').replace('\n','_').replace(' ','_') for item in df.columns]))
    df=df.rename(columns=new_col_dict)
    return(df)

def create_DB_index(table_name,index_columns):    
    index_columns=index_columns.split('|')
    for i in range(len(index_columns)):
        try:
            s='CREATE INDEX index_'+table_name+'_'+str(i)+' ON '+table_name+'('+str(index_columns[i])+')'
            SD.conn_raw.cursor().execute(s)
            print('Index: index_'+table_name+'_'+str(i), '\nCreated on columns:', index_columns[i] )
            SD.conn_raw.commit()
        except:
            SD.conn_raw.rollback()
            print('Error on table',table_name,'\n no columns:' ,index_columns[i])
            
def defragment_hyper(hyper, Folder, DB_NAME):
    '''
    Defragment hyper. every time a modification is done on the hyper the size increases.
    (even if objects are deleted.)
    
    
    '''
    Output_Folder= dataiku.Folder(folder_name,ignore_flow=True)
    path=os.path.join(Output_Folder.get_path(),DB_NAME)
    path_output=os.path.join(Output_Folder.get_path(),'defr_'+ DB_NAME)
    
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
            
        try:
            os.remove(path)        
            os.rename(path_output, path)
            print(f"Successfully defragmented ")
        except Exception as e:
            print(e)
        
def bitlist_to_string(bitlist):
    s=''.join(list(map(str,bitlist)))
    return(s)

def string_to_bitlist(string):
    bit=list(map(int,string))
    return(bit)

def tobits(query):
    result=[]
    for c in query:
        bits = bin(ord(c))[2:]
        bits = '00000000'[len(bits):] + bits
        result.extend([int(b) for b in bits])
    result=bitlist_to_string(result)
    return(result)

def frombits(bits):
    bits=string_to_bitlist(bits)
    chars=[]
    for b in range(int(len(bits)/8)):
        byte = bits[b*8:(b+1)*8]
        chars.append(chr(int(''.join([str(bit) for bit in byte]),2)))
    return ''.join(chars)
            
            
            
            
            
            