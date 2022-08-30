"""
This class allows you to connect to UBS tableau production server using the Personal Access Token created on tableau server. 
No use of personal username and password.

Pre-requisites:
- Personal Access Token to be created on Tableau server: http://tableau.ubs.net/
- Token name and Token key should be also present in the REFERENCE_AND_MAPPING_TABLES.xlsx 


- use below code to get the data (for one sheet only)
tableauInst().downloadDataFromWorkbook(workbook='Dashboard name', path='Download path', view_name = 'View name if more then one')

- use below code to download more than one sheet

tableau = tableauInst()
tableau.downloadDataFromWorkbook(workbook='Dashboard name', path='Download path1', view_name = 'View name1')
tableau.downloadDataFromWorkbook(workbook='Dashboard name', path='Download path2', view_name = 'View name2')
tableau.downloadDataFromWorkbook(workbook='Dashboard name_2', path='Download path3')
"""

import tableauserverclient as TSC
import dataiku
import pandas as pd
import time
import requests
import os
import re
import RAAlib.__utils__ as Utils

    
class tableauInst():
    
    def __init__(self):
        """
        Initialise important variables:
        Get authentication string to be provided as parameter for the Tableau API
        add http options to deactivate SSL verification
        """
        try:
            self.tableau_auth is None
        except:
            # *** Pullout password ***
            # client is now a DSSClient and can perform all authorized actions.
            # For example, list the project keys for which you have access
            
            
            client = dataiku.api_client() # client is a dataikuapi.dssclient.DSSClient
            auth_info = client.get_auth_info(with_secrets=True) #get all passwords from Other Credentials
                       

            # retrieve the secret named "credential-for-my-api"
            secret_value = None
            for secret in auth_info["secrets"]:
                    if secret["key"] == "Dataiku":
                            secret_value = secret["value"]
                            break

            if not secret_value:
                    raise Exception("secret not found")
            # get Tableau Credentials
            #cred=Utils.get_tableau_auth_key()
            
            #login into tableau server and getting the token                
            self.tableau_auth = TSC.PersonalAccessTokenAuth("Dataiku", secret_value, 'prd')
            self.server = TSC.Server('http://tableau.ubs.net/', use_server_version=True)
            self.server.add_http_options({'verify': False})
           


    def downloadDataFromWorkbook(self, workbook, path, view_name=None):
        '''
        Downloads data in csv format from existing view in existing workbook on tableau.ubs.net
        
        workbook: name of the workbook on tableau
            path: path on local system do download data (ext: csv)
       view_name: name of the sheet within workbook to download from
        '''
        #setting filtering options
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                 TSC.RequestOptions.Operator.Equals,
                                 workbook))
        #tableau authorization with saved token
        with self.server.auth.sign_in(self.tableau_auth):
            #get the workbook 
            workbooks = self.server.workbooks.get(req_option)
            if len(workbooks) > 0:
                wconnect = workbooks[0][0]
                #if workbook was found populate all sheets (views) with datat
                self.server.workbooks.populate_views(wconnect)
                #find the required view
                vfound = [view for view in wconnect.views if len(view.name) > 0 and view_name is None or view.name == view_name]
                if len(vfound) > 0:
                    view_item = vfound[0]  
                    #if view was found, get the csv
                    self.server.views.populate_csv(view_item)  
                    #downlaod csv and save as path on local drive
                    with open(path,'wb') as f:  f.write(b''.join(view_item.csv))
                    print('View dowloaded as {}'.format(path))
                else:
                    #if view wasn't found, print what was found and raise exception
                    print(wconnect.views)
                    raise Exception('View: {} wasn''t found'.format(view_name))
            else:
                raise Exception('Workbook: {} wasn''t found'.format(workbook))


    def evaluateParams(self, view_to_folder, target_date, overwrite = False):

        ### EVALUATE PARAMS ###
        #setting the file date
        if target_date is None:
            import datetime
            target_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')   


        filename = '{}_{}.csv'
        #dictionary for translation of folder names to folder id's
        project = dataiku.api_client().get_project(dataiku.default_project_key())
        managed_folder_translator = {folder['name']:folder['id'] for folder in project.list_managed_folders()}

        #1. EVALUATES destination name to dataiku.Folder
        #2. EVALUATES destination schema to dataiku.Dataset
        #3. EVALUATES view and date to destination file
        #4. If overwrite = False, removes from list if file exists in folder

        filtered_view_to_folder = dict()
        for view_name, target in view_to_folder.items():
            destFolder, destSchema = target
            destFolder = project.get_managed_folder(managed_folder_translator[destFolder])
            destSchema = None if destSchema is None else project.get_dataset(destSchema)
            destFile = filename.format(target_date, view_name)
            fileInFolder = len(list(filter( lambda x: x['path'] == '/' + destFile, destFolder.list_contents()['items'] )))>0
            if not(fileInFolder) or overwrite:
                filtered_view_to_folder[view_name] = (
                    destFile, destFolder, destSchema
                )

        return filtered_view_to_folder                
                

    def downloadAndSave(self, params:list):
            workbook, view_name, target = params
            destFile, destFolder, destDataset = target
            #download view to local path
            print('Downloading from {}.{}'.format(workbook, view_name))
            start_time = time.time()
            try:
                self.downloadDataFromWorkbook(workbook, destFile, view_name)
            except requests.exceptions.ConnectionError as ce: 
                print(view_name, ce, time.time() - start_time)
                return (view_name, str(ce) + '\ntime: ' + str(time.time() - start_time))
            except requests.exceptions.ReadTimeout as rt:
                print(view_name, rt, time.time() - start_time)
                return (view_name, str(rt) + '\ntime: ' + str(time.time() - start_time))
            except Exception as e:
                print(view_name, e, time.time() - start_time)
                return (view_name, str(e) + '\ntime: ' + str(time.time() - start_time))
            #mapping
            if(destDataset is not None):
                print('Mapping to {}'.format(destDataset.get_definition()['name']))
                #get the schema from target dss_folder
                target_columns = [col["name"] for col in destDataset.get_schema()['columns']]
                #read data from temp file
                tableau_data = pd.read_csv(destFile)
                #save again with target schema
                tableau_data[target_columns].to_csv(destFile, index=False)
            #upload to target dss_folder
            print('Uploading {} to {}'.format(destFile, destFolder.get_definition()['name']))
            destFolder.put_file(destFile, open(destFile, 'rb'))
            return (view_name, 'OK')    
        
    def uploadViewsToFolders(self, workbook, view_to_folder, target_date = None, timeout = 1200, parallel = False, overwrite = False, timeoutRetry = 1):
        '''
        workbook - tableau workbook name
        view_to_folder - dictionary = {view:(dss_folder, dss_dataset),
                                       view:(dss_folder, dss_dataset) ... } 
                        view - name of the sheeet in tableau workbook 
                        dss_folder - name of the folder within dataiku (not id)
                        dss_dataset - name of dataset, for map the file to, can be set to None
                        one file can be only in one project's folder, but one folder can be target to many files
        targetDate - yyyymmdd, if None date is set to yesterday (if run on 1st April, date will be set to 31 March)
        '''
        #set timeout
        self.server.add_http_options({'timeout': timeout})

        #evaluate params to dataiku variables
        view_to_folder_filtered = self.evaluateParams(view_to_folder, target_date, overwrite = overwrite)

        print('Start downloading of {} files.'.format(len(view_to_folder)))
        #going through all specified views and copying all the files to target folders

        #switch between parallel and sequenced execution
        if not parallel:
            results = [self.downloadAndSave((workbook, view_name, target)) for view_name, target in view_to_folder_filtered.items()]
        else:
            from multiprocessing.pool import ThreadPool
            params = [(workbook, view_name, target) for view_name, target in view_to_folder_filtered.items()]
            results = ThreadPool(5).imap_unordered(self.downloadAndSave, params)
            for r in results:
                print(r)

        #errors check
        errors = [r for r in results if r[1] != 'OK']

        #if there were errors, and request of retry -> retry, in other case just write errors
        if len(errors) > 0:                   

            if timeoutRetry > 0:
                #retry with larger timeout and smaller timeoutRetry
                self.uploadViewsToFolders(self, workbook, view_to_folder, target_date = target_date, timeout = timeout + 300, parallel = parallel, overwrite = False, timeoutRetry = timeoutRetry - 1)

            else:
                print('#########  ERRORS WHILE DOWNLOAD: ########')
                print (errors)
                raise Exception('ERRORS WHILE DOWNLOAD')

        print('##########  ALL VIEWS WERE DOWNLOADED  #############')
    
    def get_project_id(self, project_name):
        '''
        Get the tableau project id, based on provided name
        '''
        projects = self.get_projects_list()
        if project_name in projects:
            return projects[project_name]
        else:
            raise Exception('Project {} not found'.format(project_name))

    def get_projects_list(self):
        '''
        Return projects list, it is needed to read all projects on the server and then search on dss for the correct one
        Projects list is limited by the page_size, currently set to 300
        '''
        req_option = TSC.RequestOptions()
        with self.server.auth.sign_in(self.tableau_auth):
            #change below line if projects list dosn't fit to the size
            projects_list = self.server.projects.get(req_option.page_size(1000))
            return {i.name : i.id for i in projects_list[0]}
        
    def publish_datasource_from_folder(self, dataiku_folder_name, tableau_source, tableau_project_name, Dataset_to_upload ):
        ''' 
        Publish hyper export from dss to tableau server.
        Export should be already made
        dataiku_folder_name : folder where export is stored (should be only one file)
        tableau_source : name for the source to be published as
        tableau_project_name : tableau project where datasource should be stored
        '''
        ds_name = tableau_source
        project_name = tableau_project_name
        tableau_Input = dataiku.Folder(dataiku_folder_name,ignore_flow=True)

        #
        paths = tableau_Input.list_paths_in_partition()
        filenames = [os.path.basename(path) for path in paths]
        #print(filenames)
        r = re.compile(Dataset_to_upload)        
        filenames = list(filter(r.match, filenames))
        #print(filenames)
        path_filenames=[os.path.join(tableau_Input.get_path(),os.path.basename(filename)) for filename in filenames]        
        

        #get the tableau project id to publish the source
        project_id = self.get_project_id(project_name)
        #authorize using already saved token
        with self.server.auth.sign_in(self.tableau_auth):
            #publish datasource, with overwrite mode, so if there is already the source it will be overwritten
            self.server.datasources.publish(
                datasource_item = TSC.DatasourceItem(project_id, name=ds_name),
                file = path_filenames[0],
                mode = self.server.PublishMode.Overwrite
            )
        print("{} was published as {} in {} project on Tableau".format(filenames[0], ds_name, project_name))