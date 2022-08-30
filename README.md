# FinCrime library for Python

This library is composed as sets of modules which help in building and running flows using:

* **DATA STORAGE**: Dataiku Managed folders, SQLite FileDB, Tableau Hyper files
* **DATA ELABORATION**, TRANSFORMATION and ENCRYPTION: Python, SQLite FileDB, Tableau Hyper files
* **DATA VIZ**: Tableau, Python
* **DATA SHARING**: SQLite FILEDB, Tableau Hyper, Excel, Emails

The FLOW is run through a unique function which takes care of running steps, for each datasource, in the right order.

All underlying technology are freely available on the internet. (Python, SQLite, TableauHyperAPI)


### Pre-requisites 


The following software must be installed :
* Anaconda 
* GIT 

The following Python packages should be installed and up to date:
* conda
* pip

To check, open the *Anaconda Prompt* and type the following:

Check if PIP is up-to-date [see here](https://pip.pypa.io/en/stable/)
```
pip -V
```
Check if *conda* is up-to-date (the version should be >= 4.7.10)
```
conda info
```
If required, you can update them:
```
conda update conda
conda update pip
```
it might take several minutes..


## Authors 

* **Davide Nicolini** - *Initial work* and *Contributor*
* **Zan Krizanovski** - *Initial work* and *Contributor*