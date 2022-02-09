import pandas as pd
import zipfile
import psycopg2
import os
import sqlalchemy as sqla
from sqlalchemy import create_engine
from metrics_python.functions import return_psql_auth

uid, pwrd = return_psql_auth()

psql_connect_string = f'''postgresql+psycopg2://{uid}:{pwrd}@localhost/airlineontimemetrics'''

psql_engine =create_engine(psql_connect_string)

path = '/home/jestripe/GradSchool/MIS_581_Winter_2021/RawData'

# code from https://www.codegrepper.com/code-examples/python/python+unzip+all+files+in+directory
def unzipFiles(path):
    files = os.listdir(path)
    for file in files:
        if file.endswith('.zip'):
            filePath = path + '/' + file
            zip_file = zipfile.ZipFile(filePath)
            for names in zip_file.namelist():
                zip_file.extract(names,path)
            zip_file.close()

def read_metrics_csv(path):
    main_df = []
    files = os.listdir(path)
    for file in files:
        if file.endswith('.csv'):
            file_path = path + '/' + file
            tmp = pd.read_csv(file_path, index_col = None, header = 0, low_memory = False)
            main_df.append(tmp)
    results_df = pd.concat(main_df, axis = 0, ignore_index = True)
    return results_df



