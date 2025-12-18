import pandas as pd
import numpy as np
import time,pymongo,os,re
start_time = time.time()
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.mongo import DataBase

print(('aras').lower())
############
def aras(dumpDbARAS,mongoDbDataBaseUrl:str):
        
        try:
            dbEricsson=DataBase(uri=mongoDbDataBaseUrl, db_name=dumpDbARAS)
            df_Tehran=pd.DataFrame(dbEricsson.get("Tehran",{},{'_id':False,"Site":True,'SectorName':True,"CellsLatitude":True,
                                                               "CellsLongitude":True,"CellsAzimuth":True}))     
            if not df_Tehran.empty:
                df_Tehran=df_Tehran.drop_duplicates().reset_index(drop=True)

            df_Tehran["CellsAzimuth"] = df_Tehran["CellsAzimuth"].str.split("/", n=1, expand=True)[0]  
            df_Tehran=df_Tehran.rename(columns={"Site":"LOCATION","CellsLatitude":"LATITUDE","CellsLongitude":"LONGITUDE"
                                                ,"CellsAzimuth":"AZIMUTH","SectorName":"CELLNAME"})         

            return(df_Tehran)
        except pymongo.errors.ConnectionFailure:
            print("Error: Could not connect to MongoDB (Tehran)")
        except Exception as e:
            print(f"Error in processing Tehran : {str(e)}")
            raise
if __name__ == "__main__":
    try:
        dumpDbARAS = 'ArasData'
        #mongoDbDataBaseUrl = 'mongodb://172.27.114.59:27017/' #ghasemIP
        mongoDbDataBaseUrl = 'mongodb://localhost:27017/'
        df_Tehran = aras(dumpDbARAS, mongoDbDataBaseUrl)
        print("Operation completed successfully")
    except Exception as e:
        print(f"Script failed: {str(e)}")
