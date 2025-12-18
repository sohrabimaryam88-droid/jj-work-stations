from utiles.import_cellstatus import CellStatus
from utiles.import_rowfile import RowFile
from pathlib import Path
from utiles.import_aras2 import aras
import pandas as pd
import numpy as np
from datetime import datetime
datetoday=datetime.now().strftime("%Y-%m-%d %H:%M:%S")  
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))





def Wncs(dumpDbARAS,mongoDbDataBaseUrl,path_cellstatus,path_rowfilezip):
    try:
        Cell=CellStatus(path_cellstatus)
        row=RowFile(path_rowfilezip)
        arasdb=aras(dumpDbARAS, mongoDbDataBaseUrl)

        Cell=pd.merge(Cell,arasdb[["CELLNAME","LOCATION","LATITUDE","LONGITUDE"]],how="left",on=["CELLNAME"])
        row=pd.merge(row,arasdb[["CELLNAME","LOCATION","LATITUDE","LONGITUDE","AZIMUTH"]],how="left",on=["CELLNAME"])


        DF = row.merge(Cell, how="left", on=["PSC"], suffixes=("_CELL", "_CELLR"))
        
        coord_cols = ["LATITUDE_CELL", "LONGITUDE_CELL", "LATITUDE_CELLR", "LONGITUDE_CELLR"]
        DF[coord_cols] = DF[coord_cols].apply(pd.to_numeric, errors="coerce")
        DF = DF.dropna(subset=coord_cols)


        
        DF["DISTANCE_KM"] = haversine(
            DF["LATITUDE_CELL"],
            DF["LONGITUDE_CELL"],
            DF["LATITUDE_CELLR"],
            DF["LONGITUDE_CELLR"]
        )
        DF=DF.sort_values(by=["UtranCellId_CELL","PSC","DISTANCE_KM","numberOfEvents","numberOfDrops"])
        DF["PRIORITY"] = DF.groupby(["UtranCellId_CELL", "PSC"])["DISTANCE_KM"].rank(method="dense").astype(int)
        DF = DF[DF["PRIORITY"] <= 2]
        #DF = DF.groupby(["UtranCellId_CELL", "PSC"], group_keys=False).apply(lambda x: x.nsmallest(3, "DISTANCE_KM"))

        # print(row)

        return(DF)

    except Exception as e:
        print(f"Error in processing cell dump: {str(e)}")
        raise


if __name__=="__main__":

    

    path_cellstatus="3G Cell Status.xlsx"
    path_rowfilezip="D:\\3.CS-DICREPANCY\\7-WNCS\\appwncs\\rowfile\\rowfile.zip"
    dumpDbARAS = 'ArasData'
    mongoDbDataBaseUrl = 'mongodb://localhost:27017/'
    df=Wncs(dumpDbARAS,mongoDbDataBaseUrl,path_cellstatus,path_rowfilezip)
    datetoday = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    df.to_csv(f"ExportWncs_{datetoday}.csv", index=False)
    # chunk_size = 300000  #  

    # for i in range(0, len(df), chunk_size):
    #     df_chunk = df.iloc[i:i + chunk_size]
    #     df_chunk.to_csv(f"ExportWncs_{datetoday}_part_{i//chunk_size + 1}.csv", index=False)
    
    
    
