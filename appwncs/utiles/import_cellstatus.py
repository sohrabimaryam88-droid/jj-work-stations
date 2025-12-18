
import pandas as pd
import numpy as np
from pathlib import Path


def CellStatus(PSC_PATH):
    try:
        PSC_PATH = Path(PSC_PATH)
        Data_PSC=pd.read_excel(PSC_PATH,dtype=str)
        Data_PSC['CELLNAME']=Data_PSC['UtranCellId'].str[0:2]+Data_PSC['UtranCellId'].str[4:9]

        Data_PSC=Data_PSC[['NodeId','CELLNAME','UtranCellId','primaryScramblingCode']].drop_duplicates().reset_index(drop=True)
        Data_PSC=Data_PSC.groupby(['NodeId','CELLNAME']).agg(

                    UtranCellId=('UtranCellId', lambda x: ','.join(set(x))),
                    primaryScramblingCode=('primaryScramblingCode', lambda x: ','.join(set(x))),

                    ).reset_index()
        Data_PSC=Data_PSC.rename(columns={"NodeId":'RNC'})
        Data_PSC['RNC']=Data_PSC['RNC'].str.replace("Tehran_","")
        Data_PSC=Data_PSC.rename(columns={"primaryScramblingCode": "PSC"})
        Data_PSC["PSC"] = Data_PSC["PSC"].astype(str)
        #print(Data_PSC)
        return(Data_PSC)


    except Exception as e:
        print(f"Error in processing cell dump: {str(e)}")
        raise

if __name__=="__main__":
    PSC_PATH="3G Cell Status.xlsx"
    # PASC_BASE=Path.cwd()
    # PSC_PATH=PASC_BASE/PSC_PATH
    df=CellStatus(PSC_PATH)
    #df.to_csv("test.csv")
    print(df)

