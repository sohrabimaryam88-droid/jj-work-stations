from pathlib import Path
import time
import pandas as pd
start_time=time.time()
import csv
#**************************************************************************************************#
def Aras(ARAS_PATH):
    try:
        Data_ARAS=pd.read_excel(ARAS_PATH,'Location')
        Data_ARAS=Data_ARAS[['کد سایت','عرض جغرافیایی','طول جغرافیایی','BSC/RNC',\
                            'زاویه سکتور 1','زاویه سکتور 2','زاویه سکتور 3','زاویه سکتور 4','شهر مخابراتی','نوع سایت','وندور',"ارتفاع سکتور 1","ارتفاع سکتور 2","ارتفاع سکتور 3","ارتفاع سکتور 4"]] 
        Data_ARAS = Data_ARAS.rename(columns={'کد سایت':'LOCATION','عرض جغرافیایی':'LATITUDE','طول جغرافیایی':'LONGITUDE',\
                                              "زاویه سکتور 1":"AZIMUTHA","زاویه سکتور 2":"AZIMUTHB","زاویه سکتور 3":"AZIMUTHC","زاویه سکتور 4":"AZIMUTHD"
                                                ,'شهر مخابراتی':"CITY",'نوع سایت':"TYPE",'وندور':"VENDOR",
                                                "ارتفاع سکتور 1":"Hight_A","ارتفاع سکتور 2":"Hight_B","ارتفاع سکتور 3":"Hight_C","ارتفاع سکتور 4":"Hight_D"
                                                }) 


        Data_ARAS["LEN"]=Data_ARAS['LOCATION'].str.len()
        Data_ARAS=Data_ARAS[(Data_ARAS['LEN']==6)]                                        
        #Data_ARAS1=Data_ARAS['LOCATION']
        Data_ARAS1=Data_ARAS.dropna(subset=['AZIMUTHA','Hight_A'])
        Data_ARAS1['CELLNAME']=Data_ARAS1['LOCATION']+"A"
        Data_ARAS1=Data_ARAS1[['LOCATION','CELLNAME','BSC/RNC','LATITUDE','LONGITUDE','AZIMUTHA','CITY',"TYPE",'VENDOR','Hight_A']]
        Data_ARAS1=Data_ARAS1.rename(columns={"AZIMUTHA":"AZIMUTH",'Hight_A':"HIGHT"})

        Data_ARAS2=Data_ARAS.dropna(subset=['AZIMUTHB'])
        Data_ARAS2['CELLNAME']=Data_ARAS2['LOCATION']+"B"
        Data_ARAS2=Data_ARAS2[['LOCATION','CELLNAME','BSC/RNC','LATITUDE','LONGITUDE','AZIMUTHB','CITY',"TYPE",'VENDOR','Hight_B']]
        Data_ARAS2=Data_ARAS2.rename(columns={"AZIMUTHB":"AZIMUTH",'Hight_B':"HIGHT"})

        Data_ARAS3=Data_ARAS.dropna(subset=['AZIMUTHC'])
        Data_ARAS3['CELLNAME']=Data_ARAS3['LOCATION']+"C"
        Data_ARAS3=Data_ARAS3[['LOCATION','CELLNAME','BSC/RNC','LATITUDE','LONGITUDE','AZIMUTHC','CITY',"TYPE",'VENDOR','Hight_C']]
        Data_ARAS3=Data_ARAS3.rename(columns={"AZIMUTHC":"AZIMUTH",'Hight_C':"HIGHT"})

        Data_ARAS4=Data_ARAS.dropna(subset=['AZIMUTHD'])
        Data_ARAS4['CELLNAME']=Data_ARAS4['LOCATION']+"D"
        Data_ARAS4=Data_ARAS4[['LOCATION','CELLNAME','BSC/RNC','LATITUDE','LONGITUDE','AZIMUTHD','CITY',"TYPE",'VENDOR','Hight_D']]
        Data_ARAS4=Data_ARAS4.rename(columns={"AZIMUTHD":"AZIMUTH",'Hight_D':"HIGHT"})

        Data_ARAS=pd.concat([Data_ARAS1,Data_ARAS2,Data_ARAS3,Data_ARAS4])

        Data_ARAS["AZIMUTH"] = Data_ARAS["AZIMUTH"].str.split("/", n=1, expand=True)[0]



        return(Data_ARAS)


    except Exception as e:
        print(f"Error in processing cell dump: {str(e)}")
        raise

if __name__=="__main__":
    PSC_PATH="Aras update 20251130.xlsx"
    PASC_BASE=Path.cwd()
    ARAS_PATH=PASC_BASE/PSC_PATH
    df=Aras(ARAS_PATH)
    print(df)
    df.to_csv("EXPORT_ARAS.csv", index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)


    print("Successful")