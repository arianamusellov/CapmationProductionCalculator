# rangeS -> value is 0000 (int)
# haven't been rounding rates

import pandas as pd
import json
import couchdb

from numpy import nan
#pd.set_option("display.max_rows", None, "display.max_columns", None)

# Rate Groups (Counties and crop classes with corresponding rate group)
dfRateGr = pd.read_excel (r'D:\Capmation\PP\ProAg_MN\Minnesota - Base Rates - 2022 - PAIC.xlsx')
dfRateGr=dfRateGr.replace(nan,0)
print("NaNs o Nones: ", dfRateGr.iloc[:,2:].isnull().values.any())
print("Zeros: ",  ( dfRateGr == 0).sum())

# CPP Rates - Class A
dfA = pd.read_excel (r'D:\Capmation\PP\ProAg_MN\Minnesota - CPP Class A Rates - 2022 - PAIC.xlsx')
print("NaNs o Nones: ", dfA.iloc[:,1:].isnull().values.any())
print("Zeros: ",  ( dfA == 0).sum())

# CPP Rates - Class B
dfB = pd.read_excel (r'D:\Capmation\PP\ProAg_MN\Minnesota - CPP Class B Rates - 2022 - PAIC.xlsx')
print("NaNs o Nones: ", dfB.iloc[:,1:].isnull().values.any())
print("Zeros: ",  ( dfB == 0).sum())

# CPP Rates - Class D
dfD = pd.read_excel (r'D:\Capmation\PP\ProAg_MN\Minnesota - CPP Class D Rates - 2022 - PAIC.xlsx')
print("NaNs o Nones: ", dfD.iloc[:,1:].isnull().values.any())
print("Zeros: ",  ( dfD == 0).sum())

# #dev db
# couch = couchdb.Server('http://hailUser:hailpro1995@52.162.35.208:5984/')
# db = couch["hpdb-dev"]
# #test db
# couchT = couchdb.Server('http://test-db-user:tst-db-194581@52.159.92.148:5984/')
# dbt = couchT["hpdb-tst"]
# #prod db
# couchP = couchdb.Server('http://prod-db-user:prd-db-9185601@52.159.103.140:5984/')
# dbp = couchP["hpdb-prd"]

data=[0 for i in range(108880)]

counter = 0 #total documents = 108880 = 2722 x 5 x 8
for i in range(len(dfRateGr)): #2722

    if str(dfRateGr.loc[i][1]).upper() != "0":
        county = str(dfRateGr.loc[i][1]).upper()

    township = dfRateGr.loc[i][2]
    rangeS = dfRateGr.loc[i][3]

    if township == 9997:
        township = "OTHER"
        rangeS = "OTHER"

    groupA = dfRateGr.loc[i][4]
    groupB = dfRateGr.loc[i][5]
    groupD = dfRateGr.loc[i][6]

    indexRGA = dfA.index[dfA['Rate Group'] == groupA].tolist()[0]
    indexRGB = dfB.index[dfB['Rate Group'] == groupB].tolist()[0]
    indexRGD = dfD.index[dfD['Rate Group'] == groupD].tolist()[0]

    dfRGA = dfA.loc[indexRGA:indexRGA+4].reset_index(drop=True)
    dfRGB = dfB.loc[indexRGB:indexRGB + 4].reset_index(drop=True)
    dfRGD = dfD.loc[indexRGD:indexRGD + 4].reset_index(drop=True)

    for j in range(5): # CPPs (5)
        for z in range(2,10): # Coverage levels (8)
            coverages = list(dfRGA.columns)
            formN = "P" + str(dfRGA.loc[j][1][4:]) + "-" + str(int(coverages[z]*100))
            data[counter] = {"_id": "MN" + "_" + county + "_" + "ProAg" + "_" + formN + "_" + township + "_" + rangeS + "_2022",
                       "state": "MN", "aip": "ProAg", "form": formN, "county": county,
                       "township": township, "range": rangeS,
                       "crop_types": {"A": ["corn", "seed corn", "popcorn", "sweet corn"], "B": ["soybeans"],
                       "D": ["wheat", "rye", "oats", "sunflowers"]},
                       "crops": {"A": str(dfRGA.loc[j][z]), "B": str(dfRGB.loc[j][z]), "D": str(dfRGD.loc[j][z])}}
            print(counter, data[counter])
            # db.save(data[counter])
            counter+=1