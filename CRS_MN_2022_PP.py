import pandas as pd
import json
import couchdb

from numpy import nan

# #dev db
# couch = couchdb.Server('http://hailUser:hailpro1995@52.162.35.208:5984/')
# db = couch["hpdb-dev"]
# #test db
# couchT = couchdb.Server('http://test-db-user:tst-db-194581@52.159.92.148:5984/')
# dbt = couchT["hpdb-tst"]
# #prod db
# couchP = couchdb.Server('http://prod-db-user:prd-db-9185601@52.159.103.140:5984/')
# dbp = couchP["hpdb-prd"]

# Rate Group table
dfRG = pd.read_excel (r'D:\Capmation\PP\CRS_MN\2022 Minnesota Hail  Wind Rates - Agents.xlsx', sheet_name='Rate Groups', header = 2)
print(dfRG)
print("NaNs o Nones: ", dfRG.isnull().values.any())
print("Zeros: ",  ( dfRG == 0).sum())
# Production Hail table
dfPH = pd.read_excel (r'D:\Capmation\PP\CRS_MN\2022 Minnesota Hail  Wind Rates - Agents.xlsx', sheet_name='Production Hail', header = 3)
print("NaNs o Nones: ", dfPH.isnull().values.any())
print("Zeros: ",  ( dfPH == 0).sum())

cols = dfPH.columns.tolist()
cols[0] = "Rate Group"
dfPH.columns = cols
print(dfPH)

counter = 0
data=[0 for i in range(len(dfRG)*5*8)]

coverages = [50, 55, 60, 65, 70, 75, 80, 85]
CPPs = [100, 105, 110, 115, 120]
# 2722 counties and townships x 8 coverage levels x 5 CPPs
for i in range(len(dfRG)): # counties and towns
    county = str(dfRG.loc[i][1]).upper()
    township = dfRG.loc[i][2]
    rangeS = dfRG.loc[i][3]

    if township == "All Other":
        township = "OTHER"
        rangeS = "OTHER"

    rgA = dfRG.loc[i][4] # A, C
    rgB = dfRG.loc[i][5] # B
    rgD = dfRG.loc[i][6] # D, E

    dfPHA = dfPH.loc[dfPH['Rate Group'] == rgA].iloc[:, 2:10].reset_index(drop=True) # rate group A
    dfPHB = dfPH.loc[dfPH['Rate Group'] == rgB].iloc[:, 10:18].reset_index(drop=True) # rate group B
    dfPHC = dfPH.loc[dfPH['Rate Group'] == rgA].iloc[:, 18:26].reset_index(drop=True) # rate group A
    dfPHD = dfPH.loc[dfPH['Rate Group'] == rgD].iloc[:, 26:34].reset_index(drop=True) # rate group D
    dfPHE = dfPH.loc[dfPH['Rate Group'] == rgD].iloc[:, 34:42].reset_index(drop=True) # rate group D

    for j in range(5): # CPPs
        for z in range(8): # coverage levels
            formN = "P" + str(CPPs[j]) + "-" + str(coverages[z])
            data[counter] = {"_id": "MN" + "_" + county + "_" + "CRS" + "_" + formN + "_" + township + "_" + rangeS + "_2022",
                             "state": "MN", "aip": "CRS", "form": formN, "county": county,
                             "township": township, "range": rangeS,
                             "crop_types": {"A": ["corn","corn silage","popcorn","seed corn","sweet corn"], "B": ["soybeans"], "C":["dry beans"],
                                "D": ["wheat","oats","sunflowers","rye"],"E":["barley"]},
                             "crops": {"A": str(dfPHA.loc[j][z]), "B": str(dfPHB.loc[j][z]), "C": str(dfPHC.loc[j][z]), "D": str(dfPHD.loc[j][z]), "E": str(dfPHE.loc[j][z])}}
            print(counter, data[counter])
            # db.save(data[counter])
            counter += 1