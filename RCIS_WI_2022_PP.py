import pandas as pd
import json
import couchdb

from numpy import nan

df = pd.read_excel (r'D:\Capmation\WI Hail Rates 2022 RCIS.xlsx', sheet_name='Hail Rates Report (9)')
print("NaNs o Nones: ", df.isnull().values.any())
print("Zeros: ",  ( df == 0).sum())

# #dev db
# couch = couchdb.Server('http://hailUser:hailpro1995@52.162.35.208:5984/')
# db = couch["hpdb-dev"]
# #test db
# couchT = couchdb.Server('http://test-db-user:tst-db-194581@52.159.92.148:5984/')
# dbt = couchT["hpdb-tst"]
# #prod db
# couchP = couchdb.Server('http://prod-db-user:prd-db-9185601@52.159.103.140:5984/')
# dbp = couchP["hpdb-prd"]

data=[0 for i in range(len(df))] #

counter = 0
for i in range(24,len(df),94):
    for j in range(i,i+66,2):
        formN = df.loc[j][9][2:]
        data[counter] = {"_id": "WI" + "_" + str(
            df.loc[i][5]).upper() + "_" + "RCIS" + "_" + formN + "_" + "OTHER" + "_" + "OTHER_2022",
                         "state": "WI", "aip": "RCIS", "form": formN, "county": str(df.loc[i][5]).upper(),
                         "crop_types": {
                             "A": ["alfalfa", "barley", "corn", "mint", "popcorn", "rye", "seed corn", "sweet corn",
                                   "wheat"],
                             "B": ["dry beans", "soybeans"]},
                         "crops": {"A": str(df.loc[j][11]), "B": str(df.loc[j + 1][11])}}
        print(counter, data[counter])
        #db.save(data[counter])
        counter += 1