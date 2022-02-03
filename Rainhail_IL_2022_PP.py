import pandas as pd
import json
import couchdb

#pd.set_option("display.max_rows", None, "display.max_columns", None)

df = pd.read_excel (r'D:\Capmation\PP\Rainhail_IL\RHILPP2022.xlsx',header = 2)
print (df)
#print("NaNs o Nones: ", df.iloc[:,3:].isnull().values.any())
#print("Zeros: ",  ( df.iloc[:,3:] == 0).sum())

# #dev db
# couch = couchdb.Server('http://hailUser:hailpro1995@52.162.35.208:5984/')
# db = couch["hpdb-dev"]
# #test db
# couchT = couchdb.Server('http://test-db-user:tst-db-194581@52.159.92.148:5984/')
# dbt = couchT["hpdb-tst"]
# #prod db
# couchP = couchdb.Server('http://prod-db-user:prd-db-9185601@52.159.103.140:5984/')
# dbp = couchP["hpdb-prd"]

dfA = df.iloc[:,4:12]
dfB = df.iloc[:,12:20]
dfG = df.iloc[:,20:]

data=[0 for i in range(5000)] # 8 coverage levels x 5 CPPs x 102 counties = 4080 docs

covLevels = [50,55,60,65,70,75,80,85]

counter = 0

for i in range(len(df)):
    if i%5 == 0:
        county = str(df.loc[i][1]).upper()

    for j in range(8):
        formN = "P"+df.loc[i][3][:3]+"-"+str(covLevels[j])
        data[counter] = {"_id": "IL" + "_" + county + "_" + "RH" + "_" + formN + "_OTHER" + "_OTHER" + "_2022",
                         "state": "IL",
                         "aip": "RH",
                         "form": formN,
                         "county": county,
                         "crop_types": {"A": ["corn","alfalfa"],
                                        #"A1": ["seed corn","sweet corn","popcorn"],
                                        "B": ["soybeans","sunflowers","dry beans"],
                                        "G": ["wheat","oats","barley","rye"]},
                         "crops": {"A": str(dfA.loc[i][j]), "B": str(dfB.loc[i][j]), "G": str(dfG.loc[i][j])}}
        print(counter, data[counter])
        # db.save(data[counter])
        counter += 1
