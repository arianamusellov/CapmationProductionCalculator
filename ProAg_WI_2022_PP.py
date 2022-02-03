import pandas as pd
import json
import couchdb

from numpy import nan
#pd.set_option("display.max_rows", None, "display.max_columns", None)


dfA105 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class A  CPP 105')
dfA110 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class A  CPP 110')
dfA115 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class A  CPP 115')
dfA120 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class A  CPP 120')

dfB105 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class B  CPP 105')
dfB110 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class B  CPP 110')
dfB115 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class B  CPP 115')
dfB120 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class B  CPP 120')

dfC105 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class C  CPP 105')
dfC110 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class C  CPP 110')
dfC115 = pd.read_excel (r'D:\Capmation\PP\ProAg_WI\Wisconsin - Crop Class Rates - 2022 - PAIC.xlsx', sheet_name='Class C  CPP 115')

dfs = [dfA105,dfA110,dfA115,dfA120,dfB105,dfB110,dfB115,dfB120,dfC105,dfC110,dfC115]

for df in dfs:
    print("NaNs o Nones: ", df.isnull().values.any())
    print("Zeros: ",  ( df == 0).sum())

#dev db
couch = couchdb.Server('http://hailUser:hailpro1995@52.162.35.208:5984/')
db = couch["hpdb-dev"]
# #test db
# couchT = couchdb.Server('http://test-db-user:tst-db-194581@52.159.92.148:5984/')
# dbt = couchT["hpdb-tst"]
# #prod db
# couchP = couchdb.Server('http://prod-db-user:prd-db-9185601@52.159.103.140:5984/')
# dbp = couchP["hpdb-prd"]

data=[0 for i in range(len(dfA105)*32)]

counter = 0
for i in range(len(dfA105)): #72 counties, 32 forms (4 CPPs and 8 coverage levels) = 2304 docs
    county = str(dfA105.loc[i][1]).upper()
    for j in range(2,10): # Coverage levels
            coverages = list(dfA105.columns)
            # CPP 105
            formN = "P105-" + str(int(coverages[j]*100))
            data[counter] = {"_id": "WI" + "_" + county + "_" + "ProAg" + "_" + formN + "_OTHER" + "_OTHER" + "_2022",
                       "state": "WI", "aip": "ProAg", "form": formN, "county": county,
                       "crop_types": {"A":["corn", "seed corn", "popcorn","sweet corn"], "B":["soybeans"],
                       "C":["wheat","barley","rye","oats","sunflowers","alfalfa"]},
                       "crops": {"A": str(dfA105.loc[i][j]), "B": str(dfB105.loc[i][j]), "C": str(dfC105.loc[i][j])}}
            print(counter, data[counter])
            #db.save(data[counter])
            counter+=1
            # CPP 110
            formN = "P110-" + str(int(coverages[j] * 100))
            data[counter] = {"_id": "WI" + "_" + county + "_" + "ProAg" + "_" + formN + "_OTHER" + "_OTHER" + "_2022",
                             "state": "WI", "aip": "ProAg", "form": formN, "county": county,
                             "crop_types": {"A": ["corn", "seed corn", "popcorn", "sweet corn"], "B": ["soybeans"],
                                            "C": ["wheat", "barley", "rye", "oats", "sunflowers", "alfalfa"]},
                             "crops": {"A": str(dfA110.loc[i][j]), "B": str(dfB110.loc[i][j]),
                                       "C": str(dfC110.loc[i][j])}}
            print(counter, data[counter])
            #db.save(data[counter])
            counter += 1
            # CPP 115
            formN = "P115-" + str(int(coverages[j] * 100))
            data[counter] = {"_id": "WI" + "_" + county + "_" + "ProAg" + "_" + formN + "_OTHER" + "_OTHER" + "_2022",
                             "state": "WI", "aip": "ProAg", "form": formN, "county": county,
                             "crop_types": {"A": ["corn", "seed corn", "popcorn", "sweet corn"], "B": ["soybeans"],
                                            "C": ["wheat", "barley", "rye", "oats", "sunflowers", "alfalfa"]},
                             "crops": {"A": str(dfA115.loc[i][j]), "B": str(dfB115.loc[i][j]),
                                       "C": str(dfC115.loc[i][j])}}
            print(counter, data[counter])
            #db.save(data[counter])
            counter += 1
            # CPP 120
            formN = "P120-" + str(int(coverages[j] * 100))
            data[counter] = {"_id": "WI" + "_" + county + "_" + "ProAg" + "_" + formN + "_OTHER" + "_OTHER" + "_2022",
                             "state": "WI", "aip": "ProAg", "form": formN, "county": county,
                             "crop_types": {"A": ["corn", "seed corn", "popcorn", "sweet corn"], "B": ["soybeans"]},
                             "crops": {"A": str(dfA120.loc[i][j]), "B": str(dfB120.loc[i][j])}}
            print(counter, data[counter])
            #db.save(data[counter])
            counter += 1