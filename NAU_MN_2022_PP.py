import pandas as pd
import json
import couchdb

from numpy import nan
#pd.set_option("display.max_rows", None, "display.max_columns", None)
df = pd.read_excel (r'D:\Capmation\Minnesota 2022 Location Crop Rates - 01102022_0950_AM.xlsx', sheet_name='Minnesota 2022 Location Crop Ra')
print("NaNs o Nones: ", df.isnull().values.any())
print("Zeros: ",  ( df == 0).sum())

#print(df)
#print(df['Form Name'].value_counts())

#dev db
couch = couchdb.Server('http://hailUser:hailpro1995@52.162.35.208:5984/')
db = couch["hpdb-dev"]
# #test db
# couchT = couchdb.Server('http://test-db-user:tst-db-194581@52.159.92.148:5984/')
# dbt = couchT["hpdb-tst"]
# #prod db
# couchP = couchdb.Server('http://prod-db-user:prd-db-9185601@52.159.103.140:5984/')
# dbp = couchP["hpdb-prd"]

data=[0 for i in range(len(df))]
dataClean=[0 for i in range(len(df))]

counties = ["Aitkin",
    "Anoka",
    "Becker",
    "Beltrami",
    "Benton",
    "Big Stone",
    "Blue Earth",
    "Brown",
    "Carlton",
    "Carver",
    "Cass",
    "Chippewa",
    "Chisago",
    "Clay",
    "Clearwater",
    "Cook",
    "Cottonwood",
    "Crow Wing",
    "Dakota",
    "Dodge",
    "Douglas",
    "Faribault",
    "Fillmore",
    "Freeborn",
    "Goodhue",
    "Grant",
    "Hennepin",
    "Houston",
    "Hubbard",
    "Isanti",
    "Itasca",
    "Jackson",
    "Kanabec",
    "Kandiyohi",
    "Kittson",
    "Koochiching",
    "Lac qui Parle",
    "Lake",
    "Lake of the Woods",
    "Le Sueur",
    "Lincoln",
    "Lyon",
    "Mahnomen",
    "Marshall",
    "Martin",
    "McLeod",
    "Meeker",
    "Mille Lacs",
    "Morrison",
    "Mower",
    "Murray",
    "Nicollet",
    "Nobles",
    "Norman",
    "Olmsted",
    "Otter Tail",
    "Pennington",
    "Pine",
    "Pipestone",
    "Polk",
    "Pope",
    "Ramsey",
    "Red Lake",
    "Redwood",
    "Renville",
    "Rice",
    "Rock",
    "Roseau",
    "Scott",
    "Sherburne",
    "Sibley",
    "St Louis",
    "Stearns",
    "Steele",
    "Stevens",
    "Swift",
    "Todd",
    "Traverse",
    "Wabasha",
    "Wadena",
    "Waseca",
    "Washington",
    "Watonwan",
    "Wilkin",
    "Winona",
    "Wright",
    "Yellow Medicine"]
dictCounties = dict(zip(list(range(1,174,2)), counties))
print(dictCounties)
counter = 0
j = 0
jump = 3
aux = 0
for i in range(949*100): #94900 docs = 5 coverage levels x 5 CPPs x 949 counties x 4 types, 47450 docs if PERSONALIZED not considered
    if (i % 949) == 0: aux += 1 # a form has ended
    if aux == 3: jump = 2
    elif aux == 5:
        aux = 1
        jump = 3
    formN1 = df.loc[j][4].split(" ")
    formN = formN1[0]+"-"+formN1[2][0]
    #print(formN1)
    township = df.loc[j][2]
    rangeS = df.loc[j][3]
    county = dictCounties[df.loc[j][1]]
    if (township == 0 or rangeS == 0):
        township = 'OTHER'
        rangeS = 'OTHER'
    if (aux < 3): # still have wheat (non-personalized)
        data[counter] = {"_id": "MN" + "_" + str(county).upper() + "_" + "NAU" + "_" + formN + "_" + township + "_" + rangeS +"_2022",
                         "state": "MN", "aip": "NAU", "form": formN, "county": str(county).upper(),
                         "township": township, "range": rangeS,
                         "crop_types": {"A": ["corn", "popcorn", "sweet corn", "seed corn", "corn silage"],
               "B": ["soybeans"],
               "D": ["oats","wheat", "rye", "sunflowers"]},
                         "crops": {"A": str(df.loc[j][6]), "B": str(df.loc[j + 1][6]), "D": str(df.loc[j + 2][6])}}
        print(counter, data[counter])
        db.save(data[counter])
        counter += 1
    #else: # don't have wheat (personalized)
        # data[i] = {"_id": "MN" + "_" + str(county).upper() + "_" + "NAU" + "_" + formN + "_" + township + "_" + rangeS + "_2022",
        #            "state": "MN", "aip": "NAU", "form": formN, "county": str(county).upper(),
        #            "township": township, "range": rangeS,
        #            "crop_types": {"A": ["corn", "popcorn", "sweet corn", "seed corn", "corn silage"],
        #                           "B": ["soybeans"]},
        #            "crops": {"A": str(df.loc[j][6]), "B": str(df.loc[j + 1][6])}}

    j = j + jump
