import pandas as pd
import json
import couchdb

pd.options.mode.chained_assignment = None  # for raw-dataframe manipulation
CODES_INITIAL_INDEX = 67 - 2  # two values less for Excel reasons
CODES_FINAL_INDEX = 106 - 1  # one value less for Excel reasons
CROPS = ['A', 'B', 'D']
""" 
Codes and forms are related between them, each code has an
associated form value. They are parallel arrays
"""
codes = []  # dataframe column names as codes for crops [CODES DATAFRAME]
forms = {}  # form pattern associated with codes CODES [DATAFRAME]
counties = {}  # dictionary for county-name values [DATAFRAME]


def generate_json(county, form, a, b, d, n, w) -> dict:
    '''
    Final database requieres specific object composition in certain JSON value.
    This JSON is composed by various data according to the context. The crop
    categories are fixed variables for MN case. A dict is returned, JSON is
    generated in preproccesing of the database

            Parameters:
                    county (string): County specific name
                    form (string): Form pattern
                    a (string): Crop type value for A category
                    b (string): Crop type value for B category
                    d (string): Crop type value for D category
                    n (string): North coordinate
                    w (string): West coordinate

            Returns:
                    as_json (dict): JSON-like value
    '''

    as_json = {
        '_id': f'MN_{county.upper()}_GA_{form}_{n}_{w}_2022',
        'state': 'MN',
        'aip': 'GA',
        'form': f'{form}',
        'county': f'{county.upper()}',
        'township': f'{n}',
        'range' : f'{w}',
        'crop_types': {
            "A": ["corn", "corn silage","seed corn","popcorn","sweet corn"],
            "B": ["soybeans", "kidney beans dry"],
            "D": ["wheat","rye","oats","sunflowers","mint"]
        },
        'crops': {
            'A': f'{str(a)}',
            'B': f'{str(b)}',
            'D': f'{str(d)}'
        }
    }
    return as_json


def main() -> None: # total documents = 105400 = 2635 counties and townships x 40 PPs (including EU)
    # # dev db
    # couch = couchdb.Server('http://hailUser:hailpro1995@52.162.35.208:5984/')
    # db = couch["hpdb-dev"]
    # # test db
    # couchT = couchdb.Server('http://test-db-user:tst-db-194581@52.159.92.148:5984/')
    # dbt = couchT["hpdb-tst"]
    # # prod db
    # couchP = couchdb.Server('http://prod-db-user:prd-db-9185601@52.159.103.140:5984/')
    # dbp = couchP["hpdb-prd"]

    dataframe = pd.read_excel(r'D:\Capmation\AIP\GAI\GAI\MN\MN 2022 Rates.xlsx',
                              sheet_name='EXPORT TO SYSTEMS')
    print("NaNs o Nones: ", dataframe.isnull().values.any())
    print("Zeros: ", (dataframe == 0).sum().sum())

    codes_dataframe = pd.read_excel(r'D:\Capmation\AIP\GAI\GAI\MN\MN 2022 Rates.xlsx',
                                    sheet_name='POL FORM CODES')
    print("NaNs o Nones: ", codes_dataframe['Desc'].isnull().values.any())
    print("Zeros: ", (codes_dataframe['Desc'] == 0).sum().sum())

    with open('D:\Capmation\AIP\GAI\GAI\MN\counties.data') as f:
        data = f.read()
    data = data.split(',')
    # mapping original data with a county-name value in a dictionary
    odd_index = 1  # origina data iterates througth odd indexes
    for county in range(len(data)):
        counties[odd_index] = data[county]
        odd_index = odd_index + 2
    # creating references from codes dataframe to map main dataframe
    for code in range(CODES_INITIAL_INDEX, CODES_FINAL_INDEX):
        code_ = str(codes_dataframe.iloc[code]['Code'])
        codes.append(code_)
        # form pattern corrected and add code-form (key-value) to forms dictionary
        if code < 85:
            # there is a PPE xxx-xxx pattern, the second element is collected
            form_ = str(codes_dataframe.iloc[code]['Desc']).split(' ')[1]
        else:
            # for (EU) forms
            form_ = '(EU)'+str(codes_dataframe.iloc[code]['Desc']).split(' ')[2]

        forms['0' + code_] = 'P' + form_

    #Mapping to county name
    rows, _ = dataframe.shape
    for row in range(0, rows):
        dataframe['County'][row] = counties[dataframe['County'][row]]

    jsons = []
    counter = 0
    # dataframe.iloc[row][col_name]
    for row in range(0, rows):
        county = dataframe.iloc[row]['County']
        n = dataframe.iloc[row]['Township']
        w = dataframe.iloc[row]['Range']
        for code in codes:
            a = round(dataframe.iloc[row]['A0' + code], 2)
            b = round(dataframe.iloc[row]['B0' + code], 2)
            d = round(dataframe.iloc[row]['D0' + code], 2)
            form = forms['0' + code]
            json_ = generate_json(county, form, a, b, d, n, w)
            jsons.append(json_)
            print(counter, json_)
            # db.save(json_)
            counter += 1

    output_values = json.dumps(jsons, indent=2)
    # generating file from proccessed data
    data = open('mn_gai.json', 'wt')
    data.write(output_values)
    data.close()


if __name__ == '__main__':
    main()