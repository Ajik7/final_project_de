import pymongo
import pandas as pd
import sqlalchemy


# create a connection to the database
conn = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
client = pymongo.MongoClient("mongodb+srv://dbajikurniawan:dbajikurniawan@cluster0.xvsguzc.mongodb.net/test")

# Database Name
db = client["sample_training"]
 
# Collection Name
col_zips = db["zips"]
col_comp = db["companies"]


#read collections
df_zips = pd.DataFrame(list(col_zips.find()))
df_com = pd.DataFrame(list(col_comp.find()))


# step collection companies

##ubah element kolom di array office  menjadi list
df_office = pd.DataFrame(df_com['offices'].values.tolist())
df_data_zips = pd.DataFrame(df_zips['loc'].values.tolist())

# select column office 
df_off = df_office.assign(new=df_office[0]) 
df_office1=df_off["new"]
d = df_office1

# set column office to list
d["D"] = pd.Series(d)

#drop none value
df_companies = d["D"].dropna()

#ubah list of dict dalam office ke kolom baru
df_companies= pd.DataFrame(df_companies.values.tolist())

#join office dengan df_comp
df_merged_companies= df_com.join(df_companies)

#rename kolom df_comp office
data_companies=df_merged_companies.rename(columns = {0: 'description', 1: 'address1', 2: 'address2', 3: 'zip_code', 4: 'city', 5: 'state_code', 6: 'country_code', 7: 'latitude', 8: 'longitude'})

#drop kolom yang mengandung array
data_companies= data_companies.drop(['image','products','relationships','competitions','providerships','funding_rounds','investments','acquisition','acquisitions','offices','milestones','video_embeds','screenshots','external_links','partners', 'ipo'], axis = 1)


## Proses collection zips

##ubah element kolom di array office  menjadi list
df_data_zips = pd.DataFrame(df_zips['loc'].values.tolist())

#rename kolom y dan x menjadi lat dan long
df_data_zips=df_data_zips.rename(columns = {'y': 'latitude', 'x': 'longitude'})

#join kolom lat dan long ke df
df_merged = df_zips.join(df_data_zips)

#drop kolom loc
data_zips=df_merged.drop(columns=['loc'])

print(data_zips)
print(data_companies)

# write the dataframe to a table in the database
data_zips.to_sql('data_zips', con=conn, if_exists='replace', index=False)
# write the dataframe to a table in the database
data_companies.to_sql('data_companies', con=conn, if_exists='replace', index=False)

