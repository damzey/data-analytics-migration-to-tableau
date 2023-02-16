import os
import pandas as pd
import time
cwd = os.getcwd()

# importing and cleaning flight data from 1987

start = time.time()
df_1987 = pd.read_csv(cwd+"/flights_data/1987.csv.bz2", index_col=0)
df_1987_2 = df_1987[:16]
df_1987_2.isnull().sum()
df_1987_2.isna().sum()
df_1987_2.duplicated().sum()
df_1987_2.drop_duplicates(keep=False, inplace=True)
df_1987_2 = df_1987_2.dropna(axis=1)
df_1987_2 = df_1987_2.fillna(0)

df_1987_2.insert(0, 'Year', df_1987_2.index)
df_1987_2 = df_1987_2.reset_index(drop = True)
print('I am now listing the columns to help myself order them: ')
print(list(df_1987_2.columns))
print()
df_1987_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1987_2['FlightDate'] = df_1987_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1987_2['FlightDate'] = pd.to_datetime(df_1987_2['FlightDate']) 

df_1987_2.insert(6, 'CRSDepDateTime', "Any")
df_1987_2['CRSDepTime'] = df_1987_2['CRSDepTime'].astype(str)
df_1987_2['CRSDepTime'] = df_1987_2['CRSDepTime'].str.zfill(4)
df_1987_2["CRSDepDateTime"] = df_1987_2["FlightDate"].astype(str) + ' ' +  df_1987_2["CRSDepTime"].astype(str)
df_1987_2["CRSDepDateTime"] = pd.to_datetime(df_1987_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1987_2.insert(8, 'CRSArrDateTime', "Any")
df_1987_2['CRSArrTime'] = df_1987_2['CRSArrTime'].astype(str)
df_1987_2['CRSArrTime'] = df_1987_2['CRSArrTime'].str.zfill(4)
df_1987_2["CRSArrDateTime"] = df_1987_2["FlightDate"].astype(str) + ' ' +  df_1987_2["CRSArrTime"].astype(str)
df_1987_2["CRSArrDateTime"] = pd.to_datetime(df_1987_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1987_2['CRSElapsedTime'] = df_1987_2['CRSArrDateTime'] - df_1987_2['CRSDepDateTime'] 
df_1987_2['CRSElapsedTime']= df_1987_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1987_2['CRSElapsedTime'] = pd.to_timedelta(df_1987_2['CRSElapsedTime'])
df_1987_2 = df_1987_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1987_2['CRSElapsedTime']])
df_1987_2['CRSElapsedTime']= df_1987_2['CRSElapsedTime'].astype(int)

df_1987_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime',
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1987_2.head())
print()
print(df_1987_2.shape)
print()
print(f'Time: {time.time() - start}')
print()
print(list(df_1987_2.columns))
print()
df_1987_2 = df_1987_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1987_2.head())
print()
print(df_1987_2.isnull().sum())
print()
print(df_1987_2['Cancelled'].value_counts())
print()
print(df_1987_2['Dest'].value_counts())
print()
print(df_1987_2['Origin'].value_counts())
print()
print(df_1987_2['Diverted'].value_counts())

df_1987_2.info()

