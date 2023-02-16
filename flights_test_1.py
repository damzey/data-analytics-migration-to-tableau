import missingno
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
df_1987_2["CRSElapsedTime"] = df_1987_2["CRSElapsedTime"].astype(int)

df_1987_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime', 
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1987_2.head())
print()
print(df_1987_2.shape)
print()
df_1987_2 = df_1987_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print()
print(df_1987_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1988

start = time.time()
df_1988 = pd.read_csv(cwd+"/flights_data/1988.csv.bz2", index_col=0)
df_1988_2 = df_1988[:16]
df_1988_2.isnull().sum()
df_1988_2.isna().sum()
df_1988_2.duplicated().sum()
df_1988_2.drop_duplicates(keep=False, inplace=True)
df_1988_2 = df_1988_2.dropna(axis=1)
df_1988_2 = df_1988_2.fillna(0)

df_1988_2.insert(0, 'Year', df_1988_2.index)
df_1988_2 = df_1988_2.reset_index(drop = True)
print(list(df_1988_2.columns))
print()
df_1988_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1988_2['FlightDate'] = df_1988_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1988_2['FlightDate'] = pd.to_datetime(df_1988_2['FlightDate']) 

df_1988_2.insert(6, 'CRSDepDateTime', "Any")
df_1988_2['CRSDepTime'] = df_1988_2['CRSDepTime'].astype(str)
df_1988_2['CRSDepTime'] = df_1988_2['CRSDepTime'].str.zfill(4)
df_1988_2["CRSDepDateTime"] = df_1988_2["FlightDate"].astype(str) + ' ' +  df_1988_2["CRSDepTime"].astype(str)
df_1988_2["CRSDepDateTime"] = pd.to_datetime(df_1988_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1988_2.insert(8, 'CRSArrDateTime', "Any")
df_1988_2['CRSArrTime'] = df_1988_2['CRSArrTime'].astype(str)
df_1988_2['CRSArrTime'] = df_1988_2['CRSArrTime'].str.zfill(4)
df_1988_2["CRSArrDateTime"] = df_1988_2["FlightDate"].astype(str) + ' ' +  df_1988_2["CRSArrTime"].astype(str)
df_1988_2["CRSArrDateTime"] = pd.to_datetime(df_1988_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1988_2['CRSElapsedTime'] = df_1988_2['CRSArrDateTime'] - df_1988_2['CRSDepDateTime'] 
df_1988_2['CRSElapsedTime']= df_1988_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1988_2['CRSElapsedTime'] = pd.to_timedelta(df_1988_2['CRSElapsedTime'])
df_1988_2 = df_1988_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1988_2['CRSElapsedTime']])
df_1988_2['CRSElapsedTime'] = df_1988_2['CRSElapsedTime'].astype(int)

df_1988_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime',
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1988_2.head())
print()
print(df_1988_2.shape)
print()
print(list(df_1988_2.columns))
print()
df_1988_2 = df_1988_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1988_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1989

start = time.time()
df_1989 = pd.read_csv(cwd+"/flights_data/1989.csv.bz2", index_col=0)
df_1989_2 = df_1989[:16]
df_1989_2.isnull().sum()
df_1989_2.isna().sum()
df_1989_2.duplicated().sum()
df_1989_2.drop_duplicates(keep=False, inplace=True)
df_1989_2 = df_1989_2.dropna(axis=1)
df_1989_2 = df_1989_2.fillna(0)

df_1989_2.insert(0, 'Year', df_1989_2.index)
df_1989_2 = df_1989_2.reset_index(drop = True)
print(list(df_1989_2.columns))
print()
df_1989_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1989_2['FlightDate'] = df_1989_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1989_2['FlightDate'] = pd.to_datetime(df_1989_2['FlightDate']) 

df_1989_2.insert(6, 'CRSDepDateTime', "Any")
df_1989_2['CRSDepTime'] = df_1989_2['CRSDepTime'].astype(str)
df_1989_2['CRSDepTime'] = df_1989_2['CRSDepTime'].str.zfill(4)
df_1989_2["CRSDepDateTime"] = df_1989_2["FlightDate"].astype(str) + ' ' +  df_1989_2["CRSDepTime"].astype(str)
df_1989_2["CRSDepDateTime"] = pd.to_datetime(df_1989_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1989_2.insert(8, 'CRSArrDateTime', "Any")
df_1989_2['CRSArrTime'] = df_1989_2['CRSArrTime'].astype(str)
df_1989_2['CRSArrTime'] = df_1989_2['CRSArrTime'].str.zfill(4)
df_1989_2["CRSArrDateTime"] = df_1989_2["FlightDate"].astype(str) + ' ' +  df_1989_2["CRSArrTime"].astype(str)
df_1989_2["CRSArrDateTime"] = pd.to_datetime(df_1989_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1989_2['CRSElapsedTime'] = df_1989_2['CRSArrDateTime'] - df_1989_2['CRSDepDateTime'] 
df_1989_2['CRSElapsedTime']= df_1989_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1989_2['CRSElapsedTime'] = pd.to_timedelta(df_1989_2['CRSElapsedTime'])
df_1989_2 = df_1989_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1989_2['CRSElapsedTime']])
df_1989_2['CRSElapsedTime'] = df_1989_2['CRSElapsedTime'].astype(int)

df_1989_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime',
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1989_2.head())
print()
print(df_1989_2.shape)
print()
print(list(df_1989_2.columns))
print()
df_1989_2 = df_1989_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1989_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1990

start = time.time()
df_1990 = pd.read_csv(cwd+"/flights_data/1990.csv.bz2", index_col=0)
df_1990_2 = df_1990[:16]
df_1990_2.isnull().sum()
df_1990_2.isna().sum()
df_1990_2.duplicated().sum()
df_1990_2.drop_duplicates(keep=False, inplace=True)
df_1990_2 = df_1990_2.dropna(axis=1)
df_1990_2 = df_1990_2.fillna(0)

df_1990_2.insert(0, 'Year', df_1990_2.index)
df_1990_2 = df_1990_2.reset_index(drop = True)
print(list(df_1990_2.columns))
print()
df_1990_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1990_2['FlightDate'] = df_1990_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1990_2['FlightDate'] = pd.to_datetime(df_1990_2['FlightDate']) 

df_1990_2.insert(6, 'CRSDepDateTime', "Any")
df_1990_2['CRSDepTime'] = df_1990_2['CRSDepTime'].astype(str)
df_1990_2['CRSDepTime'] = df_1990_2['CRSDepTime'].str.zfill(4)
df_1990_2["CRSDepDateTime"] = df_1990_2["FlightDate"].astype(str) + ' ' +  df_1990_2["CRSDepTime"].astype(str)
df_1990_2["CRSDepDateTime"] = pd.to_datetime(df_1990_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1990_2.insert(8, 'CRSArrDateTime', "Any")
df_1990_2['CRSArrTime'] = df_1990_2['CRSArrTime'].astype(str)
df_1990_2['CRSArrTime'] = df_1990_2['CRSArrTime'].str.zfill(4)
df_1990_2["CRSArrDateTime"] = df_1990_2["FlightDate"].astype(str) + ' ' +  df_1990_2["CRSArrTime"].astype(str)
df_1990_2["CRSArrDateTime"] = pd.to_datetime(df_1990_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1990_2['CRSElapsedTime'] = df_1990_2['CRSArrDateTime'] - df_1990_2['CRSDepDateTime'] 
df_1990_2['CRSElapsedTime']= df_1990_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1990_2['CRSElapsedTime'] = pd.to_timedelta(df_1990_2['CRSElapsedTime'])
df_1990_2 = df_1990_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1990_2['CRSElapsedTime']])
df_1990_2['CRSElapsedTime'] = df_1990_2['CRSElapsedTime'].astype(int)

df_1990_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime',
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1990_2.head())
print()
print(df_1990_2.shape)
print()
print(list(df_1990_2.columns))
print()
df_1990_2 = df_1990_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1990_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1991

start = time.time()
df_1991 = pd.read_csv(cwd+"/flights_data/1991.csv.bz2", index_col=0)
df_1991_2 = df_1991[:16]
df_1991_2.isnull().sum()
df_1991_2.isna().sum()
df_1991_2.duplicated().sum()
df_1991_2.drop_duplicates(keep=False, inplace=True)
df_1991_2 = df_1991_2.dropna(axis=1)
df_1991_2 = df_1991_2.fillna(0)

df_1991_2.insert(0, 'Year', df_1991_2.index)
df_1991_2 = df_1991_2.reset_index(drop = True)
print(list(df_1991_2.columns))
print()
df_1991_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1991_2['FlightDate'] = df_1991_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1991_2['FlightDate'] = pd.to_datetime(df_1991_2['FlightDate']) 

df_1991_2.insert(6, 'CRSDepDateTime', "Any")
df_1991_2['CRSDepTime'] = df_1991_2['CRSDepTime'].astype(str)
df_1991_2['CRSDepTime'] = df_1991_2['CRSDepTime'].str.zfill(4)
df_1991_2["CRSDepDateTime"] = df_1991_2["FlightDate"].astype(str) + ' ' +  df_1991_2["CRSDepTime"].astype(str)
df_1991_2["CRSDepDateTime"] = pd.to_datetime(df_1991_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1991_2.insert(8, 'CRSArrDateTime', "Any")
df_1991_2['CRSArrTime'] = df_1991_2['CRSArrTime'].astype(str)
df_1991_2['CRSArrTime'] = df_1991_2['CRSArrTime'].str.zfill(4)
df_1991_2["CRSArrDateTime"] = df_1991_2["FlightDate"].astype(str) + ' ' +  df_1991_2["CRSArrTime"].astype(str)
df_1991_2["CRSArrDateTime"] = pd.to_datetime(df_1991_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1991_2['CRSElapsedTime'] = df_1991_2['CRSArrDateTime'] - df_1991_2['CRSDepDateTime'] 
df_1991_2['CRSElapsedTime']= df_1991_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1991_2['CRSElapsedTime'] = pd.to_timedelta(df_1991_2['CRSElapsedTime'])
df_1991_2 = df_1991_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1991_2['CRSElapsedTime']])
df_1991_2['CRSElapsedTime'] = df_1991_2['CRSElapsedTime'].astype(int)

df_1991_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime',
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1991_2.head())
print()
print(df_1991_2.shape)
print()
print(list(df_1991_2.columns))
print()
df_1991_2 = df_1991_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1991_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1992

start = time.time()
df_1992 = pd.read_csv(cwd+"/flights_data/1992.csv.bz2", index_col=0)
df_1992_2 = df_1992[:16]
df_1992_2.isnull().sum()
df_1992_2.isna().sum()
df_1992_2.duplicated().sum()
df_1992_2.drop_duplicates(keep=False, inplace=True)
df_1992_2 = df_1992_2.dropna(axis=1)
df_1992_2 = df_1992_2.fillna(0)

df_1992_2.insert(0, 'Year', df_1992_2.index)
df_1992_2 = df_1992_2.reset_index(drop = True)
print(list(df_1992_2.columns))
print()
df_1992_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1992_2['FlightDate'] = df_1992_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1992_2['FlightDate'] = pd.to_datetime(df_1992_2['FlightDate']) 

df_1992_2.insert(6, 'CRSDepDateTime', "Any")
df_1992_2['CRSDepTime'] = df_1992_2['CRSDepTime'].astype(str)
df_1992_2['CRSDepTime'] = df_1992_2['CRSDepTime'].str.zfill(4)
df_1992_2["CRSDepDateTime"] = df_1992_2["FlightDate"].astype(str) + ' ' +  df_1992_2["CRSDepTime"].astype(str)
df_1992_2["CRSDepDateTime"] = pd.to_datetime(df_1992_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1992_2.insert(8, 'CRSArrDateTime', "Any")
df_1992_2['CRSArrTime'] = df_1992_2['CRSArrTime'].astype(str)
df_1992_2['CRSArrTime'] = df_1992_2['CRSArrTime'].str.zfill(4)
df_1992_2["CRSArrDateTime"] = df_1992_2["FlightDate"].astype(str) + ' ' +  df_1992_2["CRSArrTime"].astype(str)
df_1992_2["CRSArrDateTime"] = pd.to_datetime(df_1992_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1992_2['CRSElapsedTime'] = df_1992_2['CRSArrDateTime'] - df_1992_2['CRSDepDateTime'] 
df_1992_2['CRSElapsedTime']= df_1992_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1992_2['CRSElapsedTime'] = pd.to_timedelta(df_1992_2['CRSElapsedTime'])
df_1992_2 = df_1992_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1992_2['CRSElapsedTime']])
df_1992_2['CRSElapsedTime'] = df_1992_2['CRSElapsedTime'].astype(int)

df_1992_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime',
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1992_2.head())
print()
print(df_1992_2.shape)
print()
print(list(df_1992_2.columns))
print()
df_1992_2 = df_1992_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1992_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1993

start = time.time()
df_1993 = pd.read_csv(cwd+"/flights_data/1993.csv.bz2", index_col=0)
df_1993_2 = df_1993[:16]
df_1993_2.isnull().sum()
df_1993_2.isna().sum()
df_1993_2.duplicated().sum()
df_1993_2.drop_duplicates(keep=False, inplace=True)
df_1993_2 = df_1993_2.dropna(axis=1)
df_1993_2 = df_1993_2.fillna(0)

df_1993_2.insert(0, 'Year', df_1993_2.index)
df_1993_2 = df_1993_2.reset_index(drop = True)
print(list(df_1993_2.columns))
print()
df_1993_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1993_2['FlightDate'] = df_1993_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1993_2['FlightDate'] = pd.to_datetime(df_1993_2['FlightDate']) 

df_1993_2.insert(6, 'CRSDepDateTime', "Any")
df_1993_2['CRSDepTime'] = df_1993_2['CRSDepTime'].astype(str)
df_1993_2['CRSDepTime'] = df_1993_2['CRSDepTime'].str.zfill(4)
df_1993_2["CRSDepDateTime"] = df_1993_2["FlightDate"].astype(str) + ' ' +  df_1993_2["CRSDepTime"].astype(str)
df_1993_2["CRSDepDateTime"] = pd.to_datetime(df_1993_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1993_2.insert(8, 'CRSArrDateTime', "Any")
df_1993_2['CRSArrTime'] = df_1993_2['CRSArrTime'].astype(str)
df_1993_2['CRSArrTime'] = df_1993_2['CRSArrTime'].str.zfill(4)
df_1993_2["CRSArrDateTime"] = df_1993_2["FlightDate"].astype(str) + ' ' +  df_1993_2["CRSArrTime"].astype(str)
df_1993_2["CRSArrDateTime"] = pd.to_datetime(df_1993_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1993_2['CRSElapsedTime'] = df_1993_2['CRSArrDateTime'] - df_1993_2['CRSDepDateTime'] 
df_1993_2['CRSElapsedTime']= df_1993_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1993_2['CRSElapsedTime'] = pd.to_timedelta(df_1993_2['CRSElapsedTime'])
df_1993_2 = df_1993_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1993_2['CRSElapsedTime']])
df_1993_2['CRSElapsedTime'] = df_1993_2['CRSElapsedTime'].astype(int)

df_1993_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime',
'ActualElapsedTime', 'ArrDelay', 'DepDelay', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1993_2.head())
print()
print(df_1993_2.shape)
print()
print(list(df_1993_2.columns))
print()
df_1993_2 = df_1993_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1993_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1994

start = time.time()
df_1994 = pd.read_csv(cwd+"/flights_data/1994.csv.bz2", index_col=0)
df_1994_2 = df_1994[:16]
df_1994_2.isnull().sum()
df_1994_2.isna().sum()
df_1994_2.duplicated().sum()
df_1994_2.drop_duplicates(keep=False, inplace=True)
df_1994_2 = df_1994_2.dropna(axis=1)
df_1994_2 = df_1994_2.fillna(0)

df_1994_2.insert(0, 'Year', df_1994_2.index)
df_1994_2 = df_1994_2.reset_index(drop = True)
print(list(df_1994_2.columns))
print()
df_1994_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1994_2['FlightDate'] = df_1994_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1994_2['FlightDate'] = pd.to_datetime(df_1994_2['FlightDate']) 

df_1994_2.insert(6, 'CRSDepDateTime', "Any")
df_1994_2['CRSDepTime'] = df_1994_2['CRSDepTime'].astype(str)
df_1994_2['CRSDepTime'] = df_1994_2['CRSDepTime'].str.zfill(4)
df_1994_2["CRSDepDateTime"] = df_1994_2["FlightDate"].astype(str) + ' ' +  df_1994_2["CRSDepTime"].astype(str)
df_1994_2["CRSDepDateTime"] = pd.to_datetime(df_1994_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1994_2.insert(8, 'CRSArrDateTime', "Any")
df_1994_2['CRSArrTime'] = df_1994_2['CRSArrTime'].astype(str)
df_1994_2['CRSArrTime'] = df_1994_2['CRSArrTime'].str.zfill(4)
df_1994_2["CRSArrDateTime"] = df_1994_2["FlightDate"].astype(str) + ' ' +  df_1994_2["CRSArrTime"].astype(str)
df_1994_2["CRSArrDateTime"] = pd.to_datetime(df_1994_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1994_2['CRSElapsedTime'] = df_1994_2['CRSArrDateTime'] - df_1994_2['CRSDepDateTime'] 
df_1994_2['CRSElapsedTime']= df_1994_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1994_2['CRSElapsedTime'] = pd.to_timedelta(df_1994_2['CRSElapsedTime'])
df_1994_2 = df_1994_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1994_2['CRSElapsedTime']])
df_1994_2['CRSElapsedTime'] = df_1994_2['CRSElapsedTime'].astype(int)

df_1994_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek',
'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1994_2.head())
print()
print(df_1994_2.shape)
print()
print(list(df_1994_2.columns))
print()
df_1994_2 = df_1994_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1994_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1995

start = time.time()
df_1995 = pd.read_csv(cwd+"/flights_data/1995.csv.bz2", index_col=0)
df_1995_2 = df_1995[:16]
df_1995_2.isnull().sum()
df_1995_2.isna().sum()
df_1995_2.duplicated().sum()
df_1995_2.drop_duplicates(keep=False, inplace=True)
df_1995_2 = df_1995_2.dropna(axis=1)
df_1995_2 = df_1995_2.fillna(0)

df_1995_2.insert(0, 'Year', df_1995_2.index)
df_1995_2 = df_1995_2.reset_index(drop = True)
print(list(df_1995_2.columns))
print()
df_1995_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1995_2['FlightDate'] = df_1995_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1995_2['FlightDate'] = pd.to_datetime(df_1995_2['FlightDate']) 

df_1995_2.insert(6, 'CRSDepDateTime', "Any")
df_1995_2['CRSDepTime'] = df_1995_2['CRSDepTime'].astype(str)
df_1995_2['CRSDepTime'] = df_1995_2['CRSDepTime'].str.zfill(4)
df_1995_2["CRSDepDateTime"] = df_1995_2["FlightDate"].astype(str) + ' ' +  df_1995_2["CRSDepTime"].astype(str)
df_1995_2["CRSDepDateTime"] = pd.to_datetime(df_1995_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1995_2.insert(8, 'CRSArrDateTime', "Any")
df_1995_2['CRSArrTime'] = df_1995_2['CRSArrTime'].astype(str)
df_1995_2['CRSArrTime'] = df_1995_2['CRSArrTime'].str.zfill(4)
df_1995_2["CRSArrDateTime"] = df_1995_2["FlightDate"].astype(str) + ' ' +  df_1995_2["CRSArrTime"].astype(str)
df_1995_2["CRSArrDateTime"] = pd.to_datetime(df_1995_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1995_2['CRSElapsedTime'] = df_1995_2['CRSArrDateTime'] - df_1995_2['CRSDepDateTime'] 
df_1995_2['CRSElapsedTime']= df_1995_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1995_2['CRSElapsedTime'] = pd.to_timedelta(df_1995_2['CRSElapsedTime'])
df_1995_2 = df_1995_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1995_2['CRSElapsedTime']])
df_1995_2['CRSElapsedTime'] = df_1995_2['CRSElapsedTime'].astype(int)

df_1995_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek',
'TailNum', 'AirTime', 'TaxiIn', 'TaxiOut', 'CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
print(df_1995_2.head())
print()
print(df_1995_2.shape)
print()
print(list(df_1995_2.columns))
print()
df_1995_2 = df_1995_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1995_2.head())
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1996

start = time.time()
df_1996 = pd.read_csv(cwd+"/flights_data/1996.csv.bz2", index_col=0)
df_1996_2 = df_1996[:16]
df_1996_2.isnull().sum()
df_1996_2.isna().sum()
df_1996_2.duplicated().sum()
df_1996_2.drop_duplicates(keep=False, inplace=True)
df_1996_2 = df_1996_2.dropna(axis=1)
df_1996_2 = df_1996_2.fillna(0)

df_1996_2.insert(0, 'Year', df_1996_2.index)
df_1996_2 = df_1996_2.reset_index(drop = True)
print(list(df_1996_2.columns))
print()
df_1996_2.insert(4, 'FlightDate', "Any")
cols= ["Year","Month","DayofMonth"]
df_1996_2['FlightDate'] = df_1996_2[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1996_2['FlightDate'] = pd.to_datetime(df_1996_2['FlightDate']) 

df_1996_2.insert(6, 'CRSDepDateTime', "Any")
df_1996_2['CRSDepTime'] = df_1996_2['CRSDepTime'].astype(str)
df_1996_2['CRSDepTime'] = df_1996_2['CRSDepTime'].str.zfill(4)
df_1996_2["CRSDepDateTime"] = df_1996_2["FlightDate"].astype(str) + ' ' +  df_1996_2["CRSDepTime"].astype(str)
df_1996_2["CRSDepDateTime"] = pd.to_datetime(df_1996_2["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')

df_1996_2.insert(8, 'CRSArrDateTime', "Any")
df_1996_2['CRSArrTime'] = df_1996_2['CRSArrTime'].astype(str)
df_1996_2['CRSArrTime'] = df_1996_2['CRSArrTime'].str.zfill(4)
df_1996_2["CRSArrDateTime"] = df_1996_2["FlightDate"].astype(str) + ' ' +  df_1996_2["CRSArrTime"].astype(str)
df_1996_2["CRSArrDateTime"] = pd.to_datetime(df_1996_2["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')

df_1996_2['CRSElapsedTime'] = df_1996_2['CRSArrDateTime'] - df_1996_2['CRSDepDateTime'] 
df_1996_2['CRSElapsedTime']= df_1996_2['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1996_2['CRSElapsedTime'] = pd.to_timedelta(df_1996_2['CRSElapsedTime'])
df_1996_2 = df_1996_2.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1996_2['CRSElapsedTime']])
df_1996_2['CRSElapsedTime'] = df_1996_2['CRSElapsedTime'].astype(int)

df_1996_2.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'ArrTime', 
'ActualElapsedTime', 'TailNum', 'AirTime', 'TaxiIn', 'TaxiOut', 
'CRSDepTime', 'CRSArrTime', 'ArrDelay', 'DepDelay'], axis=1, inplace=True)
print(df_1996_2.head())
print()
print(df_1996_2.shape)
print()
print(list(df_1996_2.columns))
print()
df_1996_2 = df_1996_2.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'CRSArrDateTime', 'CRSElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1996_2.head())
print()
print(f'Time: {time.time() - start}')
print()

