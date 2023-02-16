import os
import pandas as pd
import time

cwd = os.getcwd()

# importing and cleaning flight data from 1987

start = time.time()
df_1987 = pd.read_csv(cwd+"/flights_data/1987.csv.bz2", index_col=0)
df_1987.isnull().sum()
df_1987.isna().sum()
df_1987.duplicated().sum()
df_1987.drop_duplicates(keep=False, inplace=True)
df_1987 = df_1987.dropna(axis=1, how='all')
df_1987 = df_1987.fillna(0)

df_1987.insert(0, 'Year', df_1987.index)
df_1987 = df_1987.reset_index(drop = True)
print(list(df_1987.columns))
print()
df_1987.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1987['FlightDate'] = df_1987[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1987['FlightDate'] = pd.to_datetime(df_1987['FlightDate']) 

df_1987.insert(6, 'CRSDepDateTime', "Any")
df_1987['CRSDepTime'] = df_1987['CRSDepTime'].astype(str)
df_1987['CRSDepTime'] = df_1987['CRSDepTime'].str.zfill(4)
df_1987["CRSDepDateTime"] = df_1987["FlightDate"].astype(str) + ' ' +  df_1987["CRSDepTime"].astype(str)
df_1987["CRSDepDateTime"] = pd.to_datetime(df_1987["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1987.dropna(inplace=True)

df_1987.insert(8, 'CRSArrDateTime', "Any")
df_1987['CRSArrTime'] = df_1987['CRSArrTime'].astype(str)
df_1987['CRSArrTime'] = df_1987['CRSArrTime'].str.zfill(4)
df_1987["CRSArrDateTime"] = df_1987["FlightDate"].astype(str) + ' ' +  df_1987["CRSArrTime"].astype(str)
df_1987["CRSArrDateTime"] = pd.to_datetime(df_1987["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1987.dropna(inplace=True)

df_1987['CRSElapsedTime'] = df_1987['CRSArrDateTime'] - df_1987['CRSDepDateTime'] 
df_1987['CRSElapsedTime']= df_1987['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1987['CRSElapsedTime'] = pd.to_timedelta(df_1987['CRSElapsedTime'])
df_1987 = df_1987.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1987['CRSElapsedTime']])
df_1987.dropna(inplace=True)
df_1987["CRSElapsedTime"] = df_1987["CRSElapsedTime"].astype(int)

df_1987.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1987.head())
df_1987 = df_1987.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1987.head())
print()
print(df_1987.isnull().sum())
print()
df_1987_2 = df_1987.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1988

start = time.time()
df_1988 = pd.read_csv(cwd+"/flights_data/1988.csv.bz2", index_col=0)
df_1988.isnull().sum()
df_1988.isna().sum()
df_1988.duplicated().sum()
df_1988.drop_duplicates(keep=False, inplace=True)
df_1988 = df_1988.dropna(axis=1, how='all')
df_1988 = df_1988.fillna(0)

df_1988.insert(0, 'Year', df_1988.index)
df_1988 = df_1988.reset_index(drop = True)
print(list(df_1988.columns))
print()
df_1988.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1988['FlightDate'] = df_1988[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1988['FlightDate'] = pd.to_datetime(df_1988['FlightDate']) 

df_1988.insert(6, 'CRSDepDateTime', "Any")
df_1988['CRSDepTime'] = df_1988['CRSDepTime'].astype(str)
df_1988['CRSDepTime'] = df_1988['CRSDepTime'].str.zfill(4)
df_1988["CRSDepDateTime"] = df_1988["FlightDate"].astype(str) + ' ' +  df_1988["CRSDepTime"].astype(str)
df_1988["CRSDepDateTime"] = pd.to_datetime(df_1988["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1988.dropna(inplace=True)

df_1988.insert(8, 'CRSArrDateTime', "Any")
df_1988['CRSArrTime'] = df_1988['CRSArrTime'].astype(str)
df_1988['CRSArrTime'] = df_1988['CRSArrTime'].str.zfill(4)
df_1988["CRSArrDateTime"] = df_1988["FlightDate"].astype(str) + ' ' +  df_1988["CRSArrTime"].astype(str)
df_1988["CRSArrDateTime"] = pd.to_datetime(df_1988["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1988.dropna(inplace=True)

df_1988['CRSElapsedTime'] = df_1988['CRSArrDateTime'] - df_1988['CRSDepDateTime'] 
df_1988['CRSElapsedTime']= df_1988['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1988['CRSElapsedTime'] = pd.to_timedelta(df_1988['CRSElapsedTime'])
df_1988 = df_1988.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1988['CRSElapsedTime']])
df_1988.dropna(inplace=True)
df_1988['CRSElapsedTime'] = df_1988['CRSElapsedTime'].astype(int)

df_1988.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1988.head())
print()
df_1988 = df_1988.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1988.head())
print()
print(df_1988.isnull().sum())
print()
df_1988_2 = df_1988.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1989

start = time.time()
df_1989 = pd.read_csv(cwd+"/flights_data/1989.csv.bz2", index_col=0)
df_1989.isnull().sum()
df_1989.isna().sum()
df_1989.duplicated().sum()
df_1989.drop_duplicates(keep=False, inplace=True)
df_1989 = df_1989.dropna(axis=1, how='all')
df_1989 = df_1989.fillna(0)

df_1989.insert(0, 'Year', df_1989.index)
df_1989 = df_1989.reset_index(drop = True)
print(list(df_1989.columns))
print()
df_1989.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1989['FlightDate'] = df_1989[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1989['FlightDate'] = pd.to_datetime(df_1989['FlightDate']) 
df_1989.dropna(inplace=True)

df_1989.insert(6, 'CRSDepDateTime', "Any")
df_1989['CRSDepTime'] = df_1989['CRSDepTime'].astype(str)
df_1989['CRSDepTime'] = df_1989['CRSDepTime'].str.zfill(4)
df_1989["CRSDepDateTime"] = df_1989["FlightDate"].astype(str) + ' ' +  df_1989["CRSDepTime"].astype(str)
df_1989["CRSDepDateTime"] = pd.to_datetime(df_1989["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1989.dropna(inplace=True)

df_1989.insert(8, 'CRSArrDateTime', "Any")
df_1989['CRSArrTime'] = df_1989['CRSArrTime'].astype(str)
df_1989['CRSArrTime'] = df_1989['CRSArrTime'].str.zfill(4)
df_1989["CRSArrDateTime"] = df_1989["FlightDate"].astype(str) + ' ' +  df_1989["CRSArrTime"].astype(str)
df_1989["CRSArrDateTime"] = pd.to_datetime(df_1989["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1989.dropna(inplace=True)

df_1989['CRSElapsedTime'] = df_1989['CRSArrDateTime'] - df_1989['CRSDepDateTime'] 
df_1989['CRSElapsedTime']= df_1989['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1989['CRSElapsedTime'] = pd.to_timedelta(df_1989['CRSElapsedTime'])
df_1989 = df_1989.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1989['CRSElapsedTime']])
df_1989.dropna(inplace=True)
df_1989['CRSElapsedTime'] = df_1989['CRSElapsedTime'].astype(int)

df_1989.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1989.head())
print()
df_1989 = df_1989.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1989.head())
print()
print(df_1989.isnull().sum())
print()
df_1989_2 = df_1989.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1990

start = time.time()
df_1990 = pd.read_csv(cwd+"/flights_data/1990.csv.bz2", index_col=0)
df_1990.isnull().sum()
df_1990.isna().sum()
df_1990.duplicated().sum()
df_1990.drop_duplicates(keep=False, inplace=True)
df_1990 = df_1990.dropna(axis=1, how='all')
df_1990 = df_1990.fillna(0)

df_1990.insert(0, 'Year', df_1990.index)
df_1990 = df_1990.reset_index(drop = True)
print(list(df_1990.columns))
print()
df_1990.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1990['FlightDate'] = df_1990[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1990['FlightDate'] = pd.to_datetime(df_1990['FlightDate'])
df_1990.dropna(inplace=True)

df_1990.insert(6, 'CRSDepDateTime', "Any")
df_1990['CRSDepTime'] = df_1990['CRSDepTime'].astype(str)
df_1990['CRSDepTime'] = df_1990['CRSDepTime'].str.zfill(4)
df_1990["CRSDepDateTime"] = df_1990["FlightDate"].astype(str) + ' ' +  df_1990["CRSDepTime"].astype(str)
df_1990["CRSDepDateTime"] = pd.to_datetime(df_1990["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1990.dropna(inplace=True)

df_1990.insert(8, 'CRSArrDateTime', "Any")
df_1990['CRSArrTime'] = df_1990['CRSArrTime'].astype(str)
df_1990['CRSArrTime'] = df_1990['CRSArrTime'].str.zfill(4)
df_1990["CRSArrDateTime"] = df_1990["FlightDate"].astype(str) + ' ' +  df_1990["CRSArrTime"].astype(str)
df_1990["CRSArrDateTime"] = pd.to_datetime(df_1990["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1990.dropna(inplace=True)

df_1990['CRSElapsedTime'] = df_1990['CRSArrDateTime'] - df_1990['CRSDepDateTime'] 
df_1990['CRSElapsedTime']= df_1990['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1990['CRSElapsedTime'] = pd.to_timedelta(df_1990['CRSElapsedTime'])
df_1990 = df_1990.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1990['CRSElapsedTime']])
df_1990.dropna(inplace=True)
df_1990['CRSElapsedTime'] = df_1990['CRSElapsedTime'].astype(int)

df_1990.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1990.head())
print()
df_1990 = df_1990.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1990.head())
print()
print(df_1990.isnull().sum())
print()
df_1990_2 = df_1990.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1991

start = time.time()
df_1991 = pd.read_csv(cwd+"/flights_data/1991.csv.bz2", index_col=0)
df_1991.isnull().sum()
df_1991.isna().sum()
df_1991.duplicated().sum()
df_1991.drop_duplicates(keep=False, inplace=True)
df_1991 = df_1991.dropna(axis=1, how='all')
df_1991 = df_1991.fillna(0)

df_1991.insert(0, 'Year', df_1991.index)
df_1991 = df_1991.reset_index(drop = True)
print(list(df_1991.columns))
print()
df_1991.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1991['FlightDate'] = df_1991[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1991['FlightDate'] = pd.to_datetime(df_1991['FlightDate']) 
df_1991.dropna(inplace=True)

df_1991.insert(6, 'CRSDepDateTime', "Any")
df_1991['CRSDepTime'] = df_1991['CRSDepTime'].astype(str)
df_1991['CRSDepTime'] = df_1991['CRSDepTime'].str.zfill(4)
df_1991["CRSDepDateTime"] = df_1991["FlightDate"].astype(str) + ' ' +  df_1991["CRSDepTime"].astype(str)
df_1991["CRSDepDateTime"] = pd.to_datetime(df_1991["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1991.dropna(inplace=True)

df_1991.insert(8, 'CRSArrDateTime', "Any")
df_1991['CRSArrTime'] = df_1991['CRSArrTime'].astype(str)
df_1991['CRSArrTime'] = df_1991['CRSArrTime'].str.zfill(4)
df_1991["CRSArrDateTime"] = df_1991["FlightDate"].astype(str) + ' ' +  df_1991["CRSArrTime"].astype(str)
df_1991["CRSArrDateTime"] = pd.to_datetime(df_1991["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1991.dropna(inplace=True)

df_1991['CRSElapsedTime'] = df_1991['CRSArrDateTime'] - df_1991['CRSDepDateTime'] 
df_1991['CRSElapsedTime']= df_1991['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1991['CRSElapsedTime'] = pd.to_timedelta(df_1991['CRSElapsedTime'])
df_1991 = df_1991.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1991['CRSElapsedTime']])
df_1991.dropna(inplace=True)
df_1991['CRSElapsedTime'] = df_1991['CRSElapsedTime'].astype(int)

df_1991.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1991.head())
print()
df_1991 = df_1991.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1991.head())
print()
print(df_1991.isnull().sum())
print()
df_1991_2 = df_1991.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1992

start = time.time()
df_1992 = pd.read_csv(cwd+"/flights_data/1992.csv.bz2", index_col=0)
df_1992.isnull().sum()
df_1992.isna().sum()
df_1992.duplicated().sum()
df_1992.drop_duplicates(keep=False, inplace=True)
df_1992 = df_1992.dropna(axis=1, how='all')
df_1992 = df_1992.fillna(0)

df_1992.insert(0, 'Year', df_1992.index)
df_1992 = df_1992.reset_index(drop = True)
print(list(df_1992.columns))
print()
df_1992.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1992['FlightDate'] = df_1992[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1992['FlightDate'] = pd.to_datetime(df_1992['FlightDate']) 

df_1992.insert(6, 'CRSDepDateTime', "Any")
df_1992['CRSDepTime'] = df_1992['CRSDepTime'].astype(str)
df_1992['CRSDepTime'] = df_1992['CRSDepTime'].str.zfill(4)
df_1992["CRSDepDateTime"] = df_1992["FlightDate"].astype(str) + ' ' +  df_1992["CRSDepTime"].astype(str)
df_1992["CRSDepDateTime"] = pd.to_datetime(df_1992["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1992.dropna(inplace=True)

df_1992.insert(8, 'CRSArrDateTime', "Any")
df_1992['CRSArrTime'] = df_1992['CRSArrTime'].astype(str)
df_1992['CRSArrTime'] = df_1992['CRSArrTime'].str.zfill(4)
df_1992["CRSArrDateTime"] = df_1992["FlightDate"].astype(str) + ' ' +  df_1992["CRSArrTime"].astype(str)
df_1992["CRSArrDateTime"] = pd.to_datetime(df_1992["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1992.dropna(inplace=True)

df_1992['CRSElapsedTime'] = df_1992['CRSArrDateTime'] - df_1992['CRSDepDateTime'] 
df_1992['CRSElapsedTime']= df_1992['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1992['CRSElapsedTime'] = pd.to_timedelta(df_1992['CRSElapsedTime'])
df_1992 = df_1992.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1992['CRSElapsedTime']])
df_1992.dropna(inplace=True)
df_1992['CRSElapsedTime'] = df_1992['CRSElapsedTime'].astype(int)

df_1992.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1992.head())
print()
df_1992 = df_1992.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1992.head())
print()
print(df_1992.isnull().sum())
print()
df_1992_2 = df_1992.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1993

start = time.time()
df_1993 = pd.read_csv(cwd+"/flights_data/1993.csv.bz2", index_col=0)
df_1993.isnull().sum()
df_1993.isna().sum()
df_1993.duplicated().sum()
df_1993.drop_duplicates(keep=False, inplace=True)
df_1993 = df_1993.dropna(axis=1, how='all')
df_1993 = df_1993.fillna(0)

df_1993.insert(0, 'Year', df_1993.index)
df_1993 = df_1993.reset_index(drop = True)
print(list(df_1993.columns))
print()
df_1993.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1993['FlightDate'] = df_1993[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1993['FlightDate'] = pd.to_datetime(df_1993['FlightDate']) 

df_1993.insert(6, 'CRSDepDateTime', "Any")
df_1993['CRSDepTime'] = df_1993['CRSDepTime'].astype(str)
df_1993['CRSDepTime'] = df_1993['CRSDepTime'].str.zfill(4)
df_1993["CRSDepDateTime"] = df_1993["FlightDate"].astype(str) + ' ' +  df_1993["CRSDepTime"].astype(str)
df_1993["CRSDepDateTime"] = pd.to_datetime(df_1993["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1993.dropna(inplace=True)

df_1993.insert(8, 'CRSArrDateTime', "Any")
df_1993['CRSArrTime'] = df_1993['CRSArrTime'].astype(str)
df_1993['CRSArrTime'] = df_1993['CRSArrTime'].str.zfill(4)
df_1993["CRSArrDateTime"] = df_1993["FlightDate"].astype(str) + ' ' +  df_1993["CRSArrTime"].astype(str)
df_1993["CRSArrDateTime"] = pd.to_datetime(df_1993["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1993.dropna(inplace=True)

df_1993['CRSElapsedTime'] = df_1993['CRSArrDateTime'] - df_1993['CRSDepDateTime'] 
df_1993['CRSElapsedTime']= df_1993['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1993['CRSElapsedTime'] = pd.to_timedelta(df_1993['CRSElapsedTime'])
df_1993 = df_1993.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1993['CRSElapsedTime']])
df_1993.dropna(inplace=True)
df_1993['CRSElapsedTime'] = df_1993['CRSElapsedTime'].astype(int)

df_1993.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1993.head())
print()
df_1993 = df_1993.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1993.head())
print()
print(df_1993.isnull().sum())
print()
df_1993_2 = df_1993.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1994

start = time.time()
df_1994 = pd.read_csv(cwd+"/flights_data/1994.csv.bz2", index_col=0)
df_1994.isnull().sum()
df_1994.isna().sum()
df_1994.duplicated().sum()
df_1994.drop_duplicates(keep=False, inplace=True)
df_1994 = df_1994.dropna(axis=1, how='all')
df_1994 = df_1994.fillna(0)

df_1994.insert(0, 'Year', df_1994.index)
df_1994 = df_1994.reset_index(drop = True)
print(list(df_1994.columns))
print()
df_1994.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1994['FlightDate'] = df_1994[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1994['FlightDate'] = pd.to_datetime(df_1994['FlightDate']) 

df_1994.insert(6, 'CRSDepDateTime', "Any")
df_1994['CRSDepTime'] = df_1994['CRSDepTime'].astype(str)
df_1994['CRSDepTime'] = df_1994['CRSDepTime'].str.zfill(4)
df_1994["CRSDepDateTime"] = df_1994["FlightDate"].astype(str) + ' ' +  df_1994["CRSDepTime"].astype(str)
df_1994["CRSDepDateTime"] = pd.to_datetime(df_1994["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1994.dropna(inplace=True)

df_1994.insert(8, 'CRSArrDateTime', "Any")
df_1994['CRSArrTime'] = df_1994['CRSArrTime'].astype(str)
df_1994['CRSArrTime'] = df_1994['CRSArrTime'].str.zfill(4)
df_1994["CRSArrDateTime"] = df_1994["FlightDate"].astype(str) + ' ' +  df_1994["CRSArrTime"].astype(str)
df_1994["CRSArrDateTime"] = pd.to_datetime(df_1994["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1994.dropna(inplace=True)

df_1994['CRSElapsedTime'] = df_1994['CRSArrDateTime'] - df_1994['CRSDepDateTime'] 
df_1994['CRSElapsedTime']= df_1994['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1994['CRSElapsedTime'] = pd.to_timedelta(df_1994['CRSElapsedTime'])
df_1994 = df_1994.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1994['CRSElapsedTime']])
df_1994.dropna(inplace=True)
df_1994['CRSElapsedTime'] = df_1994['CRSElapsedTime'].astype(int)

df_1994.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek'], axis=1, inplace=True)
print(df_1994.head())
print()
df_1994 = df_1994.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1994.head())
print()
print(df_1994.isnull().sum())
print()
df_1994_2 = df_1994.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1995

start = time.time()
df_1995 = pd.read_csv(cwd+"/flights_data/1995.csv.bz2", index_col=0)
df_1995.isnull().sum()
df_1995.isna().sum()
df_1995.duplicated().sum()
df_1995.drop_duplicates(keep=False, inplace=True)
df_1995 = df_1995.dropna(axis=1, how='all')
df_1995 = df_1995.fillna(0)

df_1995.insert(0, 'Year', df_1995.index)
df_1995 = df_1995.reset_index(drop = True)
print(list(df_1995.columns))
print()
df_1995.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1995['FlightDate'] = df_1995[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1995['FlightDate'] = pd.to_datetime(df_1995['FlightDate']) 

df_1995.insert(6, 'CRSDepDateTime', "Any")
df_1995['CRSDepTime'] = df_1995['CRSDepTime'].astype(str)
df_1995['CRSDepTime'] = df_1995['CRSDepTime'].str.zfill(4)
df_1995["CRSDepDateTime"] = df_1995["FlightDate"].astype(str) + ' ' +  df_1995["CRSDepTime"].astype(str)
df_1995["CRSDepDateTime"] = pd.to_datetime(df_1995["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1995.dropna(inplace=True)

df_1995.insert(8, 'CRSArrDateTime', "Any")
df_1995['CRSArrTime'] = df_1995['CRSArrTime'].astype(str)
df_1995['CRSArrTime'] = df_1995['CRSArrTime'].str.zfill(4)
df_1995["CRSArrDateTime"] = df_1995["FlightDate"].astype(str) + ' ' +  df_1995["CRSArrTime"].astype(str)
df_1995["CRSArrDateTime"] = pd.to_datetime(df_1995["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1995.dropna(inplace=True)

df_1995['CRSElapsedTime'] = df_1995['CRSArrDateTime'] - df_1995['CRSDepDateTime'] 
df_1995['CRSElapsedTime']= df_1995['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1995['CRSElapsedTime'] = pd.to_timedelta(df_1995['CRSElapsedTime'])
df_1995 = df_1995.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1995['CRSElapsedTime']])
df_1995.dropna(inplace=True)
df_1995['CRSElapsedTime'] = df_1995['CRSElapsedTime'].astype(int)

df_1995.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek',
'TailNum', 'AirTime', 'TaxiIn', 'TaxiOut'], axis=1, inplace=True)
print(df_1995.head())
print()
df_1995 = df_1995.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1995.head())
print()
print(df_1995.isnull().sum())
print()
df_1995_2 = df_1995.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# importing and cleaning flight data from 1996

start = time.time()
df_1996 = pd.read_csv(cwd+"/flights_data/1996.csv.bz2", index_col=0)
df_1996.isnull().sum()
df_1996.isna().sum()
df_1996.duplicated().sum()
df_1996.drop_duplicates(keep=False, inplace=True)
df_1996 = df_1996.dropna(axis=1, how='all')
df_1996 = df_1996.fillna(0)

df_1996.insert(0, 'Year', df_1996.index)
df_1996 = df_1996.reset_index(drop = True)
print(list(df_1996.columns))
print()
df_1996.insert(4, 'FlightDate', "Any")
cols = ["Year","Month","DayofMonth"]
df_1996['FlightDate'] = df_1996[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns") 
df_1996['FlightDate'] = pd.to_datetime(df_1996['FlightDate']) 

df_1996.insert(6, 'CRSDepDateTime', "Any")
df_1996['CRSDepTime'] = df_1996['CRSDepTime'].astype(str)
df_1996['CRSDepTime'] = df_1996['CRSDepTime'].str.zfill(4)
df_1996["CRSDepDateTime"] = df_1996["FlightDate"].astype(str) + ' ' +  df_1996["CRSDepTime"].astype(str)
df_1996["CRSDepDateTime"] = pd.to_datetime(df_1996["CRSDepDateTime"] , infer_datetime_format=True, errors='coerce')
df_1996.dropna(inplace=True)

df_1996.insert(8, 'CRSArrDateTime', "Any")
df_1996['CRSArrTime'] = df_1996['CRSArrTime'].astype(str)
df_1996['CRSArrTime'] = df_1996['CRSArrTime'].str.zfill(4)
df_1996["CRSArrDateTime"] = df_1996["FlightDate"].astype(str) + ' ' +  df_1996["CRSArrTime"].astype(str)
df_1996["CRSArrDateTime"] = pd.to_datetime(df_1996["CRSArrDateTime"] , infer_datetime_format=True, errors='coerce')
df_1996.dropna(inplace=True)

df_1996['CRSElapsedTime'] = df_1996['CRSArrDateTime'] - df_1996['CRSDepDateTime'] 
df_1996['CRSElapsedTime']= df_1996['CRSElapsedTime'].astype(str).map(lambda x: x[7:])
df_1996['CRSElapsedTime'] = pd.to_timedelta(df_1996['CRSElapsedTime'])
df_1996 = df_1996.assign(CRSElapsedTime = [(x.seconds)/60.0 for x in df_1996['CRSElapsedTime']])
df_1996.dropna(inplace=True)
df_1996['CRSElapsedTime'] = df_1996['CRSElapsedTime'].astype(int)

df_1996.drop(['Year' , 'Month', 'DayofMonth', 'DayOfWeek', 'TailNum', 'TaxiIn', 'TaxiOut'], axis=1, inplace=True)
print(df_1996.head())
print()
df_1996 = df_1996.reindex(columns=['FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
'CRSDepDateTime', 'DepTime', 'DepDelay', 'CRSArrDateTime', 'ArrTime', 'ArrDelay',
'CRSElapsedTime', 'ActualElapsedTime', 'Cancelled', 'Diverted', 'Distance'])
print(df_1996.head())
print()
print(df_1996.isnull().sum())
print()
df_1996_2 = df_1996.sample(frac = 0.1)
print()
print(f'Time: {time.time() - start}')
print()

# Combining all seperate dataframes into one data frame

result = [df_1987_2, df_1988_2, df_1989_2, df_1990_2, df_1991_2, df_1992_2,
df_1993_2, df_1994_2, df_1995_2, df_1996_2]
all_flight_data = pd.concat(result)
all_flight_data.head()
print()
all_flight_data.shape
print()
print()
print(all_flight_data.isnull().sum())
print()
print(all_flight_data.shape)

# Export the combined data into an Excel file

cwd = os.getcwd()
all_flight_data.to_csv(cwd+ "/all_combined_data.csv")


