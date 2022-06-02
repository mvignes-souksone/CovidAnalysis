import pandas as pd
from sqlalchemy import create_engine
import requests

df = pd.read_csv ('/home/melodie/Téléchargements/archive/COVID-19 Coronavirus.csv')
print (df)

df.describe()

df.columns

df['Country'].unique()


df['Death percentage']= df['Death percentage'].apply(round)

ds = df[df['Continent'] == 'Asia']
print(ds)

ds2 = df.groupby('Continent')['Total Cases'].sum()
print(ds2)


engine = create_engine('postgresql://postgres:password@localhost:5432/postgres')
df.to_sql("covid",engine,if_exists='replace')

lcountry = df['Country'].unique()

[print(x) for x in lcountry]
l2 = lcountry[:11]
print(l2)

l = []
for x in l2:
    url = f"https://restcountries.com/v3.1/name/{x}"
    r = requests.get(url)
    l.append(r)

url = "https://restcountries.com/v3.1/all"
r = requests.get(url)
print(r)

l3 = r.json()
len(l3)



def filter_response(r):
    d = r['name']['common']

    if 'subregion' in r:
        e = r['subregion']
        my_dict = {"Country": d, "Subregion": e}
    else:
        my_dict = {"Country": d, "Subregion": 'NA'}

    return my_dict

def filter_response(r):
    return {"Country": r['name']['common'], "Subregion" : r.get('subregion', 'NA')}

li = []
for x in l3 :
    li.append(filter_response(x))

print(li)

table2 = pd.DataFrame(li)
print(table2)

table2.to_sql("subregion",engine, if_exists='replace')

df.Country.isin(table2.Country).astype(int)

df = df.merge(table2, left_on='Country', right_on='Country')

df.to_sql("covid",engine,if_exists='replace')

