import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
tqdm.pandas(desc="my bar!")


df = pd.read_csv('../output/impact_data.csv')
df.date = pd.to_datetime(df.date)
df = df.drop(columns=['Unnamed: 0'])

dr = pd.date_range(start='1/1/1998', end='31/12/2019')
dff = pd.DataFrame()
for district in tqdm(df.district.unique()):
    dfd = df[df.district == district]
    dfd.index = pd.DatetimeIndex(dfd.date)
    dfd = dfd.reindex(dr, fill_value=0)
    dfd['district'] = district
    dff = dff.append(dfd)
dff['date'] = dff.index

dff.to_csv('../output/impact_data_with_zeros.csv', index=False)

print(dff.head())