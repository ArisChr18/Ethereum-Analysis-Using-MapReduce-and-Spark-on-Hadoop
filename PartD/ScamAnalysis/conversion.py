import json
import pandas as pd

data = open('scams.json')
scams = json.load(data)

df = pd.DataFrame(columns=['Address', 'Type of Scam', 'Status'])
keys = scams["result"]
for i in keys:
  record = scams["result"][i]
  category = record["category"]
  addresses = record["addresses"]
  status = record["status"]
  for j in addresses:
    df.loc[len(df)] = [j, category, status]

df.to_csv('scams.csv', index=False)
