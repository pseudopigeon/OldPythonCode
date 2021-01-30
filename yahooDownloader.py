import pandas as pd
from yahooquery import Ticker
import json 

completed = {}
with open('yahooData.txt', 'r') as f:
    for line in f:
        completed.update(json.loads(line))

completedTickers = list(completed.keys())

dfGeneric = pd.read_csv('oldData/generic.csv')
tickers = dfGeneric['Ticker'].tolist()
print('loading tickers to do list')
toDo = list(set(tickers) - set(completedTickers))
# toDo = [x for x in tickers if x not in completedTickers]
del dfGeneric, tickers
del completed, completedTickers

print('Starting Download')

for ticker in toDo:
    temp = Ticker(ticker)
    data = temp.asset_profile
    with open('yahooData.txt', 'a') as f:
        f.write(json.dumps(data))
        f.write('\n')

