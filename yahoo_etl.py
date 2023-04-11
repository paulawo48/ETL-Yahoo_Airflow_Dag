import yfinance as yf
import csv
import requests
import pandas as pd

def get_ticker():
    url = 'https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund&asOfDate=20221230'
    response = requests.get(url)

    with open('russell-3000.csv', 'wb') as f:
        f.write(response.content)

    with open('russell-3000.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)

    empty_row_indicies = [i for i in range(len(rows)) if (len(rows[i]) == 0 or '\xa0' in rows[i])]

    start = empty_row_indicies[0] + 1
    end = empty_row_indicies[1]
    cleaned_rows = rows[start:end]

    with open('russell-3000-clean.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(cleaned_rows)

    # load Russell 3000 holdings CSV into a dataframe
    holdings = pd.read_csv('./russell-3000-clean.csv')
    ticker = holdings['Ticker'].tolist()

    return ticker

def get_yahoo_data():
    tckrs = get_ticker()

    # first stock
    data=yf.Ticker('AAPL')     
    dfall= pd.DataFrame([data.info])

    # add ticker symbol column
    dfall.insert(0, "Tckr", "AAPL")

    # append all
    for stock in tckrs[1:]:
        try:
            data=yf.Ticker(stock)
            df= pd.DataFrame([data.info])
            df.insert(0, "Tckr", stock)
            dfall=pd.concat([dfall, df],axis=0)
        except: pass

    #dfall = dfall.drop(dfall.columns[0], axis=1)

    dfall.to_csv("s3://paul-airflow-yahoo-bucket/ticker_data.csv")

