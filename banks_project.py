from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    page = requests.get(url).text
    To_HTML = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = To_HTML.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dec = {
                'Name': col[1].contents[2],
                'MC_USD_billions': col[2].contents[0]
            }
            df1 = pd.DataFrame(data_dec, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


# 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion'
def transform(df, csv_path):
    list = df['MC_USD_billions'].tolist()
    name = df['Name'].tolist()
    df = pd.DataFrame(columns=['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion'])
    df['Name'] = name
    csv_read = pd.read_csv(csv_path)
    dict = csv_read.set_index('Currency')['Rate'].to_dict()
    listt = [float("".join("".join(x.split(',')).split('\n'))) for x in list]
    df["MC_USD_Billion"] = listt
    df["MC_GBP_Billion"] = [np.round(x * dict['GBP'], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * dict['EUR'], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * dict['INR'], 2) for x in df["MC_USD_Billion"]]
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
ex_att = ['Name', 'MC_USD_billions']
table_att = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_rate = './exchange_rate.csv'

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, ex_att)
print(df)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, exchange_rate)

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')
sql_connection.close()
