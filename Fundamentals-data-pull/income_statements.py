!pip install pandas-gbq
pip install yfinance
import pandas as pd
import yfinance as yf
import requests
import json 
import csv
import datetime
import os
import glob
import io
from google.cloud import bigquery
from google.oauth2 import service_account

#Manually load list of tickers

data = '''AMZN
INTC
LRCX
META
BABA
GIS
KMI
KO
MCD
MDT
MMM
TEL
USB
XYL
URNM
ABNB
AMAT
ASML
KLAC
KLIC
MRAM
SWKS
UROY
VMFXX
BWXT
DOCS
SPOT
SILXY
CLLNY
SRUUF
YLLXF
IGN
VGT
VUG
SOXX
VONG
JEPI
SCHD
VB
VIG
VTI
FXAIX
SPAXX
VXUS
TWCGX
SPY
EMCLOUD
COMP
FROG
MCHP
IRDM
NVEC
MXL
OPRA
BELFB
EXTR
IDCC
ON
HLIT
PLAB
VEEV
TTD
SHOP
CDLX
ZI
DV
S
ZS
CRWD
ADSK
PANW
MSI
ANET
EQIX
EGIO
DOCN
NET
RKLB
ASTS
COIN
NCNO
FISV
ENPH
TOELY
QRVO
BESI
VSH
AEHR
TXN
SLAB
CRUS
NXPI
AVGO
MPWR
LSCC
SNPS
CDNS
APH
CGNX
PLPC
SYM
IOT
TMO
DHR
TT
IRM
SEIC
NOW
BDC
DIOD
JBL
CARG
CSGS
ADBE
CRM
NOW
SHOP
WDAY
PYPL
SNOW
CRWD
TEAM
DDOG
SQ
MDB
VEEV
ZS
HUBS
NET
ZM
OKTA
TWLO
PAYC
PATH
DBX
PCTY
DOCU
TOST
ESTC
PCOR
APPF
GTLB
MNDY
QLYS
BILL
SMAR
CFLT
FRSH
WIX
ZI
WK
S
FIVN
BRZE
TENB
ASAN
SQSP
CXM
CWAN
BOX
BL
NCNO
AI
SPT
FROG
RNG
DOCN
FSLY
PD
QTWO
AVDX
AVPT
AMPL
ZUO
EVBG
DH
YEXT
OLO
BIGC
VMEO
WEAV
EGHT
DOMO
NOVT
IPGP
COHR
GFS
MU
WOLF
TXN
INTC
NVDA
ADI
QCOM
NXPI
TER
ALGM
MRVL
SWKS
ASML
AMD
MPWR
ON
LSCC
QRVO
AVGO
MCHP
AMAT
LRCX
ENTG
KLAC
TSM
SYNA
MKL'''

tickers = data.split('\n')
print(tickers)

#Get Income Statements

# set the API endpoint URL with a placeholder for the ticker
url_template = "https://fmpcloud.io/api/v3/income-statement-shorten/{}?datatype=csv&period=quarter&apikey=a726d5acabd86910ef2749315f5eaa86"

# define a dictionary to store ticker dataframes
ticker_dataframes = {}

# define a list of tickers
tickers = tickers

# loop over the tickers and make an API request for each one
for ticker in tickers:
    # replace the placeholder with the current ticker
    url = url_template.format(ticker)

    # make an HTTP GET request to the API endpoint and retrieve the data in CSV format
    response = requests.get(url)
    data = response.content.decode('utf-8-sig')

    # create a dataframe from the data
    df = pd.read_csv(io.StringIO(data))

    # add a new column to the dataframe with the ticker symbol
    df['Ticker'] = ticker

    # store the dataframe in the ticker_dataframes dictionary with the ticker as the key
    ticker_dataframes[ticker] = df

# concatenate all dataframes into one
income_statements_df = pd.concat(ticker_dataframes.values(), ignore_index=True)

# save the combined dataframe as a CSV file
income_statements_df.to_csv("income_statements_quarterly.csv", index=False)


from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Read the CSV file into a DataFrame
income_statements_quarterly = pd.read_csv('income_statements_quarterly.csv')

# Drop the 'CALENDARYEAR' column if it exists
if 'CALENDARYEAR' in income_statements_quarterly.columns:
    income_statements_quarterly.drop(columns=['CALENDARYEAR'], inplace=True)

# Explicitly set the data types for columns
income_statements_quarterly = pd.DataFrame({
    'DATE': pd.to_datetime(income_statements_quarterly['date'], errors='coerce'),
    'SYMBOL': income_statements_quarterly['symbol'].astype(str),
    'REPORTEDCURRENCY': income_statements_quarterly['reportedCurrency'].astype(str),
    'CIK': income_statements_quarterly['cik'].astype(int),
    'FILLINGDATE': pd.to_datetime(income_statements_quarterly['fillingDate'], errors='coerce'),
    'ACCEPTEDDATE': pd.to_datetime(income_statements_quarterly['acceptedDate'], errors='coerce'),
    'CALENDARYEAR': income_statements_quarterly['calendarYear'].astype(int),
    'PERIOD': income_statements_quarterly['period'].astype(str),
    'REVENUE': income_statements_quarterly['revenue'].astype(int),
    'COSTOFREVENUE': income_statements_quarterly['costOfRevenue'].astype(int),
    'GROSSPROFIT': income_statements_quarterly['grossProfit'].astype(int),
    'GROSSPROFITRATIO': income_statements_quarterly['grossProfitRatio'].astype(float),
    'RESEARCHANDDEVELOPMENTEXPENSES': income_statements_quarterly['researchAndDevelopmentExpenses'].astype(int),
    'GENERALANDADMINISTRATIVEEXPENSES': income_statements_quarterly['generalAndAdministrativeExpenses'].astype(int),
    'SELLINGANDMARKETINGEXPENSES': income_statements_quarterly['sellingAndMarketingExpenses'].astype(int),
    'SELLINGGENERALANDADMINISTRATIVEEXPENSES': income_statements_quarterly['sellingGeneralAndAdministrativeExpenses'].astype(int),
    'OTHEREXPENSES': income_statements_quarterly['otherExpenses'].astype(int),
    'OPERATINGEXPENSES': income_statements_quarterly['operatingExpenses'].astype(int),
    'COSTANDEXPENSES': income_statements_quarterly['costAndExpenses'].astype(int),
    'INTERESTINCOME': income_statements_quarterly['interestIncome'].astype(int),
    'INTERESTEXPENSE': income_statements_quarterly['interestExpense'].astype(int),
    'DEPRECIATIONANDAMORTIZATION': income_statements_quarterly['depreciationAndAmortization'].astype(int),
    'EBITDA': income_statements_quarterly['ebitda'].astype(int),
    'EBITDARATIO': income_statements_quarterly['ebitdaratio'].astype(float),
    'OPERATINGINCOME': income_statements_quarterly['operatingIncome'].astype(int),
    'OPERATINGINCOMERATIO': income_statements_quarterly['operatingIncomeRatio'].astype(float),
    'TOTALOTHERINCOMEEXPENSESNET': income_statements_quarterly['totalOtherIncomeExpensesNet'].astype(int),
    'INCOMEBEFORETAX': income_statements_quarterly['incomeBeforeTax'].astype(int),
    'INCOMEBEFORETAXRATIO': income_statements_quarterly['incomeBeforeTaxRatio'].astype(float),
    'INCOMETAXEXPENSE': income_statements_quarterly['incomeTaxExpense'].astype(int),
    'NETINCOME': income_statements_quarterly['netIncome'].astype(int),
    'NETINCOMERATIO': income_statements_quarterly['netIncomeRatio'].astype(float),
    'EPS': income_statements_quarterly['eps'].astype(float),
    'EPSDILUTED': income_statements_quarterly['epsdiluted'].astype(float),
    'WEIGHTEDAVERAGESHSOUT': income_statements_quarterly['weightedAverageShsOut'].astype(int),
    'WEIGHTEDAVERAGESHSOUTDIL': income_statements_quarterly['weightedAverageShsOutDil'].astype(int),
    'LINK': income_statements_quarterly['link'].astype(str),
    'FINALLINK': income_statements_quarterly['finalLink'].astype(str),
    'TICKER': income_statements_quarterly['Ticker'].astype(str),
})

project_id = 'stock-market-data-391622'
dataset_id = 'market_information'
table_name = 'income_statements'

# Create a BigQuery client with service account credentials
credentials = service_account.Credentials.from_service_account_file('stock-market-data-391622-03ee9e592be9.json')
client = bigquery.Client(credentials=credentials, project=project_id)

# Specify the destination table in BigQuery
destination_table = f'{project_id}.{dataset_id}.{table_name}'

# Write the DataFrame to BigQuery with schema auto-detection
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

client.load_table_from_dataframe(
    income_statements_quarterly, destination_table, job_config=job_config
).result()


job_id = 'aebcfa07-f1d9-41e6-aeba-573a4c1c2fe8'  # Replace with your actual job ID
job = client.get_job(job_id)
print(job.state)