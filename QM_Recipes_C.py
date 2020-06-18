#!/usr/bin/env python3

from sqlalchemy import create_engine
import pyodbc
import urllib
import pandas as pd
import datetime


# =============================================================================
# Read data from SQL datasource into dataframe
# =============================================================================


# Define server connection and SQL query:
server = 'sqlsrv04\tx'
db = 'TXprodSTAGE'
con = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db)
query = """ WITH PBOM AS (
            SELECT
            	[Top level item]
            	,[Level]
            	,[Component qty per]
            	,[Component]
            	,[Active version]
            FROM [dbo].[ProductionBOM version by level_V]
            WHERE [Active version] = 1
            	AND [Component] LIKE '1040%'
            	AND [Top level item] > '20000000'
            )
            , VE AS (
            SELECT
            	[Item No.]
            	,COUNT([Entry No.]) AS [Count]
            	,SUM([AntalKgKaffe]) AS [AntalKgKaffe]
            	,SUM([Sales Amount (Actual)]) AS [Sales Amount (Actual)]
            	,SUM([Cost Amount (Actual) korr. export]) AS [Cost Amount (Actual) korr. export]
            FROM [NAV].[Value Entry_V]
            WHERE
            	[Item Ledger Entry Type] IN (-1,1)
            	AND [Posting Date] >= DATEADD(year, -1, getdate())
            GROUP BY
            	[Item No.]
            )
            
            SELECT
            	I.[No.] AS [Recipe]
            	,SUM(ISNULL(VE.[Count],0)) AS [Count]
            	,SUM(ISNULL(CASE
            		WHEN PBOM.[Level] = 0
            			THEN ( PBOM.[Component qty per] / ITOP.[Net Weight] ) * VE.[AntalKgKaffe]
            		ELSE [Component qty per] * VE.[AntalKgKaffe]
            	END,0)) AS [Quantity]
            	,SUM(ISNULL(CASE
            		WHEN PBOM.[Level] = 0
            			THEN ( PBOM.[Component qty per] / ITOP.[Net Weight] ) * VE.[Sales Amount (Actual)]
            		ELSE [Component qty per] * VE.[Sales Amount (Actual)]
            	END,0)) AS [Amount]
            	,SUM(ISNULL(CASE
            		WHEN PBOM.[Level] = 0
            			THEN ( PBOM.[Component qty per] / ITOP.[Net Weight] ) * VE.[Cost Amount (Actual) korr. export]
            		ELSE [Component qty per] * VE.[Cost Amount (Actual) korr. export]
            	END,0)) AS [Cost]
            FROM [NAV].[Item_V] AS I
            LEFT JOIN PBOM
            	ON I.[No.] = PBOM.[Component]
            LEFT JOIN [NAV].[Item_V] AS ITOP
            	ON PBOM.[Top level item] = ITOP.[No.]
            LEFT JOIN  VE
            	ON PBOM.[Top level item] = VE.[Item No.]
            WHERE I.[No.] LIKE '1040%'
            GROUP BY
            	I.[No.] """

# =============================================================================
# Read query and other variables
# =============================================================================
# change env variable below to switch between dev and prod SQL tables for inserts
env = 'dev'             # dev = test || cp = prod
# Read query into dataframe and create unique list for iteration and empty dataframes:
df = pd.read_sql(query, con)
df['MonetaryValue'] = df['Amount'] - df['Cost']
recipes = df.Recipe.unique()
dfQuan = pd.DataFrame()
dfCons = pd.Dataframe()
# Variables for inserting data into sql database:
params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 10.0};SERVER=sqlsrv04;DATABASE=BKI_Datastore;Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
# Other variables:
now = datetime.datetime.now()
script_name = 'QM_Recipes_C.py'
execution_id = int(now.timestamp())


# =============================================================================
# Define functions 
# =============================================================================
# Quantity and MonetaryValue score - bigger numbers are better:
def qm_score(x, para, dic):
    if x <= dic[para][0.25]:
        return 4
    elif x <= dic[para][0.5]:
        return 3
    elif x <= dic[para][0.75]:
        return 2
    else:
        return 1


# Insert data into sql database
def insert_sql(dataframe, table_name, schema):
    dataframe.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
    







