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
server = r'sqlsrv04\tx'
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
            	I.[No.] AS [ItemNo]
				,I.[Withdrawal Status] AS [Status]
            	,SUM(ISNULL(VE.[Count],0)) AS [Count]
				,DATEDIFF(d,I.[Oprettet den],getdate()) AS [Days]
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
            	I.[No.]
				,I.[Oprettet den]
				,I.[Withdrawal Status] """

# =============================================================================
# Read query and other variables
# =============================================================================
# change env variable below to switch between dev and prod SQL tables for inserts
env = 'seg'             # dev = test || seg = prod
# Variables for inserting data into sql database:
params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};SERVER=sqlsrv04;DATABASE=BKI_Datastore;Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
# Other variables:
now = datetime.datetime.now()
execution_id = int(now.timestamp())
script_name = 'QM_Recipes_C.py'
seg_type = 'Coffee Recipes'
cols_sale = ['ExecutionId', 'Timestamp', 'ItemNo', 'Quantity', 'MonetaryValue', 'Score', 'Type', 'Script']
cols_no_sale = ['ExecutionId', 'Timestamp', 'ItemNo', 'Score', 'Type', 'Script']
cols_quan = (['ExecutionId', 'Timestamp', 'Type', 'Quantile', 'Quantity',
             'MonetaryValue'])
# Read query into dataframe and create unique list for iteration and empty dataframes:
df = pd.read_sql(query, con)
df['MonetaryValue'] = df['Amount'] - df['Cost']


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
    

# =============================================================================
# SKUs with sales
# =============================================================================

# Create dataframe and skip if it's empty:
df_sales = df.loc[df['Count'] != 0]
if len(df_sales) != 0:
    # Define quantiles
    quantiles = df_sales.quantile(q=[0.25, 0.5, 0.75]).to_dict()
    # Identify quartiles per measure for each product:
    df_sales.loc[:, 'QuantityQuartile'] = df_sales['Quantity'].apply(qm_score, args=('Quantity', quantiles,))
    df_sales.loc[:, 'MonetaryQuartile'] = df_sales['MonetaryValue'].apply(qm_score, args=('MonetaryValue', quantiles,))
    # Concetenate Quartile measurements to single string:
    df_sales.loc[:, 'Score'] = df_sales.QuantityQuartile * 10 + df_sales.MonetaryQuartile
    # Create data stamps for dataframe:
    df_sales.loc[:, 'Timestamp'] = now
    df_sales.loc[:, 'Type'] = seg_type
    df_sales.loc[:, 'ExecutionId'] = execution_id
    df_sales.loc[:, 'Script'] = script_name
    # Insert dataframe to SQL database:
    insert_sql(df_sales[cols_sale], 'ItemSegmentation', env)
    # Append quantiles to dataframe:
    df_quan = pd.DataFrame.from_dict(quantiles)
    df_quan.loc[:, 'Type'] =  seg_type
    df_quan.loc[:, 'Timestamp'] = now
    df_quan.loc[:, 'ExecutionId'] = execution_id
    df_quan.loc[:, 'Quantile'] = df_quan.index
    # Insert quantiles to SQL database:
    insert_sql(df_quan[cols_quan], 'ItemSegmentationQuantiles', env)

# =============================================================================
# SKUs without sales
# =============================================================================

# Create dataframe and skip if it's empty:
df_no_sale = df.loc[df['Count'] == 0]
if len(df_no_sale) != 0:
    # Create columns with variables and relevant score
    df_no_sale.loc[:, 'Timestamp'] = now
    df_no_sale.loc[:, 'Score'] = df_no_sale['Days'].apply(lambda x: 1 if x > 90 else 2)
    df_no_sale.loc[df_no_sale['Status'] == 2, 'Score'] = 0
    df_no_sale.loc[:, 'Type'] = seg_type
    df_no_sale.loc[:, 'ExecutionId'] = execution_id
    df_no_sale.loc[:, 'Script'] = script_name
    # Insert dataframe to SQL database:
    insert_sql(df_no_sale[cols_no_sale], 'ItemSegmentation', env)

# =============================================================================
# Write to SQL log
# =============================================================================
        
df_log = pd.DataFrame(data= {'Date':now, 'Event':script_name, 'Note': 'Execution id: ' + str(execution_id)}, index=[0])
insert_sql(df_log, 'Log', 'dbo')
    
    
