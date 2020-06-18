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
            	I.[No.]
            	,SUM(ISNULL(VE.[Count],0)) AS [Count]
            	,SUM(ISNULL(CASE
            		WHEN PBOM.[Level] = 0
            			THEN ( PBOM.[Component qty per] / ITOP.[Net Weight] ) * VE.[AntalKgKaffe]
            		ELSE [Component qty per] * VE.[AntalKgKaffe]
            	END,0)) AS [Kg kaffe]
            	,SUM(ISNULL(CASE
            		WHEN PBOM.[Level] = 0
            			THEN ( PBOM.[Component qty per] / ITOP.[Net Weight] ) * VE.[Sales Amount (Actual)]
            		ELSE [Component qty per] * VE.[Sales Amount (Actual)]
            	END,0)) AS [Sales amount]
            	,SUM(ISNULL(CASE
            		WHEN PBOM.[Level] = 0
            			THEN ( PBOM.[Component qty per] / ITOP.[Net Weight] ) * VE.[Cost Amount (Actual) korr. export]
            		ELSE [Component qty per] * VE.[Cost Amount (Actual) korr. export]
            	END,0)) AS [Cost amount]
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