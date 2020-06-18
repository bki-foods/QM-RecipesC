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
server = '192.168.125.161'
db = 'BKI_IMP_EXP'
con = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';UID=' + user + ';PWD=' + password)
query = """ SELECT
            	DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) AS [Date]
            	,[ROASTER]
                ,[CUSTOMER_CODE] AS [Recipe]
                ,AVG([FINAL_TEMP_ROASTING] / 10.0) AS [End temp]
            FROM [dbo].[PRO_EXP_BATCH_DATA_ROASTER]
			WHERE DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) > DATEADD(month,DATEDIFF(month,0,GETDATE())-6,0)
                 AND [CUSTOMER_CODE] IN ('10401005','10401523','10401009','10401057','10401207','10401522','10401028','10401087','10401054','10401510')
                 AND [ROASTER] = 'R2'
            GROUP BY
            	DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0)
            	,[ROASTER]
                ,[CUSTOMER_CODE]
			HAVING COUNT(*) >= 3
            ORDER BY
                DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) ASC
                ,[CUSTOMER_CODE] ASC           	
                ,[ROASTER] ASC"""