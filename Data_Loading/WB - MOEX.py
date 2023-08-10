import pyodbc
from datetime import date
import importlib
import sys
# from moexdata import MOEXData


MOEXData = importlib.reload(sys.modules['moexdata']).MOEXData

dbConn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};Server=LAPTOP-QBI0SKOK\\LOCALDB; Database=Analysis;Trusted_Connection=yes;")
cursor = dbConn.cursor()
cursor.execute(
    "select ast.ticker from dbo.DCT_Assets ast join dbo.IND_Structures ins on ast.Id = ins.SecurityId "
    "where ins.IndexId = 1")
indexSecurities = cursor.fetchall()
dbConn.close()

mxd = MOEXData()
mxd.open_db_conn()
fromDate = date(2019, 9, 1)
toDate = date(2023, 4, 21)
mxd.set_dates(fromDate, toDate)

# data = mxd.get_http_data('IMOEX')
data = mxd.get_http_data('RU000A102ZH2')
mxd.save_data()

for sct in indexSecurities:
    data = mxd.get_http_data(sct[0])
    mxd.save_data()
print("Done")

mxd.close_db_conn()
