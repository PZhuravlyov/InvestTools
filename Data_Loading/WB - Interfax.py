from ifxdata import InterfaxData
import importlib
import sys
import pyodbc
# from datetime import datetime as dt
# from datetime import date


InterfaxData = importlib.reload(sys.modules['ifxdata']).InterfaxData

# create object from InterfaxData class
IData = InterfaxData()
# pass window login/password for proxy settings
# IData.set_proxies('login', 'password')

# get token from Interfax
IData.get_token()
# free Interfax token
IData.free_token()

IData.set_dates('2022-01-01', '2022-12-07')

# -------------------------------
# ------- BOND CONTROLLER -------
# -------------------------------

# POST Coupons
IData.set_boards(['TQOB', 'TQCB'])
IData.set_boards(None)
IData.set_asset_codes(['SU25084RMFS3', 'RU000A102RN7'])
# data = IData.get_interfax_data('Bond', 'Coupons', False)

# -------------------------------
# ------- MOEX Controller -------
# -------------------------------

# POST Securities
# IData.SetAssetCodes(None)
IData.SetAssetCodes(['AFKS', 'AFLT', 'MOEX', 'SBER', 'VTBR', 'SU25084RMFS3', 'RU000A102RN7'], False)
IData.SetBoards(['TQBR', 'TQOB', 'TQCB'])
# IData.SetBoards(None)
# data = IData.GetInterfaxData('MOEX', 'Securities', False)

# POST futures
IData.SetUnderlying('BRO')
IData.SetAssetCodes(['BRX2BRZ2', 'BRV2BRX2', 'BRZ2BRF3'])
# data = IData.GetInterfaxData('MOEX', 'Futures')

IData.SetBoards(['RFUD'])
IData.SetBoards(['TQBR'])
IData.SetAssetCodes(None)

# ----------------------------------
# ------- Archive Controller -------
# ----------------------------------

data = IData.get_interfax_data('Archive', 'History')

# ----------------------------------
# ------- Emitent Controller -------
# ----------------------------------

# Emitent list
dbConn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};Server=LAPTOP-QBI0SKOK\\LOCALDB;Database=Analysis;Trusted_Connection=yes;")

insertTemplate = "insert into dbo.IFX_Companies(FinInstId, ShortName, FullName, Sector, INN) " \
    "values(%d, '%s', '%s', '%s', '%s')"

pageIndex = 1
pageSize = 100
IData.set_paging(pageIndex, pageSize)
IData.update_body_paging()
emitents = IData.get_interfax_data('Emitent', 'Companies')
count = emitents[0]['counter']

while True:
    for emt in emitents:
        cursor = dbConn.cursor()
        if emt['shortname_rus'] is not None:
            emt['shortname_rus'] = emt['shortname_rus'].replace("'", "''")
        if emt['fullname_rus'] is not None:
            emt['fullname_rus'] = emt['fullname_rus'].replace("'", "''")
        # cursor.execute(
            # insertTemplate % (emt['fininstid'], emt['shortname_rus'], emt['fullname_rus'], emt['sector'], emt['inn']))
        # cursor.commit()
    if pageIndex * pageSize >= count:
        break
    pageIndex += 1
    IData.set_paging(pageIndex, pageSize)
    IData.update_body_paging()
    emitents = IData.get_interfax_data('Emitent', 'Companies')

dbConn.close()

# Find emitent
IData.set_searching_pattern(['Полюс'])
company = IData.get_interfax_data('Emitent', 'Find')

# Multipliers
dbConn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};Server=LAPTOP-QBI0SKOK\\LOCALDB;Database=Analysis;Trusted_Connection=yes;")
mplDate = '2023-02-15'
IData.set_dates(mplDate, mplDate)

listTemplate =\
    "select distinct ifx.FinInstId from dbo.DCT_Companies cmp join dbo.IFX_Companies ifx on cmp.INN = ifx.INN " \
    "join dbo.DCT_Assets ast on ast.CompanyId = cmp.Id where ast.Id in (select SecurityId from dbo.IND_Structures) " \
    "and ifx.Exclude = 0"

cursor = dbConn.cursor()
cursor.execute(listTemplate)
idxIds = cursor.fetchall()

insTemplate =\
    "insert into dbo.IFX_Multipliers (" \
    "FinInstId, [Date], CurrentRatio, TotalDebt, Earning, EBITDA, EnterpriseValue, EPS, EV_To_EBITDA, " \
    "Equity, FFO, FCFF, FCFE, ROE) " \
    "values('%d', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

for finInstId in idxIds:
    IData.set_fin_inst_ids(finInstId[0])
    mpl = IData.get_interfax_data('Emitent', 'Multipliers')
    mpl = mpl[0]
    cursor = dbConn.cursor()
    cursor.execute(insTemplate % (
        finInstId[0], mplDate, mpl['currentRatio'] or 'NULL', mpl['debt'] or 'NULL', mpl['earnings'] or 'NULL',
        mpl['ebitda'] or 'NULL', mpl['enterpriseValue'] or 'NULL', mpl['eps'] or 'NULL', mpl['evEbitda'] or 'NULL',
        mpl['equity'] or 'NULL', mpl['ffo'] or 'NULL', mpl['fcff'] or 'NULL', mpl['fcfe'] or 'NULL',
        mpl['roe'] or 'NULL'))
    cursor.commit()
print('End')

dbConn.close()
IData.set_fin_inst_ids([88793])
IData.set_dates('2022-12-30', '2023-02-28')
mpls = IData.get_interfax_data('Emitent', 'Multipliers')
