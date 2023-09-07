import ifxdata
import importlib
import sys
import pandas as pd
# import pyodbc
# from datetime import datetime as dt
# from datetime import date


InterfaxData = importlib.reload(sys.modules['ifxdata']).InterfaxData

# create object from InterfaxData class
IData = ifxdata.InterfaxData()
# pass window login/password for proxy settings
IData.set_proxies('pzhuravlev', 'RCJobPsw2')

# get token from Interfax
IData.get_token()
# free Interfax token
IData.free_token()



dct = {'FinToolId': {1, 2}, 'CouponPeriod': {3, 4}, 'PeriodFrom': {5, 6}, 'PeriodTo': {7, 8}, 'PayPerBond': {9, 0}}
parsedData = pd.DataFrame(dct)

# -------------------------------
# ------- MOEX Controller -------
# -------------------------------

# getting info for specified asset codes
IData.set_asset_codes(['AFKS', 'AFLT', 'MOEX', 'SBER', 'VTBR', 'SU25084RMFS3', 'RU000A102RN7'])
data = IData.get_interfax_data('MOEX', 'Securities', False)

# getting info for all securities traded on specified boards
IData.set_asset_codes(None)
IData.set_boards(['TQBR', 'TQOB', 'TQCB'])
IData.set_boards(['TQCB'])
# IData.set_boards(None)
data = IData.get_interfax_data('MOEX', 'Securities', False)

# POST futures
IData.set_underlying('BRO')
IData.set_asset_codes(['BRX2BRZ2', 'BRV2BRX2', 'BRZ2BRF3'])
data = IData.get_interfax_data('MOEX', 'Futures')

# ----------------------------------
# ------- Archive Controller -------
# ----------------------------------

IData.set_dates('2023-01-01', '2023-07-31')
IData.set_asset_codes(['AFKS'])
data = IData.get_interfax_data('Archive', 'History')

IData.set_currencies('USD', 'RUB')
data = IData.get_interfax_data('Archive', 'CurrencyRateHistory')

#data = IData.get_interfax_data('Info', 'Calendar')

# -------------------------------
# ------- BOND CONTROLLER -------
# -------------------------------

IData.set_boards(['TQOB', 'TQCB'])
IData.set_boards(None)
IData.set_asset_codes(['SU25084RMFS3', 'RU000A102RN7'])
data = IData.get_interfax_data('Bond', 'Coupons', True)

IData.save_data_to_db()

IData.__parse_coupons_response()


data = IData.get_interfax_data('Bond', 'AuctionData')

# ----------------------------------
# ------- Emitent Controller -------
# ----------------------------------


roughData = IData.get_interfax_data('Info', 'Calendar', False)

data = IData.get_interfax_data('Info', 'Calendar', True)
