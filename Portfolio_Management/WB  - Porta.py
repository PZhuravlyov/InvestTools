# import numpy as np
# import pandas as pd
from datetime import datetime
import importlib
import sys
import porta
import ofz

# portfolio class
Portfolio = importlib.reload(sys.modules['porta']).Portfolio
port = porta.Portfolio()

startDate = datetime(2021, 1, 1)
endDate = datetime(2023, 2, 15)
port.set_dates(startDate, endDate)
port.open_db_conn()
# load portfolio structure
port.set_portfolio_by_id(2, True, 1000000)

port.set_durations()
port.get_spot_curve_coefficients()
# print(port.ofz_spot_rate('2023-02-08', 3))

port.get_market_data()
port.close_db_conn()

port.reshape_as_daily()
# port.reshape_as_weekly()
# port.reshape_as_monthly()

port.calculate_covariance()
port.calculate_intra_risk_metrics()

port.save_data()




importlib.reload(sys.modules['ofz'])

gcurve = ofz.OFZ(True)
startDate = datetime(2022, 9, 21)
endDate = datetime(2023, 4, 7)

gcurve.set_dates(startDate, endDate)
gcurve.get_spot_curve_coefficients()



# gcurve.open_db_conn()
gcurve.g_spread(['RU000A104YT6','RU000A100WA8'])

gcurve.close_db_conn()

terms = [1 ,2, 3, 4, 5, 7, 10]
gcurve.calculate_yields(terms)
gcurve.pca(terms)
