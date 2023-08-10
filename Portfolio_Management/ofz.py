import pyodbc
import numpy as np
from numpy.linalg import eig
import pandas as pd


# OFZ curve functionality
class OFZ:

    def __init__(self, connect_db=False):
        # db connection string
        self.dbConnString = "Driver={SQL Server Native Client 11.0};Server=LAPTOP-QBI0SKOK\\LOCALDB;" \
                            "Database=Analysis;Trusted_Connection=yes;"

        # price dates diapason
        self.fromDate = self.toDate = None
        # data frequency
        self.Frequency = 'Daily'

        # db connection
        self.dbConn = None
        if connect_db:
            self.dbConn = pyodbc.connect(self.dbConnString)

        # OFZ spot curve static coefficient names
        self.OFZ_A = np.zeros(9)
        self.OFZ_B = np.zeros(9)
        self.__init_ofz_static_coefficients()

        # OFZ spot curve dynamic coefficient names
        self.OFZCNames = ['B1', 'B2', 'B3', 'T1', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9']
        # OFZ spot curve dynamic coefficient values
        self.OFZCVals = pd.DataFrame()
        # OFZ yields
        self.OFZYields = None

        # covariation matrix
        self.cov = None
        # eigen values and vectors
        self.eval = self.evec = None

        self.YTMSeries = None
        self.GSpread = None

    # Open db connection
    def open_db_conn(self):
        if self.dbConn is not None:
            return
        self.dbConn = pyodbc.connect(self.dbConnString)

    # Close db connection
    def close_db_conn(self):
        if self.dbConn is None:
            return
        self.dbConn.close()
        self.dbConn = None

    # set dates period
    def set_dates(self, date_from, date_to):
        self.fromDate = date_from
        self.toDate = date_to

    # Init static params
    def __init_ofz_static_coefficients(self):
        self.OFZ_A[0] = 0.0
        self.OFZ_A[1] = 0.6
        for ind in range(2, 9, 1):
            self.OFZ_A[ind] = self.OFZ_A[ind - 1] + self.OFZ_A[1] * (1.6 ** (ind - 1))

        self.OFZ_B[0] = self.OFZ_A[1]
        for ind in range(1, 9, 1):
            self.OFZ_B[ind] = self.OFZ_B[ind - 1] * 1.6

    # Init dynamic curve coefficients for specified fromDate & toDate diapason
    def get_spot_curve_coefficients(self):
        self.OFZCVals = pd.DataFrame()
        if self.dbConn is None:
            return
        for cf in self.OFZCNames:
            cursor = self.dbConn.cursor()
            cursor.execute(
                "select [Date], %s from dbo.MOEX_SpotCurveCoeffs where [Date] between '%s' and '%s'" %
                (cf, self.fromDate, self.toDate))
            rows = cursor.fetchall()
            series = {row[0]: row[1] for row in rows}
            self.OFZCVals[cf] = pd.Series(list(series.values()), index=pd.to_datetime(list(series.keys())))

    # OFZ yield value for specified term
    def ofz_spot_rate(self, date, term):
        if date not in self.OFZCVals.index:
            return None
        cfs = self.OFZCVals.loc[date]
        g_t = 0
        for ind in range(0, 9, 1):
            g_t += cfs['G' + str(ind + 1)] * np.exp(-((term - self.OFZ_A[ind]) ** 2) / (self.OFZ_B[ind] ** 2))
        g_t += \
            cfs['B1'] + \
            (cfs['B2'] + cfs['B3']) * cfs['T1'] / term * (1 - np.exp(-term / cfs['T1'])) - \
            cfs['B3'] * np.exp(-term / cfs['T1'])
        # continuous compounding rate in basis points
        # rate = 10000 * (np.exp(g_t / 10000) - 1)
        rate = np.exp(g_t / 10000) - 1
        return rate

    # Calculate yields for specified terms
    def calculate_yields(self, terms):
        # empty data frame
        self.OFZYields = pd.DataFrame()
        # for each term fill & add ofz yields time series
        for term in terms:
            t_series = [self.ofz_spot_rate(dt, term) for dt in self.OFZCVals.index]
            self.OFZYields[term] = pd.Series(list(t_series), index=self.OFZCVals.index)

    # Perform PCA
    def pca(self, terms):
        yield_change = self.OFZYields / self.OFZYields.shift(-1) - 1
        yield_change.dropna(inplace=True)
        self.cov = yield_change.cov()
        self.eval, self.evec = eig(self.cov)

    def g_spread(self, isins):
        for ind in range(len(isins)):
            cursor = self.dbConn.cursor()
            cursor.execute("exec dbo.ExtendedAssetPriceSeries '%s', '%s', '%s'" %
                           (isins[ind], self.fromDate, self.toDate))
            rows = cursor.fetchall()
            series = {row[1]: self.ofz_spot_rate(row[1], 5) for row in rows}
            print(series)
            # self.YTMSeries[self.tickers[ind]] =\
            #   pd.Series(list(series.values()), index=pd.to_datetime(list(series.keys())))
