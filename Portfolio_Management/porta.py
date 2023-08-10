import numpy as np
import pandas as pd
import pyodbc
# from collections.abc import Iterable
# import datetime


class Portfolio:

    def __init__(self):
        # db connection string
        self.dbConnString = "Driver={SQL Server Native Client 11.0};Server=LAPTOP-QBI0SKOK\\LOCALDB;" \
                            "Database=Analysis;Trusted_Connection=yes;"
        # db connection
        self.dbConn = None

        # delete portfolio VAR template
        self.portDelTemplate =\
            "delete from dbo.RM_VARs where PortfolioId = %d and SecurityId is Null " \
            "and StartDate = '%s' and EndDate = '%s' and Frequency = '%s'"
        # portfolio insert template
        self.portInsTemplate =\
            "insert into dbo.RM_VARs (PortfolioId, StartDate, EndDate, Frequency, udVAR, VAR) " \
            "values (%d, '%s', '%s', '%s', %f, %f)"

        # security delete template
        self.secDelTemplate =\
            "delete from dbo.RM_VARs where PortfolioId = %d and SecurityId = %d " \
            "and StartDate = '%s' and EndDate = '%s' and Frequency = '%s'"
        # security insert template
        self.secInsTemplate =\
            "insert into dbo.RM_VARs (PortfolioId, SecurityId, StartDate, EndDate, Frequency, udVAR, mVAR, cVAR) " \
            "values (%d, %d, '%s', '%s', '%s', %f, %f, %f)"

        # portfolio Id / volume / asset num
        self.portfolioId = self.portVolume = self.ast_num = None
        # set of the portfolio securities
        self.ids = self.tickers = self.quantities = self.weights = self.volumes = self.durations = \
            self.covs = self.betas = self.VARs = self.mVARs = self.cVARs = None

        # price dates diapason
        self.fromDate = self.toDate = None
        # data frequency
        self.Frequency = 'Daily'

        # quotes / returns data frame
        self.roughPriceSeries = self.priceSeries = self.returnSeries = None
        # covariance / correlation matrix
        self.covMatrix = self.corrMatrix = None
        # beta vector of assets to portfolio
        self.intra_betas = None

        # portfolio variance, volatility, and VAR
        self.portVariance = self.portVolatility = self.portVAR = None

    # ----- GENERAL FUNCTIONALITY BLOCK -----

    # open db connection
    def open_db_conn(self):
        if self.dbConn is not None:
            return
        self.dbConn = pyodbc.connect(self.dbConnString)

    # close db connection
    def close_db_conn(self):
        if self.dbConn is None:
            return
        self.dbConn.close()
        self.dbConn = None

    # set dates period
    def set_dates(self, date_from, date_to):
        self.fromDate = date_from
        self.toDate = date_to

    # reset portfolio data
    def reset_portfolio_data(self):
        self.ids = []
        self.tickers = []
        self.quantities = []
        self.weights = []
        self.durations = []

    # reset portfolio metrics
    def reset_portfolio_metrics(self):
        self.ast_num = len(self.tickers)
        self.volumes = np.zeros(self.ast_num)
        self.covs = np.zeros(self.ast_num)
        self.betas = np.zeros(self.ast_num)
        self.VARs = np.zeros(self.ast_num)
        self.mVARs = np.zeros(self.ast_num)
        self.cVARs = np.zeros(self.ast_num)

    # add cash asset to portfolio
    def add_cash_asset(self):
        self.ids.append(-1)
        self.tickers.append('CASH')
        self.quantities.append(0)
        self.weights.append(0)
        self.weights[-1] = 1 - np.array(self.weights[:-1]).sum()
        self.durations.append(None)

    # initialize portfolio by Id
    def set_portfolio_by_id(self, port_id, with_cash=False, port_volume=None):
        if self.dbConn is None:
            return
        self.portVolume = port_volume
        self.reset_portfolio_data()

        # get data from db
        self.portfolioId = port_id
        cursor = self.dbConn.cursor()
        cursor.execute("exec dbo.PortfolioStructure %d" % port_id)
        data = cursor.fetchall()

        # add securities to portfolio
        for itm in data:
            self.ids.append(itm[0])
            self.tickers.append(itm[1])
            self.quantities.append(itm[2])
            self.weights.append(itm[3])
            # init durations by zeros for bonds
            self.durations.append(0 if (itm[1][:2] == 'RU' or itm[1][:2] == 'SU') else None)

        # add cash asset to portfolio
        if with_cash:
            self.add_cash_asset()

        # create empty arrays
        self.reset_portfolio_metrics()

    # ----- PRICE LOADING AND ALIGNING BLOCK -----

    # set duration
    def set_duration(self, ticker, value):
        if ticker not in self.tickers:
            return
        self.durations[self.tickers.index(ticker)] = value

    # temporary function - to be deleted
    def set_durations(self):
        self.set_duration('SU26215RMFS2', 1.5)
        self.set_duration('SU26222RMFS8', 1.7)
        self.set_duration('SU26226RMFS9', 1.9)
        self.set_duration('RU000A104SU6', 2.1)
        self.set_duration('RU000A103KG4', 2.7)

    # get market data
    def get_market_data(self):
        # loading prices
        self.__load_prices_from_db()
        # aligning rough data
        self.__align_rough_data()
        # calculating portfolio value / securities weights
        self.__update_portfolio_volumes()

    # loading prices from db
    def __load_prices_from_db(self):
        self.roughPriceSeries = pd.DataFrame()
        if self.dbConn is None:
            return
        # loading shares prices
        for ind in range(self.ast_num):
            if self.tickers[ind] == 'CASH' or self.durations[ind] is not None:
                continue
            cursor = self.dbConn.cursor()
            cursor.execute("exec dbo.AssetPriceSeries %d, '%s', '%s'" %
                           (self.ids[ind], self.fromDate, self.toDate))
            rows = cursor.fetchall()
            series = {row[0]: row[1] for row in rows}
            self.roughPriceSeries[self.tickers[ind]] =\
                pd.Series(list(series.values()), index=pd.to_datetime(list(series.keys())))

        # calculating ofz spot rate for bonds
        for ind in range(self.ast_num):
            if self.durations[ind] is None:
                continue
            self.roughPriceSeries[self.tickers[ind]] = \
                pd.Series([-1] * len(self.roughPriceSeries[self.tickers[0]]), index=self.roughPriceSeries.index)
            # for dt in self.roughPriceSeries.index:
                # if dt not in self.OFZCVals.index:
                #    continue
                # self.roughPriceSeries.loc[dt, self.tickers[ind]] = \
                #    self.ofz_spot_rate(dt, self.durations[ind]) / 10000

        # filling dummy cash prices
        if 'CASH' in self.tickers:
            cash_series =\
                pd.Series([1] * len(self.roughPriceSeries[self.tickers[0]]), index=self.roughPriceSeries.index)
            self.roughPriceSeries['CASH'] = cash_series

    # align loaded data
    def __align_rough_data(self):
        self.roughPriceSeries.fillna(method='ffill', inplace=True)
        self.roughPriceSeries.fillna(method='bfill', inplace=True)
        # init used price series to daily format by default
        self.priceSeries = self.roughPriceSeries

    # calculate portfolio/securities value/weights
    def __update_portfolio_volumes(self):
        end_date = self.roughPriceSeries.index[-1]
        if self.portVolume is None:
            self.portVolume = 0
            for ind in range(self.ast_num):
                self.volumes[ind] = self.quantities[ind] * self.roughPriceSeries.loc[end_date, self.tickers[ind]]
                self.portVolume += self.volumes[ind]
            for ind in range(self.ast_num):
                self.weights[ind] = self.volumes[ind] / self.portVolume
        else:
            for ind in range(self.ast_num):
                self.volumes[ind] = self.portVolume * self.weights[ind]

    # ----- RESHAPING PRICE SERIES BLOCK -----

    # reshape price series to daily format
    def reshape_as_daily(self):
        self.priceSeries = self.roughPriceSeries
        self.Frequency = 'Daily'
        pass

    # get daily series with filled NA items
    def __get_daily_series(self):
        # daily dates
        daily_dates = pd.date_range(self.fromDate, self.toDate, freq='D')
        # reindexing to daily dates with filling NA items
        series = self.roughPriceSeries.reindex(daily_dates, method='ffill')
        return series.fillna(method='bfill')

    # reshape price series to weekly format
    def reshape_as_weekly(self):
        # weekly dates
        weekly_dates = pd.date_range(self.fromDate, self.toDate, freq='W-FRI')
        # reindexing to weekly dates
        self.priceSeries = self.__get_daily_series().reindex(weekly_dates)
        self.Frequency = 'Weekly'

    # reshape price series to monthly format
    def reshape_as_monthly(self):
        # monthly dates
        weekly_dates = pd.date_range(self.fromDate, self.toDate, freq='M')
        # reindexing to monthly dates
        self.priceSeries = self.__get_daily_series().reindex(weekly_dates)
        self.Frequency = 'Monthly'

    # ----- CALCULATING RISK METRICS BLOCK -----

    # calculate covariance matrix
    def calculate_covariance(self):
        # calculate returns
        self.returnSeries = self.priceSeries / self.priceSeries.shift(1) - 1
        self.returnSeries.dropna(inplace=True)
        # correction for duration for bonds
        for ind in range(self.ast_num):
            if self.durations[ind] is not None:
                self.returnSeries[self.tickers[ind]] *= -self.durations[ind]
        # covariance matrix
        self.covMatrix = self.returnSeries.cov()
        # correlation matrix
        self.corrMatrix = self.returnSeries.corr()

    # calculate risk metrics
    def calculate_intra_risk_metrics(self):
        # portfolio variance and volatility
        bv = np.dot(self.weights, self.covMatrix)
        self.portVariance = np.dot(bv, self.weights)
        self.portVolatility = np.sqrt(self.portVariance)

        # portfolio VAR
        self.portVAR = 1.96 * self.portVolatility * self.portVolume

        # asset betas
        self.betas = bv / self.portVariance
        # assets VARs
        ast_volats = np.array([np.sqrt(self.covMatrix.iloc[i, i]) for i in range(self.ast_num)])
        # undiversified asset VARs
        self.VARs = 1.96 * ast_volats * self.weights * self.portVolume
        # marginal VARs
        self.mVARs = self.portVAR / self.portVolume * self.betas
        #  component VARs
        self.cVARs = self.mVARs * self.volumes

    # delete data
    def __del_data(self):
        if self.dbConn is None:
            return
        # delete portfolio data
        cursor = self.dbConn.cursor()
        cursor.execute(self.portDelTemplate % (self.portfolioId, self.fromDate, self.toDate, self.Frequency))
        cursor.commit()
        # delete securities data
        for sec_id in self.ids:
            cursor = self.dbConn.cursor()
            cursor.execute(self.secDelTemplate % (self.portfolioId, sec_id, self.fromDate, self.toDate, self.Frequency))
            cursor.commit()

    # save data
    def save_data(self):
        if self.dbConn is None:
            return
        # clear data
        self.__del_data()
        # insert portfolio metrics
        cursor = self.dbConn.cursor()
        cursor.execute(self.portInsTemplate %
                       (self.portfolioId, self.fromDate, self.toDate, self.Frequency, sum(self.VARs), self.portVAR))
        cursor.commit()
        # insert security metrics
        for ind in range(len(self.ids)):
            cursor = self.dbConn.cursor()
            cursor.execute(self.secInsTemplate %
                           (self.portfolioId, self.ids[ind], self.fromDate, self.toDate, self.Frequency,
                            self.VARs[ind], self.mVARs[ind], self.cVARs[ind]))
            cursor.commit()
