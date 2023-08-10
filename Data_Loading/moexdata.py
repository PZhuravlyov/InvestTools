import requests
import pyodbc
import datetime as dt


class MOEXData:

    def __init__(self):
        # url template for requesting data
        self.url_template =\
            'https://iss.moex.com/iss/history/engines/stock/markets/%s/boards/%s/securities/%s/' \
            'securities.json?iss.meta=off&from=%s&till=%s'
        # url used for requests
        self.url = None
        # historical dates diapason
        self.fromDate = self.toDate = None
        # http request header
        self.headers = {'Content-Type': 'application/json'}

        # db connection string
        self.dbConnString = "Driver={SQL Server Native Client 11.0};Server=LAPTOP-QBI0SKOK\\LOCALDB;" \
                            "Database=Analysis;Trusted_Connection=yes;"
        # db connection
        self.dbConn = None

        # index finding string
        self.findIndTemplate =\
            "select id from dbo.IND_Indices where [Name] = '%s'"
        # security finding string
        self.findSecTemplate =\
            "select Id, BoardCode from dbo.DCT_Assets where ExchangeId = 1 and Ticker = '%s'"

        # index data deleting string
        self.delIndTemplate = "delete from dbo.MD_IndexPrices where IndexId = %d and [Date] between '%s' and '%s'"
        # security data deleting string
        self.delSecTemplate = "delete from dbo.MD_SecurityQuotes where AssetId = %d and [Date] between '%s' and '%s'"
        # index data inserting string
        self.indInsTemplate =\
            "insert into dbo.MD_IndexPrices (IndexId, Date, [Open], Low, High, [Close]) " \
            "values(%d, '%s' ,%f, %f, %f, %f)"
        # security data inserting string
        self.secInsTemplate =\
            "insert into dbo.MD_SecurityQuotes " \
            "(AssetId, Date, [Open], Low, High, [Close], YTM_Close, Accrued) " \
            "values(%d,'%s',%f, %f, %f, %f, %s, %s)"

        # moex indices boards
        self.indexBoards =\
            {'IMOEX': 'SNDX', 'MOEXBMI': 'SNDX', 'RTSI': 'RTSI', 'RGBITR': 'SNDX', 'RUCBITR': 'SNDX'}
        # assets classes linked to trading boards
        self.boardClasses =\
            {'SNDX': 'index', 'RTSI': 'index', 'TQBR': 'shares', 'TQCB': 'bonds', 'TQOB': 'bonds', 'TQTF': 'shares'}

        # moex code / id / trading board of requested instrument
        self.moexCode = self.instrumentId = self.boardName = None
        # returned json data
        self.jsColumns = self.jsData = None

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
    def set_dates(self, from_date, to_date):
        self.fromDate = from_date
        self.toDate = to_date

    # make requested url
    def __make_url_string(self, moex_code, moex_board=None, from_date=None, to_date=None):
        # init dates
        if from_date is None:
            from_date = self.fromDate
        if to_date is None:
            to_date = self.toDate
        self.__get_ticker_info()
        # defining trading board
        if moex_code in self.indexBoards:
            board = self.indexBoards[moex_code]
        else:
            if moex_board is None:
                board = self.boardName
            else:
                board = moex_board
        # defining asset class
        asset_class = self.boardClasses[board]
        # forming url
        self.url =\
            self.url_template % (asset_class, board, moex_code,
                                 from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))

    # make http request
    def get_http_data(self, moex_code, moex_board=None):
        self.moexCode = moex_code
        self.jsData = []
        # get data by 2 months blocks
        from_date = self.fromDate
        to_date = min(self.toDate, from_date + dt.timedelta(days=60))
        while from_date <= self.toDate:
            # make requested url
            self.__make_url_string(moex_code, moex_board, from_date, to_date)
            # request data
            response = requests.get(self.url, headers={'Content-Type': 'application/json'})
            # extract data from http response
            if response.status_code == 200:
                self.jsColumns = response.json()['history']['columns']
                self.jsData.extend(response.json()['history']['data'])
            from_date = to_date + dt.timedelta(days=1)
            to_date = min(self.toDate, from_date + dt.timedelta(days=60))
        return self.jsData

    # get info for requested MOEX code
    def __get_ticker_info(self):
        if self.dbConn is None or self.moexCode is None:
            return
        cursor = self.dbConn.cursor()
        # get data for index ticker
        if self.moexCode in self.indexBoards:
            cursor.execute(self.findIndTemplate % self.moexCode)
            self.instrumentId = cursor.fetchone()[0]
        # get data for security ticker
        else:
            cursor.execute(self.findSecTemplate % self.moexCode)
            self.instrumentId, self.boardName = cursor.fetchone()
        cursor.close()

    # delete market data for transferred ticker
    def __delete_market_data(self):
        cursor = self.dbConn.cursor()
        # delete index data
        if self.moexCode in self.indexBoards:
            cursor.execute(self.delIndTemplate % (self.instrumentId,
                           self.fromDate.strftime("%Y-%m-%d"), self.toDate.strftime("%Y-%m-%d")))
        # delete security data
        else:
            cursor.execute(self.delSecTemplate % (self.instrumentId,
                           self.fromDate.strftime("%Y-%m-%d"), self.toDate.strftime("%Y-%m-%d")))
        cursor.close()

    # save data to db
    def save_data(self):
        if self.dbConn is None or self.jsData is None:
            return
        # delete market data
        self.__delete_market_data()
        # save index prices
        if self.moexCode in self.indexBoards:
            self.__save_index_prices()
        # save security quotes
        else:
            self.__save_security_quotes()

    # save security quotes
    def __save_security_quotes(self):
        # data positions
        date_pos, open_pos, low_pos, high_pos, close_pos =\
            self.jsColumns.index('TRADEDATE'), self.jsColumns.index('OPEN'), self.jsColumns.index('LOW'),\
            self.jsColumns.index('HIGH'), self.jsColumns.index('CLOSE')
        ytm_pos = self.jsColumns.index('YIELDCLOSE') if 'YIELDCLOSE' in self.jsColumns else None
        acr_pos = self.jsColumns.index('ACCINT') if 'ACCINT' in self.jsColumns else None

        for sct in self.jsData:
            # check number of trades
            if sct[4] <= 0:
                continue
            cursor = self.dbConn.cursor()
            sql_script = self.secInsTemplate % (
                self.instrumentId, sct[date_pos], sct[open_pos], sct[low_pos], sct[high_pos], sct[close_pos],
                sct[ytm_pos] if ytm_pos is not None else 'NULL',
                sct[acr_pos] if acr_pos is not None else 'NULL')
            cursor.execute(sql_script)
            cursor.commit()
            cursor.close()
        return self.instrumentId

    # save index prices
    def __save_index_prices(self):
        # data positions
        date_pos, open_pos, low_pos, high_pos, close_pos = \
            self.jsColumns.index('TRADEDATE'), self.jsColumns.index('OPEN'), self.jsColumns.index('LOW'), \
            self.jsColumns.index('HIGH'), self.jsColumns.index('CLOSE')

        for ind in self.jsData:
            cursor = self.dbConn.cursor()
            sql_script = self.indInsTemplate % (
                self.instrumentId, ind[date_pos], ind[open_pos], ind[low_pos], ind[high_pos], ind[close_pos])
            cursor.execute(sql_script)
            cursor.commit()
            cursor.close()
        return self.instrumentId
