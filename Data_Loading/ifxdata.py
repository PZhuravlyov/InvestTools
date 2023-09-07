# import numpy as np
import pandas as pd
import pyodbc
import requests
from datetime import date, timedelta
from collections.abc import Iterable
import math


# Class for getting data from InterFax Web API
class InterfaxData:

    # interfax base url
    baseurl = 'https://dh2.efir-net.ru/v2'
    # interfax login & password
    login = 'rencred-api2'
    password = '137Qwd'
    # SQL DB connection string
    sqlConnectString = """Driver={SQL Server Native Client 11.0};Server=CLSDB3034\\CLSDB3034;
Database=AnalyticDev;Trusted_Connection=yes;"""

    def __init__(self):
        # http request url
        self.url = None
        # token used for authorized access
        self.token = None
        # sql connection
        self.sql_conn = None

        # initialize controller set        
        self.__init_controllers()
        # initialize realized action methods for each controller
        self.__init_archive_actions()
        self.__init_bond_actions()
        self.__init_emitent_actions()
        self.__init_moex_actions()
        self.__init_info_actions()

        self.proxies = self.body = None
        self.toDate = date.today() - timedelta(days=1)
        self.fromDate = self.toDate - timedelta(days=7)
        
        # link for saving data to DB
        # used in called function save_data_to_db()
        self.__saveDataToDB = None

        self.ifxCodes = {}
        self.ifxIds = {}
        
        self.baseCurr = 'USD'
        self.quotedCurr = 'RUB'

        self.underlying = None
        self.boards = None
        self.assetCodes = None
        self.finInstIds = None

        self.cmpnSearchPattern = None

        self.roughData = self.parsedData = self.savedData = None

        self.pageNum = self.pageSize = None

    # Init controller set
    def __init_controllers(self):
        # set of functions getting InterFax data
        self.controllers = {'Archive': {}, 'Bond': {}, 'CorporateAction': {},
                            'Emitent': {}, 'Indicator': {}, 'Info': {}, 'Rating': {}, 'MOEX': {}}
        # set of parsers
        self.parsers = {'Archive': {}, 'Bond': {}, 'CorporateAction': {},
                        'Emitent': {}, 'Indicator': {}, 'Info': {}, 'Rating': {}, 'MOEX': {}}
    
        # set of saving functions
        self.db_manager = {'Archive': {}, 'Bond': {}, 'CorporateAction': {},
                           'Emitent': {}, 'Indicator': {}, 'Info': {}, 'Rating': {}, 'MOEX': {}}

    # Return controller names
    def controllers(self):
        return self.controllers.keys()

    # Return action names for the specified controller
    def controller_actions(self, controller):
        if controller not in self.controllers:
            return None
        return self.controllers[controller].keys()

    # ---------------------------
    # ------- Authorizing -------
    # ---------------------------

    # Connect to SQL DB
    def sql_connect(self):
        if self.sql_conn is not None:
            return
        self.sql_conn = pyodbc.connect(InterfaxData.sqlConnectString)

    # Disconnect from SQL DB
    def sql_disconnect(self):
        if self.sql_conn is None:
            return
        self.sql_conn.close()
        self.sql_conn = None

    # Set Http[s] proxy params
    def set_proxies(self, login, password):
        self.proxies = {'http': 'http://%s:%s@proxy.rccf.ru:8080' % (login, password),
                        'https': 'http://%s:%s@proxy.rccf.ru:8080' % (login, password)}

    # Authorize and Get Interfax Token
    def get_token(self):
        if self.token is not None:
            return self.token

        self.url = InterfaxData.baseurl + '/Account/Login'
        self.body = {'login': InterfaxData.login, 'password': InterfaxData.password}

        tkn = self.__do_post_request()
        self.token = None if tkn is None else tkn['token']

        return self.token

    # Disconnect from Interfax
    def free_token(self):
        if self.token is None:
            return

        self.url = InterfaxData.baseurl + '/Account/Logoff'
        self.body = self.token = None

        self.__do_post_request()

    # ------- MAIN FUNCTION FOR REQUESTING DATA -------
    def get_interfax_data(self, controller, action, parse=False):
        # check controller name
        if controller not in self.controllers:
            return -1
        # check action name
        if action not in self.controllers[controller]:
            return -2
        self.url = InterfaxData.baseurl + '/%s/%s' % (controller, action)
        # set requested method body
        self.controllers[controller][action]()
        # get requested data
        self.roughData = self.__do_post_request()
        # init the saving link
        # self.__saveDataToDB = self.db_manager[controller][action]
        # return data
        return self.parsers[controller][action]() if parse else self.roughData

    # Make http POST request
    def __do_post_request(self):
        if self.token is None:
            self.headers = {'Content-Type': 'application/json'}
        else:
            self.headers = {'authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'}
        response = requests.post(self.url, json=self.body, headers=self.headers, proxies=self.proxies, verify=False)
        return response.json() if response.status_code == 200 else None

    # Method called for saving data to DB
    def save_data_to_db(self):
        if self.__saveDataToDB is None:
            print("No data to save")
        else:
            self.__saveDataToDB()
            print("Data have been saved")

    # ---------------------------------------------------------------
    # ------- Setting params used when setting actions bodies -------
    # ---------------------------------------------------------------

    # Set page num / page size
    def set_paging(self, page_num, page_size):
        self.pageNum = page_num
        self.pageSize = page_size

    # Update body paging
    def update_body_paging(self):
        if self.pageNum is not None:
            # self.body.update({'pageNum': self.pageNum})
            self.body['pageNum'] = self.pageNum
        if self.pageSize is not None:
            # self.body.update({'pageSize': self.pageSize})
            self.body['pageSize'] = self.pageSize

    # Set date period
    def set_dates(self, from_date, to_date):
        self.fromDate = from_date
        self.toDate = to_date

    # Set boards as a filter
    def set_boards(self, boards):
        self.boards = list(boards) if isinstance(boards, Iterable) else None

    # Set futures underlying
    def set_underlying(self, underlying):
        self.underlying = underlying
        self.set_boards(None)

    # Set asset codes as a filter for the http request body
    def set_asset_codes(self, codes):
        self.ifxCodes = {}
        self.ifxIds = {}
        if not isinstance(codes, Iterable):
            return
        for code in codes:
            if code in self.ifxCodes:
                continue
            self.ifxCodes[code] = None

        self.__update_ifx_codes()

    # Updating IFX codes for passed MOEX codes
    def __update_ifx_codes(self):
        if len(self.ifxCodes) <= 0:
            return
        data = self.get_interfax_data('MOEX', 'Securities')
        for item in data:
            self.ifxCodes[item['secid']] = item['fintoolid']
            self.ifxIds[item['fintoolid']] = {'secid': item['secid'], 'ISIN': item['isin'], 'id_iss': item['id_iss']}

    # Set base and quoted currencies for getting exchange rates
    def set_currencies(self, base_curr, quoted_curr):
        self.baseCurr = base_curr
        self.quotedCurr = quoted_curr

    def set_searching_pattern(self, pattern):
        self.cmpnSearchPattern = pattern

    def set_fin_inst_ids(self, fin_inst_ids):
        self.finInstIds = fin_inst_ids

    # ------------------------------------------
    # ------- Archive controller actions -------
    # ------------------------------------------
    
    # Init all realized methods for Archive controller
    def __init_archive_actions(self):
        self.controllers['Archive']['History'] = self.__set_archive_history_body
        self.controllers['Archive']['CurrencyRateHistory'] = self.__set_archive_currencyratehistory_body

    #  set request body for Archive/History method
    def __set_archive_history_body(self):
        key = list(self.ifxIds.keys())[0]
        self.body = \
            {'dateFrom': self.fromDate, 'dateTo': self.toDate, 'issId': self.ifxIds[key]['id_iss'], 'step': 1440}
            
    # Set request body for Archive/CurrencyRateHistory method
    def __set_archive_currencyratehistory_body(self):
        self.body = \
            {'dateFrom': self.fromDate, 'dateTo': self.toDate, 'baseCurrency': self.baseCurr,
             'quotedCurrency': self.quotedCurr, 'step': 1440}

    # ---------------------------------------
    # ------- Bond controller actions -------
    # ---------------------------------------
    
    # Init all realized methods for Bond controller
    def __init_bond_actions(self):
        # action methods
        self.controllers['Bond']['AuctionData'] = self.__set_bond_auction_body
        self.controllers['Bond']['Coupons'] = self.__set_bond_coupons_body
        self.controllers['Bond']['Convertation'] = self.__set_bond_convertation_body
        # parsers
        self.parsers['Bond']['Coupons'] = self.__parse_bond_coupons_response
        # savers
        self.db_manager['Bond']['Coupons'] = self.__save_bond_coupons_from_rough_data

    # Init request body for Bond/Auction method
    def __set_bond_auction_body(self):
        self.body = {'filter': 'id_fintool = %s' % list(self.ifxIds.keys())[0]}

    # Init request body for Bond/Coupon method
    def __set_bond_coupons_body(self):
        ids = str(list(self.ifxIds.keys())).replace('[', '(').replace(']', ')')
        self.body = {'filter': 'id_fintool in %s' % ids}
        pass

    # Init request body for Bond/Convertation method
    def __set_bond_convertation_body(self):
        pass

    # Init request body for Bond/Coupons method
    def __parse_bond_coupons_response(self):
        if self.roughData is None:
            return None
        dct = {'FinToolId': {}, 'CouponPeriod': {}, 'PeriodFrom': {}, 'PeriodTo': {}, 'PayPerBond': {},
               'CouponRate': {}}
        self.parsedData = pd.DataFrame(dct)

        for datum in self.roughData:
            row = {'FinToolId': int(datum['id_fintool']), 'CouponPeriod': datum['id_coupon'],
                   'PeriodFrom': datum['begin_period'], 'PeriodTo': datum['end_period'],
                   'PayPerBond': datum['pay_per_bond'], 'CouponRate': datum['coupon_rate']}

            self.parsedData = self.parsedData.append(row, ignore_index=True)

        # init the saving link
        # self.__saveDataToDB = self.db_manager['Bond']['Coupons']

        return self.parsedData

    # Saving data to DB from Bond/Coupons method from rough data
    # Used for setting self.__saveDataToDB link in get_interfax_data method
    def __save_bond_coupons_from_rough_data(self):
        # connect to db
        self.sql_connect()
        
        # list of instruments Ids
        ids = str(list(self.ifxIds.keys())).replace('[', '(').replace(']', ')')        

        # delete existed data
        cursor = self.sql_conn.cursor()
        cursor.execute("delete from dbo.ACF_Coupons where FintoolId in %s" % ids)
        cursor.commit()

        # disconnect from db
        self.sql_disconnect()

    # Saving data to DB from Bond/Coupons method from parsed data
    def __save_bond_coupons_from_parsed_data(self):

        self.savedData = self.parsedData

        # connect to db
        self.sql_connect()

        # list of instruments Ids
        ids = str(list(self.ifxIds.keys())).replace('[', '(').replace(']', ')')

        # delete existed data
        cursor = self.sql_conn.cursor()
        cursor.execute("delete from dbo.ACF_Coupons where FintoolId in %s" % ids)
        cursor.commit()

        # from "insert" script template
        ins_sql = "insert into dbo.ACF_Coupons (FintoolId, CouponPeriod, StartDate, EndDate, [Value]" \
                  " values(%s, %s, '%s', '%s', %f)"

        # insert data from parsed data set
        for index, row in self.savedData.iterrows():
            # skip empty data
            if math.isnan(row['PayPerBond']):
                continue
            # from end execute sql command
            cursor.execute(ins_sql % (row['FinToolId'], row['CouponPeriod'], row['PeriodFrom'][:10],
                                      row['PeriodTo'][:10], row['PayPerBond']))
            cursor.commit()

        # disconnect from db
        self.sql_disconnect()

        # reset the saving link
        self.__saveDataToDB = None

    # ------------------------------------------
    # ------- Emitent controller actions -------
    # ------------------------------------------

    # Init all realized methods for Emitent controller
    def __init_emitent_actions(self):
        self.controllers['Emitent']['Companies'] = self.__set_emitent_companies_body
        self.controllers['Emitent']['Find'] = self.__set_emitent_find_body
        self.controllers['Emitent']['Multipliers'] = self.__set_emitent_multipliers_body

    # Set request body for Emitent/Companies method
    def __set_emitent_companies_body(self):
        self.body = {}
        self.update_body_paging()

    # Set request body for Emitent/Find method
    def __set_emitent_find_body(self):
        self.body = {'codes': self.cmpnSearchPattern}

    # Set request body for Emitent/Multipliers method
    def __set_emitent_multipliers_body(self):
        self.body = {"fininstIds": [self.finInstIds]}
        if self.fromDate is not None:
            self.body['startDate'] = self.fromDate
        if self.toDate is not None:
            self.body['endDate'] = self.toDate

    # ---------------------------------------
    # ------- MOEX controller actions -------
    # ---------------------------------------

    # Init all realized methods for MOEX controller
    def __init_moex_actions(self):
        self.controllers['MOEX']['Securities'] = self.__set_moex_securities_body
        self.controllers['MOEX']['Futures'] = self.__set_moex_futures_body

    # Init request body for MOEX/Securities method
    def __set_moex_securities_body(self):
        self.body = {'pageNum': 1, 'pageSize': 100000}
        if self.ifxCodes is not None and len(self.ifxCodes) > 0:
            codes = list(self.ifxCodes.keys())
            self.body['filter'] = "secid in %s" % (str(codes).replace('[', '(').replace(']', ')'))
        if self.boards is not None:
            board_filter = 'boardid in ' + str(self.boards).replace('[', '(').replace(']', ')')
            if 'filter' in self.body:
                self.body['filter'] = self.body['filter'] + ' and ' + board_filter
            else:
                self.body['filter'] = board_filter

    # Set request body for MOEX/Futures method
    def __set_moex_futures_body(self):
        self.body = {'filter': 'EXPIRATION_DATE > #%s#' % self.toDate}
        if self.underlying is not None:
            self.body['filter'] += " and BA_SECID = '%s'" % self.underlying
        # self.body = {'filter' : "BA_SECID = 'VTBR'"}

    # ---------------------------------------
    # ------- Info controller actions -------
    # ---------------------------------------

    # Init all realized methods for Info controller
    def __init_info_actions(self):
        self.controllers['Info']['Calendar'] = self.__set_info_calendar_body

        self.parsers['Info']['Calendar'] = self.__parse_info_calendar_response

    def __set_info_calendar_body(self):
        today = date.today()
        self.body =\
            {"eventTypes": ["DIV"], "fields": ["isiNcode", "nickname", "pay1Security", "fixDate", "faceFTName",
                                               "recomendFixDate", "recomendPay1Security", "rateDate"],
             "filter": f"(recomendFixDate >= #{today}#) and faceFTName='RUB'"}
    
    def __parse_info_calendar_response(self):
        if self.roughData is None:
            return None
        dct = {'Isin': {}, 'name': {}, 'recomendFixDate': {}}
        self.parsedData = pd.DataFrame(dct)
        
        for datum in self.roughData['timeTableFields']:
            row = {'Isin': datum['isiNcode'], 'name': datum['nickname'], 'recomendFixDate': datum['recomendFixDate']}
            self.parsedData = self.parsedData.append(row, ignore_index=True)
        
        return self.parsedData
