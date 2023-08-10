# import numpy as np
import pandas as pd
import pyodbc
import requests
from datetime import date, timedelta
from collections.abc import Iterable


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
        # initialize action methods for each controller
        self.__init_archive_actions()
        self.__init_bond_actions()
        self.__init_emitent_actions()
        self.__init_moex_actions()

        self.proxies = self.body = None
        self.toDate = date.today() - timedelta(days=1)
        self.fromDate = self.toDate - timedelta(days=7)

        self.ifxCodes = {}
        self.ifxIds = {}

        self.underlying = None
        self.boards = None
        self.assetCodes = None
        self.finInstIds = None

        self.cmpnSearchPattern = None

        self.roughData = None

        self.pageNum = self.pageSize = None

    # Init controller set
    def __init_controllers(self):
        self.controllers = {'Archive': {}, 'Bond': {}, 'CorporateAction': {},
                            'Emitent': {}, 'Indicator': {}, 'Info': {}, 'Rating': {}, 'MOEX': {}}
        self.parsers = {'Archive': {}, 'Bond': {}, 'CorporateAction': {},
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

    def __update_ifx_codes(self):
        if len(self.ifxCodes) <= 0:
            return
        data = self.get_interfax_data('MOEX', 'Securities')
        for item in data:
            self.ifxCodes[item['secid']] = item['fintoolid']
            self.ifxIds[item['fintoolid']] = {'secid': item['secid'], 'ISIN': item['isin'], 'id_iss': item['id_iss']}

    def __update_fintoolids_for_asset_codes_from_db(self):
        if self.assetCodes is None:
            self.assetFintoolIds = self.fintoolIds = None
            return
        cursor = self.sql_conn.cursor()
        cds = str(self.assetCodes).replace('[', '(').replace(']', ')')
        cursor.execute("select FintoolId from dbo.DCT_Assets where MOEX_Code in %s" % cds)
        data = cursor.fetchall()
        self.assetFintoolIds = list(set([ids[0] for ids in data]))
        cursor.close()

    def set_searching_pattern(self, pattern):
        self.cmpnSearchPattern = pattern

    def set_fin_inst_ids(self, fin_inst_ids):
        self.finInstIds = fin_inst_ids

    # ------------------------------------------
    # ------- Archive controller actions -------
    # ------------------------------------------
    def __init_archive_actions(self):
        self.controllers['Archive']['History'] = self.__set_archive_history_body

    def __set_archive_history_body(self):
        key = list(self.ifxIds.keys())[0]
        self.body = \
            {'dateFrom': self.fromDate, 'dateTo': self.toDate, 'issId': self.ifxIds[key]['id_iss'], 'step': 1440}

    # ---------------------------------------
    # ------- Bond controller actions -------
    # ---------------------------------------
    def __init_bond_actions(self):
        # action methods
        self.controllers['Bond']['AuctionData'] = self.__set_bond_auction_body
        self.controllers['Bond']['Coupons'] = self.__set_bond_coupons_body
        # parsers
        self.parsers['Bond']['Coupons'] = self.__parse_coupons_response

    def __set_bond_auction_body(self):
        self.body = {'filter': 'id_fintool = %s' % list(self.ifxIds.keys())[0]}

    def __set_bond_coupons_body(self):
        ids = str(list(self.ifxIds.keys())).replace('[', '(').replace(']', ')')
        self.body = {'filter': 'id_fintool in %s' % ids}

    def __parse_coupons_response(self):
        if self.roughData is None:
            return None
        parsed_data = pd.DataFrame()
        for datum in self.roughData:
            # parsed_data.append((datum['id_fintool'], datum['coupon_rate']))
            parsed_data.append((datum['id_fintool'], datum['pay_date']))

        return parsed_data

    # ------------------------------------------
    # ------- Emitent controller actions -------
    # ------------------------------------------
    def __init_emitent_actions(self):
        self.controllers['Emitent']['Companies'] = self.__set_emitemt_companies_body
        self.controllers['Emitent']['Find'] = self.__set_emitent_find_body
        self.controllers['Emitent']['Multipliers'] = self.__set_emitent_multipliers_body

    def __set_emitemt_companies_body(self):
        self.body = {}
        self.update_body_paging()

    def __set_emitent_find_body(self):
        self.body = {'codes': self.cmpnSearchPattern}

    def __set_emitent_multipliers_body(self):
        self.body = {"fininstIds": [self.finInstIds]}
        if self.fromDate is not None:
            self.body['startDate'] = self.fromDate
        if self.toDate is not None:
            self.body['endDate'] = self.toDate

    # ---------------------------------------
    # ------- MOEX controller actions -------
    # ---------------------------------------
    def __init_moex_actions(self):
        self.controllers['MOEX']['Securities'] = self.__set_moex_securities_body
        self.controllers['MOEX']['Futures'] = self.__set_moex_futures_body

    def __set_moex_securities_body(self):
        self.body = {'pageNum': 1, 'pageSize': 100000}
        if self.ifxCodes is not None:
            codes = list(self.ifxCodes.keys())
            self.body['filter'] = "secid in %s" % (str(codes).replace('[', '(').replace(']', ')'))
        if self.boards is not None:
            board_filter = 'boardid in ' + str(self.boards).replace('[', '(').replace(']', ')')
            if 'filter' in self.body:
                self.body['filter'] = self.body['filter'] + ' and ' + board_filter
            else:
                self.body['filter'] = board_filter

    def __set_moex_futures_body(self):
        self.body = {'filter': 'EXPIRATION_DATE > #%s#' % self.toDate}
        if self.underlying is not None:
            self.body['filter'] += " and BA_SECID = '%s'" % self.underlying
        # self.body = {'filter' : "BA_SECID = 'VTBR'"}
