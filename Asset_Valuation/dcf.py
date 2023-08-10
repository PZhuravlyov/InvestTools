import pandas as pd
from datetime import datetime
import re
# import openpyxl

"""
Class for a company valuation using a specified DCF model
To do list:
    2 companies valuation from different industries
    Integration with DB to automatic scenarios and reporting loading
"""


class DCFModel:

    def __init__(self):
        # dates of the valuation calendar
        self.past_dates = self.valuation_date = self.future_dates = None

        # rows specifying elements of the DCF model
        self.row_names = None
        # model future periods time series e.g. GDP, inflation
        self.series_names = None
        # model future period constants e.g. tax rate
        self.constant_names = None

        # data frame with model elements formulas
        self.model_formulas = None
        # data frame with model elements values
        self.model_values = None

        # dictionary with time series values
        self.series_values = None
        # dictionary with constant values
        self.constant_values = None

        # approach to terminal value calculation (enumeration used)
        self.terminal_valuation = None

    """
    Setting the valuation calendar
    actual_dates - dates with historical data from financial reporting
    valuation_date - date of a company valuation
    prospect_dates - dates for forecast data
    """
    def set_valuation_calendar(self, past_dts, valuation_dt, future_dts):
        # reset model data frames
        self.model_formulas = self.model_values = None
        # should be an ordered tuple
        self.past_dates = [datetime.strptime(date, "%Y-%m-%d") for date in past_dts]
        # scalar value
        self.valuation_date = datetime.strptime(valuation_dt, "%Y-%m-%d")
        # should be an ordered tuple
        self.future_dates = [datetime.strptime(date, "%Y-%m-%d") for date in future_dts]

    """
    Specifying model constituents: should be ordered lists
    """
    def specify_model(self, rows, time_series, constants):
        # copying model specification
        self.row_names = rows[:]
        self.series_names = time_series[:]
        self.constant_names = constants[:]

        # reset model data frames
        self.model_formulas = self.model_values = None

        # clear time series and constant values
        self.series_values = {}
        self.constant_values = {}

    """
    Creating empty model template (formula and value data frames)
    """
    def create_model_template(self):
        # checking existence of the model dates
        if self.future_dates is None or self.row_names is None:
            return -1
        # uniting past and future dates in one set
        dates = self.past_dates + self.future_dates
        # number of rows
        row_num = len(self.row_names)
        # empty dictionary filled with the model dates as the dictionary keys
        template = {}
        # filling the dictionary
        for date in dates:
            template[date] = list(range(0, row_num))
        # creating the model data frames from the dictionary
        self.model_formulas = pd.DataFrame(template, columns=dates, index=self.row_names)
        self.model_formulas[:] = None
        self.model_values = pd.DataFrame(template, columns=dates, index=self.row_names)
        self.model_values[:] = None

    """
    Loading actual financial data from DB
    """
    def load_actual_data_from_db(self):
        pass

    """
    Setting a row formula expanding from the start_date to the last future date
    """
    def set_general_row_formula(self, row, start_date, formula):
        # check existence of and create if needed the model templates
        if self.model_formulas is None:
            self.create_model_template()
        # check existence of the row in the model template
        if row not in self.model_formulas.index:
            return -1
        # check existence of the column in the model template
        if start_date not in self.model_formulas.columns:
            return -1
        # set specified formula from the start_date to the last column of the row
        self.model_formulas.loc[row, start_date:] = formula
        return 0

    """
    Setting time series value; should be an ordered list
    """
    def set_time_series(self, series, value):
        if series not in self.series_names:
            return -1
        self.series_values[series] = value[:]

    """
    Setting constant value
    """
    def set_constant(self, constant, value):
        if constant not in self.constant_names:
            return -1
        self.constant_values[constant] = value

    """
    Convert formulas data frame to the excel format
    """
    def convert_model_to_excel(self, is_rc_format=False):
        # check existence of the formulas data frame
        if self.model_formulas is None:
            return -1
        # combining the model items in one set
        item_names = self.row_names + ['', ''] + self.series_names + ['', ''] + self.constant_names
        # copy formulas to the converted data frame
        result = self.model_formulas[:]

        # starting row and column indices on the Excel sheet
        start_row_index = 2
        start_col_code = ord('B') + len(self.past_dates)

        # Generate general cell link
        def get_general_link():
            # forming general cell link
            row_ind = item_names.index(row_name)
            general_cell_link = chr(
                start_col_code + (int(item[len(row_name) + 1: -1]) if len(row_name) < len(item) else 0)) + str(
                start_row_index + row_ind)
            return general_cell_link

        # Generate r[]c[] cell link
        def get_rc_link():
            # forming R[]C[] cell link
            row_ind = item_names.index(row_name) - ind
            rc_link = 'R[' + str(row_ind) + ']' + \
                      'C[' + (item[len(row_name) + 1: -1] if len(row_name) < len(item) else '0') + ']'
            return rc_link

        # defining used function
        get_link = get_rc_link if is_rc_format else get_general_link

        # converting string name to RC format
        for date in result.columns:
            for ind, row in enumerate(result.index):
                # checking existence
                if result.loc[row, date] is None:
                    continue
                # formula in the R[]C[] format
                formula = result.loc[row, date]
                # dividing into the row components
                items = re.findall(r"[^0-9 .(]\w+[[]?[-]?\d*[]]?", result.loc[row, date])
                # for each row component
                for item in items:
                    # discarding [-x] part
                    row_name = item[:item.find('[')] if item.find('[') >= 0 else item[:]
                    # checking presence of the item in the model rows
                    if row_name not in item_names:
                        continue
                    # forming cell link
                    cell_link = get_link()
                    # replacing the found item with the cell link
                    formula = formula.replace(item, cell_link)
                # saving formula
                result.loc[row, date] = '=' + formula
        return result

    # export model to excel in the R[]C[] format
    def export_to_excel(self, is_rc_format=False):
        ex_mdl = self.convert_model_to_excel(is_rc_format)
        ex_mdl.to_excel(r'd:\DCF_Model.xlsx', index=True, header=True)


# functional programming for performed operations
# standard block (micro chip like)
# iterator form model expansion
# export model RC[]


if __name__ == '__main__':
    dcf = DCFModel()

    past_dates = ['2020-12-31', '2021-12-31']
    valuation_date = '2022-11-07'
    future_dates = ['2022-12-31', '2023-12-31', '2024-12-31', '2025-12-31', '2026-12-31']

    dcf.set_valuation_calendar(past_dates, valuation_date, future_dates)

    row_names = ['Sales', 'EBIT_margin', 'EBIT', 'NI', 'IncFC', 'IncWC', 'FCFF']
    time_series_names = ['sales_growth', 'inc_fc_rate', 'inc_wc_rate']
    constant_names = ['tax_rate']

    dcf.specify_model(row_names, time_series_names, constant_names)

    dcf.create_model_template()

    dcf.set_general_row_formula('Sales', '2022-12-31', 'Sales[-1] * sales_growth')
    dcf.set_general_row_formula('EBIT_margin', '2022-12-31', '0.1')
    dcf.set_general_row_formula('EBIT', '2022-12-31', 'Sales * EBIT_margin')
    dcf.set_general_row_formula('NI', '2022-12-31', 'EBIT * (1 - tax_rate)')
    dcf.set_general_row_formula('IncFC', '2022-12-31', 'NI * inc_fc_rate')
    dcf.set_general_row_formula('IncWC', '2022-12-31', 'NI * inc_wc_rate')
    dcf.set_general_row_formula('FCFF', '2022-12-31', 'NI - IncFC - IncWC')
    # dcf.set_general_row_formula('', '', '')

    sales_growth = [0.1, 0.1, 0.1, 0.1, 0.1]
    dcf.set_time_series('sales_growth', sales_growth)
    inc_fc_rate = [0.35, 0.35, 0.3, 0.25, 0.25]
    dcf.set_time_series('inc_fc_rate', inc_fc_rate)
    inc_wc_rate = [0.2, 0.2, 0.15, 0.15, 0.1]
    dcf.set_time_series('inc_wc_rate', inc_wc_rate)

    tax_rate = 0.4
    dcf.set_constant('tax_rate', tax_rate)

#    print(dcf.model_formulas)
#    print(dcf.model_formulas.columns)
#    mdl = dcf.convert_model_to_excel()
#    print(mdl)
    dcf.export_to_excel()
