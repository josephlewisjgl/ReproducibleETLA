import dateutil
import re 
import pandas as pd

class TransformData(object):
    '''
    Transform the shark attacks dataset
    '''
    def __init__(self, data:pd.DataFrame, logging_path:str):
        '''
        Initialise with a dataset to transform and a path to build an error log

        :param data: Data to transform 
        :param logging_path: Path to store log messages
        '''

        self.data = data
        self.logging_path = logging_path
        self.messages = {}
        
    
    def check_duplicates(self, remove:bool=False):
        '''
        Check for duplicates 

        :param remove: Boolean set True if the dupes should be removed 
        '''

        # find num duplicates
        dupes = len(self.data) - len(self.data.drop_duplicates())
        if dupes > 0:
            self.messages.update({'Duplicates':dupes})

        if remove:
            self.data.drop_duplicates(inplace=True)

        
    def check_row_na(self, remove:bool=False):
        '''
        Check for NA values 
        
        :param remove: Boolean set True if the NA rows should be removed 
        '''

        # first drop index
        self.data.drop('Unnamed: 0', axis=1, inplace=True)

        # find number of blank rows 
        na = len(self.data) - len(self.data.dropna(how='all'))
        if na > 0:
            self.messages.update({'NA': na})

        if remove:
            self.data.dropna(how='all', inplace=True)

    
    def fill_na_vals(self, to_fill:str, method:str=None, val:object=None):
        '''
        Fill NA values with either a specified fill value or a method to fill with

        :param to_fill: Column to fill na values in 
        :param method: Method to use to fill values 
        :param val: Alternatively a specific value can be used to fill NA 
        '''

        # find number of null records 
        na = self.data[to_fill].isna().sum()

        self.messages.update({'FilledNA': na})

        # error handle bad methods
        if method and method not in ('backfill', 'bfill', 'pad', 'ffill', 'mean'):
            self.messages.append({'FillNA': f'Failed on filling {to_fill}. Please use valid method.'})
            return

        # choose a method or val to fill 
        if method:
            self.data[to_fill] = self.data[to_fill].fillna(method=method, inplace=True)

        if val:
            values = {to_fill: val}
            self.data.fillna(value=values, inplace=True)


    def check_type(self, data_types:dict, change:bool=False):
        '''
        Take a dict of col names and their data type and check that they match expected 

        :param data_types: dictionary of col names and expected dtypes 
        :param change: Boolean true if you want to change them to the datatypes listed (could cause errors)
        '''

        # loop through checking the data types 
        for col, data_type in data_types.items():
            if self.data.dtypes[col] != data_type:
                self.messages.update({f'DTypes {col}': 'Incorrect datatype in col'})

            if change:
                try:
                    self.data[col] = self.data.astype({col: data_type})
                except:
                    self.messages.update({f'DTypes {col}': 'Error in converting col please check this is a valid conversion'})


    @staticmethod
    def clean_date_col(date_to_infer:str):
        '''
        Clean a date column (strip out text and infer date type then set date object)

        :param date_to_infer: The date to infer 
        '''
        
        # handle none 
        if pd.isna(date_to_infer):
            return 'Unknown'
        
        # strip out text before date and after date
        date_to_infer = re.sub('^([^0-9])*', '', date_to_infer)
        date_to_infer = re.sub('[A-z]*$', '', date_to_infer)
        date_to_infer = re.sub('--', '-', date_to_infer)
        
        # parse the date and return 
        try:
            date = dateutil.parser.parse(date_to_infer)
        except dateutil.parser.ParserError:
            return 'Unknown'

        return date
    

    def bespoke_format_columns(self):
        '''
        A bespoke function added to extend functionality for specific formatting of Shark Attacks data
        '''
        
        # clean up date column 
        self.data['Date'] = self.data.apply(lambda row: self.clean_date_col(row['Date']), axis=1)

    
    def complete_transform(self):
        '''
        Complete the transformation (return df and save error log)
        '''

        # build messages df 
        messages = pd.DataFrame(self.messages, index=[0])

        # output to JSON 
        messages.to_json(self.logging_path)

        return self.data

