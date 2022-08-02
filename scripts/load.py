import pandas as pd


class LoadData(object):
    '''
    Complete the ETL process by loading data into a specified format based on function call
    '''

    def __init__(self, data:pd.DataFrame):
        '''
        Initialise with a dataframe to load

        :param data: the dataframe to load 
        '''

        self.data = data

    
    def load_csv(self, path:str):
        '''
        Load to a csv file 

        :param path: Path to the csv file to load data into
        '''

        self.data.to_csv(path)

    
    def load_excel(self, path):
        '''
        Load to an xlsx file 

        :param path: Path to the xlsx file to load data into 
        '''

        self.data.to_excel(path)


    def load_json(self, path):
        '''
        Load to an json file 

        :param path: Path to the json file to load data into 
        '''

        self.data.to_json(path)


    def load_pickle(self, path):
        '''
        Load to an pkl file 

        :param path: Path to the pkl file to load data into 
        '''

        self.data.to_pickle(path)

    