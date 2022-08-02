import pandas as pd 


class ExtractData(object):
    '''
    Simple class to hold and find new extracted data.
    '''
    def __init__(self, path:str):
        '''
        Initialise with a path to a data set
        :param path: Path to the dataset looking to load in
        '''
        self.path = path 

        self.data = None

    
    def load_data(self):
        '''
        Load in the new data 
        '''
        try:
            data = pd.read_csv(self.path)
        except UnicodeDecodeError:
            print('There has been an encoding error, please check the file you are loading is in \
                UTF-8 encoding.')
        
        self.data = data


    def rationalise_data(self):
        '''
        Rationalise an existing and new dataset to check for changes and only append new records
        '''
        
        # read in existing data set:
        master = pd.read_csv('data/master.csv')

        # select only records that do not have their case no. in master 
        self.data = self.data[~self.data.CaseNumber.isin(master['CaseNumber'])]

    