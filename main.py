from scripts.extract import ExtractData
from scripts.transform import TransformData
from scripts.load import LoadData

import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--filename', '-f', help='The filename or path to process')

if __name__ == '__main__':

    # parse args 
    args = parser.parse_args()

    # run processing pipeline
    extractor = ExtractData(args.filename)
    extractor.load_data()

    # handle no rationalisation
    try:
        extractor.rationalise_data()
    except FileNotFoundError:
        print('No master file found so data has not been rationalised and is instead in a new file named master.csv')
        pass

    df = extractor.data
    
    transformer = TransformData(df, 'logging/log.json')
    transformer.check_duplicates(True)
    transformer.check_row_na(True)
    transformer.check_type({'Year':'float64'}, change=True)
    transformer.fill_na_vals('Area', val='Unknown')
    transformer.fill_na_vals('Injury', '')
    transformer.bespoke_format_columns()

    df = transformer.complete_transform()

    loader = LoadData(df)
    loader.load_csv('data/master.csv')
    