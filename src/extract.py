import pandas as pd
import requests
import json


class Extract:

    def __init__(self):
        # load the json config file
        self.data_sources = json.load(open('config.json'))
        self.api = self.data_sources['data_sources']['api']
        self.csv = self.data_sources['data_sources']['csv']

    def get_api_data(self, api_name):
        # since we have multiple api, pass in the api_name
        # for e.g. pass in api_name as pollution or economy
        api_url = self.api[api_name]
        response = requests.get(api_url)
        # response.json() will convert json data into python dictionary
        return response.json()

    def get_csv_data(self, csv_name):
        # since we can have multiple csv's, pass in the csv file name
        df = pd.read_csv(self.csv[csv_name])
        return df


if __name__ == '__main__':
    ext = Extract()
    # print(ext.get_api_data('pollution'))
    # print(ext.get_csv_data('cryptoMarkets'))


