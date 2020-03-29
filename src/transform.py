from extract import Extract
from mongodb import MongoDB

import urllib
import pandas as pd
import numpy as np


class Transformation:

    def __init__(self, datasource, dataset):
        self.csv_df = pd.DataFrame()

        # create the Extract object
        extract_obj = Extract()

        if datasource == 'api':
            self.data = extract_obj.get_api_data(dataset)
            func_name = datasource + "_" + dataset
            getattr(self, func_name)()
        elif datasource == 'csv':
            self.data = extract_obj.get_csv_data(dataset)
            func_name = datasource + "_" + dataset
            getattr(self, func_name)()
        else:
            print('Unknown data source!!! Please try again...')

    def api_economy(self):
        print('Executing api_economy')
        gdp_india = {}
        for record in self.data['records']:
            gdp = dict()

            # taking out yearly GDP value from records
            gdp['GDP_in_rs_cr'] = int(record['gross_domestic_product_in_rs_cr_at_2004_05_prices'])
            gdp_india[record['financial_year']] = gdp
            gdp_india_yrs = list(gdp_india)

        for i in range(len(gdp_india_yrs)):
            if i == 0:
                pass
            else:
                key = 'GDP_Growth_' + gdp_india_yrs[i]
                # calculating GDP growth on yearly basis
                gdp_india[gdp_india_yrs[i]][key] = round(((gdp_india[gdp_india_yrs[i]]['GDP_in_rs_cr'] -
                                                           gdp_india[gdp_india_yrs[i - 1]]['GDP_in_rs_cr']) /
                                                          gdp_india[gdp_india_yrs[i - 1]]['GDP_in_rs_cr']) * 100, 2)

        # convert to pandas dataframe
        gdp_india = pd.DataFrame(list(gdp_india.items()), columns=['financial_year', 'gdp_growth'])

        # connect to mongodb
        mongodb_obj = MongoDB('etluser', 'etluser', 'localhost', 'GDP')
        mongodb_obj.insert_into_db(gdp_india, 'India_GDP')

    def api_pollution(self):
        print('Executing api_pollution')
        air_data = self.data['results']

        # converting nested data into linear structure
        air_list = []
        for data in air_data:
            for measurement in data['measurements']:
                air_dict = dict()
                air_dict['city'] = data['city']
                air_dict['country'] = data['country']
                air_dict['parameter'] = measurement['parameter']
                air_dict['value'] = measurement['value']
                air_dict['unit'] = measurement['unit']
                air_list.append(air_dict)

        # convert the list of dicts into pandas dataframe
        df = pd.DataFrame(air_list, columns=air_dict.keys())

        # connection to mongo db
        mongodb_obj = MongoDB('etluser', 'etluser', 'localhost', 'pollution_data')
        # inser into mongo db
        mongodb_obj.insert_into_db(df, 'Air_Quality_India')

    def csv_cryptoMarkets(self):
        print('Executing csv_cryptoMarkets')
        assets_code = ['BTC', 'ETH', 'XRP', 'LTC']

        self.csv_df['open'] = self.data[['open', 'asset']]\
            .apply(lambda x: (float(x[0]) * 0.75) if x[1] in assets_code else np.nan, axis=1)
        self.csv_df['close'] = self.data[['close', 'asset']]\
            .apply(lambda x: (float(x[0]) * 0.75) if x[1] in assets_code else np.nan, axis=1)
        self.csv_df['high'] = self.data[['high', 'asset']]\
            .apply(lambda x: (float(x[0]) * 0.75) if x[1] in assets_code else np.nan, axis=1)
        self.csv_df['low'] = self.data[['low', 'asset']]\
            .apply(lambda x: (float(x[0]) * 0.75) if x[1] in assets_code else np.nan, axis=1)

        self.csv_df.dropna(inplace=True)

        self.csv_df.to_csv('../output/crypto-market-GBP.csv', index=False)


if __name__ == '__main__':
    cyptomarket = Transformation('csv', 'cryptoMarkets')
    pollution = Transformation('api', 'pollution')



