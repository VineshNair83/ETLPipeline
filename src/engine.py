from transform import Transformation
import json


class Engine:

    def __init__(self, datasource, dataset):
        print('creating Transformation for {}'.format(dataset))
        trans_obj = Transformation(datasource, dataset)


if __name__ == '__main__':
    etl_data = json.load(open('config.json'))

    for datasource, dataset in etl_data['data_sources'].items():
        if datasource in ('api', 'csv'):
            for data in dataset:
                print('\n--------', datasource, data, '-----------')
                print('creating Engine for {}'.format(data))
                main_obj = Engine(datasource, data)



