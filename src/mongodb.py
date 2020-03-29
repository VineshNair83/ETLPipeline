from pymongo import MongoClient
import pandas as pd


class MongoDB:
    def __init__(self, user, password, host, db_name, port='27017', authsource='admin'):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.authSource = authsource
        self.uri = 'mongodb://' + self.host + ':' + self.port

        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            print('MongoDB connection successful!')
        except Exception as e:
            print('MongoDB connection unsuccessful. ERROR!')
            print(e)

    def insert_into_db(self, data, collection):
        if isinstance(data, pd.DataFrame):
            try:
                self.db[collection].insert_many(data.to_dict('records'))
                print('Data inserted successfully')
            except Exception as e:
                print('OOPS! Some Error occurred')
                print(e)
        else:
            try:
                print('insert into mongodb as python collection')
                print('type of data to be inserted {}'.format(type(data)))
                self.db[collection].insert_many(data)
                print('Data inserted successefully')
            except Exception as e:
                print('OOPS! Some Error occurred')
                print(e)

    def read_from_db(self, collection):
        try:
            db_data = pd.DataFrame(list(self.db[collection].find()))
            print('Data fetched successfully')
            return db_data
        except Exception as e:
            print('OOPS! Some Error occurred')
            print(e)


if __name__ == '__main__':
    mdb = MongoDB('etluser', 'etluser', 'localhost', 'etl')
    data = mdb.read_from_db('customers')
    print(data)