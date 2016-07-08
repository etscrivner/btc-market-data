import csv
import datetime
import decimal
import glob
import os
import sqlite3


def unix_epoch_to_datetime(timestamp):
    """Converts a timestamp in Unix Epoch format into a datetime.

    :param timestamp: the unix epoch timestamp
    :type timestamp: int
    :return: datetime.datetime
    """
    return datetime.datetime.fromtimestamp(int(timestamp))


def filename_to_exchange_name(file_name):
    """Convert the given file name into an exchange name.

    :param file_name: the name of the file
    :type file_name: basestring
    :return: the exchange name for the given file.
    """
    return os.path.basename(file_name.split("USD")[0])


class Application(object):
    """Entry point for application that gathers application information."""

    DATABASE_FILE = 'market_data.db'

    def __init__(self):
        self.db = sqlite3.connect(self.DATABASE_FILE)

        self.db.execute('''
        CREATE TABLE IF NOT EXISTS historical_trades
        (id INTEGER PRIMARY KEY,
         created_at DATETIME,
         exchange_name VARCHAR(60),
         usd_price DECIMAL(10, 2),
         btc_price DECIMAL(10, 2))
        ''')
        self.db.commit()

        self.db.execute('''
        CREATE INDEX IF NOT EXISTS historical_trades_exchange_name
        ON historical_trades(exchange_name)
        ''')
        self.db.commit()

        self.db.execute('''
        CREATE INDEX IF NOT EXISTS historical_trades_created_at
        ON historical_trades(created_at)
        ''')
        self.db.commit()

    def execute(self):
        for file_name in glob.glob('raw-price-data/*.csv'):
            exchange_name = filename_to_exchange_name(file_name)
            with open(file_name, 'r') as historical_data:
                data_reader = csv.reader(historical_data)
                for idx, row in enumerate(data_reader):
                    self.insert_row_into_database(row, exchange_name)
                    if idx % 10000:
                        self.db.commit()

    def insert_row_into_database(self, row, exchange_name):
        # Insert the row into the database
        query = '''
        INSERT OR REPLACE INTO historical_trades (
          created_at, exchange_name, usd_price, btc_price
        ) VALUES(
          "{:%Y-%m-%d %H:%M:%S}",
          "{}",
          {},
          {}
        )
        '''.format(
            unix_epoch_to_datetime(row[0]),
            exchange_name,
            decimal.Decimal(row[1]),
            decimal.Decimal(row[2])
        )
        self.db.execute(query)


if __name__ == '__main__':
    Application().execute()
