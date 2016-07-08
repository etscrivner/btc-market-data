import collections
import csv
from decimal import Decimal as D
import datetime
import glob
import os
import sys


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

    def __init__(self):
        self.volume_btc = collections.defaultdict(
            lambda: collections.defaultdict(D))
        self.volume_usd = collections.defaultdict(
            lambda: collections.defaultdict(D))
        self.open_price = collections.defaultdict(
            lambda: collections.defaultdict(dict))
        self.close_price = collections.defaultdict(
            lambda: collections.defaultdict(dict))
        self.high = collections.defaultdict(
            lambda: collections.defaultdict(D))
        self.low = collections.defaultdict(
            lambda: collections.defaultdict(D))

    def execute(self):
        for file_name in glob.glob('raw-price-data/*.csv'):
            exchange_name = filename_to_exchange_name(file_name)
            print "\n{}:".format(exchange_name)
            with open(file_name, 'r') as historical_data:
                data_reader = csv.reader(historical_data)
                for idx, row in enumerate(data_reader):
                    if idx > 0 and (idx % 10000) == 0:
                        sys.stdout.write('.')
                        sys.stdout.flush()
                    self.update_prices(row, exchange_name)

        print
        field_names = [
            'Date',
            'Open (USD)',
            'Close (USD)',
            'High (USD)',
            'Low (USD)',
            'Volume (USD)',
            'Volume (BTC)'
        ]
        for exchange_name in self.volume_usd.keys():
            with open('{}_results.csv'.format(exchange_name), 'w') as dataout:
                writer = csv.DictWriter(dataout, field_names)
                writer.writeheader()
                sorted_dates = sorted(self.volume_usd[exchange_name].keys())
                for next_date in sorted_dates:
                    next_row = {
                        'Date': next_date,
                        'Open (USD)': self.open_price[exchange_name][next_date]['price_usd'],
                        'Close (USD)': self.close_price[exchange_name][next_date]['price_usd'],
                        'High (USD)': self.high[exchange_name][next_date],
                        'Low (USD)': self.low[exchange_name][next_date],
                        'Volume (USD)': self.volume_usd[exchange_name][next_date],
                        'Volume (BTC)': self.volume_btc[exchange_name][next_date]
                    }
                    writer.writerow(next_row)

    def update_prices(self, row, exchange_name):
        dt = unix_epoch_to_datetime(row[0])
        date = dt.date()
        usd_exchange_rate = D(row[1])
        amount_btc = D(row[2])
        self.volume_btc[exchange_name][date] += amount_btc
        self.volume_usd[exchange_name][date] += amount_btc * usd_exchange_rate

        if self.high[exchange_name][date]:
            self.high[exchange_name][date] = max(
                self.high[exchange_name][date], usd_exchange_rate)
        else:
            self.high[exchange_name][date] = usd_exchange_rate

        if self.low[exchange_name][date]:
            self.low[exchange_name][date] = min(
                self.low[exchange_name][date], usd_exchange_rate)
        else:
            self.low[exchange_name][date] = usd_exchange_rate

        if self.open_price[exchange_name][date]:
            if dt < self.open_price[exchange_name][date]['time']:
                self.open_price[exchange_name][date] = {
                    'time': dt,
                    'price_usd': usd_exchange_rate
                }
        else:
            self.open_price[exchange_name][date] = {
                'time': dt,
                'price_usd': usd_exchange_rate
            }

        if self.close_price[exchange_name][date]:
            if dt > self.close_price[exchange_name][date]['time']:
                self.close_price[exchange_name][date] = {
                    'time': dt,
                    'price_usd': usd_exchange_rate
                }
        else:
            self.close_price[exchange_name][date] = {
                'time': dt,
                'price_usd': usd_exchange_rate
            }


if __name__ == '__main__':
    Application().execute()
