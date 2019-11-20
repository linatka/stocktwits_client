from api import StockTwitsClient, StockTwitsClientException
from api import default_logger
import configparser
import pickle
import pathlib
import time, datetime
import os
import argparse


def flatten_json(y):
    """
    https://towardsdatascience.com/flattening-json-objects-in-python-f5343c794b10
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


class Buffer(object):

    def __init__(self, storage_path, filename, reusedir=False, buffer_duration=3600):
        self.__directory = storage_path
        try:
            pathlib.Path(storage_path).mkdir(parents=True, exist_ok=reusedir)
        except FileExistsError as e:
            default_logger.warning('Storage directory already exists')
            raise
        self.__filename = filename
        self.__buffer = []
        self.__start_ts = None
        self.__buffer_duration = buffer_duration

    def accumulate(self, points):
        if len(self.__buffer) == 0:
            self.__start_ts  = time.time()
        self.__buffer.extend(points)
        if time.time() - self.__start_ts >= self.__buffer_duration:
            self.dump()

    def dump(self):
        ts = datetime.datetime.utcfromtimestamp(self.__start_ts).strftime('%Y-%m-%dT%H:%M:%S')
        filepath = os.path.join(self.__directory, self.__filename + ts + '.pkl')
        print(filepath)
        with open(filepath, 'wb') as file:
            pickle.dump(self.__buffer, file)
        self.__buffer = []


class NewsHandler(object):

    def __init__(self, cfg_file, delay=9):
        self.client = StockTwitsClient(cfg_file)
        self.__cfg = configparser.ConfigParser()
        self.__cfg.read(cfg_file)
        self.__delay = delay

    def subscribe(self, wl_id, callback=None):

        if callback is None:
            buffer = Buffer(self.__cfg['Storage']['path'], 'st_', True,
                            int(self.__cfg['Buffer']['duration']))
            callback = buffer.accumulate
        cursor = {}
        while(True):
            try:
                if 'max' in cursor:
                    cursor['since'] = cursor['max']
                    del cursor['max']
                stream = self.client.get_watchlist_stream(wl_id, params=cursor)
                if stream is not None:
                    ts = time.time()
                    for i in range(len(stream['messages'])):
                        stream['messages'][i]['recv_time'] = ts
                    callback(stream['messages'])
                    cursor = stream['cursor']
                time.sleep(self.__delay)
            except (StockTwitsClientException, Exception) as e:
                default_logger.error('Error in subscription for {}: {}'.format(wl_id, e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Subscribe to StockTwits watch list and save data in binary format.')
    parser.add_argument('-c', '--config', required=True, help='config file')
    parser.add_argument('-w', '--wl', required=True, help='watch list id or name; number always is considered like id')
    args = parser.parse_args()

    nh = NewsHandler(args.config)
    if args.wl.isdigit():
        wl_id = int(args.wl)
    else:
        wlists = nh.client.get_watchlists()
        for wl in wlists:
            if wl['name'] == args.wl:
                wl_id = wl['id']
                break
        else:
            nh.client.logger("Invalid watchlist name: {}".format(args.wl))
            exit(1)
    nh.subscribe(wl_id)
