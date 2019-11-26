from .crawler import Crawler
import configparser
import logging
import datetime
import itertools
import time


__all__ = ['StockTwitsClient', 'StockTwitsClientException', 'default_logger', 'logger']


# StockTwits url
ST_BASE_URL = 'https://api.stocktwits.com/api/2/'


def logger(logPath, fileName):
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler("{0}/{1}.log".format(logPath,fileName)),
            logging.StreamHandler()
        ])
    return logging


default_logger = logger('.', 'st')


class StockTwitsClientException(Exception):
    pass


class StockTwitsClient(object):
    """
    StockTwits API Client

    https://api.stocktwits.com/developers/docs/api

    """
    
    def __init__(self, cfg_file, logger=default_logger):
        self.__cfg = configparser.ConfigParser()
        self.__cfg.read(cfg_file)
        self.__params = {}
        self.logger = logger
        self.cr = Crawler(logger)
        if 'Connection' in self.__cfg and 'access_token' in self.__cfg['Connection']:
            self.__params['access_token'] = self.__cfg['Connection']['access_token']
        else:
            self.logger.warning("Section 'Connection' or 'access_token' is missed in config file")

    def get_watchlists(self):
        """
        Get list of watchlists
        """
        wls = self.cr.get_json(ST_BASE_URL + 'watchlists.json', params=self.__params)
        return wls['watchlists']

    def create_watchlist(self, name, symbols=None):
        """
        Create new watchlist
        """
        wl = self.cr.post_json(ST_BASE_URL + 'watchlists/create.json', params=self.__params,
                              data={'name': name})
        return wl['watchlist']

    def get_watched_stocks(self, wl_id):
        """
        Get list of symbols being watched by specified StockTwits watchlist
        """
        wl = self.cr.get_json(ST_BASE_URL + 'watchlists/show/{}.json'.format(wl_id), params=self.__params)
        wl = wl['watchlist']['symbols']
        return [s['symbol'] for s in wl]

    def get_stock_stream(self, symbol, params=None):
        """
        Gets stream of messages for given symbol

        Parameters:
            symbol: str
                ticker symbol, stock ID, or RIC code of the symbol
            params: dict
                http request parameters (https://api.stocktwits.com/developers/docs/api#streams-symbol-docs)
        """
        if not params:
            params = {}
        all_params = self.__params.copy()
        all_params.update(params)
        return self.cr.get_json(ST_BASE_URL + 'streams/symbol/{}.json'.format(symbol), params=all_params)

    def get_watchlist_stream(self, wl_id, params=None):
        """
        Gets up to 30 messages from Watchlist (wl_id) according to additional params
        """
        if not params:
            params = {}
        all_params = self.__params.copy()
        all_params.update(params)
        return self.cr.get_json(ST_BASE_URL + 'streams/watchlist/{}.json'.format(wl_id), params=all_params)

    def add_to_watchlist(self, symbols, wl_id, chunksize=10):
        """
        Adds list of symbols to StockTwits watchlist.  Returns list of added symbols
        """
        params = self.__params.copy()
        remainder = symbols.copy()
        added = []
        while len(remainder) > 0:
            symbols = ','.join(remainder[:chunksize])  # must be a csv list
            remainder = remainder[chunksize:]
            data = {'symbols' : symbols}
            try:
                resp = self.cr.post_json(ST_BASE_URL + 'watchlists/{}/symbols/create.json'.format(wl_id), params=params,
                                     data=data, retries=1, timeout=5)
                if resp['response']['status'] == 200:
                    added = added + [s['symbol'] for s in resp['symbols']]
                else:
                    self.logger.warning("Can't add symbols to watchlist (code {})" .format(resp['response']['status']))
            except Exception as e:
                self.logger.error('Errors occurred during adding to wl {}: {}'.format(wl_id, e))
        return added

    def delete_from_watchlist(self, symbol, wl_id):
        """
        Removes a single "symbols" (str) from watchlist.  Returns True on success, False on failure
        """
        params = self.__params.copy()
        params['symbols'] = symbol
        resp = self.cr.post_json(ST_BASE_URL + 'watchlists/{}/symbols/destroy.json'.format(wl_id), params=params)
        if resp['response']['status'] == 200:
            return [s['symbol'] for s in resp['symbols']]
        else:
            return resp.raise_for_status()

    def get_trending_stocks(self, ):
        """
        Returns list of trending stock symbols
        """
        trending = self.cr.get_json(ST_BASE_URL + 'trending/symbols.json', params=self.__params)['symbols']
        symbols = [s['symbol'] for s in trending]
        return symbols

    def clean_watchlist(self, wl_id):
        """
        Tries to delete all symbols from watch lists.
        Returns deleted symbols
        """
        wl = self.cr.get_json(ST_BASE_URL + 'watchlists/show/{}.json'.format(wl_id),
                      params=self.__params)['watchlist']['symbols']
        deleted = []
        for sym in wl:
            self.logger.info("Removing {} from {}".format(sym, wl_id))
            try:
                self.delete_from_watchlist(sym['symbol'], wl_id=wl_id)
                deleted.append(sym['symbol'])
            except Exception as e:
                self.logger.error("Error deleting symbol from watchlist: {}".format(e))
        return deleted

    def historical(self, symbol, since, delay=3, retries=5):
        messages = []
        cursor = {}
        date = str(datetime.datetime.today().date())
        while date >= since and retries > 0:
            try:
                stream = self.get_stock_stream(symbol, params=cursor)
                if stream is not None:
                    cursor['max'] = stream['cursor']['max']
                    messages.append(stream['messages'])
                    date = messages[-1][-1]['created_at'].split('T')[0]
                else:
                    retries -= 1
            except Exception as e:
                if 'max' not in cursor:
                    cursor['max'] = -1
                self.logger.error("Error occurred during loading till {}: {}"\
                                  .format(cursor['max'], e))
                retries -= 1
            time.sleep(delay)
        return list(itertools.chain(*messages))

