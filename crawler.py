import requests
import json
import time


class Crawler():
    """ Uses `requests` library to GET and POST to Stocktwits, and also to convert responses to JSON
    """

    def __init__(self, logger):
        self.logger = logger

    def request(self, method, url, params, timeout=10, retries=3, data=None):
        """ Requests for a few times before giving up if a timeout
        """

        sleep_s = 0.1
        factor = 2

        if data is None:
            data = {}
        self.logger.debug('{} to {}'.format(method, url))
        resp = None
        for i in range(retries):
            try:
                resp = requests.request(method, url, params=params, data=data, timeout=timeout)
            except requests.Timeout:
                self.logger.error('timeout to {}\n'.format(url))
                time.sleep(sleep_s)
                sleep_s *= factor
                factor *= factor
            if resp is not None:
                break
        try:
            return json.loads(resp.content.decode())
        except (json.JSONDecodeError, TypeError, Exception, ) as e:
            self.logger.error('Exception {} during GET to {} \n'.format(e, url))

    def get_json(self, url, params, timeout=5, retries=3, data=None):
        """ Tries to GET a few times in a loop before giving up if a timeout
        """
        return self.request('GET', url, params, timeout=timeout, retries=retries, data=data)

    def post_json(self, url, params, timeout=5, retries=3, data=None):
        """ Tries to POST a few times in a loop before giving up if a timeout
        """
        return self.request('POST', url, params, timeout=timeout, retries=retries, data=data)
        
