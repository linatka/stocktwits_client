#!/usr/bin/env python3

from setuptools import setup


setup(name='stocktwits-api',
      version='1.0',
      description='StockTwits Data loading API and Utils',
      packages=['stapi'],
      scripts=['utils/stocktwits_load_historical.py', 'utils/stocktwits_news_handler.py'],
      install_requires=[
            'requests>=2.22.0',
            ]
      )