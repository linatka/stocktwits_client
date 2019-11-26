#!/usr/bin/python3

from stapi.api import StockTwitsClient
import pickle
import time 
import argparse
import os

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', help="file with instruments", required=True)
    parser.add_argument('-d', '--date', help="start date", required=True)
    parser.add_argument('-c', '--config', help="stocktwits api config file", required=True)
    parser.add_argument('-s', '--storage', help="output directory", required=True)
    args = parser.parse_args()

    start = time.time()
    
    symbols = []
    with open(args.file, 'r') as file:
        for line in file:
            symbols.append(line.strip())
            
    client = StockTwitsClient(args.config, )
    
    data = {}
    n = 0
    for s in symbols:
        data[s] = client.historical(s, args.date, delay=8, retries=1)
        n += 1
        print(n, s)
        with open(os.path.join(args.storage, '{}.pkl'.format(s)), 'wb') as file:
            pickle.dump(data, file)
        data.clear()       
 
    end = time.time()
    print('Ellapsed: {}s'.format(end - start))
    print('Data saved in {}'.format(args.storage))
