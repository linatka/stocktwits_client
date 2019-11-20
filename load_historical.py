#!/usr/bin/python3

import sys
from api import StockTwitsClient
import pickle
import time 


if __name__ == '__main__':
    
    if len(sys.argv) != 5:
        print('Invalid arguments number.\nUsage: {} symbols_file since_date st_config output_dir'.format(sys.argv[0]))
        exit(1)
    
    start = time.time()
    
    symbols = []
    with open(sys.argv[1], 'r') as file:
        for line in file:
            symbols.append(line.strip())
            
    client = StockTwitsClient(sys.argv[3], )
    
    data = {}
    n = 0
    for s in symbols:
        data[s] = client.historical(s, sys.argv[2], delay=8, retries=1)
        n += 1
        print(n, s)
        with open('{}/{}.pkl'.format(sys.argv[4], s), 'wb') as file:
            pickle.dump(data, file)
        data.clear()       
 
    end = time.time()
    print('Ellapsed: {}s'.format(end - start))
    print('Data saved in {}'.format(sys.argv[4]))
